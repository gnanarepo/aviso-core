# Memory Optimization: `/gbm/deals_results` API

**Problem:** Containers with 16 GB RAM OOM intermittently when serving tenants with ~80K opportunities via `/gbm/deals_results`.

**Root cause summary:** The data pipeline loads the entire result dataset into Python heap memory in multiple separate copies simultaneously. Peak memory = (MongoDB wire data) + (processed dict) + (JSON serialization string) + (revenue dummy records). These compound — fixes must be applied together for meaningful relief.

---

## Status Summary

| Fix | Description | Status | Est. Savings |
|-----|-------------|--------|-------------|
| Fix C | MongoDB field projection + batch_size | ✅ DONE | 300–600 MB |
| Fix A | Stream JSON per-deal in view layer | ✅ DONE | 2–4 GB spike |
| Fix E | Lightweight extid-only query (no double retrieve_deals) | ✅ DONE | 800 MB – 1.2 GB |
| Fix B | Single-pass streaming in generator (no record_dict for fast path) | 🔴 PENDING | 800 MB – 1.2 GB |
| Fix D | Return generator from retrieve_deals (no dict() collapse) | 🔴 PENDING | 800 MB – 1.2 GB |
| Fix F | Replace deepcopy(res) with targeted shallow copy | 🔴 PENDING | 1.6–9.6 GB (tenant-specific) |
| Fix G | Stream S3 reads for pre-computed periods | 🔴 PENDING | 200–600 MB |

**`basic_results` API fixes are fully deferred** — to be taken up after `deals_results` is tested end-to-end. See separate section at the bottom.

### Latency / DB round-trip fixes (separate from memory — see dedicated section below)

| Fix | Description | Status | Impact |
|-----|-------------|--------|--------|
| Fix H | Versioned hierarchy: batch ancestor lookups (one full-hierarchy load per distinct ts) | ✅ DONE | Removes ~(records × drilldowns × splits) `$graphLookup` round-trips |
| Fix I | Reuse the view generator across periods/models (load 8 hierarchies once) | 🔴 PENDING | Removes N×8 hierarchy loads per multi-period request |
| Fix J | Increase result-cursor `batch_size` (2000 → 10000) | 🔴 PENDING | ~39 → ~8 `getMore` round-trips per 80K read |
| Fix K | Replace deprecated `.count()` in `validate_results` with `count_documents` | 🔴 PENDING | Removes a full-collection count per request |
| Fix L | Explicit session for the `no_cursor_timeout` cursor | 🔴 PENDING | Stability: avoids 30-min idle cursor kill |
| Fix M | Serve versioned lookups from the as-of in-memory table (drop per-ts DB load) | ⚠️ NEEDS SIGN-OFF | Removes all versioned hierarchy DB calls — **semantic change** |

---

## ✅ Completed Fixes

---

### Fix C — MongoDB field projection + batch_size

**File:** [result_Utils.py:1266–1278](deal_result/result_Utils.py#L1266)

**Problem:** The `.find()` fetched every field in every document. The loop body only ever accesses 4 fields from `record['object']`: `extid`, `uipfield`, `dimensions`, `results`. `batch_size=500` caused 160 round-trips for 80K docs.

**Change applied:**
```python
# BEFORE
all_results = gbm_db[res_cls.getCollectionName()].find(
    criteria,
    batch_size=500,
    no_cursor_timeout=True
)

# AFTER (current code at lines 1266–1278)
_projection = {
    'object.extid': 1,
    'object.uipfield': 1,
    'object.dimensions': 1,
    'object.results': 1,
    '_id': 0,
}
all_results = gbm_db[res_cls.getCollectionName()].find(
    criteria,
    _projection,
    batch_size=2000,
    no_cursor_timeout=True
)
```

`last_modified_time` is used only in the query *criteria* (filter) — not read from the returned document — so it is not needed in the projection.

**Savings:** 30–60% reduction in bytes transferred from MongoDB. For 80K deals: ~300–600 MB less per request.

---

### Fix A — Stream JSON per-deal in view layer

**File:** [deals_results.py:110–180](api/deals_results.py#L110)

**Problem:** `json.dumps(entire_period_result_dict)` created a single giant string (2–4 GB) before any bytes hit the wire, negating the benefit of `StreamingHttpResponse`.

**Change applied:** Both `yield_period_results()` and `yield_timestamp_results()` now stream the `results` sub-dict entry by entry. The outer JSON structure is unchanged — only the mechanism differs.

```python
# BEFORE
yield '%s\n' % json.dumps(deals_results_by_period(...)[period])   # one giant string

# AFTER (current code)
deal_results = period_data['results']
yield '{\n'
for key, val in period_data.items():
    if key == 'results':
        continue
    yield '%s: %s,\n' % (json.dumps(key), json.dumps(val))
yield '"results": {\n'
for i, (deal_id, deal_data) in enumerate(deal_results.items()):
    if i:
        yield ','
    yield '%s: %s\n' % (json.dumps(deal_id), json.dumps(deal_data))
yield '}\n'
yield '}\n'
```

**Edge case preserved:** `return_files_list=True` / `return_chipotle_files_list=True` returns a plain list — those code paths fall back to `json.dumps` (small payload, no streaming needed). Handled by the `if not isinstance(period_data, dict) or 'results' not in period_data` guard.

**Savings:** Eliminates the JSON serialization spike entirely. For a 1.5 GB result dict: ~3–4 GB transient peak removed.

---

### Fix E — Lightweight extid-only query replaces double retrieve_deals

**File:** [result_Utils.py:1948–1968](deal_result/result_Utils.py#L1948)

**Problem:** In the `get_results_from_as_of` + live chipotle path, `retrieve_deals()` was called **twice** for the same cache key. The first call (just to get `.keys()` for `changed_deals`) loaded all 80K deals unnecessarily.

**Change applied:**
```python
# BEFORE
changed_deals.extend(list(retrieve_deals(model, ck, include_uip, node).keys()))  # full load for IDs only

# AFTER (current code at lines 1954–1968)
_res_cls, _, _, _id_criteria = get_results_objects(
    ds_name='OppDS', model_name=model, cache_key=ck,
    get_results_from_as_of=get_results_from_as_of
)
_gbm_db = ConnectionFactory.get_mongo_db(
    tenant=sec_context.name, db_type='gbm', cname=cname
)
_id_cursor = _gbm_db[_res_cls.getCollectionName()].find(
    _id_criteria, {'object.extid': 1, '_id': 0},
    batch_size=5000, no_cursor_timeout=True
)
changed_deals.extend([rec['object']['extid'] for rec in _id_cursor])
```

**Savings:** Eliminates a complete redundant 800 MB – 1.2 GB load for every timestamp-mode request with `get_results_from_as_of`.

---

## 🔴 Pending Fixes

---

### Fix B — Single-pass streaming in `get_individual_results_generator`

**File:** [result_Utils.py:1300–1481](deal_result/result_Utils.py#L1300)

**Priority:** Implement next. This is the largest single remaining memory win.

**Problem — current two-pass design (lines 1300–1329):**

```python
# Pass 1: accumulate ALL 80K records into record_dict — always, regardless of missing_flds
record_dict = {}
ext_id_list = []
for record in all_results:
    obj = record['object']
    extid = obj['extid']
    if first_record:
        # ... detect missing_flds on first record ...
        first_record = False
    if missing_flds:
        ext_id_list.append(record['object']['extid'])
    record_dict[extid] = {'uipfield': obj['uipfield'],
                          'dimensions': obj['dimensions'],
                          'results': obj['results']}   # ALL 80K records, always

# Pass 2: iterate record_dict to yield (lines 1373–1468)
for record in record_iterator:
    obj = record_dict[record.ID if missing_flds else record]
    ...
    yield (extid, output)
```

`record_dict` is built for **all 80K records** on every request, even when `missing_flds` is empty (the common case). The two-pass design only pays off when `missing_flds` is non-empty.

**Production data (Splunk):** The `missing_flds` warning (line 1315) fires ~36 times per 24 hours. Affected tenants: silverpeak.com, github.com. Missing fields: `frozen_Owner`, `frozen_Level_0–4`. This is a real production path — **it must be preserved exactly**.

---

**Approach: peek first record + two explicit code paths**

The existing first-record detection already peeks at the first record to compute `missing_flds`. Extend this: after detecting `missing_flds`, use `itertools.chain` to put the first record back into the stream so both paths see all records uniformly.

**Step 1 — Replace lines 1300–1329 with an explicit peek + chain:**

```python
import itertools

try:
    first_raw = next(all_results)
except StopIteration:
    return  # empty result set

first_obj = first_raw['object']
uip_flds_to_std_fld_map = {x: add_prefix(x, 'as_of_') for x in first_obj.get('uipfield', {})}
dim_flds_to_std_fld_map = {x: add_prefix(x) for x in first_obj.get('dimensions', {})}
cached_flds = set(uip_flds_to_std_fld_map.values()) | set(dim_flds_to_std_fld_map.values())
missing_flds = set(full_uip_fld_map.values()) - cached_flds - view_gen.foreign_fields
if custom_fields:
    missing_flds = set([fld for fld in list(missing_flds) if fld in custom_fields])
if missing_flds:
    logger.warning(
        "WARNING: Getting results for model %s with cache_key %s required going back to uip for %s",
        model_name, cache_key, missing_flds)

# Put first record back — both paths iterate all_records_chained uniformly
all_records_chained = itertools.chain([first_raw], all_results)
```

**Step 2 — Move revenue config setup before the path split** (currently at lines 1330–1372 — these only read from `ds_inst.models['common'].config`, not from records, so they are safe to move):

```python
updated_ids = []
segment_config = ds_inst.models['common'].config.get('segment_config', {})
rev_schedule_config = ds_inst.models['common'].config.get('rev_schedule_config', {})
rev_period = current_period(epoch(time_horizon.begins)).mnemonic
# ... all existing rev_schedule_config.get(...) setup blocks unchanged ...
```

**Step 3 — Define `_iter_records()` normalizing both paths to `(extid, obj, std_uips)` tuples:**

```python
def _iter_records():
    if not missing_flds:
        # FAST PATH: single pass — no record_dict, yield immediately
        for record in all_records_chained:
            obj = record['object']
            extid = obj['extid']
            if get_results_from_as_of:
                updated_ids.append(extid)
            std_uips = {v: obj['uipfield'].get(k, 'N/A') for k, v in uip_flds_to_std_fld_map.items()}
            std_uips.update({v: obj['dimensions'].get(k, 'N/A') for k, v in dim_flds_to_std_fld_map.items()})
            yield extid, obj, std_uips
    else:
        # SLOW PATH: two-pass — unchanged behavior for UIP cache-miss tenants
        record_dict = {}
        ext_id_list = []
        for record in all_records_chained:
            obj = record['object']
            extid = obj['extid']
            ext_id_list.append(extid)
            record_dict[extid] = obj   # store obj directly (Fix C projection already limits fields)
        uip_args = dict(dataset=ds_inst.name, extid_list=ext_id_list)
        for uip_record in sec_context.etl.uip('KnownUIPRecordIterator', **uip_args):
            extid = uip_record.ID
            obj = record_dict[extid]
            if get_results_from_as_of:
                updated_ids.append(extid)
            std_uips = {v: obj['uipfield'].get(k, 'N/A') for k, v in uip_flds_to_std_fld_map.items()}
            std_uips.update({v: obj['dimensions'].get(k, 'N/A') for k, v in dim_flds_to_std_fld_map.items()})
            std_uips.update({fname: uip_record.getCacheValueF(fname, time_horizon)[1]
                             for fname in missing_flds})
            yield extid, obj, std_uips
```

**Step 4 — Single yield loop from `_iter_records()`. The 96-line revenue branching block (lines 1373–1468) moves inside this loop, logic unchanged:**

```python
for extid, obj, std_uips in _iter_records():
    flat_rec = {'res': obj['results'], 'uip': std_uips}
    view_list = view_gen.get_views(flat_rec)
    if not view_list:
        continue
    if hs_impl == 'A':
        output = view_gen.format_output(view_list, rev_disp_map)
    else:
        view_list = list(flatten_view(view) for view in view_list)
        output = view_list[0] if perspective else view_list
    # ... entire revenue branching yield block (lines 1407–1468) moved here unchanged ...
```

The deleted-IDs tail block (lines 1470–1481) stays where it is — it runs after the generator is exhausted and uses `updated_ids`, which `_iter_records()` populates via closure.

**What changes vs what stays the same:**

| | Changed | Unchanged |
|--|---------|-----------|
| Fast path (no missing_flds) | No `record_dict` built; inline yield | All yield logic, all revenue branching |
| Slow path (missing_flds) | `record_dict[extid] = obj` instead of wrapper dict | Everything else: two-pass, UIP lookup, getCacheValueF |
| Revenue branching (96 lines) | Moved inside the new loop body | Logic is bit-for-bit identical |
| Output format | — | Identical |

**Estimated savings:** ~800 MB – 1.2 GB per request for fast-path tenants (`record_dict` never built). Slow-path tenants see minor savings from the removed wrapper dict overhead.

**Risk factors:**
1. `itertools.chain([first_raw], all_results)` — PyMongo cursors are plain iterables; chaining works correctly. Verify once in a test environment.
2. `_iter_records` is a nested generator closing over outer-scope variables. All variables it uses are defined in the enclosing function scope before `_iter_records` is defined — Python closure works correctly.
3. `updated_ids` must be declared (`updated_ids = []`) before `_iter_records` is defined so the closure captures the list reference.
4. Empty result set: handled by the `try/except StopIteration` on `next(all_results)`.

**Verification:**
- Run against silverpeak.com or github.com (missing_flds tenant): confirm warning still logs, response is identical.
- Run against a normal tenant: confirm no warning, response identical, memory lower.
- Run with `get_results_from_as_of` set: confirm `updated_ids` is populated correctly.
- Run with empty result set (invalid cache key): confirm graceful return.

---

### Fix D — Stop collapsing the generator in `retrieve_deals()`

**File:** [result_Utils.py:~1867](deal_result/result_Utils.py#L1867)

**Dependency:** Implement after Fix B. Fix B saves memory inside the generator; Fix D prevents that saved memory from being re-materialized by `dict()` at the call site.

**Problem:**
```python
# Current — generator collapsed to dict immediately (~line 1867)
ret_val = dict(get_individual_results_generator(...))   # forces full materialization of 80K records
return ret_val
```

**Fix:**
```python
return get_individual_results_generator(...)   # return generator directly
```

**Callers that must be updated** (search for `retrieve_deals(` in result_Utils.py):

| Location | Current usage | Required change |
|----------|--------------|----------------|
| `deals_results_by_period` (~line 1769) | `results[period]['results'] = retrieve_deals(...)` | Must consume as a stream; cannot store as dict and re-read |
| `deals_results_by_period` (~line 1783) | `generate_subscription_recs(..., results[period]['results'])` | Function must accept a generator/iterable |
| `deals_results_by_period` (~line 1793) | `generate_revenue_recs(..., results[period]['results'])` | Same |
| `deals_results_by_period` (~line 1803) | `transform_score(results[period]['results'], ...)` | Must work per-record or accept a generator |
| `deals_results_by_timestamp` (~line 1998–2000) | `all_deals = retrieve_deals(...)` | `all_deals` becomes a one-use generator; filter loop that follows still works |
| Error check (~line 1888) | `if len(ret_val.keys()) > 0 and '_error' in ...` | Cannot call `len()` on a generator; must peek without consuming |

**Error check fix (line ~1888):**
```python
# Before
if len(ret_val.keys()) > 0 and '_error' in list(ret_val.keys())[0]:

# After — peek without consuming
import itertools
ret_val_a, ret_val_b = itertools.tee(ret_val)
first_key = next(iter(ret_val_a), None)
if first_key is not None and '_error' in first_key:
    ...
# Use ret_val_b as the generator going forward (ret_val_a is consumed)
```

**Estimated savings:** Eliminates the second full copy of 80K records (~800 MB – 1.2 GB) that `dict()` creates from the generator output.

**Risk:** Medium — multiple callers must be updated. Audit all call sites before implementing.

---

### Fix F — Replace `deepcopy(res)` in revenue schedule generators

**File:** [result_Utils.py:289, 298, 326, 336, 349, 579, 586, 606, 630](deal_result/result_Utils.py#L289)

**Problem:** Revenue generator functions (`generate_expiration_date_renewal_rec`, `generate_appannie_dummy_recs`, `generate_subscription_single_rec`, `generate_revenue_single_rec`) call `deepcopy(res)` per deal to create synthetic "dummy" records. With 80K deals and 2–4 dummy records each: 160K–320K deep copies active in memory = **1.6–9.6 GB**.

```python
# Example (lines ~289–298)
ret_val[opp_id] = deepcopy(res)       # full deep copy — 10–30 KB each
ret_val[dummy_id] = deepcopy(res)     # another full deep copy
```

**Fix:** Audit each `deepcopy(res)` site to identify which nested keys are mutated on the returned copy. Replace with a shallow copy + explicit copy of only those nested fields:

```python
import copy

def _copy_deal(res, mutated_nested_keys=('__segs',)):
    new = dict(res)   # shallow copy of top-level
    for k in mutated_nested_keys:
        if k in new and isinstance(new[k], (list, dict)):
            new[k] = copy.copy(new[k])   # one level deep — sufficient for lists/dicts of primitives
    return new
```

**Audit required per site:** For each `deepcopy(res)` call, identify which keys on the returned copy are mutated (e.g. `dummy['CloseDate'] = ...`, `dummy['__segs'] = ...`). Keys that are only read can be shared safely.

**Estimated savings:** 1.6–9.6 GB (tenant-specific — only applies when revenue schedule config is enabled).

**Risk:** Medium — requires careful per-site audit. A missed mutation will cause data corruption between records.

---

### Fix G — Stream S3 reads for pre-computed periods

**File:** [result_Utils.py:~1820](deal_result/result_Utils.py#L1820)

**Problem:** For non-live (cached) periods where an S3 file exists, the entire file is loaded into memory at once:
```python
results[period] = gnana_storage.read_daily_results_from_s3(file_name_new)   # full load
```
For 80K deals the pre-computed file is 200–600 MB, held while the view layer streams it.

**Fix:** Add a streaming variant to `gnana_storage` that uses boto3 streaming download + incremental JSON/CSV parsing, yielding deal records one at a time. Integrate with the existing streaming yield loop in `yield_period_results`.

**Lower priority:** The S3 cache path is less frequently hit than the live/chipotle path for current-quarter requests. Address after Fix B and Fix D.

---

## Latency / DB Round-Trip Optimizations — `/gbm/deals_results`

**Problem (separate from memory):** For a tenant with ~80K opportunities the request makes **hundreds of thousands of MongoDB round-trips**. On an in-VPC ECS container each is sub-millisecond, so it hides; from a laptop over VPN each is tens of milliseconds, so the same request takes 20+ minutes. The dominant term is the per-record versioned hierarchy lookup.

**Round-trip breakdown (one `retrieve_deals`, 76,824-record `custom_live` run, 8 drilldowns):**

| Source | Calls |
|--------|-------|
| Setup (dataset/model/run-config) | handful |
| `validate_results` — `findDocuments` + full `.count()` | 2 |
| View-generator build — 8 hierarchies × `$graphLookup` | 8 (× per period/model) |
| Result cursor — 76,824 ÷ `batch_size=2000` | ~39 `getMore` |
| **Per-record versioned `get_ancestors`** — records × drilldowns × splits | **~615,000** `$graphLookup` (the killer) |

---

### Fix H — Batch versioned hierarchy ancestor lookups ✅ DONE

**File:** [deal_result/hierarchy_service.py:693](deal_result/hierarchy_service.py#L693) (`CoolerHierarchyService.get_ancestors`)

**Problem:** In versioned mode `ts = versioned_ts(record)` is the deal's close date (non-None for won deals, i.e. essentially every deal). The versioned branch issued one recursive `$graphLookup` **per node, per drilldown, per split, per record** — ~615K round-trips for an 80K run. Meanwhile the full hierarchy was already loaded into memory at init but only the *unversioned* path used it.

**Change applied:** Load the entire hierarchy as-of a given `ts` **once**, cache it keyed by `ts` in `self.ancestor_cache`, and serve every node for that `ts` from memory.

```python
# BEFORE — one $graphLookup per node
anc_dict = {rec['node']: rec['ancestors']
            for rec in self.fetch_ancestors(as_of=ts, nodes=[node], db=db)}
return anc_dict[node] + [node]

# AFTER — one full-hierarchy load per distinct ts, then in-memory
table = self.ancestor_cache.get(ts)
if table is None:
    table = self.load_ancestors_from_db(ts)   # full hierarchy as-of ts
    self.ancestor_cache[ts] = table
return table[node] + [node]                    # fresh list per call; fallback unchanged
```

**Why output is identical:** In `fetch_ancestors` the `nodes` filter only decides which documents *enter* the pipeline; each node's `ancestors` chain is built by `$graphLookup` from its own `$parent` links under `restrictSearchWithMatch: criteria` (the time filter), independent of which other nodes are matched. So `table[node]` == the old `nodes=[node]` result. `load_ancestors_from_db(ts)` is exactly the full (unfiltered) form of the same query. `+ [node]` still returns a fresh list, and the `'unmapped'`/`'not_in_hier'` KeyError fallback is unchanged.

**Round-trips:** ~615,000 → **number of distinct close-date `ts` values** (day-granular, so small and bounded). The unversioned path (`ts is None`) is untouched.

**Cache scope:** `self.ancestor_cache` is a per-instance dict; the service is rebuilt per request ([viewgen_service.py:41](deal_result/viewgen_service.py#L41)), so it is discarded at request end — hierarchy edits between requests are always re-read from the DB, never served stale.

**Memory note:** caches one `{node: [ancestors]}` table per distinct `ts` (freed at request end). Bounded by hierarchy size × distinct close dates. If a tenant ever has sub-day close-date granularity (high-cardinality `ts`), this trades round-trips for memory; revisit Fix M in that case.

**Verification:** diff the JSON response for a versioned tenant (e.g. nutanix.com `custom_live`) before/after — deal count, seg keys, and field values must be identical; DB op count and wall-clock drop sharply.

---

### Fix I — Reuse the view generator across periods/models 🔴 PENDING

**File:** [deal_result/result_Utils.py:1246](deal_result/result_Utils.py#L1246)

`CoolerViewGeneratorService` is rebuilt inside every `retrieve_deals`/period, and each build reloads all 8 hierarchies from the DB (~18s over a high-latency link — the two back-to-back batches of 8 `versioned mode` log lines). Memoize the view generator per request keyed by `hier_asof` (= `time_horizon.as_of`) so the hierarchy loads happen once. Must key by `hier_asof` (not globally) so different as-of runs still get correct hierarchies. Output-preserving.

---

### Fix J — Increase result-cursor `batch_size` 🔴 PENDING

**File:** [deal_result/result_Utils.py:1276](deal_result/result_Utils.py#L1276)

Bump `batch_size=2000` → `10000`. 76,824 docs = ~8 `getMore` round-trips instead of ~39. One-line, output-preserving; disproportionately helps high-latency (local) runs. Keep an eye on per-batch memory (Fix C projection already keeps docs small).

---

### Fix K — Replace deprecated `.count()` in `validate_results` 🔴 PENDING

**File:** [deal_result/result_Utils.py:970](deal_result/result_Utils.py#L970), via [aviso/framework/mongodb.py:151](../../aviso-infrastructure/aviso/framework/mongodb.py#L151)

`res_cls.find_count(...)` uses the deprecated `.count(criteria)` (full-collection count). Switch to `count_documents`. Note the warning-only intent is preserved; this is just correctness + a lighter query.

---

### Fix L — Explicit session for the `no_cursor_timeout` cursor 🔴 PENDING

**File:** [deal_result/result_Utils.py:1273](deal_result/result_Utils.py#L1273)

PyMongo warns that `no_cursor_timeout=True` without an explicit session can still time out after 30 minutes. For long local runs, open a session (`with gbm_db.client.start_session() as s:` and pass `session=s` to `.find(...)`) and close it after iteration. Stability, not speed.

---

### Fix M — Serve versioned lookups from the as-of in-memory table ⚠️ NEEDS PRODUCT SIGN-OFF

**File:** [deal_result/hierarchy_service.py:693](deal_result/hierarchy_service.py#L693)

Fix H still does one full-hierarchy load per distinct `ts`. If the hierarchy is stable across the query's time window, versioned lookups could be served from the single as-of table (`self.hier_with_ancestors`, loaded at init) with **zero** per-`ts` DB calls. **This is a semantic change** — it resolves ancestors as-of the run rather than as-of each deal's close date — so it can change output for deals whose owner's hierarchy position changed between close date and run time. Do **not** ship without product confirmation that as-of-run resolution is acceptable.

---

## `/gbm/basic_results` — Deferred

**Status:** All 4 fixes below are deferred. Do not start until `deals_results` is tested end-to-end.

File: [data_load.py](api/data_load.py)

| # | Fix | File:Line | Effort | Est. Savings | Risk |
|---|-----|-----------|--------|-------------|------|
| DA-1 | Conditional `object.history` fetch | [data_load.py:161](api/data_load.py#L161) | 30 mins | 1–5 GB | Very Low |
| DA-2 | Eliminate `deepcopy(basic_results_dict)` | [data_load.py:405, 427](api/data_load.py#L405) | 1–2 hrs | 800 MB – 2 GB | Low |
| DA-3 | `StreamingHttpResponse` instead of `JsonResponse` | [data_load.py:559](api/data_load.py#L559) | 1–2 hrs | 2–8 GB spike | Low |
| DA-4 | Eliminate `list → dict → list` round-trip | [data_load.py:400, 438](api/data_load.py#L400) | 2–4 hrs | 800 MB – 1.2 GB | Low |

---

## Verification Checklist

After each fix, before moving to the next:

1. **Functional correctness:** Hit the API for a known tenant and diff the JSON response body against a pre-fix baseline. Deal count, field values, and structure must be identical.
2. **Memory measurement:** Record container RSS before and after via `docker stats` or `kubectl top pod`. Compare peak RSS during a full 80K-opp request.
3. **Edge-case paths to cover:**
   - `get_results_from_as_of` + timestamp mode (exercises Fix E path)
   - `force_uip_and_hierarchy=true` (deletes S3 cache, forces full recompute)
   - `return_chipotle_files_list=true` (returns file names, not data — should be unaffected by all fixes)
   - A request from silverpeak.com or github.com (exercises the `missing_flds` slow path — critical for Fix B)
   - A tenant with revenue schedule config enabled (exercises Fix F after it is applied)
4. **Streaming response validation:** Confirm the response is valid JSON. Use `curl --no-buffer` to verify chunked output and `python -m json.tool` to catch malformed JSON.
