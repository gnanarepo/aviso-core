# Memory Optimization: `deals_results` & `basic_results` APIs

**Problem:** Containers with 16 GB RAM OOM intermittently when serving tenants with ~80K opportunities via `/gbm/deals_results`.

**Root cause summary:** The data pipeline loads the entire result dataset into Python heap memory in multiple separate copies simultaneously. Peak memory = (MongoDB data) + (processed dict) + (JSON serialization string) + (revenue dummy records). These compound each other — fixes must be applied together for meaningful relief.

---

## API: `/gbm/deals_results`

Files: [deals_results.py](api/deals_results.py), [result_Utils.py](deal_result/result_Utils.py)

---

### P1-A — MongoDB field projection `[CRITICAL · 1–2 hrs · Very Low Risk]`

**File:** [result_Utils.py:1267–1271](deal_result/result_Utils.py#L1267)

**Issue:** The MongoDB `.find()` fetches every field in every document. With 80K deals, each document contains many fields we never use. Only four fields under `object` are ever accessed in the loop body: `extid`, `uipfield`, `dimensions`, `results`.

```python
# CURRENT — fetches entire document (no projection)
all_results = gbm_db[res_cls.getCollectionName()].find(
    criteria,
    batch_size=500,
    no_cursor_timeout=True
)
```

**Fix:**
```python
# AFTER — project only what the loop body actually reads
_projection = {
    'object.extid': 1,
    'object.uipfield': 1,
    'object.dimensions': 1,
    'object.results': 1,
    'last_modified_time': 1,  # used in get_results_from_as_of filter
    '_id': 0
}
all_results = gbm_db[res_cls.getCollectionName()].find(
    criteria,
    _projection,
    batch_size=2000,          # was 500; fewer round-trips for 80K docs (160 → 40)
    no_cursor_timeout=True
)
```

**Why `batch_size=2000` is safe:** MongoDB streams batches; Python processes one document at a time from the cursor. Larger batch_size only affects round-trip count, not total memory held by Python.

**Estimated savings:** 30–60% reduction in bytes transferred from MongoDB and deserialized into Python objects. For 80K deals this is ~300–600 MB.

---

### P1-B — Stream JSON per-deal instead of serializing entire period at once `[CRITICAL · 2–4 hrs · Low Risk]`

**File:** [deals_results.py:113–124, 127–144](api/deals_results.py#L113)

**Issue:** `json.dumps()` on the entire period result dict creates a **single giant string** in memory — 2–4× the size of the dict — before any bytes reach the HTTP client. `StreamingHttpResponse` is already in use but is negated by this one-shot serialization.

```python
# CURRENT — serializes ALL 80K deals to one string, then yields it
def yield_period_results(self, periods, ...):
    yield '{\n'
    for x, period in enumerate(periods):
        if x:
            yield ','
        yield '%s:\n' % json.dumps(period)
        yield '%s\n' % json.dumps(deals_results_by_period(...)[period])  # ← THE PROBLEM
    yield '}\n'
```

**Fix:** Stream `results` sub-dict entry by entry. The outer JSON structure stays identical (same keys, same format) — only the mechanism changes.

```python
def yield_period_results(self, periods, ...):
    yield '{\n'
    for x, period in enumerate(periods):
        if x:
            yield ','
        period_data = deals_results_by_period([period], ...)[period]
        deal_results = period_data.get('results', {})

        # Yield metadata fields first
        yield '%s: {\n' % json.dumps(period)
        yield '"ck": %s,\n' % json.dumps(period_data.get('ck'))
        yield '"timestamp": %s,\n' % json.dumps(period_data.get('timestamp'))
        yield '"is_curr_q": %s,\n' % json.dumps(period_data.get('is_curr_q'))
        yield '"results": {\n'

        # Stream one deal at a time — never hold all deals as a single string
        for i, (deal_id, deal_data) in enumerate(deal_results.items()):
            if i:
                yield ','
            yield '%s: %s\n' % (json.dumps(deal_id), json.dumps(deal_data))

        yield '}\n}\n'
    yield '}\n'
```

Apply the same pattern to `yield_timestamp_results` ([deals_results.py:126–144](api/deals_results.py#L126)).

**Estimated savings:** Eliminates the serialization spike entirely — for a 1.5 GB result dict this removes a 3–4 GB transient peak.

---

### P1-C — Eliminate double `retrieve_deals()` call in timestamp path `[HIGH · 1–2 hrs · Low Risk]`

**File:** [result_Utils.py:1939–1946](deal_result/result_Utils.py#L1939)

**Issue:** In the `get_results_from_as_of` + live chipotle code path, `retrieve_deals()` is called **twice** for the same cache key. The first call (line 1946) loads all 80K deals just to extract `.keys()` for `changed_deals`. The second call (line 1998/2000) loads all 80K deals again for actual use. Two full loads = 2× peak memory in this path.

```python
# CURRENT — loads full deal data just to get deal IDs
if get_results_from_as_of and not changed_deals:
    ...
    if is_curr_q:
        chip_asof, chip_ck = get_latest_chipotle()
        if chip_asof and ...:
            asof, ck, model = chip_asof, chip_ck, 'bookings_rtfm'
            changed_deals.extend(list(retrieve_deals(model, ck, ...).keys()))  # LINE 1946 — full load

# ...then later, in the timestamps loop:
all_deals = retrieve_deals(model, ck, include_uip, node, fields=fields_to_fetch)  # LINE 1998 — full load AGAIN
```

**Fix:** Replace the first call with an IDs-only MongoDB query that fetches only `object.extid`:

```python
# AFTER — lightweight ID-only query
if get_results_from_as_of and not changed_deals:
    ...
    if is_curr_q:
        chip_asof, chip_ck = get_latest_chipotle()
        if chip_asof and ...:
            asof, ck, model = chip_asof, chip_ck, 'bookings_rtfm'
            # Fetch only extids — no full deal data needed here
            changed_deals.extend(
                _get_deal_ids_only(model, ck, get_results_from_as_of=get_results_from_as_of)
            )
```

Add a helper `_get_deal_ids_only()` that runs the same MongoDB criteria with projection `{'object.extid': 1, '_id': 0}` and returns a list of extids. This requires extracting the `criteria` building logic from `get_results_objects()` — or just inlining the query.

**Estimated savings:** Eliminates a complete redundant 800 MB – 1.2 GB load for every timestamp-mode request with `get_results_from_as_of`.

---

### P2-A — Single-pass streaming in `get_individual_results_generator` `[CRITICAL · 2–4 hrs · Medium Risk]`

**File:** [result_Utils.py:1293–1322](deal_result/result_Utils.py#L1293)

**Issue:** The function does a full **two-pass** scan over all 80K records. Pass 1 (lines 1294–1317) builds `record_dict` holding ALL records in memory. Pass 2 (lines 1366–1461) iterates `record_dict` to process and `yield`. Both passes run for every request even though the two-pass design only exists to handle a rare edge case (`missing_flds` non-empty = UIP cache miss).

```python
# CURRENT — two-pass, both passes keep full dataset in memory
record_dict = {}                          # accumulates ALL records
ext_id_list = []
for record in all_results:               # PASS 1: load 80K records into record_dict
    extid = obj['extid']
    record_dict[extid] = {
        'uipfield': obj['uipfield'],
        'dimensions': obj['dimensions'],
        'results': obj['results']
    }

# ... determine missing_flds ...

for record in record_iterator:           # PASS 2: iterate record_dict to yield
    obj = record_dict[record]
    # ... process and yield ...
```

**Fix:** Split into two code paths:

```python
# AFTER
first_record = True
record_dict = {}      # only populated in the rare missing_flds path
ext_id_list = []
missing_flds = set()

for record in all_results:
    obj = record['object']
    extid = obj['extid']

    if first_record:
        # One-time setup: detect missing fields
        uip_flds_to_std_fld_map = {x: add_prefix(x, 'as_of_') for x in obj.get('uipfield', {})}
        dim_flds_to_std_fld_map = {x: add_prefix(x) for x in obj.get('dimensions', {})}
        cached_flds = set(uip_flds_to_std_fld_map.values()) | set(dim_flds_to_std_fld_map.values())
        missing_flds = set(full_uip_fld_map.values()) - cached_flds - view_gen.foreign_fields
        if custom_fields:
            missing_flds = set([fld for fld in list(missing_flds) if fld in custom_fields])
        first_record = False

    if missing_flds:
        # RARE PATH: must accumulate for UIP lookup — keep two-pass behaviour
        record_dict[extid] = {'uipfield': obj['uipfield'],
                              'dimensions': obj['dimensions'],
                              'results': obj['results']}
        ext_id_list.append(extid)
    else:
        # COMMON PATH: process and yield immediately — no accumulation
        std_uips = {v: obj['uipfield'].get(k, 'N/A') for k, v in uip_flds_to_std_fld_map.items()}
        std_uips.update({v: obj['dimensions'].get(k, 'N/A') for k, v in dim_flds_to_std_fld_map.items()})
        flat_rec = {'res': obj['results'], 'uip': std_uips}
        view_list = view_gen.get_views(flat_rec)
        if view_list:
            # ... revenue schedule branching and yield (same as existing Pass 2 body) ...
            yield (extid, output)

# If missing_flds path was taken, fall through to existing Pass 2 using record_dict
if record_dict:
    # existing Pass 2 code for UIP cache-miss records
    ...
```

**Risk note:** The `missing_flds` check was previously done after loading all records (first-record detection inside the loop is already there at line 1299). This refactor moves the yield inline. Validate that `uip_flds_to_std_fld_map` and `dim_flds_to_std_fld_map` are initialized before the fast path uses them (they are — first_record sets them up on the very first iteration).

**Estimated savings:** ~800 MB – 1.2 GB per request for the common path (no `record_dict` accumulated).

---

### P2-B — Stop collapsing the generator in `retrieve_deals()` `[HIGH · 4–8 hrs · Medium Risk]`

**File:** [result_Utils.py:1866–1879](deal_result/result_Utils.py#L1866)

**Issue:** `get_individual_results_generator` is a proper Python generator (uses `yield`). `retrieve_deals()` defeats this by wrapping it in `dict()` on line 1867, forcing full materialization of all 80K records before returning to the caller.

```python
# CURRENT — generator collapsed to dict immediately
def retrieve_deals(model, cache_key, ...):
    ...
    try:
        ret_val = dict(get_individual_results_generator(...))  # ← full materialization
    ...
    return ret_val
```

**Fix:** Return the generator directly. This is the largest-scope change because all callers of `retrieve_deals()` must be updated to handle a generator (single-iteration) instead of a re-readable dict.

```python
# AFTER
def retrieve_deals(model, cache_key, ...):
    ...
    return get_individual_results_generator(...)   # returns a generator, not a dict
```

**Callers that must be updated:**

| Location | Current usage | Required change |
|----------|--------------|----------------|
| [result_Utils.py:1769](deal_result/result_Utils.py#L1769) | `results[period]['results'] = retrieve_deals(...)` | Must stream — can no longer store as dict and re-read |
| [result_Utils.py:1783](deal_result/result_Utils.py#L1783) | `generate_subscription_recs(rev_period, ..., results[period]['results'])` | `generate_subscription_recs` must accept an iterable |
| [result_Utils.py:1793](deal_result/result_Utils.py#L1793) | `generate_revenue_recs(rev_period, ..., results[period]['results'])` | Same |
| [result_Utils.py:1998–2000](deal_result/result_Utils.py#L1998) | `all_deals = retrieve_deals(...)` | `all_deals` becomes a generator; filtering loop on 2005 already iterates it |
| [result_Utils.py:1803](deal_result/result_Utils.py#L1803) | `transform_score(results[period]['results'], ...)` | `transform_score` must work per-record |
| Error check [result_Utils.py:1888](deal_result/result_Utils.py#L1888) | `if len(ret_val.keys()) > 0 and '_error' in ...` | Peek at first item without consuming the generator |

**This fix only pays off after P2-A is done** — otherwise `record_dict` still holds everything in memory inside the generator.

**Estimated savings:** Eliminates the second full-copy of the dataset (~800 MB – 1.2 GB) that `dict()` creates from the generator.

---

### P3-A — Reduce `deepcopy(res)` in revenue schedule generators `[HIGH · 4–8 hrs · Medium Risk]`

**File:** [result_Utils.py:289, 298, 326, 336, 349, 579, 586, 606, 630](deal_result/result_Utils.py#L289)

**Issue:** Revenue schedule generator functions (`generate_expiration_date_renewal_rec`, `generate_appannie_dummy_recs`, `generate_subscription_single_rec`, `generate_revenue_single_rec`) call `deepcopy(res)` for each deal to create "dummy" records. With 80K deals each producing 2–4 dummy records, this creates 160K–320K deep copies concurrently in memory.

```python
# Example from generate_expiration_date_renewal_rec (lines ~289–298):
ret_val[opp_id] = deepcopy(res)      # full deep copy — 10–30 KB each
ret_val[dummy_id] = deepcopy(res)    # another full deep copy
```

**Fix:** Replace `deepcopy(res)` with a targeted shallow copy + explicit copy of only the nested fields that are actually mutated:

```python
# AFTER — only copy what gets mutated
import copy

def _copy_deal(res, mutated_nested_keys=('__segs',)):
    """Shallow copy the deal dict, deep-copy only the nested fields we will mutate."""
    new = dict(res)
    for k in mutated_nested_keys:
        if k in new and isinstance(new[k], (list, dict)):
            new[k] = copy.copy(new[k])  # one level deep is enough for lists/dicts of primitives
    return new
```

**Audit required:** For each `deepcopy(res)` site, verify which nested keys are modified on the returned copy. Any nested key that is mutated needs an explicit `copy.copy()`. Keys that are only read can be shared safely.

**Estimated savings:** This is tenant-specific (only applies when revenue schedule config is enabled), but can be 1.6–9.6 GB in the worst case.

---

### P3-B — Stream S3 reads for pre-computed periods `[MEDIUM · 4–8 hrs · Medium Risk]`

**File:** [result_Utils.py:1820](deal_result/result_Utils.py#L1820)

**Issue:** For non-live (cached) periods where an S3 file exists, the entire file is read into memory at once:

```python
results[period] = gnana_storage.read_daily_results_from_s3(file_name_new)  # full load
```

For 80K deals the pre-computed CSV/JSON file is 200–600 MB. It is then held while `json.dumps` serializes it (another 400 MB – 1.2 GB spike).

**Fix:** Modify `gnana_storage.read_daily_results_from_s3()` to accept a streaming mode (or add a companion `stream_daily_results_from_s3()`) that yields deal records one at a time using `boto3` streaming download + incremental JSON/CSV parsing. This integrates with Fix P1-B (streaming JSON output).

**Estimated savings:** 200–600 MB for cached non-live period requests. Lower priority since live/chipotle requests dominate the problem.

---

## API: `/gbm/basic_results`

File: [data_load.py](api/data_load.py)

These are separate from the `deals_results` issues but follow the same patterns and are worth fixing together.

---

### DA-1 — Conditional `object.history` fetch `[CRITICAL · 30 mins · Very Low Risk]`

**File:** [data_load.py:160–161](api/data_load.py#L160)

**Issue:** `object.history` (all historical field values per opportunity) is added to the MongoDB projection unconditionally. It is only used inside the `if self.from_timestamp and self.changed_fields_only` branch ([data_load.py:214](api/data_load.py#L214)). History objects are enormous — they store every past value for every field. For 80K deals this is typically 1–5 GB of data loaded unnecessarily for requests that don't use it.

```python
# CURRENT — always fetched regardless of whether it will be used
required_fields.add("object.history")   # line 161
```

**Fix:**
```python
# AFTER — only fetch when the code path that reads it is reachable
if self.from_timestamp and self.changed_fields_only:
    required_fields.add("object.history")
```

This is a 1-line change. No other code reads `object.history` outside the guarded branch.

**Estimated savings:** 1–5 GB for any request where `changed_fields_only=False` or `from_timestamp=0` (the vast majority of requests).

---

### DA-2 — Eliminate `deepcopy(basic_results_dict)` `[HIGH · 1–2 hrs · Low Risk]`

**File:** [data_load.py:405–409, 427–433](api/data_load.py#L405)

**Issue:** Before calling revenue record generators, the entire `basic_results_dict` (80K deals in memory) is deep-copied, then cleared, then rebuilt:

```python
# CURRENT — deep-copies entire dict just to iterate over it
basic_results_dict_copy = deepcopy(basic_results_dict)   # full copy — 800 MB+ duplicate
basic_results_dict = {}
for opp_id, res in basic_results_dict_copy.items():
    output_dict = generate_appannie_dummy_recs(rev_period, rev_schedule_config, opp_id, res)
    basic_results_dict.update(output_dict)
```

The `deepcopy` is only needed because the code clears `basic_results_dict = {}` while iterating. This is unnecessary — just iterate the original and build a new dict:

```python
# AFTER — iterate original directly, build new dict without copying
new_results_dict = {}
for opp_id, res in basic_results_dict.items():
    output_dict = generate_appannie_dummy_recs(rev_period, rev_schedule_config, opp_id, res)
    new_results_dict.update(output_dict)
basic_results_dict = new_results_dict
```

Apply the same pattern to the `expiration_date_renewals_rec` branch ([data_load.py:427–433](api/data_load.py#L427)).

**Safety check:** `generate_appannie_dummy_recs` and `generate_expiration_date_renewal_rec` must not mutate `res` in-place. If they do, use `res = dict(res)` (shallow copy of the single deal) rather than `deepcopy(basic_results_dict)` (copy of all 80K deals).

**Estimated savings:** 800 MB – 2 GB (eliminates the redundant full-dict copy at peak time).

---

### DA-3 — `StreamingHttpResponse` instead of `JsonResponse` `[HIGH · 1–2 hrs · Low Risk]`

**File:** [data_load.py:559](api/data_load.py#L559)

**Issue:** `JsonResponse(final_results, safe=False, status=200)` serializes the entire `final_results` list (potentially 80K+ records after revenue schedule expansion) to a single JSON string before sending a single byte. This is identical to the `deals_results` problem (Fix P1-B) and causes a 2–4× memory spike at response time.

```python
# CURRENT — entire list serialized to one string in memory
return JsonResponse(final_results, safe=False, status=200)   # line 559
```

**Fix:**
```python
# AFTER — stream one record at a time
import json
from django.http import StreamingHttpResponse

def _stream_json_list(records):
    yield '['
    first = True
    for rec in records:
        if not first:
            yield ','
        yield json.dumps(rec)
        first = False
    yield ']'

response = StreamingHttpResponse(
    _stream_json_list(final_results),
    content_type='application/json'
)
response.status_code = 200
return response
```

**Estimated savings:** Eliminates the JSON serialization spike. For 80K records with revenue schedule expansion (up to 320K entries) this removes a 2–8 GB transient spike.

---

### DA-4 — Eliminate `list → dict → list` round-trip in `revenue_schedule()` `[MEDIUM · 2–4 hrs · Low Risk]`

**File:** [data_load.py:398–439](api/data_load.py#L398)

**Issue:** `revenue_schedule()` converts the results list to a dict (line 400 via `get_results_dict`), processes it, then converts back to a list (line 438 via `get_results_list`). `get_results_dict` ([line 448–452](api/data_load.py#L448)) uses `rec.pop('extid')` which **mutates the original records** to strip the `extid` field, requiring the round-trip to restore it. This creates an extra full copy of the dataset.

```python
# CURRENT — list → dict (mutating original) → process → list (restoring extid)
basic_results_dict = self.get_results_dict(basic_results)    # mutates records; creates dict copy
# ... process ...
basic_results = self.get_results_list(basic_results_dict)    # creates list copy
```

**Fix:** Stop mutating the original records. Use `extid` as a lookup key without `pop()`, and pass deals as `(opp_id, record)` pairs directly:

```python
# AFTER — process in a single pass as (opp_id, record) tuples
new_results = []
for rec in basic_results:
    opp_id = rec['extid']                      # read without pop
    record = {k: v for k, v in rec.items() if k != 'extid'}   # exclude extid from payload
    output_dict = generate_appannie_dummy_recs(rev_period, rev_schedule_config, opp_id, record)
    for new_opp_id, new_record in output_dict.items():
        new_record['extid'] = new_opp_id
        new_results.append(new_record)
basic_results = new_results
```

This requires removing `get_results_dict` and `get_results_list` calls and refactoring the three revenue schedule branches similarly. The `rev_schedule_by_period` function (which operates on the dict) also needs to accept `(opp_id, record)` pairs.

**Estimated savings:** Eliminates one full copy of the dataset (~800 MB – 1.2 GB). Simpler than it looks because `get_results_dict`/`get_results_list` are simple wrappers.

---

## Summary Table

### `/gbm/deals_results` — Priority Order

| # | Fix | File:Line | Effort | Est. Savings | Risk |
|---|-----|-----------|--------|-------------|------|
| P1-A | MongoDB field projection + batch_size | [result_Utils.py:1267](deal_result/result_Utils.py#L1267) | 1–2 hrs | 300–600 MB | Very Low |
| P1-B | Stream JSON per-deal (no single-shot `json.dumps`) | [deals_results.py:113](api/deals_results.py#L113) | 2–4 hrs | 2–4 GB spike | Low |
| P1-C | Eliminate double `retrieve_deals()` call | [result_Utils.py:1946](deal_result/result_Utils.py#L1946) | 1–2 hrs | 800 MB – 1.2 GB | Low |
| P2-A | Single-pass streaming in generator (no `record_dict` accumulation) | [result_Utils.py:1293](deal_result/result_Utils.py#L1293) | 2–4 hrs | 800 MB – 1.2 GB | Medium |
| P2-B | Return generator from `retrieve_deals()` (no `dict()` collapse) | [result_Utils.py:1867](deal_result/result_Utils.py#L1867) | 4–8 hrs | 800 MB – 1.2 GB | Medium |
| P3-A | Replace `deepcopy(res)` with targeted shallow copy | [result_Utils.py:289+](deal_result/result_Utils.py#L289) | 4–8 hrs | 1.6–9.6 GB (tenant-specific) | Medium |
| P3-B | Stream S3 reads | [result_Utils.py:1820](deal_result/result_Utils.py#L1820) | 4–8 hrs | 200–600 MB | Medium |

### `/gbm/basic_results` — Priority Order

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
   - `get_results_from_as_of` + timestamp mode (covers P1-C)
   - `force_uip_and_hierarchy=true` (deletes S3 cache, forces full recompute)
   - `return_chipotle_files_list=true` (returns file names, not data — should be unaffected)
   - A request with `missing_flds` non-empty (covers P2-A rare path)
   - A tenant with revenue schedule config enabled (covers P3-A)
4. **Streaming response validation:** Confirm the response is valid JSON even when streamed (some clients may buffer anyway — use `curl` with `--no-buffer` to verify chunked output).
