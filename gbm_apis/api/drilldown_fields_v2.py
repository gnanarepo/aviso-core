import itertools
import json
import logging

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

from django.conf import settings
from django.http import HttpResponse, HttpResponseBadRequest

from gbm_apis.domainmodel.datameta import Dataset
from gbm_apis.framework.mixins import AvisoCompatibilityMixin
from utils import is_true
from utils.date_utils import current_period, epoch, period_details_by_mnemonic
from gbm_apis.framework.baseView import AvisoView
from aviso.settings import sec_context
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

logger = logging.getLogger("aviso-core.%s" % __name__)


@method_decorator(csrf_exempt, name="dispatch")
class DrilldownFieldsV2(AvisoCompatibilityMixin, AvisoView):
    """
    API endpoint to fetch drilldown field combinations (v2).
    Inherits from AvisoView but patches Auth and Request/Response for compatibility.
    """

    http_method_names = ["post"]
    restrict_to_roles = {AvisoView.Role.Gnacker}
    as_json = False

    def post(self, request, *args, **kwargs):
        # 1. Parameter Parsing
        periods = request.GET.getlist("period", [])
        owner_mode = is_true(request.GET.get("owner_mode", False))

        if not periods:
            return HttpResponseBadRequest(
                json.dumps({"error": "Need at least one period specified"}),
                content_type="application/json"
            )

        # 2. Logic Execution
        try:
            if owner_mode:
                drilldown = request.GET.get("drilldown")
                if not drilldown:
                    return HttpResponseBadRequest(
                        json.dumps({"error": "Mandatory to provide drilldown in owner_mode"}),
                        content_type="application/json"
                    )
                res = drilldown_values_by_owner_v2(drilldown, periods)
            else:
                try:
                    raw_body = request.body.decode('utf-8')
                    payload = json.loads(raw_body)
                    fields = payload.get("fields_list")
                except (json.JSONDecodeError, AttributeError):
                    return HttpResponseBadRequest(
                        json.dumps({"error": "Invalid JSON body"}),
                        content_type="application/json"
                    )

                if not fields:
                    return HttpResponseBadRequest(
                        json.dumps({"error": "Need to provide 'fields_list' argument in post body"}),
                        content_type="application/json"
                    )

                res = drilldown_values_by_period(periods, fields)

            return HttpResponse(json.dumps(res), content_type="application/json")

        except Exception as e:
            logger.exception("Error in DrilldownFieldsV2")
            return HttpResponse(
                json.dumps({"error": str(e), "message": "Internal Server Error"}),
                status=500,
                content_type="application/json"
            )

def drilldown_fields_v2(periods, owner_mode=False, drilldown=None, fields_list=None):
    """
    Plain function equivalent of the DrilldownFieldsV2.post() logic.

    Args:
        periods (list[str]): List of period strings. Required.
        owner_mode (bool): Toggle between owner mode or normal mode.
        drilldown (str): Required only if owner_mode=True.
        fields_list (list[str]): Required only if owner_mode=False.

    Returns:
        dict: The drilldown result data.

    Raises:
        ValueError: If mandatory parameters are missing.
    """

    if not periods:
        raise ValueError("Need at least one period specified")

    if owner_mode:
        if not drilldown:
            raise ValueError("Mandatory to provide drilldown in owner_mode")
        return drilldown_values_by_owner_v2(drilldown, periods)

    else:
        if not fields_list:
            raise ValueError("Need to provide fields_list argument in post body")
        return drilldown_values_by_period(periods, fields_list)


def _get_period_bounds(period):
    """
    Helper to determine the datetime bounds for a requested period.
    """
    if not period:
        cp = current_period()
        return cp.get("begin")

    try:
        period_details = period_details_by_mnemonic(period)
    except Exception:
        period_details = period_details_by_mnemonic(period, period_type="CM")

    return period_details.begin


def _fetch_raw_hierarchy(db=None) -> List[Dict[str, Any]]:
    """
    Fetch raw hierarchy nodes for the tenant.
    """
    if sec_context is None:
        logger.warning("sec_context unavailable; returning empty hierarchy")
        return []

    try:
        hierarchy_coll = (db or sec_context.tenant_db)["hierarchy"]
        return list(hierarchy_coll.find({}, {"_id": 0, "node": 1, "parent": 1, "from": 1, "to": 1}))
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Failed to fetch raw hierarchy", exc_info=exc)
        return []


def build_global_territory_owners(groups: Iterable[Iterable[str]], period: Optional[str], leaf_field: str):
    """
    Build ownership drilldown combinations for global territories.

    Args:
        groups: List of drilldown field groupings.
        period: Period identifier to fetch data for.
        leaf_field: Leaf node field from hier_config.

    Returns:
        dict: Structure { group_name: { leaf_node: [ [(key, val), ...] ] } }
    """
    logger.info(
        "[build_global_territory_owners] | period=%s | leaf_field=%s | groups=%s",
        period,
        leaf_field,
        groups,
    )

    period_begin = _get_period_bounds(period)
    begin_xldate = epoch(period_begin).as_xldate()
    record_filter = {"object.terminal_date": {"$gte": begin_xldate}}
    asof_epoch = epoch().as_epoch()

    required_fields = {leaf_field}
    for group in groups:
        for fld in group:
            required_fields.add(fld.replace("as_of_", ""))

    if sec_context is None or not getattr(sec_context, "etl", None):
        logger.warning("sec_context.etl not available; returning empty drilldown set")
        return {}

    records = sec_context.etl.uip(
        "UIPIterator", dataset="OppDS", record_filter=record_filter, fields_requested=list(required_fields)
    )

    hierarchy = _fetch_raw_hierarchy()
    parents = {}
    for element in hierarchy:
        f, t = element.get("from"), element.get("to")
        if (not f or f <= asof_epoch) and (not t or asof_epoch <= t):
            parents[element["node"]] = element["parent"]

    global_children_set = set()
    visited_nodes: Dict[str, bool] = {}
    for node in parents:
        node_str = str(node)
        if node_str in visited_nodes:
            if visited_nodes[node_str]:
                global_children_set.add(node)
            continue

        curr = node_str
        depth = 0
        is_global_child = False
        path = [node_str]

        while curr and depth < 50:
            if curr == "Global":
                is_global_child = True
                break
            curr = parents.get(curr)
            if curr:
                path.append(str(curr))
            depth += 1

        for path_node_str in path:
            visited_nodes[path_node_str] = is_global_child
        if is_global_child:
            global_children_set.add(node)

    group_configs = []
    for group in groups:
        group_name = "|".join(group)
        fields_map = []
        for fld in group:
            fields_map.append((fld, fld.replace("as_of_", "")))
        group_configs.append((group_name, fields_map))

    staging = defaultdict(lambda: defaultdict(set))
    territory_key = leaf_field
    processed_count = 0
    skipped_count = 0

    for record in records:
        feat_map = record.featMap
        if territory_key not in feat_map:
            skipped_count += 1
            continue

        territory_data = feat_map[territory_key]
        if not territory_data:
            skipped_count += 1
            continue

        leaf_node = territory_data[-1][1]
        if leaf_node not in global_children_set:
            skipped_count += 1
            continue

        processed_count += 1
        for group_name, field_mappings in group_configs:
            current_row = []
            for fld_label, map_key in field_mappings:
                if map_key in feat_map:
                    field_data = feat_map[map_key]
                    if field_data:
                        current_row.append([fld_label, field_data[-1][1]])
                    else:
                        current_row.append([fld_label, "N/A"])
                else:
                    current_row.append([fld_label, "N/A"])
            staging[group_name][leaf_node].add(tuple(tuple(pair) for pair in current_row))

    logger.info(
        "build_global_territory_owners: processed %s records, skipped %s records",
        processed_count,
        skipped_count,
    )

    final_period_result = {}
    for g_name, nodes in staging.items():
        final_period_result[g_name] = {}
        for node, rows in nodes.items():
            final_period_result[g_name][node] = [[[fld, val] for fld, val in row] for row in rows]

    return final_period_result


def drilldown_values_by_owner_v2(drilldown: str, periods: Iterable[str]):
    """
    Fetch drilldown combos by owner using ETL hierarchy data.
    """
    ds = Dataset.getByNameAndStage(name="OppDS")
    viewgen_config = ds.models["common"].config.get("viewgen_config", {})
    hier_config = viewgen_config.get("hier_config", {})

    leaf_field = ""
    if drilldown in hier_config:
        leaf_field = hier_config[drilldown]["leaf_field"]
    if leaf_field:
        leaf_field = leaf_field.replace("as_of_", "")

    node_config = viewgen_config.get("node_config", {})
    if drilldown not in node_config:
        groups = [node_config.get("default", {}).get("fields", [])]
    else:
        groups = [node_config[drilldown].get("fields", [])]

    output = {}
    for period in periods:
        final_period_result = build_global_territory_owners(groups, period=period, leaf_field=leaf_field)
        output[period] = final_period_result
    logger.info("fetching drill down values by owner v2 for drilldown: %s completed.", drilldown)
    return output


def deals_results_by_period(periods: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """
    Lightweight helper to fetch deal results for the requested periods.
    """
    results: Dict[str, Dict[str, Any]] = {}
    if sec_context and getattr(sec_context, "gbm", None):
        gbm_client = sec_context.gbm
        for period in periods:
            try:
                url = f"/gbm/deals_results?period={period}"
                results[period] = gbm_client.api(url, None) or {"results": {}}
            except Exception as exc:
                logger.exception("Failed to fetch deals results for %s", period, exc_info=exc)
                results[period] = {"results": {}}
    else:
        logger.warning("GBM client not configured; returning empty results for periods %s", list(periods))
        for period in periods:
            results[period] = {"results": {}}
    return results


def drilldown_values_by_period(periods: Iterable[str], groups: Iterable[Iterable[str]]):
    """
    Given a set of drilldown groups, return all possible combos of those field values.
    """
    output: Dict[str, Dict[str, List[List[tuple]]]] = {}
    curr_period = current_period().mnemonic

    for period in periods:
        imr = deals_results_by_period([period])
        if period != curr_period:
            curr_imr = deals_results_by_period([curr_period])

        output[period] = {}
        for group in groups:
            group_set = set()
            for rec in imr.get(period, {}).get("results", {}).values():
                rec_output = []
                for field in group:
                    try:
                        if isinstance(rec[field], dict):
                            rec_output.append([(field, val) for val in set(rec[field].values())])
                        else:
                            rec_output.append([(field, rec[field])])
                    except KeyError:
                        rec_output.append([(field, "N/A")])
                group_set |= set(itertools.product(*rec_output))

            if period != curr_period:
                for rec in curr_imr.get(curr_period, {}).get("results", {}).values():
                    rec_output = []
                    for field in group:
                        try:
                            if isinstance(rec[field], dict):
                                rec_output.append([(field, val) for val in set(rec[field].values())])
                            else:
                                rec_output.append([(field, rec[field])])
                        except KeyError:
                            rec_output.append([(field, "N/A")])
                    group_set |= set(itertools.product(*rec_output))

            group_name = "|".join(group)
            output[period][group_name] = list(group_set)

    return output
