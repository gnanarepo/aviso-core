import json
import logging
from django.http.response import StreamingHttpResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from aviso.utils import is_none, is_true
from gbm_apis.deal_result.result_Utils import deals_results_by_period, deals_results_by_timestamp
from gbm_apis.framework.baseView import AvisoView
from gbm_apis.framework.mixins import AvisoCompatibilityMixin

logger = logging.getLogger('gnana.%s' % __name__)

@method_decorator(csrf_exempt, name='dispatch')
class DealsResultsAPIView(AvisoCompatibilityMixin, AvisoView):
    """
    API View to stream deals results.
    Refactored to use AvisoView + Compatibility Mixin for auth & stability.
    """

    # --- Configuration ---
    http_method_names = ['get', 'post']
    restrict_to_roles = {AvisoView.Role.Gnacker}

    # CRITICAL: We return StreamingHttpResponse, so we must disable
    # AvisoView's attempt to wrap responses in JSON.
    as_json = False

    def get(self, request, *args, **kwargs):
        periods = request.GET.getlist('period', [])
        timestamps = [int(x) for x in request.GET.getlist('timestamp', [])]
        get_results_from_as_of = int(request.GET.get('get_results_from_as_of', 0))
        node = request.GET.get('node')

        include_uip = is_true(request.GET.get('include_uip', True))
        force_uip_and_hierarchy = is_true(request.GET.get('force_uip_and_hierarchy', False))
        allow_live = is_true(request.GET.get('allow_live', True))
        return_files_list = is_true(request.GET.get('return_files_list', False))
        return_chipotle_files_list = is_true(request.GET.get('return_chipotle_files_list', False))

        fields = request.GET.getlist('fields', [])

        id_list = request.GET.getlist('id_list', '')

        if not timestamps:
            return StreamingHttpResponse(
                self.yield_period_results(periods, include_uip, node,
                                          get_results_from_as_of=get_results_from_as_of,
                                          fields=fields,
                                          return_files_list=return_files_list,
                                          return_chipotle_files_list=return_chipotle_files_list,
                                          id_list=id_list),
                status=200, content_type='application/json')
        elif len(periods) == 1:
            return StreamingHttpResponse(
                self.yield_timestamp_results(periods[0], timestamps, include_uip, node,
                                             get_results_from_as_of=get_results_from_as_of, fields=fields,
                                             force_uip_and_hierarchy=force_uip_and_hierarchy, allow_live=allow_live,
                                             return_files_list=return_files_list,
                                             return_chipotle_files_list=return_chipotle_files_list),
                status=200, content_type='application/json')
        else:
            raise Exception("Invalid Request: Multiple periods provided with timestamps.")

    def post(self, request, *args, **kwargs):
        # 1. Parse Parameters
        periods = request.GET.getlist('period', [])

        try:
            # Use raw body decoding for safety
            raw_body = request.body.decode('utf-8') if hasattr(request, 'body') else ''
            body = json.loads(raw_body) if raw_body else {}
        except json.JSONDecodeError:
            body = {}

        fields = []
        if not is_none(body.get('fields', None)):
            fields = body.get('fields', None)

        opp_ids = []
        if not is_none(body.get('opp_ids', None)):
            opp_ids = body.get('opp_ids', None)

        get_results_from_as_of = int(request.GET.get('get_results_from_as_of', 0))
        timestamps = [int(x) for x in (request.GET.getlist('timestamp', []))]
        node = request.GET.get('node')
        include_uip = is_true(request.GET.get('include_uip', True))
        force_uip_and_hierarchy = is_true(request.GET.get('force_uip_and_hierarchy', False))
        allow_live = is_true(request.GET.get('allow_live', True))
        return_files_list = is_true(request.GET.get('return_files_list', False))

        if not timestamps:
            return StreamingHttpResponse(
                self.yield_period_results(periods, include_uip, node,
                                          get_results_from_as_of=get_results_from_as_of,
                                          fields=fields, opp_ids=opp_ids,
                                          return_files_list=return_files_list),
                status=200, content_type='application/json')
        elif len(periods) == 1:
            return StreamingHttpResponse(
                self.yield_timestamp_results(periods[0], timestamps, include_uip, node,
                                             get_results_from_as_of=get_results_from_as_of, fields=fields,
                                             opp_ids=opp_ids, force_uip_and_hierarchy=force_uip_and_hierarchy,
                                             allow_live=allow_live, return_files_list=return_files_list),
                status=200, content_type='application/json')
        else:
            raise Exception("Invalid Request: Multiple periods provided with timestamps.")

    # --- Helper Generators (Logic kept as-is) ---

    def yield_period_results(self, periods, include_uip, node, get_results_from_as_of=0, fields=[], opp_ids=[],
                             return_files_list=False, return_chipotle_files_list=False,
                             id_list=[]):
        yield '{\n'
        for x, period in enumerate(periods):
            if x:
                yield ','
            yield '%s:\n' % json.dumps(period)
            period_data = deals_results_by_period([period], include_uip=include_uip, node=node,
                                                  get_results_from_as_of=get_results_from_as_of,
                                                  fields=fields,
                                                  return_files_list=return_files_list,
                                                  return_chipotle_files_list=return_chipotle_files_list,
                                                  id_list=id_list)[period]
            # File-list modes return a plain list — small payload, serialize as before
            if not isinstance(period_data, dict) or 'results' not in period_data:
                yield '%s\n' % json.dumps(period_data)
                continue
            # Stream deal results one at a time to avoid a single giant json.dumps string
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
        yield '}\n'

    def yield_timestamp_results(self, period, timestamps, include_uip, node, get_results_from_as_of=0, fields=[],
                                opp_ids=[], force_uip_and_hierarchy=False, allow_live=True, return_files_list=False,
                                return_chipotle_files_list=False):
        changed_deals = []
        yield '{\n'
        for x, timestamp in enumerate(timestamps):
            if x:
                yield ','
            yield '%s:\n' % json.dumps(str(timestamp))
            timestamp_data = deals_results_by_timestamp(period, [timestamp], include_uip=include_uip,
                                                        node=node,
                                                        get_results_from_as_of=get_results_from_as_of,
                                                        fields=fields, opp_ids=opp_ids,
                                                        changed_deals=changed_deals,
                                                        force_uip_and_hierarchy=force_uip_and_hierarchy,
                                                        allow_live=allow_live,
                                                        return_files_list=return_files_list,
                                                        return_chipotle_files_list=return_chipotle_files_list)[timestamp]
            # File-list modes return a plain list — small payload, serialize as before
            if not isinstance(timestamp_data, dict) or 'results' not in timestamp_data:
                yield '%s\n' % json.dumps(timestamp_data)
                continue
            # Stream deal results one at a time to avoid a single giant json.dumps string
            deal_results = timestamp_data['results']
            yield '{\n'
            for key, val in timestamp_data.items():
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
        yield '}\n'