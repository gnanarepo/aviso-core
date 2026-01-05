import json
import logging
from django.views import View
from django.http.response import StreamingHttpResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from aviso.utils import is_none, is_true
from deal_result.result_Utils import deals_results_by_period, deals_results_by_timestamp

logger = logging.getLogger(__name__)


@method_decorator(csrf_exempt, name='dispatch')
class DealsResultsAPIView(View):


    def get(self, request, *args, **kwargs):
        periods = request.GET.getlist('period', [])
        timestamps = [int(x) for x in request.GET.getlist('timestamp', [])]
        get_results_from_as_of = int(request.GET.get('get_results_from_as_of', 0))
        node = request.GET.get('node')

        include_uip = is_true(request.GET.get('include_uip', True))
        force_uip_and_hierarchy = is_true(request.GET.get('force_uip_and_hierarchy', False))
        allow_live = is_true(request.GET.get('allow_live', True))
        return_files_list = is_true(request.GET.get('return_files_list', False))

        fields = request.GET.getlist('fields', [])

        if not timestamps:
            return StreamingHttpResponse(
                self.yield_period_results(periods, include_uip, node,
                                          get_results_from_as_of=get_results_from_as_of,
                                          fields=fields,
                                          return_files_list=return_files_list),
                status=200, content_type='application/json')
        elif len(periods) == 1:
            return StreamingHttpResponse(
                self.yield_timestamp_results(periods[0], timestamps, include_uip, node,
                                             get_results_from_as_of=get_results_from_as_of, fields=fields,
                                             force_uip_and_hierarchy=force_uip_and_hierarchy, allow_live=allow_live,
                                             return_files_list=return_files_list),
                status=200, content_type='application/json')
        else:
            # Matches your TODO
            raise Exception("Invalid Request: Multiple periods provided with timestamps.")

    def post(self, request, *args, **kwargs):
        # 1. Parse Parameters (Copied from your original POST logic)
        periods = request.GET.getlist('period', [])

        try:
            body = json.loads(request.body)
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
            # Matches your TODO
            raise Exception("Invalid Request: Multiple periods provided with timestamps.")

    def yield_period_results(self, periods, include_uip, node, get_results_from_as_of=0, fields=[], opp_ids=[],
                             return_files_list=False):
        yield '{\n'
        for x, period in enumerate(periods):
            if x:
                yield ','
            yield '%s:\n' % json.dumps(period)
            yield '%s\n' % json.dumps(deals_results_by_period([period], include_uip=include_uip, node=node,
                                                              get_results_from_as_of=get_results_from_as_of,
                                                              fields=fields,
                                                              return_files_list=return_files_list)[period])
        yield '}\n'

    def yield_timestamp_results(self, period, timestamps, include_uip, node, get_results_from_as_of=0, fields=[],
                                opp_ids=[], force_uip_and_hierarchy=False, allow_live=True, return_files_list=False):
        changed_deals = []
        yield '{\n'
        for x, timestamp in enumerate(timestamps):
            if x:
                yield ','
            yield '%s:\n' % json.dumps(str(timestamp))
            yield '%s\n' % json.dumps(deals_results_by_timestamp(period, [timestamp], include_uip=include_uip,
                                                                 node=node,
                                                                 get_results_from_as_of=get_results_from_as_of,
                                                                 fields=fields, opp_ids=opp_ids,
                                                                 changed_deals=changed_deals,
                                                                 force_uip_and_hierarchy=force_uip_and_hierarchy,
                                                                 allow_live=allow_live,
                                                                 return_files_list=return_files_list)[timestamp])
        yield '}\n'
