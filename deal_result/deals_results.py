import json
from pymongo import MongoClient
from aviso.framework.views import AvisoView

from django.http.response import StreamingHttpResponse
from aviso.utils import is_none, is_true
from deal_result.result_Utils import deals_results_by_period, deals_results_by_timestamp
from utils.data_load_utils import get_drilldowns, get_dd_list
from data_load.tenants import     get_static_postgress_data
from tenants import ms_connection_strings
import logging
logger = logging.getLogger(__name__)

class DealsResults:
    '''
    Fetches the deal results. It can run in either two modes
        periods mode - you feed in a list of periods to get the latest results for
        timestamp mode - you feed a single period and a list of timestamps and it will grab the latest
            result that was available at that timestamp

    Parameters:
        period {list of str} - a list of period menmonics

    Optional Parameters:
        timestamp {list of int} - a list of epoch times to get the latest for, if using this parameter
            you have to specify a single period for all the timestamps

    Returns:
        A dict of results and relevant metadata for each period/timestamp including
            results - the deals results being requested
            cache_key - the cache key that the results come from
            timestamp - the asof of the results
            curr_q - if the period providied is the current quarter's results
    '''
    http_method_names = ['get', 'post']
    restrict_to_roles = {AvisoView.Role.Gnacker}
    def get(self,  periods,
            timestamps,
            get_results_from_as_of,
            fields,
            node,
            force_uip_and_hierarchy,
            include_uip,
            allow_live,
            return_files_list):
        periods=periods if isinstance(periods,list) else [periods]
        timestamps=timestamps if isinstance(timestamps,list) else [timestamps]
        get_results_from_as_of=get_results_from_as_of
        fields=fields
        node=node
        force_uip_and_hierarchy=force_uip_and_hierarchy
        include_uip=include_uip
        allow_live=allow_live
        return_files_list=return_files_list
        if  not timestamps:
            return StreamingHttpResponse(
                self.yield_period_results(periods, include_uip, node,  get_results_from_as_of=get_results_from_as_of,
                                          fields=fields, return_files_list=return_files_list),
                status=200, content_type='application/json')
        elif len(periods) == 1:
            return StreamingHttpResponse(
                self.yield_timestamp_results(periods[0], timestamps, include_uip, node,
                                             get_results_from_as_of=get_results_from_as_of, fields=fields,
                                             force_uip_and_hierarchy=force_uip_and_hierarchy, allow_live=allow_live,
                                             return_files_list=return_files_list),
                status=200, content_type='application/json')
        else:
            # TODO: I dont remember where error to raise
            raise

    def post(self, request, *args, **kwargs):
        periods = request.GET.getlist('period', [])
        body = json.load(request)
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
                self.yield_period_results(periods, include_uip, node,  get_results_from_as_of=get_results_from_as_of,
                                          fields=fields, opp_ids=opp_ids, return_files_list=return_files_list),
                status=200, content_type='application/json')
        elif len(periods) == 1:
            return StreamingHttpResponse(
                self.yield_timestamp_results(periods[0], timestamps, include_uip, node,
                                             get_results_from_as_of=get_results_from_as_of, fields=fields,
                                             opp_ids=opp_ids, force_uip_and_hierarchy=force_uip_and_hierarchy,
                                             allow_live=allow_live, return_files_list=return_files_list),
                status=200, content_type='application/json')
        else:
            # TODO: I dont remember where error to raise
            raise

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

def get_static_configs():
    data = get_static_postgress_data()
    common_model = data['models']['common']

    # Extract the requested fields using .get() for safety
    viewgen_config = common_model['config']['viewgen_config']
    uipfield = data['params']['general']['uipfield']
    win_score_config=common_model['config'].get('win_score_config',{})

    return viewgen_config, uipfield,win_score_config

def get_deals_results(periods, timestamps, get_results_from_as_of, fields, node, force_uip_and_hierarchy,include_uip,allow_live,return_files_list):
    service = DealsResults()
    data=service.get(
        periods=periods,
        timestamps=timestamps,
        get_results_from_as_of=get_results_from_as_of,
        fields=fields,
        node=node,
        force_uip_and_hierarchy=force_uip_and_hierarchy,
        include_uip=include_uip,
        allow_live=allow_live,
        return_files_list=return_files_list
    )
    for _ in data:
        pass

