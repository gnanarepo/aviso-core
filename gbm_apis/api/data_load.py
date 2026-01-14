import os

from aviso.settings import sec_context
from aviso.utils.dateUtils import TimeHorizon

import logging
import collections
from abc import ABC, abstractmethod
from json import loads
from copy import deepcopy

from django.conf import settings
from django.http import JsonResponse, HttpResponse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from pymongo import MongoClient

from gbm_apis.data_load.tenants import ms_connection_strings
from gbm_apis.domainmodel.datameta import Dataset
from gbm_apis.framework.baseView import AvisoView
from gbm_apis.framework.mixins import AvisoCompatibilityMixin
from utils.date_utils import epoch, current_period
from utils.misc_utils import prune_pfx
from utils.result_utils import generate_appannie_dummy_recs, generate_expiration_date_renewal_rec, generate_revenue_recs
from utils.data_load_utils import get_drilldowns, get_dd_list


logger = logging.getLogger('gnana.%s' % __name__)



class CriteriaBuilder(ABC):
    def __init__(self, data_load, boq, eoq):
        self.data_load = data_load
        self.boq = boq
        self.eoq = eoq

    @abstractmethod
    def get_criteria(self):
        pass


class ChipotleCriteriaBuilder(CriteriaBuilder):
    def get_criteria(self):
        if self.data_load.from_timestamp:
            return {'last_modified_time': {'$gte': self.data_load.from_timestamp}}
        return {'object.extid': {'$in': self.data_load.id_list}}

class CurrentQuarterCriteriaBuilder(CriteriaBuilder):
    def get_criteria(self):
        return {'terminal_date': {'$gte': self.boq}}

class PastQuarterCriteriaBuilder(CriteriaBuilder):
    def get_criteria(self):
        return {
            'terminal_date': {'$gte': self.boq},
            'created_date': {'$lte': self.eoq}
        }



class DataLoad:
    def __init__(
            self,
            id_list,
            tenant_name,
            stack,
            gbm_stack,
            pod,
            etl_stack,
            period,
            run_type='chipotle',
            from_timestamp=0,
            changed_fields_only=False):
        self.id_list = id_list
        self.from_timestamp = from_timestamp
        self.tenant_name = tenant_name
        self.stack = stack
        self.gbm_stack = gbm_stack
        self.pod = pod
        self.etl_stack = etl_stack
        self.changed_fields_only = changed_fields_only
        self.run_type = run_type
        self.period = period

    def get_basic_results(self):
        ds = Dataset.getByNameAndStage('OppDS', None)
        use_core_show = ds.models['common'].config.get('fastlane_config', {}).get('use_core_show')
        viewgen_config = ds.models['common'].config.get('viewgen_config', {})

        th = TimeHorizon()
        boq = epoch(th.beginsF).as_xldate()
        eoq = epoch(th.horizonF).as_xldate()

        uipfield = ds.params['general']['uipfield']
        record_filter = ds.get_model_filter('bookings_rtfm')
        drilldowns = get_drilldowns(self.tenant_name, self.stack, viewgen_config)
        ms_connection_string = ms_connection_strings(self.pod)
        print('Connecting to MongoDB for {} {}'.format(self.tenant_name, self.pod))
        client = MongoClient(ms_connection_string)
        db = client[self.tenant_name.split('.')[0] + '_db_' + self.etl_stack]

        # Fetch uipfields from OppDS Data
        coll = db[sec_context.name + '.OppDS._uip._data']
        criteria_builder = self._get_criteria_strategy(boq=boq, eoq=eoq)
        criteria = criteria_builder.get_criteria()
        deals = list(coll.find(criteria, {'_id': 0}))

        print(f"got result length of deals: {len(deals)}")

        print('Fetched data from OppDS collection in MongoDB for {}'.format(sec_context.name))

        final_deals = []
        from_timestamp_xl = (self.from_timestamp / 1000.0) / 86400 + 25569
        for deal in deals:
            if use_core_show:
                allow_deal = DataLoad.passes_record_filter(deal['object']['extid'], deal['object']['values'],
                                                  record_filter) and DataLoad.core_show(deal['object']['history'], boq, eoq)
            else:
                allow_deal = DataLoad.is_active(deal['object']['history'], boq) and DataLoad.passes_record_filter(deal['object']['extid'],
                                                                                                deal['object'][
                                                                                                    'values'],
                                                                                                record_filter)

            temp = {'extid': deal['object']['extid']}
            temp['is_delete'] = False if allow_deal else True

            values = deal['object']['values']
            if self.from_timestamp and self.changed_fields_only:
                history = deal['object']['history']
                for fld in uipfield:
                    value = history.get(prune_pfx(fld), [[0.0, 'N/A']])
                    if value[-1][0] > from_timestamp_xl:
                        temp[fld] = value[-1][1]
            else:
                for fld in uipfield:
                    value = values.get(prune_pfx(fld), 'N/A')
                    try:
                        temp[fld] = loads(value)
                    except:
                        temp[fld] = value
            print('Computing drilldowns for {} {}'.format(deal['object']['extid'], self.tenant_name))
            drilldown_list, split_fields = get_dd_list(viewgen_config, values, drilldowns, True)
            print('Computed drilldowns for {} {}'.format(deal['object']['extid'], self.tenant_name))
            temp['__segs'] = drilldown_list
            if split_fields:
                for fld, val in split_fields.items():
                    if self.from_timestamp and self.changed_fields_only:
                        if fld in temp:
                            temp[fld] = val
                    else:
                        temp[fld] = val
            match temp.get('terminal_fate'):
                case 'W':
                    temp['win_prob'] = 1
                case 'L':
                    temp['win_prob'] = 0
            final_deals.append(temp)

        return final_deals

    def _get_criteria_strategy(self, boq, eoq):
        if self.run_type == 'chipotle':
            return ChipotleCriteriaBuilder(self, boq=boq, eoq=eoq)

        elif self.period  and self.run_type == 'current':
            return CurrentQuarterCriteriaBuilder(self, boq=boq, eoq=eoq)

        elif self.period and self.run_type == 'historic':
            return PastQuarterCriteriaBuilder(self, boq=boq, eoq=eoq)

    @staticmethod
    def passes_record_filter(opp_id, data, record_filter):
        if not record_filter:
            return True
        import re
        for filter_expr, values in record_filter.items():
            if filter_expr[-1:] != ')':
                filter_type, feature = 'in', filter_expr
            else:
                filter_type, feature = filter_expr[0:-1].split('(')
            feature = feature.split(',')[0]
            if filter_type == 'in':
                if feature in data:
                    if loads(data[feature]) not in values:
                        return False
                else:
                    return False
            if filter_type == 'not_in':
                if feature in data and loads(data[feature]) in values:
                    return False
            if filter_type == 'exclude_ids':
                if opp_id in values:
                    return False
            if filter_type == 'include_ids':
                if opp_id not in values:
                    return False
            if filter_type == 'matches':
                for x in values:
                    if not re.match(x, str(loads(data[feature]))):
                        return False
            if filter_type == 'not_matches':
                for x in values:
                    if not re.match(x, str(loads(data[feature]))):
                        return False
        return True

    @staticmethod
    def is_active(data, boq):
        stage_data = data.get('StageTrans_adj', [])
        if not stage_data:
            return False
        stage_at_start = DataLoad.getasof(stage_data, boq)
        stage_now = stage_data[-1][1]
        if stage_now == 'N/A':
            return False
        terminal_fate_data = data.get('terminal_fate', [])
        if terminal_fate_data:
            terminal_date = terminal_fate_data[-1][0]
            terminal_fate = terminal_fate_data[-1][1]
            if terminal_fate == 'W':
                if stage_at_start in '99' and terminal_date <= boq:
                    return False
            if terminal_fate == 'L':
                if stage_at_start in '-1' and stage_now in '-1':
                    return False
        return True

    @staticmethod
    def core_show(data, boq, eoq):
        from time import time
        time_now_xl = (time() / 86400) + 25569  # current as-of date in Excel format

        close_date_data = data.get('CloseDate_adj', [])
        if not close_date_data:
            return False

        # Correct: use getasof instead of last entry
        close_date_now = DataLoad.getasof(close_date_data, time_now_xl)

        stage_trans_adj_now = DataLoad.getasof(data.get('StageTrans_adj', [[0, 'N/A']]), time_now_xl)
        stage_trans_adj_boq = DataLoad.getasof(data.get('StageTrans_adj', [[0, 'N/A']]), boq)
        stage_trans_now = DataLoad.getasof(data.get('StageTrans', [[0, 'N/A']]), time_now_xl)
        terminal_fate_now = DataLoad.getasof(data.get('terminal_fate', [[0, 'N/A']]), time_now_xl)

        core_asof_unborn = (stage_trans_adj_now == 'N/A') and (stage_trans_now == 'N/A')
        core_latest_won = terminal_fate_now == 'W'
        core_asof_won = (stage_trans_adj_now == '99') and core_latest_won
        core_begin_won = (stage_trans_adj_boq == '99') and core_latest_won

        a = (stage_trans_adj_now == '99') or (stage_trans_now == '99')
        b = close_date_now <= eoq
        c = core_latest_won
        d = core_asof_won
        e = abs(eoq - time_now_xl) > 1000
        core_asof_unadj_won = (a and b and c and e) or (d and not e)

        core_asof_lost = (stage_trans_adj_now == '-1') and not core_asof_unadj_won
        core_begin_lost = (stage_trans_adj_boq == '-1')
        f = close_date_now != boq
        core_asof_not_won = not (f and (core_asof_won and core_begin_won))
        core_asof_not_lost = not (f and (core_asof_lost and core_begin_lost))

        return core_asof_not_won and core_asof_not_lost and not core_asof_unborn

    @classmethod
    def getasof(cls, history, date):
        try:
            sliced = [x for x in history if x[0] <= date]
            return sliced[-1][1]
        except:
            return 'N/A'


class RevenueSchedule:

    def __init__(self, period, basic_results):
        self.period = period
        self.basic_results = basic_results

    def revenue_schedule(self):
        basic_results = self.basic_results
        oppds = Dataset.getByNameAndStage(name='OppDS')
        rev_schedule_config = oppds.models['common'].config.get('rev_schedule_config', {})
        drilldown = rev_schedule_config.get('drilldown', 'Revenue')
        close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
        rev_schedule_field = rev_schedule_config.get('rev_schedule_field', 'RevSchedule')
        if rev_schedule_config.get('prd_rev_schedule', False):
            # convert basic_results to dict format
            extids_before_rev_schedule = [record['extid'] for record in basic_results]
            basic_results_dict = self.get_results_dict(basic_results)
            basic_results_dict = self.rev_schedule_by_period(rev_schedule_field, basic_results_dict)
            rev_period = self.period if self.period else current_period(a_datetime=epoch().as_datetime()).mnemonic
            logger.info('fetching records for period: %s', rev_period)
            if rev_schedule_config.get('appannie_prd_rev_schedule', False):
                basic_results_dict_copy = deepcopy(basic_results_dict)
                basic_results_dict = {}
                for opp_id, res in basic_results_dict_copy.items():
                    output_dict = generate_appannie_dummy_recs(rev_period, rev_schedule_config, opp_id, res)
                    basic_results_dict.update(output_dict)
            elif rev_schedule_config.get('expiration_date_renewals_rec', False):
                renewal_drilldown = rev_schedule_config.get('renewal_drilldown', 'Renewal')
                close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
                expiration_date_fld = rev_schedule_config.get('expiration_date_fld', 'ExpirationDate')
                type_fld = rev_schedule_config.get('type_fld', 'Type')
                renewal_vals = rev_schedule_config.get('renewal_vals', ['Renewal'])
                basic_results_dict_copy = deepcopy(basic_results_dict)
                basic_results_dict = {}
                for opp_id, res in basic_results_dict_copy.items():
                    output_dict = generate_expiration_date_renewal_rec(rev_period, renewal_drilldown, close_date_fld,
                                                                       expiration_date_fld, type_fld, renewal_vals,
                                                                       opp_id, res)
                    basic_results_dict.update(output_dict)
            else:
                basic_results_dict = generate_revenue_recs(rev_period, drilldown, close_date_fld, basic_results_dict)

            # convert basic_results to list format
            basic_results = self.get_results_list(basic_results_dict)
            extids_in_basic_results = [record['extid'] for record in basic_results]
            exluded_extids = [extid for extid in extids_before_rev_schedule if extid not in extids_in_basic_results]
            for opp_id in exluded_extids:
                basic_results.append({'extid': opp_id,
                                      'is_delete': True})

        if basic_results:
            return basic_results
        return {'error': 'No records returned from etl OppDS for given criteria'}
    def get_results_dict(self, res_list):
        ret_val = {}
        for rec in res_list:
            ret_val[rec.pop('extid')] = rec
        return ret_val

    def get_results_list(self, res_dict):
        ret_val = []
        for opp_id, rec in res_dict.items():
            rec['extid'] = opp_id
            ret_val.append(rec)
        return ret_val

    def rev_schedule_by_period(self, rev_schedule_field, basic_results_dict):
        for deal_id, fld_val in basic_results_dict.items():
            rev_schedule = basic_results_dict[deal_id].get(rev_schedule_field, {})
            if rev_schedule in ['N/A', 0.0]:
                rev_schedule = {}
            basic_results_dict[deal_id]['as_of_raw_rev_schedule_amounts'], basic_results_dict[deal_id][
                'as_of_raw_rev_schedule_dates'] = \
                self.get_rev_schedule(rev_schedule)
        return basic_results_dict

    def get_rev_schedule(self, rev_schedule):
        if not rev_schedule:
            return {}, {}
        rev_schedule_prd = collections.defaultdict(int)
        rev_schedule_prd_date = {}
        for ts, amt in rev_schedule.items():
            prd = current_period(a_datetime=epoch(float(ts)).as_datetime()).mnemonic
            rev_schedule_prd[prd] += amt
            if prd not in rev_schedule_prd_date:
                rev_schedule_prd_date[prd] = float(ts)
            else:
                rev_schedule_prd_date[prd] = max(rev_schedule_prd_date[prd], float(ts))
        return dict(rev_schedule_prd), rev_schedule_prd_date


@method_decorator(csrf_exempt, name='dispatch')
class DataLoadAPIView(AvisoCompatibilityMixin, AvisoView):
    """
    Django API View to trigger DataLoad and RevenueSchedule via GET request.
    Converted to AvisoView for consistency.
    """

    http_method_names = ['get']
    restrict_to_roles = {AvisoView.Role.Gnacker}
    as_json = False

    def get(self, request, *args, **kwargs):
        try:
            tenant_name = request.headers.get("X-Tenant-Name") or os.environ.get('TENANT_NAME')
            stack = os.environ.get('STACK')
            gbm_stack = os.environ.get('GBM_STACK')
            pod = os.environ.get('POD')
            etl_stack = os.environ.get('ETL_STACK')
            period = request.GET.get('period')
            run_type = request.GET.get('run_type', 'chipotle')

            id_list_raw = request.GET.get('id_list', '')
            if id_list_raw:
                id_list = [x.strip() for x in id_list_raw.split(',') if x.strip()]
            else:
                id_list = []

            try:
                from_timestamp = int(request.GET.get('from_timestamp', 0))
            except ValueError:
                from_timestamp = 0

            changed_fields_param = request.GET.get('changed_fields_only', 'false')
            changed_fields_only = changed_fields_param.lower() == 'true'

            if not all([tenant_name, stack, pod, etl_stack]):
                return JsonResponse(
                    {"error": "Missing required fields (tenant_name, stack, pod, etl_stack)"},
                    status=400
                )

            logger.info(f"Starting API process for {tenant_name}")

            # 4. Initialize DataLoad
            loader = DataLoad(
                id_list=id_list,
                tenant_name=tenant_name,
                stack=stack,
                gbm_stack=gbm_stack,
                pod=pod,
                etl_stack=etl_stack,
                period=period,
                run_type=run_type,
                from_timestamp=from_timestamp,
                changed_fields_only=changed_fields_only
            )

            # 5. Get Basic Results
            basic_results = loader.get_basic_results()

            # 6. Initialize RevenueSchedule
            scheduler = RevenueSchedule(period=period, basic_results=basic_results)
            final_results = scheduler.revenue_schedule()

            # 7. Return Response
            return JsonResponse({
                "status": "success",
                "count": len(final_results) if isinstance(final_results, list) else 0,
                "data": final_results
            }, status=200)

        except Exception as e:
            logger.error(f"API Error: {str(e)}", exc_info=True)
            return JsonResponse(
                {"status": "error", "message": str(e)},
                status=500
            )