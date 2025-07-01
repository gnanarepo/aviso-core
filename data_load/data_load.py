from datetime import datetime
import pytz
from aviso.settings import sec_context
from dateutil.tz import gettz

from json import loads
import collections
import logging
from copy import deepcopy
from domainmodel.datameta import Dataset
from utils.date_utils import epoch, current_period
from utils.result_utils import generate_subscription_same_dd_recs, generate_appannie_dummy_recs, \
    generate_subscription_recs, generate_expiration_date_renewal_rec, generate_expiration_date_renewal_rec_github, \
    generate_revenue_recs

logger = logging.getLogger('gnana.%s' % __name__)


class DataLoad:
    def __init__(self, id_list, from_timestamp=0):
        self.id_list = id_list
        self.from_timestamp = from_timestamp

    def get_basic_results(self):
        ds = Dataset.getByNameAndStage('OppDS', None)
        use_core_show = ds.models['common'].config.get('fastlane_config', {}).get('use_core_show')
        record_filter = ds.get_model_filter('bookings_rtfm')

        db = sec_context.tenant_db

        # Get Current Period
        coll = db[sec_context.name + '.Period._uip._data']
        records = list(coll.find({}, {'_id': 0}))
        print('Fetched data from Period collection in MongoDB for {}'.format(sec_context.name))
        timezone = 'US/Pacific'  # default timezone
        for record in records:
            if bool(record['object']['values'].get('TimeZoneSidKey')):
                timezone = loads(record['object']['values']['TimeZoneSidKey'])
                break
        current_date = datetime.now().astimezone(pytz.timezone(timezone)).strftime('%Y%m%d')

        for record in records:
            if bool(record['object']['values'].get('Type')):
                if loads(record['object']['values'].get('Type')).lower() == 'quarter':
                    start_date = loads(record['object']['values']['StartDate'])
                    end_date = loads(record['object']['values']['EndDate'])
                    if start_date <= current_date <= end_date:
                        boq = datetime.strptime(start_date, '%Y%m%d').replace(
                            tzinfo=gettz(timezone)).timestamp() / 86400 + 25569
                        eoq = datetime.strptime(end_date, '%Y%m%d').replace(
                            tzinfo=gettz(timezone)).timestamp() / 86400 + 25569 + 1
                        break

        # Fetch uipfields from OppDS Data
        coll = db[sec_context.name + '.OppDS._uip._data']
        criteria = {'last_modified_time': {'$gt': self.from_timestamp}} if self.from_timestamp else {
            'object.extid': {'$in': self.id_list}}
        deals = list(coll.find(criteria, {'_id': 0}))
        print('Fetched data from OppDS collection in MongoDB for {}'.format(sec_context.name))

        final_deals = []
        for deal in deals:
            if use_core_show:
                allow_deal = DataLoad.passes_record_filter(deal['object']['extid'], deal['object']['values'],
                                                  record_filter) and DataLoad.core_show(deal['object']['history'], boq, eoq)
            else:
                allow_deal = DataLoad.is_active(deal['object']['history'], boq) and DataLoad.passes_record_filter(deal['object']['extid'],
                                                                                                deal['object'][
                                                                                                    'values'],
                                                                                                record_filter)
            if allow_deal:
                final_deals.append(deal)
            else:
                print(
                    'Skipping deal {} for {} due to record filter'.format(deal['object']['extid'], sec_context.name))

        return final_deals

    @staticmethod
    def passes_record_filter(opp_id, data, record_filter):
        if not record_filter:
            return True
        from json import loads
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
            # if filter_type == 'range_in':
            # if filter_type == 'range_not_in':
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
        time_now_xl = (time() / 86400) + 25569
        close_date_data = data.get('CloseDate_adj', [])
        if not close_date_data:
            return False
        close_date_now = data['CloseDate_adj'][-1][1]
        stage_trans_adj_now = data.get('StageTrans_adj', [[0, 'N/A']])[-1][1]
        stage_trans_adj_boq = DataLoad.getasof(data.get('StageTrans_adj', [[0, 'N/A']]), boq)
        stage_trans_now = data.get('StageTrans', [[0, 'N/A']])[-1][1]
        terminal_fate_now = DataLoad.getasof(data.get('terminal_fate', [[0, 'N/A']]), time_now_xl)
        core_asof_unborn = (stage_trans_adj_now == 'N/A') and (stage_trans_now == 'N/A')
        core_latest_won = terminal_fate_now == 'W'
        core_asof_won = (stage_trans_adj_now == u'99') and core_latest_won
        core_begin_won = (stage_trans_adj_boq == u'99') and core_latest_won
        a = (stage_trans_adj_now == '99') or (stage_trans_now == '99')
        b = close_date_now <= eoq
        c = core_latest_won
        d = core_asof_won
        e = abs(eoq - time_now_xl) > 1000
        core_asof_unadj_won = (a and b and c and e) or (d and not e)
        core_asof_lost = (stage_trans_adj_now == '-1') and not core_asof_unadj_won
        core_begin_lost = stage_trans_adj_boq == '-1'
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
        segment_config = oppds.models['common'].config.get('segment_config', {})
        drilldown = rev_schedule_config.get('drilldown', 'Revenue')
        close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
        filt_config = rev_schedule_config.get('filt_config', {})
        rev_schedule_field = rev_schedule_config.get('rev_schedule_field', 'RevSchedule')
        if rev_schedule_config.get('prd_rev_schedule', False):
            # convert basic_results to dict format
            basic_results_dict = self.get_results_dict(basic_results)
            basic_results_dict = self.rev_schedule_by_period(rev_schedule_field, basic_results_dict)
            rev_period = self.period if self.period else current_period(a_datetime=epoch().as_datetime()).mnemonic
            logger.info('fetching records for period: %s', rev_period)
            if rev_schedule_config.get('prd_subs_schedule_same_dd', False):
                if not isinstance(drilldown, list):
                    drilldown = [drilldown]
                basic_results_dict = generate_subscription_same_dd_recs(rev_period, drilldown, close_date_fld,
                                                                        basic_results_dict, filt_config)
            elif rev_schedule_config.get('appannie_prd_rev_schedule', False):
                basic_results_dict_copy = deepcopy(basic_results_dict)
                basic_results_dict = {}
                for opp_id, res in basic_results_dict_copy.items():
                    output_dict = generate_appannie_dummy_recs(rev_period, rev_schedule_config, opp_id, res)
                    basic_results_dict.update(output_dict)
            elif rev_schedule_config.get('prd_subs_schedule', False):
                basic_results_dict = generate_subscription_recs(rev_period, drilldown, close_date_fld,
                                                                basic_results_dict)
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
            elif rev_schedule_config.get('github_metered_billing', False):
                renewal_drilldown = rev_schedule_config.get('renewal_drilldown', 'Renewal')
                close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
                expiration_date_fld = rev_schedule_config.get('expiration_date_fld', 'ExpirationDate')
                type_fld = rev_schedule_config.get('type_fld', 'Type')
                renewal_vals = rev_schedule_config.get('renewal_vals', ['Renewal'])
                basic_results_dict_copy = deepcopy(basic_results_dict)
                basic_results_dict = {}
                for opp_id, res in basic_results_dict_copy.items():
                    output_dict = generate_expiration_date_renewal_rec_github(rev_period, renewal_drilldown,
                                                                              close_date_fld,
                                                                              expiration_date_fld, type_fld,
                                                                              renewal_vals, segment_config,
                                                                              rev_schedule_config,
                                                                              opp_id, res)
                    basic_results_dict.update(output_dict)
            else:
                basic_results_dict = generate_revenue_recs(rev_period, drilldown, close_date_fld, basic_results_dict)

            # convert basic_results to list format
            basic_results = self.get_results_list(basic_results_dict)
        return basic_results

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
