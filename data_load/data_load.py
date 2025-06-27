import collections
import json
import logging
from copy import deepcopy

from aviso.settings import sec_context
from aviso.utils.dateUtils import TimeHorizon
from django.http import HttpResponseBadRequest

from domainmodel.datameta import Dataset
from utils.date_utils import epoch, current_period
from utils.misc_utils import prune_pfx
from utils.result_utils import generate_subscription_same_dd_recs, generate_appannie_dummy_recs, \
    generate_subscription_recs, generate_expiration_date_renewal_rec, generate_expiration_date_renewal_rec_github, \
    generate_revenue_recs
from .viewgen_service import CoolerViewGeneratorService

logger = logging.getLogger('gnana.%s' % __name__)


class DataLoad:
    def __init__(self, period, id_list, from_timestamp=0):
        self.period = period
        self.id_list = id_list
        self.from_timestamp = from_timestamp

    def fetch_records(self):
        # In case of hier only drilldowns, this method constructs various fields like amount, ratios etc.

        if (not self.period or not self.from_timestamp) and not self.id_list:
            return HttpResponseBadRequest('Need to provide period and from_timestamp arguments or id_list')

        ds = Dataset.getByNameAndStage('OppDS', None)
        uips = ds.params['general']['uipfield']
        use_core_show = ds.models['common'].config.get('fastlane_config', {}).get('use_core_show')
        dt = epoch().as_datetime()
        gen = CoolerViewGeneratorService(perspective=None, hier_asof=dt)

        th = TimeHorizon()
        self.model_inst = ds.get_model_instance('bookings_rtfm', th, [])

        criteria = {'last_modified_time': {'$gt': self.from_timestamp}} if self.from_timestamp else {
            'object.extid': {'$in': self.id_list}}
        rec_filter = ds.get_model_filter('bookings_rtfm')
        fieldmap = {}
        for f in uips:
            oppds_field = prune_pfx(f)
            fieldmap.update({f: oppds_field})

        filter_related_fields = [filter_expr if filter_expr[-1:] != ')' else filter_expr[0:-1].split('(')[1]
                                 for filter_expr, values in rec_filter.items()]

        act_filter_related_fields = [
            field if "use_value" not in field else field.split(',')[0] if "use_value" not in field.split(',')[0] \
                else field.split(',')[1] for field in filter_related_fields]

        logger.info("Getting the prepared records. Criteria : %s ", (criteria))
        prepared_records = sec_context.etl.uip('UIPIterator', dataset='OppDS', record_filter=criteria,
                                               fields_requested=list(fieldmap.values()) + [
                                                   self.model_inst.stage_field_name] + act_filter_related_fields)
        basic_results = []

        for record in prepared_records:
            logger.info("The featMap of deal {} is {}".format(record.ID, record.featMap))
            filter_status = record.passes_filterF(rec_filter, epoch().as_epoch())
            model_prep = self.model_inst.prepare(record)
            try:
                if model_prep:
                    field_list = (model_prep or [])
                    record.compress(field_list)
            except:
                logger.error('Unable to prepare record %s', record.ID)
                if self.model_inst.raise_prepare_errors:
                    raise
                else:
                    continue

            if use_core_show:
                allow_deal = filter_status[0] and self.core_show(record)
            else:
                allow_deal = self.is_active(record) and filter_status[0]

            if allow_deal:
                result_record = {}
                oppds_record = {}
                record.encode(oppds_record)
                oppds_record = oppds_record['values']
                for f, oppds_field in fieldmap.items():
                    result_record[f] = json.loads(oppds_record.get(oppds_field, '"N/A"'))
                flat_rec = {'res': {}, 'uip': result_record}
                result_record = gen.get_views(flat_rec)
                result_record['extid'] = record.ID
                basic_results.append(result_record)
            else:
                if use_core_show:
                    logger.info(
                        "The deal {} is filtered out : filter_status[0] is {} and self.core_show(record) is {}".format(
                            record.ID, filter_status[0], self.core_show(record)))
                else:
                    logger.info(
                        "The deal {} is filtered out : filter_status[0] is {} and self.is_active(record) is {}".format(
                            record.ID, filter_status[0], self.is_active(record)))

        return basic_results


    def is_active(self, record):
        """
        Same as the is_active method in forecast-base model, just without the is_closed condition.
        Always make sure these two are the same

        :param record:
        :return:
        """

        stage_at_start = record.getAsOfDateF(self.model_inst.stage_field_name,
                                             self.model_inst.time_horizon.beginsF)
        stage_now = self.model_inst.forecast_params.get("stage_override",
                                                        record.getAsOfDateF(self.model_inst.stage_field_name,
                                                                            self.model_inst.time_horizon.as_ofF))
        if stage_now == 'N/A':
            return False

        if stage_at_start in self.model_inst.win_stages and record.win_date and record.win_date <= self.model_inst.time_horizon.beginsF:
            return False

        if stage_at_start in self.model_inst.lose_stages and stage_now in self.model_inst.lose_stages:
            return False

        return True

    def core_show(self, record):
        if record.getAsOfDateF('CloseDate_adj', self.model_inst.time_horizon.as_ofF) == 'N/A':
            return False
        core_asof_unborn = (record.getAsOfDateF('StageTrans_adj', self.model_inst.time_horizon.as_ofF) == 'N/A') and (
                record.getAsOfDateF('StageTrans', self.model_inst.time_horizon.as_ofF) == 'N/A')
        # core_begin_unborn = record.getAsOfDateF('StageTrans_adj',self.model_inst.time_horizon.beginsF) == 'N/A'
        core_latest_won = record.getAsOfDateF('terminal_fate', self.model_inst.time_horizon.as_ofF) == 'W'
        core_asof_won = (record.getAsOfDateF('StageTrans_adj',
                                             self.model_inst.time_horizon.as_ofF) == u'99') and core_latest_won
        core_begin_won = (record.getAsOfDateF('StageTrans_adj',
                                              self.model_inst.time_horizon.beginsF) == u'99') and core_latest_won
        # core_horizon_won = (record.getAsOfDateF('StageTrans_adj', self.model_inst.time_horizon.horizonF) == u'99') and core_latest_won
        # core_won_event = core_horizon_won and not core_begin_won

        a = (record.getAsOfDateF('StageTrans_adj', self.model_inst.time_horizon.as_ofF) == '99') or (
                record.getAsOfDateF('StageTrans', self.model_inst.time_horizon.as_ofF) == '99')
        b = record.getAsOfDateF('CloseDate_adj', self.model_inst.time_horizon.as_ofF) <= epoch(
            self.model_inst.time_horizon.horizonF).as_xldate()
        c = core_latest_won
        d = core_asof_won
        e = abs(epoch(self.model_inst.time_horizon.horizonF).as_xldate() - epoch(
            self.model_inst.time_horizon.as_ofF).as_xldate()) > 1000
        core_asof_unadj_won = (a and b and c and e) or (d and not e)

        core_asof_lost = (record.getAsOfDateF('StageTrans_adj',
                                              self.model_inst.time_horizon.as_ofF) == '-1') and not core_asof_unadj_won
        core_begin_lost = record.getAsOfDateF('StageTrans_adj', self.model_inst.time_horizon.beginsF) == '-1'
        f = record.getAsOfDateF('CloseDate_adj', self.model_inst.time_horizon.as_ofF) != epoch(
            self.model_inst.time_horizon.beginsF).as_xldate()
        core_asof_not_won = not (f and (core_asof_won and core_begin_won))
        core_asof_not_lost = not (f and (core_asof_lost and core_begin_lost))
        # core_existingpipe = (not core_begin_won and not core_begin_lost) and ((self.model_inst.time_horizon.beginsF > self.model_inst.time_horizon.as_ofF) or not core_begin_unborn)
        core_show = core_asof_not_won and core_asof_not_lost and not core_asof_unborn

        return core_show


class RevenueSchedule:

    def __init__(self, period):
        self.period = period

    def revenue_schedule(self):
        basic_results = []
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
