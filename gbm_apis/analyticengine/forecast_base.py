import logging
import numpy as np

from aviso.utils import mathUtils
from aviso.utils.dateUtils import  epoch
from aviso.utils.dateUtils import datetime2xl, current_period, get_all_periods, get_all_periods_array, datestr2xldate, epoch2xl
from gbm_apis.deal_result.splitter_service import ViewGeneratorService

# from ..analyticengine import AnalysisModel
from .Utils import shift_date





# DO NOT REMOVE THE BELOW UNUSED IMPORTS, THEY ARE REQUIRED FOR EVAL



logger = logging.getLogger('gnana.%s' % __name__)




UNSUPPORTED_CYCLE_LENGTH = 'For now only cycle length of quarters is supported'

forecast_defaults = {
    "cycle_length": "Q",
    "num_cycles": 18,
    "num_cycles_unborn": 4,
    "decay": 0.00300451,
    "small_kernel_weight": 0.0001,
    "max_kernel_weight": 1.0,
    "small_num_neighbors": 25,
    "small_num_neighbors_ratio": 0.25,
    "owner_diff_dist": 0.25,  # soon deprecated
    "days_in_stage_bw": 1.0,
    "days_to_close_dist_add": 0.25,
    "days_to_close_bw": 91.0,
    "quarter_start_delay": 0.0,
    "quarter_horizon_delay": 0.0,
    "acv_bw": 1.0,  # soon deprecated
    "seasonality": [1],
    "close_date_updated_beyond_horizon_implies_forecast_zero": False,
    "close_date_max_days_past_eoq": -1,
    "win_date_basis": "close_date",
    "remove_latest_data": False,
    "remove_latest_data_from": None,
    "remove_latest_data_field_exceptions": None,
    "remove_latest_data_fields": None,
    "num_neighbor_details": 0,
    "run_forecast": True,
    "tiny": mathUtils.tiny,
    "eacv_model": 'default',  # possible values: 'default', 'acv', 'qntl_krnl_reg'
    "acv_pctl_krnl_bw": 0.0,  # just match input_pctl=output_pctl
    "separate_zero_acv_for_eacv_model": False,
    "eACV_for_zero_acv_diff": False,
    # the following filter stuff is deprecated-- do not use
    "apply_filters": False,
    "filter_active_acv_lo": -50000000,
    "filter_active_acv_hi": 50000000,
    "filter_close_acv_lo": -50000000,
    "filter_close_acv_hi": 50000000,
    "filter_begin_acv_lo": -50000000,
    "filter_begin_acv_hi": 50000000,
    "win_date_from_uip": True,  # using this as False is deprecated
    "terminal_fate_field": "terminal_fate",
    "output_unconditional_probs": False,
    "calib_with_vectors": False,
    "ignore_latest_observations": 0,
}


class AnalysisModel(object):


    """ Base class for all analysis models

    All implementation class must define some metadata
    allow_aggregation: Dimensions will be honored by this model
    predicts: Produces future values.  Horizon is important.
    compares_with_history: Compares each record with history
    produce_individual_result: If True, stores individual records;
                               If False, stores group results.

    .. graphviz::

        digraph "Known Models" {
            rankdir = BT
            node [shape=record]
            ForecastBaseModel -> AnalysisModel
            ForecastModel -> ForecastBaseModel
            Forecast2Model -> ForecastBaseModel
            UnbornModel -> ForecastBaseModel
            RecordCounter -> AnalysisModel
        }

    """
    combine_options = []
    ignore_closure_errors = False

    def __init__(self, field_map, config, time_horizon, dimensions):

        self.cache = {}
        self.probe = False
        self.dimensions = dimensions or []
        self.time_horizon = time_horizon
        self.config = config
        self.require_post_process = False
        self.splitter_config = config.get('splitter_config', {}).copy()
        self.splitter_config["uip_only"] = True
        self.drilldown_config = config.get('ds_params', {}).get('general', {}).get("drilldown_config", {})
        self.drilldown_config["drilldowns"] = self.dimensions

        hs_impl = self.drilldown_config.get('hs_impl', 'G')
        if hs_impl == 'A':
            from gbm_apis.deal_result.viewgen_service import CoolerViewGeneratorService
            self.viewgen_svc = CoolerViewGeneratorService(hier_asof=self.time_horizon.as_ofF)
        else:
            self.viewgen_svc = ViewGeneratorService(
                drilldown_config=self.drilldown_config,
                splitter_config=self.splitter_config,
                base_ts=self.time_horizon.as_ofF,
            )
            self.drilldown_flds = self.viewgen_svc.hier_svc.component_fields
            self.level_groupby_flds = self.viewgen_svc.hier_svc.level_groupby_flds


    def cached_or_not(self, extid, observation_time, name, fn):
        try:
            return self.cache[(extid, observation_time, name,)]
        except KeyError:
            val = fn()
            self.cache[(extid, observation_time, name,)] = val
            return val

    # Following methods are created to assist in development of new
    # models with the right signature
    def is_active(self, record):
        """ Called by the framework to decide if the record is considered
        active by the model
        """
        raise NotImplementedError()

    def is_history(self, record):
        """ Called by the framework to decide if the record is considered
        historical by the model
        """
        raise NotImplementedError()

    def is_closed(self, record):
        """ Called by the framework to decide if the record is closed
        in the current period
        """
        raise NotImplementedError()

    def is_future(self, record):
        """ Called by the framework to decide if the record is in the future
        and should simply be ignored
        """
        return False

    def process_observation(self, active_record, obs):
        """ Called by the framework to let the model process the observation
        visa-vis the active record.

        For models that compare with history, both active_record and
        observation will be present.

        For models that do not compare with history, either the active_record,
        or the observation is present.
        """
        raise NotImplementedError()

    def process_closure(self, active_record):
        """ Called to process a record closed in the current period """
        raise NotImplementedError()

    def create_observations(self, record):
        """
        Observations are passed back for the model to process, and they are
        pass through objects for the caller.  We are simply returning number
        of timepoints we are interested in. When we get the observation, we
        need to know which record, hence record is included in the returned
        comprehension.

        If no observations return blank.  For performance reasons, None
        is not checked by the caller

        Note that, framework expects the observation to be iterable.

        :param record: Record for which observations are to be created
        """
        raise NotImplementedError()

    def get_total_results(self):
        """ Called to get the total of all the records processed.
        """
        raise NotImplementedError()

    def combine_results(self, results, combine_options):
        """ Called to combine the results from multiple workers into a
        single result
        """
        raise NotImplementedError()

    def get_result(self, record):
        """ Called to retrieve the results for a single record.

        This is called only when the model declares comparison with history.
        For models that do not compare, unit results are not applicable
        """
        raise NotImplementedError()

    def prepare(self, record):
        """
        Gives an opportunity for the model prepare the record.  It will
        be cheaper to pre-create any data needed during prepare.
        """
        pass

    def determine_deps(self):
        """
        Gives the model an opportunity to alert the task v2 framework that
        it will need the results of additional dependent tasks.
        """
        return {}

    def incorporate_deps(self, dep_results):
        """
        Gives the model an opportunity to incorporate any information needed from
        the results of dependent tasks into its state.
        """
        return



class ForecastBaseModel(AnalysisModel):

    """Defines the base model related to the forecasts, so that all the common
    configuration can be done here

    """

    def __init__(self, field_map, config, time_horizon, dimensions):
        super(ForecastBaseModel, self).__init__(field_map, config, time_horizon, dimensions)
        # Setup variables for field map
        self.field_map = field_map
        self.stage_field_name = field_map['stage']
        self.orig_stage_fld = field_map.get('orig_stage', None)
        if not self.orig_stage_fld:
            self.orig_stage_fld = self.stage_field_name.replace("_adj", "")
            self.field_map['orig_stage'] = self.orig_stage_fld
            logger.warning("WARNING: 'orig_stage' was not supplied defaulting to %s", self.orig_stage_fld)
        self.raw_stage_fld = field_map.get('raw_stage', 'Stage_adj')
        # + '_conv' # we will create a converted version for the model in prepare
        self.amount_field_name = field_map['acv']
        self.close_date_field_name = field_map['close_date']
        self.orig_close_date_fld = field_map.get('orig_close_date', None)
        if not self.orig_close_date_fld:
            self.orig_close_date_fld = self.close_date_field_name.replace("_adj", "")
            self.field_map['orig_close_date'] = self.orig_close_date_fld
            logger.warning("WARNING: 'orig_close_date' was not supplied defaulting to %s", self.orig_close_date_fld)
        self.type_field_name = field_map['type']
        self.business_unit_field_name = field_map.get('business_unit', None)
        self.geo_field_name = field_map.get('geo', None)
        self.forecast_category_field_name = field_map.get('forecast_category', None)
        self.owner_field_name = field_map['owner']
        self.probability_field_name = field_map.get('probability', 'Probability')
        # Set up parameters for the Forecast models
        self.win_stages = config['win_stages']
        self.lose_stages = config['lose_stages']
        self.forecast_params = forecast_defaults.copy()
        self.forecast_params.update(config.get('algorithm_params', {}))
        self.forecast_params['verbose_mode'] = config.get('verbose_mode', False)
        self.cycle_length = self.forecast_params['cycle_length']
        self.eacv_model = self.forecast_params['eacv_model']
        self.acv_pctl_krnl_bw = self.forecast_params['acv_pctl_krnl_bw']
        self.max_kernel_weight = self.forecast_params['max_kernel_weight']
        self.apply_filters = self.forecast_params['apply_filters']
        self.close_date_max_days_past_eoq = self.forecast_params['close_date_max_days_past_eoq']
        self.close_date_updated_beyond_horizon_implies_forecast_zero = self.forecast_params[
            'close_date_updated_beyond_horizon_implies_forecast_zero']
        # list of quarter mnemonics to filter out... accepts string in format like 2014Q1
        self.filt_out_qs = set(self.forecast_params.get('filt_out_qs', []))
        # config for operations to be added
        self.post_enforcements = self.forecast_params.get('post_enforcements', [])
        self.include_zombie_opps = self.forecast_params.get('include_zombie_opps', False) #include opps that were lost at boq and as of but lived in between

        if not self.forecast_params['win_date_from_uip']:
            print ('*************************************************')
            print ('**** Not using win_date from UIP....BAD IDEA ****')
            print ('**** Note:   some legacy tests still do this ****')
            print ('*************************************************')

        # Initialize the results with blank map
        self.results = {}

        # Setup the time line to use and the time decay
        self.history_timeline_details = []

    def populate_history_timeline_details(self):

        history_timeline_details = []

        filt_out_qs = self.filt_out_qs
        forecast_params = self.forecast_params
        num_cycles = int(forecast_params['num_cycles'])
        holidays = eval(str(forecast_params.get('holiday', 'None')))
        anchor_period_type = forecast_params.get('anchor_period_type', self.cycle_length)

        logger.debug('timeline details:')
        dates_info = self.time_horizon.begins.strftime('%Y%m%d%H%M%S') + '_'
        dates_info += self.time_horizon.as_of.strftime('%Y%m%d%H%M%S') + '_'
        dates_info += self.time_horizon.horizon.strftime('%Y%m%d%H%M%S')
        logger.debug(dates_info)
        from numpy import array, where
        begins_period = current_period(self.time_horizon.begins, period_type=self.cycle_length)
        as_of_period = current_period(self.time_horizon.as_of, period_type=self.cycle_length)
        horizon_period = current_period(self.time_horizon.horizon, period_type=self.cycle_length)
        all_periods = array(get_all_periods(self.cycle_length))

        as_of_period_index = where(all_periods == as_of_period)[0][0]
        horizon_period_index = where(all_periods == horizon_period)[0][0]
        cycle_shift = horizon_period_index - as_of_period_index
        ignore_latest_observations = forecast_params['ignore_latest_observations']
        cycle_shift = ignore_latest_observations + cycle_shift
        all_poi = [self.time_horizon.begins, self.time_horizon.as_of, self.time_horizon.horizon]
        counter = cycle_shift
        while True:
            counter = counter + 1
            hist_dtl =[]
            for x in all_poi:
                hist_dtl.append(shift_date(x, -counter, self.cycle_length, anchor_period_type, holidays, True))
            hist_begins_dtl = hist_dtl[0]
            hist_as_of_dtl = hist_dtl[1]
            hist_horizon_dtl = hist_dtl[2]

            if hist_begins_dtl['period_mnemonic'] not in filt_out_qs:
                if hist_as_of_dtl['period_mnemonic'] not in filt_out_qs:
                    if hist_horizon_dtl['period_mnemonic'] not in filt_out_qs:
                        history_timeline_details.append(hist_dtl)
            if len(history_timeline_details) == num_cycles:
                break
        return history_timeline_details

    def calculate_multi_timeline_details(self, as_of_date_list, horizon_date_list, anchor_type=None, period_type=None):
        """

        Returns:
            object:
        """

        multi_timeline_details = []

        filt_out_qs = self.filt_out_qs
        forecast_params = self.forecast_params
        num_cycles = int(forecast_params['num_cycles'])
        holidays = eval(str(forecast_params.get('holiday', 'None')))

        anchor_period_type = anchor_type or forecast_params.get('anchor_period_type', self.cycle_length)
        self.cycle_length = period_type or self.cycle_length

        from numpy import array, where
        begins_period = current_period(self.time_horizon.begins, period_type=self.cycle_length)
        as_of_period = current_period(self.time_horizon.as_of, period_type=self.cycle_length)
        horizon_period = current_period(self.time_horizon.horizon, period_type=self.cycle_length)
        all_periods = array(get_all_periods(self.cycle_length))
        # we assume all the dates in the list are in the same period so cycle_shit is the same for all
        as_of_period_index = where(all_periods == as_of_period)[0][0]
        horizon_period_index = where(all_periods == horizon_period)[0][0]
        cycle_shift = horizon_period_index - as_of_period_index
        ignore_latest_observations = forecast_params['ignore_latest_observations']
        cycle_shift = ignore_latest_observations + cycle_shift

        counter = np.int(cycle_shift)
        while True:
            counter = counter + 1
            hist_begins_dtl = shift_date(
                self.time_horizon.begins, -counter, self.cycle_length, anchor_period_type, holidays, True)
            if hist_begins_dtl['period_mnemonic'] in filt_out_qs:
                continue
            as_of_tuple = ()
            for date in as_of_date_list:
                hist_as_of_dtl = shift_date(date, -counter, self.cycle_length, anchor_period_type, holidays, True)
                as_of_tuple = as_of_tuple + (hist_as_of_dtl,)
            if as_of_date_list:
                if as_of_tuple[0]['period_mnemonic'] in filt_out_qs:
                    continue
            horizon_tuple = ()
            for date in horizon_date_list:
                hist_horizon_dtl = shift_date(date, -counter, self.cycle_length, anchor_period_type, holidays, True)
                horizon_tuple = horizon_tuple + (hist_horizon_dtl,)
            if horizon_date_list:
                if horizon_tuple[0]['period_mnemonic'] in filt_out_qs:
                    continue
            multi_timeline_details.append((hist_begins_dtl, as_of_tuple, horizon_tuple))
            if len(multi_timeline_details) == num_cycles:
                break

        return multi_timeline_details

    def new_calculate_multi_timeline_details(self, date_list, anchor_type=None, period_type=None, num_cycles=None):
        """
        Parameters:
            date_list: a 1-D array (m,) of dates that you want corresponding historic dates for
            anchor_type: 'M' or 'Q'
            period_type: 'M' or 'Q'

        Returns:
            A 2-D array with num_cycle (n) historic dates per date_list element, resulting
            in shape (m,n)
        """
        datetime_list = [epoch(np.long(dt)).as_datetime() for dt in date_list]
        #why?
        filt_out_qs = self.filt_out_qs
        forecast_params = self.forecast_params
        #this way we can manually override how far back we look for the new aggregate forecasts cause i'm lazy
        #this functionality should probably move to the timeline since that's where it
        if num_cycles is None:
            num_cycles = int(forecast_params['num_cycles'])
        holiday_str = str(forecast_params.get('holiday', 'None'))
        holidays = eval(holiday_str)

        output = np.zeros((len(date_list), num_cycles), dtype=np.long)

        anchor_period_type = anchor_type or forecast_params.get('anchor_period_type', self.cycle_length)
        self.cycle_length = period_type or self.cycle_length

        begins_period = current_period(self.time_horizon.begins, period_type=self.cycle_length)
        as_of_period = current_period(self.time_horizon.as_of, period_type=self.cycle_length)
        horizon_period = current_period(self.time_horizon.horizon, period_type=self.cycle_length)
        prds, beg_end_arr = get_all_periods_array(self.cycle_length)
        # we assume all the dates in the list are in the same period so cycle_shit is the same for all
        as_of_period_index = np.searchsorted(beg_end_arr[0], as_of_period.end)
        horizon_period_index = np.searchsorted(beg_end_arr[0], horizon_period.end)
        cycle_shift = horizon_period_index - as_of_period_index
        ignore_latest_observations = forecast_params['ignore_latest_observations']
        cycle_shift = ignore_latest_observations + cycle_shift

        counter = cycle_shift
        idx = 0

        #counter is how many back you're checking, starts at cycle shift + ignore
        while (output == 0).any():
            counter += 1

            #shifts and checks the first date of the time horizon so you don't waste time shifting each val if it's false
            hist_begins_dtl = shift_date(self.time_horizon.begins, -counter, self.cycle_length, anchor_period_type, holidays, True, holiday_str)
            if hist_begins_dtl['period_mnemonic'] in filt_out_qs:
                continue

            #this is awkward but whatever, it caches it anyway, so the overhead isn't huge
            test_val = shift_date(datetime_list[0], -counter, self.cycle_length, anchor_period_type, holidays, True, holiday_str)
            if test_val['period_mnemonic'] in filt_out_qs:
                continue

            #if it passes the tests, shift each date and fill the output arrayget_decay_factors
            for i, date in enumerate(datetime_list):
                shift_val = shift_date(date, -counter, self.cycle_length, anchor_period_type, holidays, True, holiday_str)
                output[i, idx] = shift_val['epoch_date']

            idx += 1
        return output

    def populate_historical_times(self):

        begins_date = self.time_horizon.begins
        asOfDate = self.time_horizon.as_of
        horizon_date = self.time_horizon.horizon
        filt_out_qs = self.filt_out_qs
        forecast_params = self.forecast_params

        num_cycles = int(forecast_params['num_cycles'])
        logger.debug("==================> Number of cycles is %s" % num_cycles)

        self.history_timeline_details = self.populate_history_timeline_details()
        timesbegin = [x[0]['xl_date'] for x in self.history_timeline_details]
        timesfrom = [x[1]['xl_date'] for x in self.history_timeline_details]
        timesto = [x[2]['xl_date'] for x in self.history_timeline_details]
        return timesbegin, timesfrom, timesto

    def setuptimeline(self):
        from ..analyticengine import get_decay_factors

        asOfDate = self.time_horizon.as_of
        horizon_date = self.time_horizon.horizon
        forecast_params = self.forecast_params
        anchor_period_type = forecast_params.get('anchor_period_type', self.cycle_length)
        use_anchor_for_horizon_eoq = forecast_params.get('use_anchor_for_horizon_eoq', True)
        horizon_eoq_period_type = anchor_period_type if use_anchor_for_horizon_eoq else self.cycle_length

        self.as_of = self.time_horizon.as_ofF
        self.horizon_boq = datetime2xl(current_period(horizon_date).begin)

        self.horizon_eoq = datetime2xl(current_period(horizon_date, period_type=horizon_eoq_period_type).end)
        # we should never filter out current period
        self.filt_out_qs = self.filt_out_qs - set([current_period(asOfDate)[0]])

        decay = forecast_params['decay']
        seasonality = forecast_params['seasonality']
        self.timesbegin, self.timesfrom, self.timesto = self.populate_historical_times()
        lastQ = current_period(asOfDate).begin
        self.max_days_in_stage = max(1.0, (asOfDate - lastQ).days)
        self.timedecay = get_decay_factors(self.as_of, self.timesfrom, self.timesto, decay, seasonality)

        return

    # Following 2 methods decide if a record is active or history.
    # A record can be neither active, or history. In which case, framework
    # assumes it's fate is decided in the current period

    def is_active(self, record):
        ''' Called by the framework to determine if the record
        is considered to be active'''
        if self.is_closed(record):
            return False

        stage_at_start = record.getAsOfDateF(self.stage_field_name,
                                             self.time_horizon.beginsF)
        # stage_now = record.getAsOfDateF(self.stage_field_name,
        #                                self.time_horizon.as_ofF)
        stage_now = self.forecast_params.get("stage_override", record.getAsOfDateF(self.stage_field_name,
                                                                                   self.time_horizon.as_ofF))
        if stage_now == 'N/A':
            return False

        if stage_at_start in self.win_stages and record.win_date and record.win_date <= self.time_horizon.beginsF:
            return False

        if stage_at_start in self.lose_stages and stage_now in self.lose_stages:
            return False

        return True

    def is_history(self, record):
        # stage = record.getAsOfDateF(self.stage_field_name,
        #                           self.time_horizon.beginsF)
        # if stage in self.win_stages or stage in self.lose_stages:
        return True
        # return False

    def is_closed(self, record):
        stage_at_start = record.getAsOfDateF(self.stage_field_name,
                                             self.time_horizon.beginsF)
        stage_now = self.forecast_params.get("stage_override", record.getAsOfDateF(self.stage_field_name,
                                                                                   self.time_horizon.as_ofF))
        won_in_correct_window = False
        if stage_now in self.win_stages:
            win_date = record.win_date
            if win_date >= self.time_horizon.beginsF and win_date <= self.time_horizon.as_ofF:
                won_in_correct_window = True

        lost_in_correct_window = False
        if self.include_zombie_opps:
            if stage_now in self.lose_stages:
                lose_date = record.lose_date
                if lose_date >= self.time_horizon.beginsF and lose_date <= self.time_horizon.as_ofF:
                    lost_in_correct_window = True
        else:
            if stage_at_start not in self.lose_stages and stage_now in self.lose_stages:
                lost_in_correct_window = True

        if (won_in_correct_window or lost_in_correct_window):
            return True

    def process_closure(self, record):
        pass

    def prepare(self, record):
        """
        Gives an opportunity for the model prepare the record.  It will
        be cheaper to pre-create any data needed during prepare.
        """
        super(ForecastBaseModel, self).prepare(record)
        self.remove_latest_data(record)
        did_something = self.process_close_date(record)

        try:  # latest acv in base currency
            self.acv_latest = mathUtils.excelToFloat(record.getLatest(self.amount_field_name))
        except:
            self.acv_latest = 0.0

        record.close_date_too_far = False
        if self.close_date_max_days_past_eoq >= 0.0:
            try:
                close_date_as_of = float(record.getAsOfDateF(self.close_date_field_name, self.time_horizon.as_ofF))
                if (close_date_as_of - self.time_horizon.horizonF) > self.close_date_max_days_past_eoq:
                    record.close_date_too_far = True
            except:
                pass
        if self.close_date_updated_beyond_horizon_implies_forecast_zero:
            record.close_date_updated_beyond_horizon = False
            try:
                close_date_begin = float(record.getAsOfDateF(self.close_date_field_name, self.time_horizon.beginsF))
                close_date_as_of = float(record.getAsOfDateF(self.close_date_field_name, self.time_horizon.as_ofF))
                if ((close_date_begin <= self.time_horizon.horizonF) and (close_date_as_of > self.time_horizon.horizonF)) \
                        or ((close_date_begin > self.time_horizon.horizonF) and (close_date_as_of > close_date_begin)):
                    record.close_date_updated_beyond_horizon = True
            except:
                pass

        # TODO: Should probably return [stage_fld] if did_something and self.forecast_params['win_date_from_uip']
        return ['Stage'] if did_something else []

    def process_close_date(self, record):
        winStages = self.win_stages
        record.win_date = None
        record.lose_date =None
        stage_fld = self.stage_field_name
        if self.forecast_params['win_date_from_uip']:
            ret_value = False
            terminal_fate_field = self.forecast_params['terminal_fate_field']
            if terminal_fate_field in record.featMap:
                if record.featMap[terminal_fate_field][-1][1] == 'W':
                    record.win_date = record.featMap[terminal_fate_field][-1][0]
                elif record.featMap[terminal_fate_field][-1][1] == 'L':
                    record.lose_date = record.featMap[terminal_fate_field][-1][0]
            # to make sure that win_dates are being used we will remove fake win stages:
            # this will ensure that if win_dates are not calculated properly, the model will give no wins
            if not record.win_date:
                if stage_fld in record.featMap:
                    stage_ts = record.featMap[stage_fld]
                    for pair in stage_ts:
                        if pair[1] in winStages:
                            ret_value = True
                            record.remove_feature_value(pair[0], stage_fld)
            return ret_value

        # if win_date_from_uip is False
        ######################################
        #
        # This code will be removed as soon
        # as we determine that there are no
        # failures
        #
        #######################################
        winStages = self.win_stages
        close_date_fld = self.close_date_field_name

        if self.forecast_params['win_date_basis'] == 'close_date':
            if close_date_fld in record.featMap and 'Stage' in record.featMap and record.getLatest(stage_fld) in winStages:
                stageAdd = {}
                stageTransAdd = {}
                for date, stage_trans_val in sorted(record.featMap[stage_fld]):
                    if stage_trans_val in winStages:
                        # it is a win so get the latest close date
                        closedDate = record.getLatest(close_date_fld)
                        if closedDate == 'N/A':
                            continue
                        try:
                            closedDateF = float(closedDate)
                        except:
                            closedDateF = datestr2xldate(closedDate)

                        # TODO: at the moment the value of closed date does not have
                        # time zone info.
                        # For now i assume the local time is california and to be sure about
                        # daylight saving issues, i add 8 hours = 1/3 day to the standard time
                        closedDateF = int(closedDateF) + 0.333333333333333333
                        # end hack

                        record.win_date = closedDateF
                        StageAsOfClosedDate = record.getAsOfDateF(stage_fld, closedDateF)
                        if StageAsOfClosedDate not in winStages:
                            stageAdd[closedDateF] = record.getAsOfDateF('Stage', date)
                            stageTransAdd[closedDateF] = stage_trans_val
                        break
                for date in stageAdd:
                    record.add_feature_value(date, 'Stage', stageAdd[date])
                for date in stageTransAdd:
                    record.add_feature_value(date, stage_fld, stageTransAdd[date])
                if len(list(stageAdd.keys())) > 0:
                    maxdateAdded = max(list(stageAdd.keys()))
                    datelist = list(map(lambda x: x[0], record.featMap[stage_fld]))
                    for date in datelist:
                        if date > maxdateAdded:
                            record.remove_feature_value(date, stage_fld)
                            record.remove_feature_value(date, 'Stage')
                # finally adjust created date so that the record is not dropped in case the win date was back-dated
                record.created_date = min(record.created_date, record.featMap[stage_fld][0][0])
            return True
        elif self.forecast_params['win_date_basis'] == 'stage':
            if stage_fld in record.featMap:
                for date, stage_value in record.featMap[stage_fld]:
                    if stage_value in winStages:
                        record.win_date = date
                        break
            return True
        else:
            raise ValueError("ERROR: win_date_basis must be one of 'stage' or 'close_date' (it was '%s')" %
                             (self.forecast_params['win_date_basis']))
        ########
        # end of code to be removed
        ########

    def remove_latest_data(self, record):
        if self.forecast_params['remove_latest_data'] == True:

            if self.forecast_params['remove_latest_data_from']:
                cut_off = epoch2xl(self.forecast_params['remove_latest_data_from'])
            else:
                cut_off = self.time_horizon.as_ofF

            fields_to_remove = self.forecast_params["remove_latest_data_fields"]
            if fields_to_remove == None:
                fields_to_remove = record.featMap

            field_exceptions = self.forecast_params['remove_latest_data_field_exceptions']
            if field_exceptions == None:
                field_exceptions = []

            for feat in fields_to_remove:
                if feat not in record.featMap:
                    continue
                if feat in field_exceptions:
                    continue
                if len(record.featMap[feat]) > 0:
                    first_date = record.featMap[feat][0][0]
                    # if field was imported after this cut_off leave it as is
                    # (assume this only happens for fields whose value is not time transient)
                    if first_date >= cut_off:
                        continue
                    reverse_datelist = sorted(list(map(lambda x: x[0], record.featMap[feat])), reverse=True)
                    for this_date in reverse_datelist:
                        if this_date > cut_off:
                            record.remove_feature_value(this_date, feat)
                        else:
                            break

    def get_acv_as_of(self, record, as_of, first_value_on_or_after=False):
        try:
            return mathUtils.excelToFloat(
                record.getAsOfDateF(
                    self.amount_field_name,
                    as_of,
                    first_value_on_or_after=first_value_on_or_after))
        except:
            return 0.0

    def get_acv_latest(self, record):
        try:
            return mathUtils.excelToFloat(
                record.getLatest(
                    self.amount_field_name))
        except:
            return 0.0
