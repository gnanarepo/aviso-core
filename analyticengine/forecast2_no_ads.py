import logging

from aviso.utils.dateUtils import epoch

from  analyticengine.forecast2 import Forecast2


def cachekey2epoch(key):
    dates = key.split('_')
    dates = list(map(lambda x: epoch(x), dates))
    return dates

logger = logging.getLogger('gnana.%s' % __name__)


class Forecast2_no_ds(Forecast2):

    """
This model just provides the given probabilities if the close date is within the correct range
    """
    allow_aggregation = True
    predicts = True
    compares_with_history = False
    produce_individual_result = True
    ignore_closure_errors = True
    combine_prefix = 'epf'

    calibration_options = ['menu']

    def __init__(self, field_map, config, time_horizon, dimensions):
        super(Forecast2_no_ds, self).__init__(field_map, config, time_horizon, dimensions)
        self.results = {}
        self.loaded_latest_epf_result = False
        self.latest_epf_result = None

    def process_observation(self, record, observation):
        # We only care about active values.  Let go of observations
        if not record:
            return
        if not self.history_timeline_details:
            self.setuptimeline()
        begins = self.time_horizon.beginsF
        as_of = self.time_horizon.as_ofF
        horizon = self.time_horizon.horizonF
        try:
            active_vals = self.active_value_cache[record.ID]
        except:
            active_vals = self.values_in_active(record)
            self.active_value_cache[record.ID] = active_vals

        close_date = active_vals.close_date
        acv = active_vals.acv
        prob = record.getAsOfDateF(self.probability_field_name, as_of)
        if prob != 'N/A':
            probability = max(0.0, min(float(prob), 99.0))
        else:
            try:
                probability = max(0.0, min(float(record.getAsOfDateF(self.stage_field_name, as_of)), 99.0))
            except:
                probability = 0.0
        ret_value = {}
        if begins < close_date <= horizon and as_of < close_date:
            win_prob = probability / 100.0
        else:
            win_prob = 0.0
        ret_value['win_prob'] = win_prob
        ret_value['eACV'] = acv
        ret_value['forecast'] = acv * win_prob
        ret_value['current_stage'] = active_vals.stage
        ret_value['current_type'] = active_vals.type
        ret_value['current_owner'] = active_vals.owner
        ret_value['active_amount'] = acv
        ret_value['days_to_close'] = active_vals.days_to_close
        ret_value['eACV_deviation'] = 0.0
        ret_value['existing_pipe'] = record.featMap[self.stage_field_name][0][0] <= begins

        self.results[record.ID] = ret_value

        return self.results[record.ID]

    def get_result(self, record):
        try:
            return self.results[record.ID]
        except:
            return None

    def get_total_results(self):
        return self.results
