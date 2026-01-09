from collections import namedtuple
import logging
import math

from aviso.framework.diagnostics import probe_util
from aviso.utils.dateUtils import xl2datetime, datestr2xldate
import cython


from ..analyticengine.NoresultException import NoResultException
from ..analyticengine import CombineResults
from ..analyticengine.forecast_base import ForecastBaseModel




logger = logging.getLogger('gnana.%s' % __name__)

basestring = str
long = int


def unicode_conversion(text):
    if isinstance(text, bytes):
        return str(text, 'utf-8')
    else:
        return text

unicode = unicode_conversion

config_restrict_paths = ['datasets.<>.maps', 'datasets.<>.filetypes']




totally_dissimilar = cython.declare(cython.double, -1.0)
log_of_two = cython.declare(cython.double, math.log(2.0))

# try to keep raw uip fields at top and transformations on bottom
active_details_list = [
    'stage',
    'acv',
    'close_date',
    'owner',
    'type',
    'days_in_stage',
    'days_to_close',
    'tacv',
]
active_details = namedtuple('active_details', active_details_list)

observation_details_list = active_details_list + [
    'hist_as_of',
    'hist_horizon',
    'decay_factor',
    'stage_horizon',
    'acv_horizon',
    'destiny',
    'time_to_destiny',
]
observation_details = namedtuple('observation_details', observation_details_list)


# noinspection PyStatementEffect
"""
    This model generates an expected value for a set of items, based on the
    historical observations.

    Data Needs:
       'Stage' or some kind of state describing where the record is
       'Amount' an indicator the economic value
       'Onwer' Responsible Party

Timeline is setup and for each record, multiple observations are created for
each time line entry

        ___ R1 ___        ___ R2 ___    . . . . . . .  <each historical entry>
        t1       t2       t1       t2
        A        L        A        W

 a1
 :
 :
Each Active
Entry

Running Averages are computed as follows:

                                Conditional    Unconditional   close_neighbours
                                Stage:Value
w[a1,1] = e(-dist(a1,R1,t1))   {'A':sigma(w1)}    {'A':1}           0
w[a1,2] = e(-dist(a1,R1,t2))   {'A':sigma(w1),
                                'L':sigma(w2)}    {'A':1, 'L':1}    0

                                 :
                                 :

                                        {'A':CSA}

get_result will do the following function:

                        cprob(a1, A) =  CSA / CSA+CSL+CSW+..
                        uprob(a1, A) =

                        prob(a1, A) = cprob(a1, A) if close_nei > x
                                    = cprob(a1, A) + uprob(a1, A)


NOTE: This comment is for general understanding of how the code works
internally.  Not for documentation purpose.

"""


@cython.cclass
class RunningResults:

    # cython.declare() is slow when not compiling
    # so we only use it when we are compiling
    if cython.compiled:
        conditional_results = cython.declare(dict)
        unconditional_results = cython.declare(dict)
        neighbor_details = cython.declare(list)
        historic_details = cython.declare(list)
        close_neighbors = cython.declare(cython.int)
        total_neighbors = cython.declare(cython.int)
        conditional_sum_acv_diff = cython.declare(cython.double)
        conditional_sum_acv_diff_sq = cython.declare(cython.double)
        unconditional_sum_acv_diff = cython.declare(cython.double)
        unconditional_sum_acv_diff_sq = cython.declare(cython.double)
        conditional_sum_acv_diff_weights = cython.declare(cython.double)
        unconditional_sum_acv_diff_weights = cython.declare(cython.double)
        sum_winning_neighbor_weights = cython.declare(cython.double)
        count_winning_neighbors = cython.declare(cython.double)
        conditional_time_to_win = cython.declare(cython.double)
        unconditional_time_to_win = cython.declare(cython.double)

    def __init__(self):
        self.conditional_results = {}
        self.unconditional_results = {}
        self.neighbor_details = []
        self.historic_details = []
        self.close_neighbors = 0
        self.total_neighbors = 0
        self.conditional_sum_acv_diff = 0.0
        self.conditional_sum_acv_diff_sq = 0.0
        self.unconditional_sum_acv_diff = 0.0
        self.unconditional_sum_acv_diff_sq = 0.0
        self.conditional_sum_acv_diff_weights = 0.0
        self.unconditional_sum_acv_diff_weights = 0.0
        self.sum_winning_neighbor_weights = 0.0
        self.count_winning_neighbors = 0.0
        self.conditional_time_to_win = 0.0
        self.unconditional_time_to_win = 0.0


@cython.cfunc
@cython.returns(cython.double)
@cython.locals(max_days_in_stage=cython.double)
def get_similarity(active_record_values, historic_values,
                   forecast_params, max_days_in_stage):
    """ Returns -1.0 (totally_dissimilar) if the items are not similar at all,
    otherwise expected to return something between 0.0 and 1.0 """

    # cython.declare() is slow when not compiling,
    # so we only use it when we are compiling
    if cython.compiled:
        distance = cython.declare(cython.double)
        ret_val = cython.declare(cython.double)

        # For unpacking forecast_params
        owner_diff_dist = cython.declare(cython.double)
        days_in_stage_bw = cython.declare(cython.double)
        days_to_close_dist_add = cython.declare(cython.double)
        days_to_close_bw = cython.declare(cython.double)
        acv_bw = cython.declare(cython.double)

        gap_in_days = cython.declare(cython.double)

        # For unpacking active_record_values
        days_in_stage = cython.declare(cython.double)
        days_to_close = cython.declare(cython.double)
        tacv = cython.declare(cython.double)

        # For unpacking historic_values
        hist_days_in_stage = cython.declare(cython.double)
        hist_days_to_close = cython.declare(cython.double)
        hist_tacv = cython.declare(cython.double)

    stage_now = active_record_values.stage
    owner = active_record_values.owner
    current_type = active_record_values.type
    days_in_stage = active_record_values.days_in_stage
    days_to_close = active_record_values.days_to_close
    tacv = active_record_values.tacv

    hist_stage = historic_values.stage
    hist_owner = historic_values.owner
    hist_type = historic_values.type
    hist_days_in_stage = historic_values.days_in_stage
    hist_days_to_close = historic_values.days_to_close
    hist_tacv = historic_values.tacv

    # Start adding distances
    distance = 0.0
    if stage_now != hist_stage:
        return totally_dissimilar

    if current_type != hist_type:
        return totally_dissimilar  # distance will be s large that won't make a contrib

    owner_diff_dist = forecast_params["owner_diff_dist"]
    if owner != hist_owner:
        distance += owner_diff_dist

    # We are calculating the difference in the days in stage between
    # observation and active record.  However, we use a max_days cutoff
    # Squared distance to max ratio allows us to properly asses
    # similarity and always keeps the distance >= 0
    days_in_stage_bw = forecast_params["days_in_stage_bw"]

    # Use the C library functions if we are compiling otherwise use Python ones
    if cython.compiled:
        gap_in_days = ((libc.math.fmax(days_in_stage, max_days_in_stage)
                        - libc.math.fmax(hist_days_in_stage, max_days_in_stage)))
    else:
        gap_in_days = ((max(days_in_stage, max_days_in_stage)
                        - max(hist_days_in_stage, max_days_in_stage)))

    distance += (gap_in_days / max_days_in_stage / days_in_stage_bw) ** 2

    days_to_close_dist_add = forecast_params["days_to_close_dist_add"]
    days_to_close_bw = forecast_params["days_to_close_bw"]
    if days_to_close >= 999.0 or hist_days_to_close >= 999.0:
        distance += days_to_close_dist_add
    else:
        distance += ((days_to_close - hist_days_to_close) / days_to_close_bw) ** 2
    acv_bw = forecast_params["acv_bw"]

    distance += ((tacv - hist_tacv) / log_of_two / acv_bw) ** 2

    # Use the C library functions if we are compiling otherwise use Python ones;
    # Turning the distance into similarity
    if cython.compiled:
        ret_val = libc.math.exp(-distance)
    else:
        ret_val = math.exp(-distance)

    return ret_val


class Forecast(ForecastBaseModel):

    """
This model generates a forecast of expected dollor amount that can be
potentially closed in a given timeframe

Configuration:
    * win_stages (Array of Strings): Which stages are considered to be
      winning stages.
    * lose_stages (Array of Strings): Which stages are considered to be losing
      stages.
    * buffer_size (integer): Number of entries to buffer for parallel
      processing.

    Algorithm Parameters:

        .. WARNING:: Use extreme caution in changing these parameters.

+------------------------+-----------+-------------------------------------+
| Parameter Name         | Suggested | Description                         |
|                        | Value     |                                     |
+========================+===========+=====================================+
| num_std_dev            | 4.0       | ignore normal distribution after    |
|                        |           | this many standard deviations       |
+------------------------+-----------+-------------------------------------+
| cycle_length           | Q         | What is the forecasting cycle. Only |
|                        |           | Quarters are supported for now.     |
+------------------------+-----------+-------------------------------------+
| decay                  | 0.00300451| Amount of time decay per day to use |
+------------------------+-----------+-------------------------------------+
| small_kernel_weight    | 1.0E-4    | Weights smaller than this number    |
|                        |           | are not considered close neighbors  |
+------------------------+-----------+-------------------------------------+
| num_cycles             | 18.0      | Number of time periods (cycles) to  |
|                        |           | consider in history for training.   |
+------------------------+-----------+-------------------------------------+
| num_points_per_day     | 1.0       | timeline should have this many      |
|                        |           | points per day                      |
+------------------------+-----------+-------------------------------------+
|    seasonality         |   [1]     | what multiplier should be           |
|                        |           | assigned to each period. e.g.       |
|                        |           | [0,0,0,1] will only consider        |
|                        |           | every 4th periods                   |
+------------------------+-----------+-------------------------------------+
|                        |           | if it is set to True, any opp       |
|    delay_close_date    |    False  | which its close date gets           |
|    _implies_inactive   |           | delayed outside of the              |
|                        |           | horizon becomes inactive            |
+------------------------+-----------+-------------------------------------+
|                        |           | How we determine win_date           |
|     win_date_basis     |           | could be 'stage' or 'close_date'    |
+------------------------+-----------+-------------------------------------+
|    owner_diff_dist     |    0.25   | the amount of distance to add       |
|                        |           | if owners are different             |
+------------------------+-----------+-------------------------------------+
|                        |           | the bandwidth for days in stage     |
|     days_in_stage_bw   |     1.0   | in the distance function            |
+------------------------+-----------+-------------------------------------+
|  days_to_close_dist_add| 0.25      |   if either of  closed dates        |
|                        |           |   are missing add this distance     |
+------------------------+-----------+-------------------------------------+
| small_num_neighbors    | 25        | If there are less than this number  |
|                        |           | close neighbers start using         |
|                        |           | unconditional probabiity            |
+------------------------+-----------+-------------------------------------+
| sigma                  | 0.1       |  the hump around each time point    |
+------------------------+-----------+-------------------------------------+
| remove_latest_data     | False     | when running the model use the      |
|                        |           | latest available values for win     |
|                        |           | amounts, opp owner etc Should be set|
|                        |           | to True for back-testing            |
+------------------------+-----------+-------------------------------------+
|remove_latest_          | None      | The date after which alldata should |
| data_from              |           | be ignored. If remove_latest_data   |
|                        |           | True but this is not given, as_of   |
|                        |           | date is used.                       |
+------------------------+-----------+-------------------------------------+
|remove_future_          |None       | the fields that would not be        |
|  field_exceptions      |           | removed from future data            |
+------------------------+-----------+-------------------------------------+
|remove_future_          |None       | if specified, only fields in this   |
|  fields_set            |           | set will be removed  from data      |
+------------------------+-----------+-------------------------------------+
| tiny                   | 1.0e-15   | smallest weight allowed for any     |
|                        |           | observation                         |
+------------------------+-----------+-------------------------------------+
|num_neighbor_details    | 0         | The result will give details of this|
|                        |           | number of closest observations for  |
|                        |           | each active opportunity             |
+------------------------+-----------+-------------------------------------+
|   days_to_close_bw     |    91.0   |    bandwidth for days to close      |
+------------------------+-----------+-------------------------------------+
|  quarter_start_delay   |     0.0   | delay in quarter start              |
+------------------------+-----------+-------------------------------------+
|  quarter_horizon_delay |   0.0     | delay in quarter end                |
+------------------------+-----------+-------------------------------------+
|    acv_bw              |  1.0      |     acv bandwidth                   |
|                        |           |                                     |
+------------------------+-----------+-------------------------------------+






Field Map:
    * type: Which field signifies the type.  Opportunities with different type
      are trained differently.
    * stage: Which field signifies the stage of the opprtunity.
    * acv: Which field contains the amount of winnings if the opportunity is own
    * owner: Who is the owner of the opportunity. Differences in the owner are
      used to estimate similarity

.. note:: Please note that . . .

    * At present there is no way to configure a custom distance function,
      except the bandwith parameters
    * It is not advised to use the fields directly for Stage and type.  It is
      advisable to create a map for stages and types so that similar items can
      be grouped especially since historically the labels can be changing. To
      use a mapped value instead of raw value specify *fieldname*Trans instead
      of the field name.
    * Dimensions are not honored during the offlined process. They are
      generated on demand during the result creation.  Hence the DB will not
      have these values.

    """
    allow_aggregation = True
    predicts = True
    compares_with_history = True
    produce_individual_result = True
    ignore_closure_errors = True
    combine_options = ['quantiles', 'num_paths', 'num_batches', 'random_seed',
                       'num_targets', 'extra_target_quantiles', 'round_targets_to',
                       'min_output_score', 'newpipeforecast', 'newpipemodel',  'trajectory_info', 'by_period']
    combine_prefix = 'epf'

    def __init__(self, field_map, config, time_horizon, dimensions):
        super(Forecast, self).__init__(field_map, config,
                                       time_horizon, dimensions)
        self.active_value_cache = {}
        self.min_neighbors = self.forecast_params["small_num_neighbors"]
        self.num_neighbor_details = self.forecast_params['num_neighbor_details']
        if self.post_enforcements:
            self.require_post_process = True

    def process_closure(self, record):
        try:
            # produces all the results for the closed records
            current_stage = record.getAsOfDateF(self.stage_field_name,
                                                self.time_horizon.as_ofF)
            # Get the first record and first value for the min time
            birthdate = record.featMap[self.stage_field_name][0][0]
            existing_pipe = False
            if birthdate < self.time_horizon.beginsF:
                existing_pipe = True

            if self.apply_filters:
                try:
                    acv_begin = self.get_acv_as_of(record,
                                                   self.time_horizon.beginsF + 0.5)  # add 0.5 to help float comparison
                except:
                    acv_begin = 0.0
                if (self.acv_latest < self.forecast_params['filter_close_acv_lo']) \
                        or (self.acv_latest > self.forecast_params['filter_close_acv_hi']) \
                        or (acv_begin < self.forecast_params['filter_begin_acv_lo']) \
                        or (acv_begin > self.forecast_params['filter_begin_acv_hi']):
                    err_msg = "filtered out begin_acv={begin_acv} close_acv={close_acv}"
                    err_params = {'begin_acv': acv_begin, 'close_acv': self.acv_latest}
                    raise NoResultException(err_msg, err_params)

            term_cat = ''
            pipe_cat = ''
            win_prob = None
            forecast = None
            eACV = None
            if record.win_date and (record.win_date <= self.time_horizon.as_ofF) and (current_stage in self.win_stages):
                term_cat = 'won_amount'
                if existing_pipe:
                    pipe_cat = 'existing_pipe_won_amount'
                else:
                    pipe_cat = 'new_pipe_won_amount'
                win_prob = 1.0
                forecast = self.acv_latest
                eACV = self.acv_latest
            elif record.win_date and (record.win_date > self.time_horizon.as_ofF) and (current_stage in self.win_stages):
                term_cat = 'future_won_amount'
                if existing_pipe:
                    pipe_cat = 'existing_pipe_future_won_amount'
                else:
                    pipe_cat = 'new_pipe_future_won_amount'
                win_prob = 0.0
                forecast = 0.0
                eACV = 0.0

            elif current_stage in self.lose_stages:
                term_cat = 'lost_amount'
                if existing_pipe:
                    pipe_cat = 'existing_pipe_lost_amount'
                else:
                    pipe_cat = 'new_pipe_lost_amount'
                win_prob = 0.0
                forecast = 0.0
                eACV = 0.0
            else:
                return None
            result = {term_cat: self.acv_latest,
                      pipe_cat: self.acv_latest,
                      'win_prob': win_prob,
                      'forecast': forecast,
                      'eACV': eACV,
                      }
            return result
        except Exception as e:
            logger.exception(str(e))
            raise e

    def process_observation(self, record, observation):
        # cython.declare() is slow when not compiling,
        # so we only use it when we are compiling
        if cython.compiled:
            running_result = cython.declare(RunningResults)

            weight = cython.declare(cython.double)
            old_weight = cython.declare(cython.double)
            decay_factor = cython.declare(cython.double)

            hist_acv = cython.declare(cython.double)
            hist_acv_horizon = cython.declare(cython.double)
            acv_diff = cython.declare(cython.double)
            acv_diff_sq = cython.declare(cython.double)
        else:
            hist_acv = 0.0
            hist_acv_horizon = 0.0

        history_record = observation[0]
        hist_vals = observation[1]

        hist_as_of = hist_vals.hist_as_of
        hist_horizon = hist_vals.hist_horizon
        hist_stage_horizon = hist_vals.stage_horizon
        hist_acv = hist_vals.acv
        hist_acv_horizon = hist_vals.acv_horizon
        decay_factor = hist_vals.decay_factor

        try:
            actv_vals = self.active_value_cache[record.ID]
        except KeyError:
            actv_vals = self.values_in_active(record)
            self.active_value_cache[record.ID] = actv_vals

        tacv = actv_vals.tacv

        weight = get_similarity(actv_vals,
                                hist_vals,
                                self.forecast_params,
                                self.max_days_in_stage)

        weight = min(weight, self.max_kernel_weight)

        if probe_util.probing:
            params = {
                'histid': history_record.ID,
                'time': xl2datetime(hist_as_of).strftime("%y%m%d"),
                'horizon': xl2datetime(hist_horizon).strftime("%y%m%d"),
                'stage': hist_stage_horizon,
                'weight': weight
            }
            m = "Observation for {histid} at [{time} for {horizon}] is {stage}, {weight}"

            if not weight == totally_dissimilar:
                probe_util.signal(m, params, message_key='OBSERVATION_DETAIL')

        # Ignore if weight is totally_dissimilar
        # This means that completely dis-similar items are not counted
        # into the equation
        # Example: New Business Vs Renewal will return a weight of zero
        if weight == totally_dissimilar:
            return

        # if the observation is not seasonal
        if decay_factor == 0.0:
            return

        old_weight = weight

        weight *= decay_factor
        weight = max(self.forecast_params['tiny'], weight)

        if probe_util.probing:
            probe_util.signal(" {histid} "
                              "{time} "
                              "{stage} "
                              "  starting_weight {old} "
                              "  ending_weight  : {new} "
                              "  timedecay      : {decay} "
                              " ker2D : {ker2d} ",
                              {'old': old_weight,
                               'new': weight,
                               'decay': decay_factor,
                               'histid': history_record.ID,
                               'time': hist_as_of,
                               'stage': hist_stage_horizon,
                               })

        # performance is critical, since this will be called many times.
        # Assume keys are present and create on exception
        try:
            running_result = self.results[record.ID]
        except KeyError:
            running_result = RunningResults()
            self.results[record.ID] = running_result

        try:
            running_result.conditional_results[hist_stage_horizon] += weight
        except KeyError:
            running_result.conditional_results[hist_stage_horizon] = weight

        try:
            running_result.unconditional_results[hist_stage_horizon] += 1
        except KeyError:
            running_result.unconditional_results[hist_stage_horizon] = 1

        if weight > self.forecast_params["small_kernel_weight"]:
            running_result.close_neighbors += 1
            if probe_util.probing:
                probe_util.signal("Close neighbor : {histid} "
                                  "{time} ", {
                                      'histid': history_record.ID,
                                      'time': hist_as_of,
                                  })

        destiny = hist_vals.destiny
        time_to_destiny = hist_vals.time_to_destiny

        if destiny == 'W':
            running_result.sum_winning_neighbor_weights += weight
            running_result.count_winning_neighbors += 1.0
            running_result.conditional_time_to_win += weight * time_to_destiny
            running_result.unconditional_time_to_win += time_to_destiny

        if hist_stage_horizon in self.win_stages:
            separate_zero_acv = self.forecast_params["separate_zero_acv_for_eacv_model"]
            eACV_for_zero_acv_diff = self.forecast_params["eACV_for_zero_acv_diff"]
            acv_diff_contrib = (not separate_zero_acv) \
                               or (tacv == 0.0 and hist_acv == 0.0) \
                               or (tacv != 0.0 and hist_acv != 0.0)
            if eACV_for_zero_acv_diff:
                if (tacv == 0.0 and hist_acv == 0.0) or (tacv != 0.0 and hist_acv != 0.0):
                    acv_diff = hist_acv_horizon - hist_acv
                    acv_diff_sq = acv_diff ** 2
                    running_result.conditional_sum_acv_diff += weight * acv_diff
                    running_result.unconditional_sum_acv_diff += acv_diff
                    running_result.conditional_sum_acv_diff_sq += weight * acv_diff_sq
                    running_result.unconditional_sum_acv_diff_sq += acv_diff_sq
                    running_result.conditional_sum_acv_diff_weights += weight
                    running_result.unconditional_sum_acv_diff_weights += 1.0
                elif tacv == 0.0 and hist_acv != 0.0:
                    acv_diff = hist_acv_horizon
                    acv_diff_sq = 0
                    running_result.conditional_sum_acv_diff += weight * acv_diff
                    running_result.unconditional_sum_acv_diff += acv_diff
                    running_result.conditional_sum_acv_diff_sq += weight * acv_diff_sq
                    running_result.unconditional_sum_acv_diff_sq += acv_diff_sq
                    running_result.conditional_sum_acv_diff_weights += weight
                    running_result.unconditional_sum_acv_diff_weights += 1.0
            elif acv_diff_contrib:
                acv_diff = hist_acv_horizon - hist_acv
                acv_diff_sq = acv_diff ** 2
                running_result.conditional_sum_acv_diff += weight * acv_diff
                running_result.unconditional_sum_acv_diff += acv_diff
                running_result.conditional_sum_acv_diff_sq += weight * acv_diff_sq
                running_result.unconditional_sum_acv_diff_sq += acv_diff_sq
                running_result.conditional_sum_acv_diff_weights += weight
                running_result.unconditional_sum_acv_diff_weights += 1.0

        if self.num_neighbor_details > 0:
            # only keep top num_neighbor_details by new_weight
            neighbor_details = running_result.neighbor_details
            sz = min(len(neighbor_details), self.num_neighbor_details)
            neighbor_detail = (history_record.ID,
                               {
                                   'old_weight': old_weight,
                                   'new_weight': weight,
                                   'decay_factor': decay_factor,
                                   'as_of_date': hist_as_of,
                                   'hist_stage_horizon': hist_stage_horizon,
                                   'hist_acv': hist_acv,
                                   'hist_acv_horizon': hist_acv_horizon
                               }
                               )
            idx = 0
            for idx in range(sz):
                if neighbor_details[idx][1]['new_weight'] < weight:
                    neighbor_details.insert(idx, neighbor_detail)
                    break
            else:  # all new_weights were bigger than this weight, so only insert if max number has not reached
                if len(neighbor_details) < self.num_neighbor_details:
                    neighbor_details.insert(idx, neighbor_detail)
            # drop bottom entries
            while len(neighbor_details) > self.num_neighbor_details:
                neighbor_details.pop()

        return

    @staticmethod
    def transform(x):
        y = x > 0 and max(x, 1.0) or -1.0 / min(x, -1.0)
        return math.log(y)

    def values_in_active(self, rec):
        close_date = rec.getAsOfDateF(self.close_date_field_name, self.as_of)
        if close_date == 'N/A':
            close_date = -1
            days_to_close = 999
        else:
            try:
                close_date = float(close_date)
            except:
                close_date = datestr2xldate(close_date)
            days_to_close = float(close_date) - self.as_of

        acv = self.get_acv_as_of(rec, self.as_of, first_value_on_or_after=True)

        ret_tuple = active_details(
            stage=rec.getAsOfDateF(self.stage_field_name, self.as_of),
            acv=acv,
            close_date=close_date,
            owner=rec.getAsOfDateF(self.owner_field_name, self.as_of, first_value_on_or_after=True),
            type=rec.getAsOfDateF(self.type_field_name, self.as_of, first_value_on_or_after=True),
            days_in_stage=rec.lengthOfTimeAtCurrValueF(self.stage_field_name, self.as_of),
            days_to_close=days_to_close,
            tacv=self.transform(acv),
        )

        if probe_util.probing:
            probe_util.signal("Values in active rec {id} are: "
                              "  stage: [{stage}] owner: [{owner}]"
                              "  type: [{type}] days_in_stage: {length}]"
                              "  days_to_close [{days}], tACV:[{acv}]",
                              {'id': rec.ID,
                               'stage': ret_tuple.stage,
                               'owner': ret_tuple.owner,
                               'type': ret_tuple.type,
                               'length': ret_tuple.days_in_stage,
                               'days': ret_tuple.days_to_close,
                               'acv': ret_tuple.tacv
                               })

        return ret_tuple

    def values_in_history(self, rec, timeline_index):
        hist_as_of = self.history_timeline_details[timeline_index][1]['xl_date']
        hist_horizon = self.history_timeline_details[timeline_index][2]['xl_date']
        decay_factor = self.timedecay[timeline_index]
        stage = rec.getAsOfDateF(self.stage_field_name, hist_as_of)
        if (stage in self.win_stages) or (stage in self.lose_stages):
            return None
        close_date = rec.getAsOfDateF(self.close_date_field_name, hist_as_of)
        if close_date == 'N/A':
            close_date = -1
            days_to_close = 999
        else:
            try:
                close_date = float(close_date)
            except:
                import aviso.utils.dateUtils
                try:
                    close_date = aviso.utils.dateUtils.fuzzydateinput(close_date).as_xldate()
                except:
                    close_date = aviso.utils.dateUtils.fuzzydateinput(aviso.utils.dateUtils.cleanmmddyy(close_date)).as_xldate()
            days_to_close = close_date - hist_as_of

        acv = self.get_acv_as_of(rec, as_of=hist_as_of, first_value_on_or_after=True)

        final_stage = rec.getAsOfDateF(self.stage_field_name, self.as_of)
        destiny, time_to_destiny = 'N/A', 'N/A'
        if final_stage in self.win_stages:
            if rec.win_date:
                destiny = 'W'
                time_to_destiny = rec.win_date - hist_as_of
        elif final_stage in self.lose_stages:
            destiny = 'L'
            time_to_destiny = self.as_of - hist_as_of - rec.lengthOfTimeAtCurrValueF(self.stage_field_name, self.as_of)

        ret_value = observation_details(
            stage=stage,
            acv=acv,
            close_date=close_date,
            owner=rec.getAsOfDateF(self.owner_field_name, hist_as_of, first_value_on_or_after=True),
            type=rec.getAsOfDateF(self.type_field_name, hist_as_of, first_value_on_or_after=True),
            days_in_stage=rec.lengthOfTimeAtCurrValueF(self.stage_field_name, hist_as_of),
            days_to_close=days_to_close,
            tacv=self.transform(acv),
            hist_as_of=hist_as_of,
            hist_horizon=hist_horizon,
            decay_factor=decay_factor,
            acv_horizon=self.get_acv_as_of(rec, as_of=hist_horizon, first_value_on_or_after=True),
            stage_horizon=rec.getAsOfDateF(self.stage_field_name, hist_horizon),
            destiny=destiny,
            time_to_destiny=time_to_destiny,
        )

        if probe_util.probing:
            probe_util.signal("Values in history rec {histid} are: "
                              "  stage: [{stage}] owner: [{owner}] "
                              "  type: [{type}] days_in_stage: {length}] "
                              "  days_to_close [{days}], tACV:[{acv}]"
                              "  destiny: [{destiny}] time_to_destiny: [{time_to_destiny}] ",
                              {'stage': ret_value.stage,
                               'owner': ret_value.owner,
                               'type': ret_value.type,
                               'length': ret_value.days_in_stage,
                               'days': ret_value.days_to_close,
                               'acv': ret_value.tacv,
                               'destiny': ret_value.destiny,
                               'time_to_destiny': ret_value.time_to_destiny,
                               'histid': rec.ID
                               })
        return ret_value

    def create_observations(self, record):
        # if you only want bookings, then there is no need to create observations
        if not self.history_timeline_details:
            self.setuptimeline()
        if not self.forecast_params['run_forecast']:
            return []
        observations = []

        terminal_date = 10000000000.0
        if 'terminal_fate' in record.featMap:
            terminal_date = record.featMap['terminal_fate'][-1][0]
        for i, time_points in enumerate(self.history_timeline_details):
            hist_poi_begin = time_points[0]['xl_date']
            obs_date = time_points[1]['xl_date']
            max_date = max(obs_date, hist_poi_begin)
            if(record.created_date <= obs_date and max_date < terminal_date):
                obs_tuple = self.values_in_history(record, i)
                if not obs_tuple:
                    continue
                observations.append((record, obs_tuple))
        return observations

    def combine_results(self, results, combine_options):
        combine_options['quantiles_corrections'] = self.forecast_params.get("quantiles_corrections", False)

        return CombineResults.combine_results(results, combine_options, version='b')

    def get_result(self, record):
        # cython.declare() is slow when not compiling
        # so we only use it when we are compiling
        if cython.compiled:
            running_result = cython.declare(RunningResults)

        ret_value = {}

        try:
            acv = self.get_acv_as_of(record,
                                     as_of=self.as_of,
                                     first_value_on_or_after=True)

            if self.apply_filters:
                acv_begin = self.get_acv_as_of(record, as_of=self.time_horizon.beginsF)
                if (acv < self.forecast_params['filter_active_acv_lo']) \
                        or (acv > self.forecast_params['filter_active_acv_hi']) \
                        or (acv_begin < self.forecast_params['filter_begin_acv_lo']) \
                        or (acv_begin > self.forecast_params['filter_begin_acv_hi']):
                    err_msg = "filtered out begin_acv={begin_acv} active_acv={active_acv}"
                    err_params = {'begin_acv': acv_begin, 'active_acv': acv}
                    raise NoResultException(err_msg, err_params)

            # TODO: consider making this an assert because when it gets here it should really be active
            if self.is_active(record):
                birthdate = record.featMap[self.stage_field_name][0][0]
                existing_pipe = False
                if birthdate < self.time_horizon.beginsF:
                    existing_pipe = True
                ret_value['active_amount'] = acv
                if existing_pipe:
                    ret_value['existing_pipe_active_amount'] = acv
                else:
                    ret_value['new_pipe_active_amount'] = acv

            # if you only want bookings, then there is no forecast result details to be added...
            if not self.forecast_params['run_forecast']:
                return ret_value

            current_stage = record.getAsOfDateF(self.stage_field_name, self.as_of)
            current_type = record.getAsOfDateF(self.type_field_name, self.as_of, first_value_on_or_after=True)
            current_owner = record.getAsOfDateF(self.owner_field_name, self.as_of, first_value_on_or_after=True)

            if record.ID not in self.results:
                raise_exception = True
                err_msg = "no result -"
                err_params = {}
                if current_stage in self.win_stages:
                    err_msg += " opp is in win stage"
                    if record.win_date is None:
                        err_msg += " but win date could not be determined"
                    else:
                        if record.win_date > self.as_of:
                            err_msg += " but win date is in the future"
                            is_ep = record.featMap[self.stage_field_name][0][0] <= self.time_horizon.beginsF
                            # it will be won in the poi
                            if self.time_horizon.beginsF < record.win_date <= self.time_horizon.horizonF:
                                ret_value.update({'stage_probs': {current_stage: 1.0},
                                                  'eACV': acv,
                                                  'eACV_deviation': 0.0,
                                                  'win_prob': 1.0,
                                                  'forecast': acv,
                                                  'lose_prob': 0.0,
                                                  'num_close_neighbors': 0,
                                                  'current_stage': current_stage,
                                                  'current_type': current_type,
                                                  'current_owner': current_owner,
                                                  'existing_pipe': is_ep})
                            else:  # it will be won after horizon for sure
                                ret_value.update({'stage_probs': {current_stage: 1.0},
                                                  'eACV': acv, 'eACV_deviation': 0.0,
                                                  'win_prob': 0.0, 'forecast': 0.0,
                                                  'lose_prob': 0.0,
                                                  'num_close_neighbors': 0,
                                                  'current_stage': current_stage,
                                                  'current_type': current_type,
                                                  'current_owner': current_owner,
                                                  'existing_pipe': is_ep})
                            return ret_value
                        else:
                            #  win date is in the past
                            err_msg += " but win date is in the past"
                            if record.win_date >= self.time_horizon.beginsF:
                                err_msg += " and after begin date"
                            else:
                                err_msg += " and before begin date"
                else:
                    err_msg += " type '{type}' did not have history with stage '{stage}' during period of interest"
                    err_params = {'type': current_type, 'stage': current_stage}
                if raise_exception:
                    raise NoResultException(err_msg, err_params)
            running_result = self.results[record.ID]
            cn = running_result.close_neighbors
            mn = self.min_neighbors
            prob_map = running_result.conditional_results
            total_weight = float(sum(prob_map.values()))
            if not total_weight:
                raise NoResultException("Zero Total Weight", {})

            ret_value['stage_probs'] = {}
            for stage in prob_map:
                ret_value['stage_probs'][stage] = prob_map[stage] / total_weight
            if running_result.conditional_sum_acv_diff_weights == 0.0:
                acv_diff = 0.0
                acv_diff_sq = 0.0
            else:
                acv_diff = running_result.conditional_sum_acv_diff / running_result.conditional_sum_acv_diff_weights
                acv_diff_sq = (running_result.conditional_sum_acv_diff_sq /
                               running_result.conditional_sum_acv_diff_weights)

            if(cn < mn):
                un_prob_map = running_result.unconditional_results
                total_un_weight = float(sum(un_prob_map.values()))
                un_ret_value = {}
                for stage in un_prob_map:
                    un_ret_value[stage] = un_prob_map[stage] / total_un_weight

                # Make up for the lack of close neighbors by giving them the
                # unconditional average
                for stage in ret_value['stage_probs']:
                    ret_value['stage_probs'][stage] = ((cn * ret_value['stage_probs'][stage] +
                                                        (mn - cn) * un_ret_value[stage])
                                                       / float(mn))
                if running_result.unconditional_sum_acv_diff_weights == 0.0:
                    un_acv_diff = 0.0
                    un_acv_diff_sq = 0.0
                else:
                    un_acv_diff = (running_result.unconditional_sum_acv_diff /
                                   running_result.unconditional_sum_acv_diff_weights)
                    un_acv_diff_sq = (running_result.unconditional_sum_acv_diff_sq
                                      / running_result.unconditional_sum_acv_diff_weights)
                acv_diff = ((cn * acv_diff +
                             (mn - cn) * un_acv_diff)
                            / float(mn))
                acv_diff_sq = ((cn * acv_diff_sq +
                                (mn - cn) * un_acv_diff_sq)
                               / float(mn))

            eACV = acv

            if self.eacv_model != 'acv':
                eACV += acv_diff
                if acv >= 0.0 and eACV < 0.0:
                    eACV = 0.0
                if acv < 0.0 and eACV > 0.0:
                    eACV = 0.0

            win_probability = 0.0
            lose_probability = 1.0
            if self.close_date_updated_beyond_horizon_implies_forecast_zero \
                    and record.close_date_updated_beyond_horizon:
                ret_value['close_date_updated_beyond_horizon'] = True
            elif not record.close_date_too_far:
                for stage in self.win_stages:
                    win_probability += ret_value['stage_probs'].get(stage, 0.0)
                lose_probability = 0.0
                for stage in self.lose_stages:
                    lose_probability += ret_value['stage_probs'].get(stage, 0.0)

            ret_value['eACV'] = eACV
            ret_value['eACV_deviation'] = math.sqrt(max(acv_diff_sq -
                                                        acv_diff ** 2, 0.0))
            ret_value['win_prob'] = win_probability
            ret_value['forecast'] = win_probability * eACV
            ret_value['lose_prob'] = lose_probability
            ret_value['num_close_neighbors'] = cn
            ret_value['total_weight'] = total_weight
            ret_value['neighbor_details'] = running_result.neighbor_details
            ret_value['current_stage'] = current_stage
            ret_value['current_type'] = current_type
            ret_value['current_owner'] = current_owner
            birthdate = record.featMap[self.stage_field_name][0][0]
            ret_value['existing_pipe'] = False
            if birthdate < self.time_horizon.beginsF:
                ret_value['existing_pipe'] = True

            ret_value['days_to_close'] = self.active_value_cache[record.ID].days_to_close
            if running_result.sum_winning_neighbor_weights > 0.0:

                E_to_win = running_result.conditional_time_to_win / running_result.sum_winning_neighbor_weights

                if cn < mn:
                    un_E_to_win = running_result.unconditional_time_to_win / running_result.count_winning_neighbors
                    E_to_win = ((cn * E_to_win + (mn - cn) * un_E_to_win) / float(mn))

                ret_value['expected_time_to_win'] = E_to_win

        except NoResultException as e:
            logger.debug("%s - %s", record.ID, str(e))
            raise e
        except Exception as e:
            logger.exception("%s - %s", record.ID,str(e))
            raise e
        return ret_value

    def post_process_prepare(self, res_list):
        logger.info('post_process_prepare on %s results... ' % (len(res_list)))
        if self.post_enforcements:
            import re
            post_enforcements = []
            for enf_type, enf_dtls in self.post_enforcements:
                if enf_type.startswith('bounds('):
                    (cluster_str, bounds) = enf_dtls
                    if isinstance(cluster_str, basestring):
                        cluster_def = []
                        for fld_val in cluster_str.split('~'):
                            (fld, val) = fld_val.split('=')
                            if isinstance(val, bytes):
                                val = re.compile(unicode(val).strip(), re.UNICODE)
                            else:
                                val = re.compile(val.strip())
                            cluster_def.append((fld, val))
                        post_enforcements.append((enf_type, (cluster_def, bounds)))
                elif enf_type.startswith('treat('):
                    post_enforcements.append((enf_type, enf_dtls))
                else:
                    raise NotImplementedError('post_enforcement type=%s is not implemented' % (enf_type))
            self.post_enforcements = post_enforcements

    def post_process_update(self, res_rec):
        ret = False
        if self.post_enforcements:
            for enf_type, enf_dtls in self.post_enforcements:
                if enf_type.startswith('bounds('):
                    (cluster_def, bounds) = enf_dtls
                    target_fld = enf_type[7:][:-1]
                    filter_match = True
                    for fld, val in cluster_def:
                        this_val = unicode(self.try_rec_get_val(res_rec, fld, '__MISSING__')).strip()
                        if val.match('__EXISTS__$') and this_val != '__MISSING__':
                            continue
                        elif val.match('__NOT_EXISTS__$') and this_val == '__MISSING__':
                            continue
                        elif not val.search(this_val):
                            filter_match = False
                            break
                    if filter_match:
                        target_val = self.try_rec_get_val(res_rec, target_fld, '__MISSING__')
                        if target_val == '__MISSING__':
                            if len(bounds) > 2:
                                if self.try_rec_set_val(res_rec, target_fld, bounds[2], '__FAIL__') == '__FAIL__':
                                    logger.warning("WARNING: unable to set target_fld=%s for record %s" % (
                                        target_fld, res_rec.extid))
                                else:
                                    ret = True
                        else:
                            lower_bound = bounds[0]
                            upper_bound = bounds[1]
                            if lower_bound is not None and target_val < lower_bound:
                                if self.try_rec_set_val(res_rec, target_fld, lower_bound, '__FAIL__') == '__FAIL__':
                                    logger.warning("WARNING: unable to set target_fld=%s for record %s" % (
                                        target_fld, res_rec.extid))
                                else:
                                    ret = True
                            elif upper_bound is not None and target_val > upper_bound:
                                if self.try_rec_set_val(res_rec, target_fld, upper_bound) == '__FAIL__':
                                    logger.warning("WARNING: unable to set target_fld=%s for record %s" % (
                                        target_fld, res_rec.extid))
                                else:
                                    ret = True
                elif enf_type.startswith('treat('):
                    target_fld = enf_type[6:][:-1]
                    target_treatment = enf_dtls
                    if self.apply_enforcement(res_rec, target_fld, target_treatment) == '__FAIL__':
                        logger.warning("WARNING: unable to set target_fld=%s for record %s" % (
                            target_fld, res_rec.extid))
                    else:
                        ret = True
                else:
                    raise NotImplementedError('post_enforcement type=%s is not implemented' % (enf_type))
        return ret

    def try_rec_get_val(self, res_rec, fld, default_val=None):
        val = default_val
        try:
            if fld.startswith('res.'):
                val = res_rec.results[fld[4:]]
            elif fld.startswith('dim.'):
                val = res_rec.dimensions[fld[4:]]
            elif fld.startswith('uip.'):
                val = res_rec.uipfield[fld[4:]]
        except:
            pass
        return val

    def try_rec_set_val(self, res_rec, fld, val, fail_val=None):
        if fld.startswith('res.'):
            res_rec.results[fld[4:]] = val
        elif fld.startswith('dim.'):
            res_rec.dimensions[fld[4:]] = val
        elif fld.startswith('uip.'):
            res_rec.uipfield[fld[4:]] = val
        else:
            return fail_val
        return val

    def apply_enforcement(self, res_rec, fld, treatment, fail_val='__FAIL__'):
        val = eval('lambda uip=res_rec.uipfield, res=res_rec.results, dim=res_rec.dimensions: '+treatment)()
        if fld.startswith('res.'):
            res_rec.results[fld[4:]] = val
        elif fld.startswith('dim.'):
            res_rec.dimensions[fld[4:]] = val
        elif fld.startswith('uip.'):
            res_rec.uipfield[fld[4:]] = val
        else:
            return fail_val
        return val