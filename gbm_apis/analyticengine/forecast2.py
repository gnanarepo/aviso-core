from collections import namedtuple
import functools

import logging
import math
from aviso.framework.diagnostics import probe_util
import math

from ..analyticengine.forecast import Forecast
from ..analyticengine.forecast2_default_config import *
from ..analyticengine.NoresultException import NoResultException
import numpy as np
from aviso.utils.dateUtils import epoch
import cython

# DO NOT REMOVE THE BELOW UNUSED IMPORTS, THEY ARE REQUIRED FOR EVAL
logger = logging.getLogger('gnana.%s' % __name__)
long=int
totally_dissimilar = cython.declare(cython.double, -1.0)
log_of_two_py = math.log(2.0)


def probe_namedtuple(a_tuple, update_dict):
    if probe_util.probing:
        sig_val = dict(a_tuple._asdict())
        sig_val.update(update_dict)
        sig_text = functools.reduce(lambda x, y: x + y, list(map(lambda x: x + ':[{' + x + '}], ', var_list)))
        probe_util.signal(sig_text, sig_val)


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


@cython.cclass
class RunningResultsVector:
    # vectorized running results to allow transparent vector computation with many alg
    # params

    # cython.declare() is slow when not compiling
    # so we only use it when we are compiling
    if cython.compiled:
        vector_size = cython.declare(cython.int)
        conditional_results = cython.declare(dict)
        unconditional_results = cython.declare(dict)

        close_neighbors = cython.declare(list)
        total_neighbors = cython.declare(cython.int)
        sum_winning_neighbor_weights = cython.declare(list)
        count_winning_neighbors = cython.declare(cython.int)

    def __init__(self, vector_size):
        self.vector_size = vector_size
        self.conditional_results = {}
        self.unconditional_results = {}

        self.close_neighbors = [0 for i in range(vector_size)]
        self.total_neighbors = 0
        self.sum_winning_neighbor_weights = [0.0 for i in range(vector_size)]
        self.count_winning_neighbors = 0


@cython.cfunc
@cython.returns(cython.double)
@cython.locals(max_days_in_stage=cython.double)
@cython.locals(distance_variables_length=cython.int)
def get_similarity(actv_vals,
                   hist_vals,
                   distance_variables,
                   distance_variables_index,
                   distance_variables_length,
                   forecast_params,
                   max_days_in_stage,
                   menu,
                   horizon_boq,
                   horizon_eoq):

    if cython.compiled:
        distance = cython.declare(cython.double)
        ret_val = cython.declare(cython.double)

        hist_int = cython.declare(cython.long)
        act_int = cython.declare(cython.long)
        hist_f = cython.declare(cython.double)
        act_f = cython.declare(cython.double)
        dist = cython.declare(cython.double)
        dd = cython.declare(cython.double)

    # Start adding distances
    distance = forecast_params.get('min_distance', 0.0)

    for index in range(distance_variables_length):
        variable_index = distance_variables_index[index]
        variable = distance_variables[index]
        if menu[variable]['category'] == 'dissimilar':
            act_int = actv_vals[variable_index]
            hist_int = hist_vals[variable_index]
            if hist_int != act_int:
                return totally_dissimilar
        elif menu[variable]['category'] == 'dissimilar_if_none':
            # TODO: Make this not suck.
            if actv_vals[variable_index] == None:
                return totally_dissimilar
            else:
                distance += actv_vals[variable_index]
        elif menu[variable]['category'] == 'dist':
            dist = menu[variable]['value']
            act_int = actv_vals[variable_index]
            hist_int = hist_vals[variable_index]
            if hist_int != act_int:
                distance += dist
        elif menu[variable]['category'] == 'diffsq':
            act_f = actv_vals[variable_index]
            hist_f = hist_vals[variable_index]
            dd = (act_f - hist_f) ** 2
            distance += dd
        elif menu[variable]['category'] == 'max':
            act_f = actv_vals[variable_index]
            hist_f = hist_vals[variable_index]
            if cython.compiled:
                dd = math.fmax(act_f, hist_f)
            else:
                dd = max(act_f, hist_f)
            distance += dd
        elif menu[variable]['category'] == 'diffsq-0':
            act_f = actv_vals[variable_index]
            hist_f = hist_vals[variable_index]
            dd = 0.0
            if act_f != 0.0 and hist_f != 0.0:
                dd = (act_f - hist_f) ** 2
                distance += dd

        else:
            raise ValueError(
                'menu[' + variable + "]['category'] has to be either dissimilar, dist, diffsq, max or diffsq-0")

    # Hessam: I did not fully do this logic in default config- esp. the part about +3
    # will do once i find a config/tenant that actually uses it

    #     if 'forecast_category' in menu:
    #         if hist_forecast_category != 'N/A':
    #             if forecast_category != 'N/A':
    #                 if menu['forecast_category']['category'] == 'dissimilar':
    #                     if forecast_category != hist_forecast_category:
    #                         return totally_dissimilar
    #                 elif menu['forecast_category']['category'] == 'dist':
    #                     forecast_category_diff_dist = menu['forecast_category']['value']
    #                     if forecast_category != hist_forecast_category:
    #                         distance += forecast_category_diff_dist
    #                 else:
    #                     raise ValueError("menu['forecast_category']['category'] has to be either dissimilar or dist")
    #         elif forecast_category != 'N/A':
    #             distance += 3

    # Use the C library functions if we are compiling otherwise use Python ones;
    # Turning the distance into similarity
    distance *= forecast_params.get('coeff_distance', 1.0)
    if cython.compiled:
        ret_val = math.exp(-distance)
    else:
        ret_val = math.exp(-distance)

    return ret_val


@cython.cfunc
#@cython.returns(cython.double)
@cython.locals(max_days_in_stage=cython.double)
@cython.locals(distance_variables_length=cython.int)
def get_similarity_array(actv_vals,
                         hist_vals,
                         distance_variables,
                         distance_variables_index,
                         distance_variables_length,
                         forecast_params,
                         max_days_in_stage,
                         menu,
                         horizon_boq,
                         horizon_eoq):

    if cython.compiled:
        #distance  = cython.declare(cython.double)
        #ret_val = cython.declare(cython.double)
        hist_int = cython.declare(cython.long)
        act_int = cython.declare(cython.long)
        #hist_f = cython.declare(cython.double)
        #act_f = cython.declare(cython.double)
        #dist = cython.declare(cython.double)
        #dd =  cython.declare(cython.double)

    # Start adding distances

    distance = forecast_params.get('min_distance', 0.0) * np.ones(menu['ndarray_shape'])

    for index in range(distance_variables_length):
        variable_index = distance_variables_index[index]
        variable = distance_variables[index]
        if menu[variable]['category'] == 'dissimilar':
            act_int = actv_vals[variable_index]
            hist_int = hist_vals[variable_index]
            if hist_int != act_int:
                return totally_dissimilar
        elif menu[variable]['category'] == 'dist':
            dist = menu[variable]['value']
            act_int = actv_vals[variable_index]
            hist_int = hist_vals[variable_index]
            if hist_int != act_int:
                distance += dist
        elif menu[variable]['category'] == 'diffsq':
            act_f = actv_vals[variable_index]
            hist_f = hist_vals[variable_index]
            dd = (act_f - hist_f) ** 2
            distance += dd
        elif menu[variable]['category'] == 'max':
            act_f = actv_vals[variable_index]
            hist_f = hist_vals[variable_index]
            if cython.compiled:
                dd = math.fmax(act_f, hist_f)
            else:
                dd = max(act_f, hist_f)
            distance += dd
        elif menu[variable]['category'] == 'diffsq-0':
            act_f = actv_vals[variable_index]
            hist_f = hist_vals[variable_index]
            dd = 0.0
            if np.any(act_f) and np.any(hist_f):
                dd = (act_f - hist_f) ** 2
                distance += dd

        else:
            raise ValueError(
                'menu[' + variable + "]['category'] has to be either dissimilar, dist, diffsq, max or diffsq-0")

    # Hessam: I did not fully do this logic in default config- esp. the part about +3
    # will do once i find a config/tenant that actually uses it

    #     if 'forecast_category' in menu:
    #         if hist_forecast_category != 'N/A':
    #             if forecast_category != 'N/A':
    #                 if menu['forecast_category']['category'] == 'dissimilar':
    #                     if forecast_category != hist_forecast_category:
    #                         return totally_dissimilar
    #                 elif menu['forecast_category']['category'] == 'dist':
    #                     forecast_category_diff_dist = menu['forecast_category']['value']
    #                     if forecast_category != hist_forecast_category:
    #                         distance += forecast_category_diff_dist
    #                 else:
    #                     raise ValueError("menu['forecast_category']['category'] has to be either dissimilar or dist")
    #         elif forecast_category != 'N/A':
    #             distance += 3

    # Turning the distance into similarity
    distance *= forecast_params.get('coeff_distance', 1.0)
    ret_val = np.exp(-distance)

    return ret_val


class Forecast2(Forecast):

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
|   calib_with_vectors   |  False    | The model will produce a vector of  |
|                        |           | probs generated w/ diff alg params. |
+------------------------+-----------+-------------------------------------+





Field Map:
    * type: Which field signifies the type.  Opportunities with different type
      are trained differently.
    * stage: Which field signifies the stage of the opportunity.
    * acv: Which field contains the amount of winnings if the opportunity is own
    * owner: Who is the owner of the opportunity. Differences in the owner are
      used to estimate similarity

.. note:: Please note that . . .

    * At present there is no way to configure a custom distance function,
      except the bandwidth parameters
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
    # combine_options derived from forecast

    calibration_options = ['menu']

    def __init__(self, field_map, config, time_horizon, dimensions):
        super(Forecast2, self).__init__(field_map, config, time_horizon, dimensions)
        self.field_map.update(field_map)
        self.active_value_cache = {}
        self.close_date_field_name = field_map["close_date"]
        self.owner_field_name = field_map['owner']
        self.type_field_name = field_map['type']
        self.min_neighbors_ratio = self.forecast_params["small_num_neighbors_ratio"]
        self.min_neighbors = self.forecast_params["small_num_neighbors"]

        self.active_variable_config = {}  # dict(mandetory_active_variable_config)
        self.active_variable_config.update(
            self.forecast_params.get('active_variable_config', default_active_variable_config))

        self.history_variable_config = dict(mandetory_history_variable_config)
        self.history_variable_config.update(
            self.forecast_params.get('history_variable_config', default_history_variable_config))
        self.history_variable_config.update(self.active_variable_config)

        self.active_variables = self.forecast_params.get('active_variables', active_details_list)
        self.history_variables = self.active_variables + mandetory_history_details_list
        self.history_variables = self.history_variables + \
                                 self.forecast_params.get('history_variables', history_details_list)

        self.active_details = namedtuple('active_details', self.active_variables)
        self.history_details = namedtuple('history_details', self.history_variables)

        self.forecast_menu = self.forecast_params.get('distance_config', {})
        if not self.forecast_menu:
            self.forecast_menu = default_distance_config
        self.distances_used = self.forecast_params.get('distances_used', default_distances_used)

        input_distance_variables = self.forecast_params.get('distance_variables', default_distance_variables)
        self.num_distance_types = len(self.distances_used)
        if self.num_distance_types < 1:
            raise ValueError(
                'You at least need 1 distance function used. Update distances_used in your forecast params.')
        self.distance_variables = {}
        for dist_type in self.distances_used:
            self.distance_variables[dist_type] = input_distance_variables[dist_type]

        active_fields = list(self.active_details._fields)
        self.distance_variables_index = {}
        for dist_type in self.distances_used:
            self.distance_variables_index[dist_type] = list(map(
                lambda x: active_fields.index(x), self.distance_variables[dist_type]))
        self.distance_variables_length = {}
        for dist_type in self.distances_used:
            self.distance_variables_length[dist_type] = len(self.distance_variables[dist_type])
        # to be backwards compatible with legacy runs

        if 'owner_diff_dist' in self.forecast_params and 'owner_code' in self.forecast_menu:
            if 'value' in self.forecast_menu['owner_code']:
                self.forecast_menu['owner_code']['value'] = self.forecast_params['owner_diff_dist']

        self.forecast_menu.update(self.forecast_params.get('calibration menu', {}))

        if self.forecast_params.get('previous_next_time_in_stage', None) and 'stage' in self.forecast_menu:
            self.forecast_menu['stage_int']['category'] = 'dissimilar_if_none'

        # set up ndarrays for calculation
        self.calib_with_vectors = (self.forecast_params.get('calib_with_vectors', False))
        if self.calib_with_vectors:
            # for model parameter that can be made an array, check if it's an array and keep track of the ones that are
            params_list = []
            param_vals = {}

            for kernel_param in ['small_kernel_weight', 'max_kernel_weight', 'small_num_neighbors', 'small_num_neighbors_ratio']:
                if isinstance(self.forecast_params[kernel_param], list):
                    params_list.append(kernel_param)
                    param_vals[kernel_param] = self.forecast_params[kernel_param]

            for variable in self.distance_variables[self.distances_used[0]]:
                if self.forecast_menu[variable]['category'] == 'dist' and isinstance(self.forecast_menu[variable]['value'], list):
                    params_list.append(variable)
                    param_vals[variable] = self.forecast_menu[variable]['value']

            bw_params = [param for param in sorted(list(self.forecast_params.keys()))
                         if param.endswith('_bw') and isinstance(self.forecast_params[param], list)]
            for param in bw_params:
                params_list.append(param)
                param_vals[param] = self.forecast_params[param]

            # Params List and Param Vals look something like this
            #params_list = ['a','b','c']
            #param_vals = {'a':[1,2],'b':[10,20,30],'c':[100,200,300]}
            shape = list(map(lambda x: len(param_vals[x]), params_list))

            # ndarray_dict['a'] gives the ndarray of the values of the parameter a to be used in the computation
            # the arrays are set so that, if 'a' is the ith parameter in params_list, the ith coordinate of the result
            # ndarray determines which value of 'a' was used.
            ndarray_dict = {}
            for (index, param) in enumerate(params_list):
                ndarray_dict[param] = np.zeros(shape)
                for coords in np.ndindex(*shape):
                    ndarray_dict[param][coords] = param_vals[params_list[index]][coords[index]]

            for variable in self.distance_variables[self.distances_used[0]]:
                if 'value' in self.forecast_menu[variable]:
                    if variable in ndarray_dict:
                        self.forecast_menu[variable]['value'] = ndarray_dict[variable]

            if "small_num_neighbors_ratio" in ndarray_dict:
                self.min_neighbors_ratio = ndarray_dict["small_num_neighbors_ratio"]
            if "small_num_neighbors" in ndarray_dict:
                self.min_neighbors = ndarray_dict["small_num_neighbors"]
            if "max_kernel_weight" in ndarray_dict:
                self.max_kernel_weight = ndarray_dict["max_kernel_weight"]
            if "small_kernel_weight" in ndarray_dict:
                self.forecast_params["small_kernel_weight"] = ndarray_dict["small_kernel_weight"]

            for param in bw_params:
                self.forecast_params[param] = ndarray_dict[param]

            self.forecast_menu['ndarray_shape'] = shape
            self.ndarray_shape = shape

            # Baggage Flds will not be written to DB when running in lightweight mode.
            if self.forecast_params.get('lightweight', None):
                self.baggage_flds = {'current_owner',
                                     'num_close_neighbors',
                                     'num_all_neighbors',
                                     'stage_probs',
                                     'lose_prob',
                                     'days_to_close',
                                     'current_stage',
                                     'current_type',
                                     'expected_time_to_win',
                                     'historic_details',
                                     'neighbor_details',
                                     'total_weight'}

    def get_batch_task_criteria(self, format='xl'):
        """
        When running model, we will only iterate over records which match this
        filter.

        For EPF, we only examine records with terminal_date > beginning of the current period.
        Framework will take care of the restriction that the created_date < as_of

        Return None if no filtering can be done at the DB level.
        """

        if not self.history_timeline_details:
            self.setuptimeline()

        if format == 'xl':
            current_period_begin = self.time_horizon.beginsF
            #hist_begin = self.history_timeline_details[num_cycles_unborn-1][0]['period_begin_xl']
        elif format == 'epoch':
            current_period_begin = epoch(self.time_horizon.begins).as_epoch()
        else:
            raise ValueError("ERROR: input for format parameter must be either xl or epoch (it was '%s')" % format)
        return {'object.terminal_date': {'$gte': current_period_begin}}

    def get_comparison_task_criteria(self, format='xl'):
        """
        When running model, we will only compare with records which match this
        filter.

        For EPF, we only examine records with terminal_date > beginning of the current period.
        Framework will take care of the restriction that the created_date < as_of

        Return None if no filtering can be done at the DB level.
        """

        if not self.history_timeline_details:
            self.setuptimeline()

        if format == 'xl':
            last_hist_as_of = self.history_timeline_details[0][1]['xl_date']
            first_hist_as_of = self.history_timeline_details[-1][1]['xl_date']
        elif format == 'epoch':
            last_hist_as_of = self.history_timeline_details[0][1]['epoch_date']
            first_hist_as_of = self.history_timeline_details[-1][1]['epoch_date']
        else:
            raise ValueError("ERROR: input for format parameter must be either xl or epoch (it was '%s')" % format)
        return {'$and': [{'object.created_date': {'$lte': last_hist_as_of}},
                         {'object.terminal_date': {'$gte': first_hist_as_of}}]}

    def field_requested(self):
        """
        Make a list of reqired fields.
        """
        return None

    def process_observation(self, record, observation):
        # cython.declare() is slow when not compiling,
        # so we only use it when we are compiling
        if cython.compiled:
            running_result = cython.declare(RunningResults)
            running_result_vector = cython.declare(RunningResultsVector)

            weight = cython.declare(cython.double)
            weight_list = cython.declare(list)
            old_weight = cython.declare(cython.double)
            decay_factor = cython.declare(cython.double)

            acv_as_of = cython.declare(cython.double)
            acv_horizon = cython.declare(cython.double)
            acv_diff = cython.declare(cython.double)
            acv_diff_sq = cython.declare(cython.double)

        history_record = observation[0]
        hist_vals = observation[1]
        hist_as_of = hist_vals.hist_as_of
        hist_horizon = hist_vals.hist_horizon
        hist_horizon_eoq = hist_vals.hist_horizon_eoq
        decay_factor = hist_vals.decay_factor
        stage_at_horizon = hist_vals.stage_horizon
        stage_at_horizon_eoq = hist_vals.stage_horizon_eoq
        try:
            acv_as_of = hist_vals.acv
        except:
            acv_as_of = 0.0
        acv_horizon = hist_vals.acv_horizon

        try:
            actv_vals = self.active_value_cache[record.ID]
        except KeyError:
            actv_vals = self.values_in_active(record)
            self.active_value_cache[record.ID] = actv_vals

        try:
            active_trans_acv = actv_vals.tacv
        except:
            try:
                active_trans_acv = actv_vals.acv
            except:
                active_trans_acv = 0.0
        try:
            active_stage = actv_vals.stage
        except:
            active_stage = 'N/A'

        if active_stage in self.win_stages and not record.win_date:
            return

        menu = self.forecast_menu

        # TODO: Move this logic inside get_similarity w/ array of historical stages.
        if self.forecast_params.get('previous_next_time_in_stage', None):
            previous_next_time_uses_horizon_eoq = self.forecast_params.get('previous_next_time_uses_horizon_eoq', True)
            previous_date, next_date = history_record.firstKnownTimesF(self.stage_field_name, active_stage, hist_as_of)
            pnt_horizon = hist_horizon
            if previous_next_time_uses_horizon_eoq:
                pnt_horizon = hist_horizon_eoq
            gap_in_stage_days = None
            which = 0
            if previous_date:
                which = -1
                if next_date and next_date - hist_as_of < hist_as_of - previous_date:
                    which = 1
            elif next_date:
                which = 1
            if which == 1 and next_date <= pnt_horizon:
                gap_in_stage_days = next_date - hist_as_of
            elif which == -1 and previous_date >= pnt_horizon - 92:
                gap_in_stage_days = hist_as_of - previous_date
            if gap_in_stage_days is not None:
                decay_per_day = self.forecast_params.get('decay_per_day', 30)
                ratio = pnt_horizon - hist_as_of
                if ratio:
                    gap_in_stage_days *= decay_per_day / ratio
                elif gap_in_stage_days:
                    gap_in_stage_days = None
            else:
                gap_in_stage_days = None
            actv_vals = actv_vals._replace(stage_int=gap_in_stage_days)

        if not self.calib_with_vectors:
            if self.forecast_params.get('use_old_similarity', False):
                weight = get_similarity_old(actv_vals,
                                            hist_vals,
                                            self.forecast_params,
                                            self.max_days_in_stage,
                                            menu,
                                            self.horizon_eoq)
            else:
                weight_list = []
                for dist_type in self.distances_used:
                    weight_list.append(get_similarity(actv_vals,
                                                      hist_vals,
                                                      self.distance_variables[dist_type],
                                                      self.distance_variables_index[dist_type],
                                                      self.distance_variables_length[dist_type],
                                                      self.forecast_params,
                                                      self.max_days_in_stage,
                                                      menu,
                                                      self.horizon_boq,
                                                      self.horizon_eoq))
                if self.num_distance_types == 1:
                    weight = weight_list[0]

                weight = min(weight, self.max_kernel_weight)

            # TODO: why do we have this here when we already have the num_close_neighbors below?
            historic_records_details = self.forecast_params.get("historic_records_details", False)
            if historic_records_details:
                try:
                    running_result = self.results[record.ID]
                except KeyError:
                    running_result = RunningResults()
                historic_details = running_result.historic_details
                historic_detail = (history_record.ID, dict(hist_vals._asdict()))
                historic_details.append(historic_detail)

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

            # performance is critical, since this will be called many times.
            # Assume keys are present and create on exception
            try:
                running_result = self.results[record.ID]
            except KeyError:
                running_result = RunningResults()
                self.results[record.ID] = running_result

            try:
                running_result.conditional_results[stage_at_horizon] += weight
            except KeyError:
                running_result.conditional_results[stage_at_horizon] = weight

            try:
                running_result.unconditional_results[stage_at_horizon] += 1
            except KeyError:
                running_result.unconditional_results[stage_at_horizon] = 1

            running_result.total_neighbors += 1
            if weight > self.forecast_params["small_kernel_weight"]:
                running_result.close_neighbors += 1

            try:
                destiny = hist_vals.destiny
                time_to_destiny = hist_vals.time_to_destiny

                if destiny == 'W':
                    running_result.sum_winning_neighbor_weights += weight
                    running_result.count_winning_neighbors += 1.0
                    running_result.conditional_time_to_win += weight * time_to_destiny
                    running_result.unconditional_time_to_win += time_to_destiny
            except:
                pass

            # TODO: this is a bad idea, it could look past as_of data of active and is data snoop.
            if self.num_neighbor_details or (stage_at_horizon_eoq in self.win_stages):
                acv_final = hist_vals.acv_final

            if stage_at_horizon_eoq in self.win_stages:
                separate_zero_acv = self.forecast_params["separate_zero_acv_for_eacv_model"]
                eACV_for_zero_acv_diff = self.forecast_params["eACV_for_zero_acv_diff"]
                acv_diff_cap = self.forecast_params.get('acv_diff_cap', None)
                acv_diff_contrib = (not separate_zero_acv) \
                                   or (active_trans_acv == 0.0 and acv_as_of == 0.0) \
                                   or (active_trans_acv != 0.0 and acv_as_of != 0.0)
                if eACV_for_zero_acv_diff:
                    if (active_trans_acv == 0.0 and acv_as_of == 0.0) or (active_trans_acv != 0.0 and acv_as_of != 0.0):
                        acv_diff = acv_final - acv_as_of
                        if acv_diff_cap:
                            if acv_diff > acv_diff_cap:
                                acv_diff = acv_diff_cap
                            elif acv_diff < - acv_diff_cap:
                                acv_diff = -acv_diff_cap
                        acv_diff_sq = acv_diff ** 2
                        running_result.conditional_sum_acv_diff += weight * acv_diff
                        running_result.unconditional_sum_acv_diff += acv_diff
                        running_result.conditional_sum_acv_diff_sq += weight * acv_diff_sq
                        running_result.unconditional_sum_acv_diff_sq += acv_diff_sq
                        running_result.conditional_sum_acv_diff_weights += weight
                        running_result.unconditional_sum_acv_diff_weights += 1.0
                    elif active_trans_acv == 0.0 and acv_as_of != 0.0:
                        acv_diff = acv_final
                        acv_diff_sq = 0
                        running_result.conditional_sum_acv_diff += weight * acv_diff
                        running_result.unconditional_sum_acv_diff += acv_diff
                        running_result.conditional_sum_acv_diff_sq += weight * acv_diff_sq
                        running_result.unconditional_sum_acv_diff_sq += acv_diff_sq
                        running_result.conditional_sum_acv_diff_weights += weight
                        running_result.unconditional_sum_acv_diff_weights += 1.0
                elif acv_diff_contrib:
                    acv_diff = acv_final - acv_as_of
                    if acv_diff_cap:
                        if acv_diff > acv_diff_cap:
                            acv_diff = acv_diff_cap
                        elif acv_diff < - acv_diff_cap:
                            acv_diff = -acv_diff_cap
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
                detail = dict(hist_vals._asdict())
                detail.update({'old_weight': old_weight, 'new_weight': weight, })
                neighbor_detail = (history_record.ID, detail)
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

        # calibrate with vectors
        else:

            weight_list = []
            for dist_type in self.distances_used:
                weight_list.append(get_similarity_array(actv_vals,
                                                        hist_vals,
                                                        self.distance_variables[dist_type],
                                                        self.distance_variables_index[dist_type],
                                                        self.distance_variables_length[dist_type],
                                                        self.forecast_params,
                                                        self.max_days_in_stage,
                                                        menu,
                                                        self.horizon_boq,
                                                        self.horizon_eoq))
                if self.num_distance_types == 1:
                    weight_array = weight_list[0]

            # Ignore if weight is totally_dissimilar
            # This means that completely dis-similar items are not counted
            # into the equation
            # Example: New Business Vs Renewal will return a weight of zero
            if isinstance(weight_array, float) and weight_array == totally_dissimilar:
                return

            weight_array = np.minimum(weight_array, self.max_kernel_weight)

            # if the observation is not seasonal
            if decay_factor == 0.0:
                return

            weight_array *= decay_factor
            weight_array = np.maximum(weight_array, self.forecast_params['tiny'])

            # performance is critical, since this will be called many times.
            # Assume keys are present and create on exception
            try:
                running_result_vector = self.results[record.ID]
            except KeyError:
                running_result_vector = RunningResultsVector(np.product(np.array(self.ndarray_shape)))
                self.results[record.ID] = running_result_vector

            try:
                running_result_vector.conditional_results[stage_at_horizon] += weight_array
            except KeyError:
                running_result_vector.conditional_results[stage_at_horizon] = weight_array

            try:
                running_result_vector.unconditional_results[stage_at_horizon] += 1
            except KeyError:
                running_result_vector.unconditional_results[stage_at_horizon] = 1

            running_result_vector.total_neighbors += 1
            running_result_vector.close_neighbors = \
                list(np.array(running_result_vector.close_neighbors).reshape(
                    self.ndarray_shape) + (weight_array > self.forecast_params["small_kernel_weight"]))

            try:
                destiny = hist_vals.destiny

                if destiny == 'W':
                    running_result_vector.sum_winning_neighbor_weights = \
                        list(np.array(running_result_vector.sum_winning_neighbor_weights).reshape(
                            self.ndarray_shape) + weight_array)
                    running_result_vector.count_winning_neighbors += 1
            except:
                pass

            # MATT TODO write array acv model

        return

    @staticmethod
    def transform(x):
        y = x > 0 and max(x, 1.0) or -1.0 / min(x, -1.0)
        return math.log(y)

    def set_values(self,
                   record,
                   variable_def,
                   var_list,
                   record_detail,
                   obs_time,
                   obs_horizon,
                   obs_horizon_boq,
                   obs_horizon_eoq,
                   decay_factor,
                   run_as_of,
                   stage,
                   as_of_spread=0
                   ):
        forecast_params = self.forecast_params
        as_of = obs_time
        horizon = obs_horizon
        horizon_boq = obs_horizon_boq
        horizon_eoq = obs_horizon_eoq
        env_variables = {'as_of': as_of,
                         'as_of_spread': as_of_spread,
                         'horizon': horizon,
                         'horizon_boq': horizon_boq,
                         'horizon_eoq': horizon_eoq,
                         'run_as_of': run_as_of,
                         'decay_factor': decay_factor,
                         'run_as_of': run_as_of,
                         'stage': stage}
        functions = {'get_as_of': record.getAsOfSpreadDateF,  # record.getAsOfDateF ,
                     'get_length_of_time': record.lengthOfTimeAtCurrValueF,
                     'get_latest_timestamp': record.getLatestTimeStamp,
                     'get_latest': record.getLatest}
        values = {}
        for variable_name in var_list:

            if variable_name not in variable_def:
                raise ValueError('You asked for variable', variable_name, 'but did not define it')

            if variable_def[variable_name].get('env_variable', False):
                try:
                    value = env_variables[variable_def[variable_name]['value']]
                except:
                    raise ValueError('Exception in calculating for env variable ' +
                                     variable_name + ' for record ' + record.ID)
                values[variable_name] = value
                continue

            if 'variable_name' in variable_def[variable_name]:
                # this variable is derived from another variable
                try:
                    value = values[variable_def[variable_name]['variable_name']]
                except:
                    raise ValueError('the value ', variable_def[variable_name][
                        'variable_name'] + 'should be defined first in variable defs to be used in' + variable_name)

                expression_try = variable_def[variable_name]['expression_try']
                error_msg = ''
                done = False
                for expression in expression_try:
                    if not done:
                        try:
                            value = eval(expression)
                            done = True
                        except:
                            error_msg += 'Exception in the expression_try for variable ' + \
                                         variable_name + ' for record ' + record.ID + ' with value ' + str(value)  + \
                                         ' and expression ' + expression + '. '
                if not done:
                    raise ValueError(error_msg)
                values[variable_name] = value
                continue

            field_name = variable_def[variable_name]['field_name']
            time = variable_def[variable_name].get('time', None)
            function = variable_def[variable_name].get('function', 'get_as_of')
            fv = variable_def[variable_name].get('fv', False)
            as_of_spread = variable_def[variable_name].get('as_of_spread', None)
            default_value = variable_def[variable_name].get('default_value', 'N/A')
            data_field_name = self.field_map.get(field_name, field_name)
            params = [data_field_name]
            if time:
                params.append(env_variables[time])
            if fv:
                params.append(fv)
            if as_of_spread is not None:
                if as_of_spread:
                    params.append(as_of_spread)
            value = functions[function](*params)
            if value == 'N/A':
                value = default_value

            expression_try = variable_def[variable_name].get('expression_try', None)
            if expression_try:
                error_msg = ''
                done = False
                for expression in expression_try:
                    if not done:
                        try:
                            value = eval(expression)
                            done = True
                        except:
                            error_msg += 'Exception in the expression_try for variable ' + \
                                         variable_name + ' for record ' + record.ID + ' with value ' + str(value)  + \
                                         ' and expression ' + expression + '. '
                if not done:
                    raise ValueError(error_msg)

            transform = variable_def[variable_name].get('transform', None)
            if transform:
                value = eval(transform)
            values[variable_name] = value

        ret_tuple = record_detail(**values)

        return ret_tuple

    def values_in_active(self, active_record):
        spread = self.forecast_params.get("as_of_spread", 0)
        stage = self.forecast_params.get("stage_override", active_record.getAsOfSpreadDateF(self.stage_field_name,
                                                                                            self.as_of,
                                                                                            spread=spread))
        #stage = active_record.getAsOfDateF(self.stage_field_name, self.as_of)
        return self.set_values(active_record,
                               self.active_variable_config,
                               self.active_variables,
                               self.active_details,
                               self.as_of,
                               self.time_horizon.horizonF,
                               self.horizon_boq,
                               self.horizon_eoq,
                               1.0,
                               self.as_of,
                               stage,
                               )

    def values_in_history(self, historical_record, timeline_index):
        hist_as_of = self.history_timeline_details[timeline_index][1]['xl_date']
        hist_horizon = self.history_timeline_details[timeline_index][2]['xl_date']
        stage = historical_record.getAsOfDateF(self.stage_field_name, hist_as_of)
        if (stage in self.win_stages) or (stage in self.lose_stages):
            return None
        hist_horizon_eoq = self.history_timeline_details[timeline_index][2]['period_end_xl']
        hist_horizon_boq = self.history_timeline_details[timeline_index][2]['period_begin_xl']

        decay_factor = self.timedecay[timeline_index]

        return self.set_values(historical_record,
                               self.history_variable_config,
                               self.history_variables,
                               self.history_details,
                               hist_as_of,
                               hist_horizon,
                               hist_horizon_boq,
                               hist_horizon_eoq,
                               decay_factor,
                               self.as_of,
                               stage,
                               )

    def get_result(self, record):
        # cython.declare() is slow when not compiling
        # so we only use it when we are compiling
        if cython.compiled:
            running_result = cython.declare(RunningResults)
            running_result_vector = cython.declare(RunningResultsVector)

        ret_value = {}

        try:
            active_vals = self.active_value_cache[record.ID]
        except:
            try:
                active_vals = self.values_in_active(record)
                self.active_value_cache[record.ID] = active_vals
            except:
                raise ValueError("active cache should exist at this point in get_result for record " + record.ID)
        try:
            try:
                acv = active_vals.acv
            except:
                acv = 0.0

            if self.apply_filters:
                acv_begin = self.get_acv_as_of(record, as_of=self.time_horizon.beginsF)
                if (acv < self.forecast_params['filter_active_acv_lo']) \
                        or (acv > self.forecast_params['filter_active_acv_hi']) \
                        or (acv_begin < self.forecast_params['filter_begin_acv_lo']) \
                        or (acv_begin > self.forecast_params['filter_begin_acv_hi']):
                    err_msg = "filtered out begin_acv={begin_acv} active_acv={active_acv}"
                    err_params = {'begin_acv': acv_begin, 'active_acv': acv}
                    raise NoResultException(err_msg, err_params)

            if not self.is_active(record):
                logger.warning('WARNING: Record %s is not active so it should not be here!', record.ID)

            birthdate = record.featMap[self.stage_field_name][0][0]
            existing_pipe = False
            if birthdate < self.time_horizon.beginsF:
                existing_pipe = True
            ret_value['active_amount'] = acv
            if existing_pipe:
                ret_value['existing_pipe_active_amount'] = acv
            else:
                ret_value['new_pipe_active_amount'] = acv
            ret_value['existing_pipe'] = existing_pipe

            # if you only want bookings, then there is no forecast result details to be added...
            if not self.forecast_params['run_forecast']:
                return ret_value
            try:
                current_stage = active_vals.stage
            except:
                current_stage = 'N/A'
            try:
                current_type = active_vals.type
            except:
                current_type = 'N/A'
            try:
                current_owner = active_vals.owner
            except:
                current_owner = 'N/A'
            try:
                orig_stage = active_vals.orig_stage
            except:
                orig_stage = "N/A"

            ret_value['current_stage'] = current_stage
            ret_value['current_type'] = current_type
            ret_value['current_owner'] = current_owner

            if orig_stage in self.win_stages:
                try:
                    orig_close_date = active_vals.orig_close_date
                except:
                    logger.warning("WARNING: unable to figure out original close date")
                    orig_close_date = 0
                win_event = 0.0
                # 5 second rule: if it is <=5 seconds to horizon and it is still active,
                # it is not going to win.
                too_little_time_to_win_anything = self.forecast_params.get(
                    'too_little_time_to_win_anything', 5.0) / 86400.0
                if self.time_horizon.horizonF - self.time_horizon.as_ofF > too_little_time_to_win_anything and \
                        self.time_horizon.beginsF <= orig_close_date <= self.time_horizon.horizonF:
                    win_event = 1.0
                fc = win_event * acv
                ret_value.update({
                    'stage_probs': {current_stage: 1.0},
                    'eACV': acv,
                    'eACV_deviation': 0.0,
                    'win_prob': win_event,
                    'forecast': fc,
                    'model_note': 'deterministic',
                    'lose_prob': 0.0,
                    'num_close_neighbors': 0,
                    'num_all_neighbors': 0,
                })
                return ret_value
            if record.ID not in self.results:
                ret_value.update({
                    'stage_probs': {current_stage: 1.0},
                    'eACV': acv,
                    'eACV_deviation': 0.0,
                    'win_prob': 0.0,
                    'forecast': 0.0,
                    'lose_prob': 0.0,
                    'model_note': 'rejection',
                    'num_close_neighbors': 0,
                    'num_all_neighbors': 0,
                })
                return ret_value

            if self.calib_with_vectors:
                running_result_vector = self.results[record.ID]
                cn = np.array(running_result_vector.close_neighbors).reshape(self.ndarray_shape)
                tn = running_result_vector.total_neighbors
                rmn = self.min_neighbors_ratio
                mn = self.min_neighbors
                # in the vector implementation, the values in the prob map
                # and total weight are ndarrays with a fixed shape
                prob_map = running_result_vector.conditional_results
                total_weight = np.sum(prob_map.values(), axis=0)
                if not np.any(total_weight):
                    raise NoResultException("Zero Total Weight", {})
            else:
                running_result = self.results[record.ID]
                cn = running_result.close_neighbors
                tn = running_result.total_neighbors
                rmn = self.min_neighbors_ratio
                mn = self.min_neighbors
                prob_map = running_result.conditional_results
                total_weight = float(sum(prob_map.values()))
                if not total_weight:
                    raise NoResultException("Zero Total Weight", {})
            ret_value['stage_probs'] = {}
            for stage in prob_map:
                ret_value['stage_probs'][stage] = prob_map[stage] / total_weight

            # MATT TODO = implement the acv model in ndarray form
            if not self.calib_with_vectors:
                if running_result.conditional_sum_acv_diff_weights == 0.0:
                    acv_diff = 0.0
                    acv_diff_sq = 0.0
                else:
                    acv_diff = running_result.conditional_sum_acv_diff / running_result.conditional_sum_acv_diff_weights
                    acv_diff_sq = (running_result.conditional_sum_acv_diff_sq /
                                   running_result.conditional_sum_acv_diff_weights)

            con_ret_value = dict(ret_value['stage_probs'])

            if self.calib_with_vectors:
                un_prob_map = running_result_vector.unconditional_results
            else:
                un_prob_map = running_result.unconditional_results
            total_un_weight = float(sum(un_prob_map.values()))

            un_ret_value = {}

            for stage in un_prob_map:
                un_ret_value[stage] = un_prob_map[stage] / total_un_weight

            rcn = 1.0 * cn / tn

            unc_coef = 0.0 if not self.calib_with_vectors else np.zeros(self.ndarray_shape)

            if not self.calib_with_vectors:
                if (cn < mn):
                    unc_coef = (mn - cn) / (1.0 * mn)
                elif (rcn < rmn):
                    unc_coef = (rmn - rcn) / (1.0 * rmn)
            else:
                mn_mask = cn < mn
                if np.any(mn_mask):
                    mn_coef = (mn - cn) / (1.0 * mn)
                    unc_coef += mn_mask * mn_coef

                rmn_mask = (1 - mn_mask) * (rcn < rmn)
                if np.any(rmn_mask):
                    rmn_coef = (rmn - rcn) / (1.0 * rmn)
                    unc_coef += rmn_mask * rmn_coef

            if np.any(unc_coef):
                con_coef = 1.0 - unc_coef
                # Make up for the lack of close neighbors by giving them the
                # unconditional average
                for stage in ret_value['stage_probs']:
                    ret_value['stage_probs'][stage] = con_coef * ret_value['stage_probs'][stage] + \
                                                      unc_coef * un_ret_value[stage]
                if not self.calib_with_vectors:
                    if running_result.unconditional_sum_acv_diff_weights == 0.0:
                        un_acv_diff = 0.0
                        un_acv_diff_sq = 0.0
                    else:
                        un_acv_diff = (running_result.unconditional_sum_acv_diff /
                                       running_result.unconditional_sum_acv_diff_weights)
                        un_acv_diff_sq = (running_result.unconditional_sum_acv_diff_sq
                                          / running_result.unconditional_sum_acv_diff_weights)
                    acv_diff = con_coef * acv_diff + unc_coef * un_acv_diff
                    acv_diff_sq = con_coef * acv_diff_sq + unc_coef * un_acv_diff_sq

            eACV = acv

            if not self.calib_with_vectors:
                if self.eacv_model != 'acv':
                    eACV += acv_diff
                    if acv >= 0.0 and eACV < 0.0:
                        eACV = 0.0
                    if acv < 0.0 and eACV > 0.0:
                        eACV = 0.0

            win_probability = 0.0 if not self.calib_with_vectors else np.zeros(self.ndarray_shape)
            un_win_probability = 0.0
            con_win_probability = 0.0 if not self.calib_with_vectors else np.zeros(self.ndarray_shape)
            lose_probability = 1.0 if not self.calib_with_vectors else np.ones(self.ndarray_shape)
            un_lose_probability = 1.0
            con_lose_probability = 1.0 if not self.calib_with_vectors else np.ones(self.ndarray_shape)

            if self.close_date_updated_beyond_horizon_implies_forecast_zero \
                    and record.close_date_updated_beyond_horizon:
                ret_value['close_date_updated_beyond_horizon'] = True
            elif not record.close_date_too_far:
                for stage in self.win_stages:
                    win_probability += ret_value['stage_probs'].get(stage, 0)
                    un_win_probability += un_ret_value.get(stage, 0)
                    con_win_probability += con_ret_value.get(stage, 0)
                lose_probability = 0.0 if not self.calib_with_vectors else np.zeros(self.ndarray_shape)
                un_lose_probability = 0.0
                con_lose_probability = 0.0 if not self.calib_with_vectors else np.zeros(self.ndarray_shape)
                for stage in self.lose_stages:
                    lose_probability += ret_value['stage_probs'].get(stage, 0)
                    un_lose_probability += un_ret_value.get(stage, 0)
                    con_lose_probability += con_ret_value.get(stage, 0)

            ret_value['eACV'] = eACV

            if not self.calib_with_vectors:
                ret_value['eACV_deviation'] = math.sqrt(max(acv_diff_sq -
                                                            acv_diff ** 2, 0.0))
            scsc = self.forecast_params.get("scaling_schedule", False)
            if scsc:
                ret_value['win_probability'] = win_probability
                scsc_filter = self.forecast_params.get("scaling_schedule_filter", False)
                valid = True
                if scsc_filter:
                    for item in scsc_filter:
                        if record.getAsOfDateF(item[0],
                                               self.time_horizon.as_ofF) == item[1]:
                            valid = False
                            break
                if valid:
                    coeff = None
                    for tpl in scsc:
                        if tpl[1] < self.time_horizon.as_ofF:
                            coeff = tpl[2]
                    if coeff is not None:  # should be less than 1
                        if isinstance(coeff, list):
                            fcoeff = 1.0
                            for tpl in coeff:
                                if tpl[0] < acv:
                                    fcoeff = tpl[1]
                            win_probability = win_probability * fcoeff
                        else:
                            win_probability = win_probability * coeff

            # EPF mult is a manual override to adjust the prob
            # by a fixed percentage.

            epf_mult = self.forecast_params.get("epf_mult", None)
            if epf_mult is not None and ret_value['existing_pipe']:
                win_probability *= epf_mult

            ret_value['win_prob'] = win_probability if not self.calib_with_vectors else win_probability.tolist()
            ret_value['forecast'] = win_probability * \
                                    eACV if not self.calib_with_vectors else (win_probability * eACV).tolist()
            ret_value['lose_prob'] = lose_probability if not self.calib_with_vectors else lose_probability.tolist()
            if (self.forecast_params.get('output_unconditional_probs', False)):
                ret_value['un_win_prob'] = un_win_probability
                ret_value['un_lose_prob'] = un_lose_probability
                ret_value[
                    'con_win_prob'] = con_win_probability if not self.calib_with_vectors else con_win_probability.tolist()
                ret_value[
                    'con_lose_prob'] = con_lose_probability if not self.calib_with_vectors else con_lose_probability.tolist()
            bins = self.forecast_params.get('tshirt_bins', False)
            if bins:
                ret_value['scaled_score'] = np.asscalar(1 + np.digitize([win_probability], bins)[0])
            ret_value['num_close_neighbors'] = cn if not self.calib_with_vectors else cn.tolist()
            ret_value['num_all_neighbors'] = tn
            ret_value['total_weight'] = total_weight if not self.calib_with_vectors else total_weight.tolist()
            if not self.calib_with_vectors:
                ret_value['neighbor_details'] = running_result.neighbor_details
                ret_value['historic_details'] = running_result.historic_details
            if(self.forecast_params.get('active_details', False)):
                ret_value['active_details'] = {fld: val for (fld, val)
                                               in zip(self.active_details._fields,
                                                      self.active_value_cache[record.ID])}

            try:
                ret_value['days_to_close'] = active_vals.days_to_close
            except:
                pass

            if self.calib_with_vectors:
                for stage in ret_value['stage_probs']:
                    ret_value['stage_probs'][stage] = ret_value['stage_probs'][stage].tolist()
            if not self.calib_with_vectors:
                if running_result.sum_winning_neighbor_weights > 0.0:

                    E_to_win = running_result.conditional_time_to_win / running_result.sum_winning_neighbor_weights
                    if cn < mn:
                        un_E_to_win = running_result.unconditional_time_to_win / running_result.count_winning_neighbors
                        E_to_win = ((cn * E_to_win + (mn - cn) * un_E_to_win) / float(mn))
                    elif rcn < rmn:
                        un_E_to_win = running_result.unconditional_time_to_win / running_result.count_winning_neighbors
                        E_to_win = ((rcn * E_to_win + (rmn - rcn) * un_E_to_win) / float(rmn))

                    ret_value['expected_time_to_win'] = E_to_win

        except NoResultException as e:
            logger.debug("%s - %s", record.ID, str(e))
            raise e
        except Exception as e:
            logger.exception("%s - %s", record.ID, str(e))
            raise e
        if self.forecast_params.get('lightweight', None):
            ret_value = {k: v for (k, v) in ret_value.items()
                         if k not in self.baggage_flds}
        return ret_value



# this function will be soon deleted -- do not use

@cython.cfunc
@cython.returns(cython.double)
@cython.locals(max_days_in_stage=cython.double)
@cython.locals(distance_variables_length=cython.int)
# this function will be soon deleted -- do not use
def get_similarity_old(actv_vals,
                       hist_vals,
                       forecast_params,
                       max_days_in_stage,
                       menu,
                       horizon_eoq):

    if cython.compiled:
        distance = cython.declare(cython.double)
        ret_val = cython.declare(cython.double)

        # For unpacking forecast_params
        owner_diff_dist = cython.declare(cython.double)
        business_unit_diff_dist = cython.declare(cython.double)
        type_unit_diff_dist = cython.declare(cython.double)
        days_in_stage_bw = cython.declare(cython.double)
        days_to_close_dist_add = cython.declare(cython.double)
        days_to_close_bw = cython.declare(cython.double)
        acv_bw = cython.declare(cython.double)
        gap_in_days = cython.declare(cython.double)

        # For unpacking actv_vals
        days_in_stage = cython.declare(cython.double)
        days_to_close = cython.declare(cython.double)
        tacv = cython.declare(cython.double)
        close_date = cython.declare(cython.double)

        # For unpacking hist_vals
        hist_days_in_stage = cython.declare(cython.double)
        hist_days_to_close = cython.declare(cython.double)
        hist_tacv = cython.declare(cython.double)
        hist_close_date = cython.declare(cython.double)
        log_of_two = cython.declare(cython.double)

    log_of_two = log_of_two_py
    horizon_eoq += 0.34
    stage_now = actv_vals.stage
    owner = actv_vals.owner
    current_type = actv_vals.type
    days_in_stage = actv_vals.days_in_stage
    days_to_close = actv_vals.days_to_close
    tacv = actv_vals.tacv
    business_unit = actv_vals.business_unit
    geo = actv_vals.business_unit
    close_date = actv_vals.close_date
    forecast_category = actv_vals.forecast_category

    hist_as_of = hist_vals.hist_as_of
    hist_horizon_eoq = hist_vals.hist_horizon_eoq + 0.34
    hist_stage = hist_vals.stage
    hist_owner = hist_vals.owner
    hist_type = hist_vals.type
    hist_days_in_stage = hist_vals.days_in_stage
    hist_days_to_close = hist_vals.days_to_close
    hist_tacv = hist_vals.tacv
    hist_business_unit = hist_vals.business_unit
    hist_geo = hist_vals.business_unit
    hist_close_date = hist_vals.close_date
    hist_forecast_category = hist_vals.forecast_category

    # Start adding distances
    distance = forecast_params.get('min_distance', 0.0)

    if menu['stage']['category'] == 'dissimilar':
        if hist_stage != stage_now:
            return totally_dissimilar

    else:
        raise ValueError("menu['stage']['category'] has to be either dissimilar or dist")

    if not tacv:
        if hist_tacv:
            return totally_dissimilar
    elif not hist_tacv:
        if tacv:
            return totally_dissimilar

    if close_date == -1:
        if hist_close_date != -1:
            return totally_dissimilar
    if hist_close_date == -1:
        if close_date != -1:
            return totally_dissimilar

    if close_date > horizon_eoq:
        if hist_horizon_eoq >= hist_close_date:
            return totally_dissimilar
    elif hist_horizon_eoq < hist_close_date:
        return totally_dissimilar

    if menu['type']['category'] == 'dissimilar':
        if current_type != hist_type:
            return totally_dissimilar
    elif menu['type']['category'] == 'dist':
        type_unit_diff_dist = menu['type']['value']
        if current_type != hist_type:
            distance += type_unit_diff_dist
    else:
        raise ValueError("menu['type']['category'] has to be either dissimilar or dist")

    if 'business_unit' in menu:
        if menu['business_unit']['category'] == 'dissimilar':
            if business_unit != hist_business_unit:
                return totally_dissimilar
        elif menu['business_unit']['category'] == 'dist':
            business_unit_diff_dist = menu['business_unit']['value']
            if business_unit != hist_business_unit:
                distance += business_unit_diff_dist
        else:
            raise ValueError("menu['business_unit']['category'] has to be either dissimilar or dist")

    if 'geo' in menu:
        if menu['geo']['category'] == 'dissimilar':
            if geo != hist_geo:
                return totally_dissimilar
        elif menu['geo']['category'] == 'dist':
            geo_diff_dist = menu['geo']['value']
            if geo != hist_geo:
                distance += geo_diff_dist
        else:
            raise ValueError("menu['geo']['category'] has to be either dissimilar or dist")

    if 'forecast_category' in menu:
        if hist_forecast_category != 'N/A':
            if forecast_category != 'N/A':
                if menu['forecast_category']['category'] == 'dissimilar':
                    if forecast_category != hist_forecast_category:
                        return totally_dissimilar
                elif menu['forecast_category']['category'] == 'dist':
                    forecast_category_diff_dist = menu['forecast_category']['value']
                    if forecast_category != hist_forecast_category:
                        distance += forecast_category_diff_dist
                else:
                    raise ValueError("menu['forecast_category']['category'] has to be either dissimilar or dist")
        elif forecast_category != 'N/A':
            distance += 3

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
        gap_in_days = ((math.fmax(days_in_stage, max_days_in_stage)
                        - math.fmax(hist_days_in_stage, max_days_in_stage)))
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
    distance *= forecast_params.get('coeff_distance', 1.0)
    if cython.compiled:
        ret_val = math.exp(-distance)
    else:
        ret_val = math.exp(-distance)
    return ret_val
