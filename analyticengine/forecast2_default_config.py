

default_active_variable_config = {
    'stage': {
        'env_variable': True,
        'value': 'stage',
    },
    'acv': {
        'field_name': 'acv',
        'function': 'get_as_of',
                    'time': 'as_of',
                    'fv': True,
        'expression_try': [
            "mathUtils.excelToFloat(value)",
            "0.0",
        ],
    },
    'stage_int': {
        'variable_name': 'stage',
        'expression_try': ['int(value)', '-999'],
    },
    'tacv': {
        'variable_name': 'acv',
        'expression_try': ['math.log(value > 0 and max(value, 1.0) or -1.0 / min(value, -1.0))'],
    },
    'type': {
        'function': 'get_as_of',
                    'field_name': 'type',
                    'time': 'as_of',
                    'fv': True,
    },
    'type_int': {
        'variable_name': 'type',
        'expression_try': ['int(value)', '-999'],
    },
    'owner': {
        'field_name': 'owner',
        'function': 'get_as_of',
        'time': 'as_of',
        'fv': True,
    },
    'close_date': {
        'field_name': 'close_date',
        'function': 'get_as_of',
                    'time': 'as_of',
                    'fv': False,
        'default_value': -1.0,
        'expression_try': ["float(value)",
                           "fuzzydateinput2xldate(value)",
                           "datestr2xldate(value)"
                           ],
    },
    'orig_close_date': {
        'field_name': 'orig_close_date',
        'function': 'get_as_of',
                    'time': 'as_of',
        'fv': False,
        'default_value': -1.0,
        'expression_try': ["float(value)",
                           "fuzzydateinput2xldate(value)",
                           "datestr2xldate(value)"
                           ],
    },
    'orig_stage': {
        'field_name': 'orig_stage',
        'function': 'get_as_of',
        'time': 'as_of',
        'fv': False,
    },
    'days_to_close': {
        'variable_name': 'close_date',
        'expression_try': [
            "value == -1.0 and 999.0 or value - as_of",
        ],
    },

    'acv_indicator': {
        'variable_name': 'tacv',
        'expression_try': ["value and 1 or 0", ],
    },
    'days_in_stage': {
        'field_name': 'stage',
        'function': 'get_length_of_time',
                    'time': 'as_of',
                    'fv': False,
    },
    'close_date_indicator': {
        'variable_name': 'close_date',
        'expression_try': ["value == -1.0 and -1 or 1", ],
    },
    'close_date_eoq_indicator': {
        'variable_name': 'close_date',
        'expression_try': ["(value > (horizon_eoq + 0.34)) and -1 or 1", ],
    },
    'days_to_close_over999': {
        'variable_name': 'days_to_close',
        'expression_try': [
            "forecast_params['days_to_close_dist_add'] if value >= 999.0 else 0.0",
        ],
    },
    'days_to_close_rescaled': {
        'variable_name': 'days_to_close',
        'expression_try': [
            "value/forecast_params['days_to_close_bw'] if value < 999.0 else 0.0",
        ],
    },
    'business_unit': {
        'field_name': 'business_unit',
        'function': 'get_as_of',
        'time': 'as_of',
        'fv': True,
        'default_value': '',
    },

    'business_unit_code': {
        'variable_name': 'business_unit',
        'expression_try': ["md5_hash(value)"],
    },

    'geo': {
        'field_name': 'geo',
        'function': 'get_as_of',
        'time': 'as_of',
        'fv': True,
        'default_value': '',
        'expression_try': ["md5_hash(value)"],
    },


    'forecast_category': {
        'field_name': 'forecast_category',
        'function': 'get_as_of',
        'time': 'as_of',
        'fv': False,
        'default_value': '',
        'expression_try': ["md5_hash(value)"],
    },
    'owner_code': {
        'variable_name': 'owner',
        'expression_try': ["md5_hash(value)"],
    },
    'days_in_stage_rescaled': {
        'variable_name': 'days_in_stage',
        'expression_try': ["max(value, self.max_days_in_stage)/(self.max_days_in_stage*forecast_params['days_in_stage_bw'])", ],
    },
    'tacv_rescaled': {
        'variable_name': 'tacv',
        'expression_try': ["value/log_of_two_py/forecast_params['acv_bw']", ],
    },
}


mandetory_history_variable_config = {
    'hist_as_of': {'env_variable': True,
                   'value': 'as_of',
                   },
    'hist_horizon': {'env_variable': True,
                     'value': 'horizon',
                     },
    'hist_horizon_eoq': {'env_variable': True,
                         'value': 'horizon_eoq',
                         },
    'decay_factor': {'env_variable': True,
                     'value': 'decay_factor',
                     },
    'stage_horizon': {
        'field_name': 'stage',
        'function': 'get_as_of',
        'time': 'horizon',
        'fv': False,
    },
    'stage_horizon_eoq': {
        'field_name': 'stage',
        'function': 'get_as_of',
        'time': 'horizon_eoq',
        'fv': False,
    },
    'acv_horizon': {
        'field_name': 'acv',
        'function': 'get_as_of',
        'time': 'horizon',
        'fv': True,
        'expression_try': ["mathUtils.excelToFloat(value)",
                           "0.0", ],
    },
    'acv_final': {
        'field_name': 'acv',
        'function': 'get_latest',
        'expression_try': ["mathUtils.excelToFloat(value)",
                           "0.0", ],
    },
}


default_history_variable_config = {
    'stage_final': {
        'field_name': 'stage',
        'function': 'get_as_of',
        'time': 'run_as_of',
        'fv': False,
    },  # actually final will be as_of of active records to avoid data snoop
    'acv_horizon': {
        'field_name': 'acv',
        'function': 'get_as_of',
                    'time': 'horizon',
                    'fv': True,
        'expression_try': ["mathUtils.excelToFloat(value)",
                           "0.0", ],
    },
    'destiny': {
        'field_name': 'terminal_fate',
        'function': 'get_as_of',
                    'time': 'as_of',
        'fv': True,
    },
    'time_to_destiny': {
        'field_name': 'terminal_fate',
        'function': 'get_latest_timestamp',
        'expression_try': ["value - as_of", "None"],  # " value != 'N/A' and  value - as_of or None"
    },


}

# try to keep raw uip fields at top and transformations on bottom
active_details_list = [
    'stage',
    'acv',
    'stage_int',
    'tacv',
    'close_date',
    'days_to_close',
    'owner',
    'type',
    'type_int',
    'acv_indicator',
    'close_date_indicator',
    'close_date_eoq_indicator',
    'business_unit',
    'business_unit_code',
    'geo',
    'forecast_category',
    'owner_code',
    'days_in_stage',
    'days_to_close_over999',
    'days_to_close_rescaled',
    'days_in_stage_rescaled',
    'tacv_rescaled',
    'orig_stage',
    'orig_close_date'
]

mandetory_history_details_list = ['hist_as_of',
                                  'hist_horizon',
                                  'hist_horizon_eoq',
                                  'decay_factor',
                                  'stage_horizon',
                                  'stage_horizon_eoq',
                                  'acv_horizon',
                                  'acv_final']


history_details_list = [
    'stage_final',  # actually final will be as_of of active records to avoid data snoop
    'destiny',
    'time_to_destiny',
]

default_distance_config = {'stage':
                           {'category': u'dissimilar',
                            },
                           'type':
                           {'category': u'dissimilar',
                            },
                           'stage_int':
                           {'category': u'dissimilar',
                            },
                           'type_int':
                           {'category': u'dissimilar',
                            },
                           'acv_indicator':
                           {'category': u'dissimilar',
                            },
                           'close_date_indicator':
                           {'category': u'dissimilar',
                            },
                           'close_date_eoq_indicator':
                           {'category': u'dissimilar',
                            },
                           'business_unit_code':
                           {'category': u'dissimilar',
                            },
                           'owner_code':
                           {'category': u'dist',
                            'value': 0.25,
                            },
                           'tacv_rescaled':
                           {'category': u'diffsq',
                            },
                           'days_in_stage_rescaled':
                           {'category': u'diffsq',
                            },
                           'days_to_close_over999':
                           {'category': 'max'},
                           'days_to_close_rescaled':
                           {'category': 'diffsq-0'},
                           }


default_distance_variables = {'distance1': [
    'stage_int',
    'acv_indicator',
    'close_date_indicator',
    'close_date_eoq_indicator',
    'type_int',
    'owner_code',
    'days_in_stage_rescaled',
    'tacv_rescaled',
    'days_to_close_over999',
    'days_to_close_rescaled',
]
}

default_distances_used = ['distance1']
