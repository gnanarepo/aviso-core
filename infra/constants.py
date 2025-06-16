from pymongo import IndexModel, DESCENDING


FM_COLL = 'fm_data'
EDW_DATA = 'edw_data'
EDW_PROCESS_UPDATE = 'edw_process_update'
DEALS_COLL = 'deals'
NEW_DEALS_COLL = 'deals_new'
QUARTER_COLL = 'quarterinfo'
FAVS_COLL = 'favorites'
AUDIT_COLL = 'audit_log'

DEALS_CI_COLL = 'deals_ci'
AI_DEALS_COLL = 'ai_deals'
FIELDS_COLL = 'fields'
DEALS_HISTORY_COLL = 'deals_history'
COMMS_COLL = 'comments'
COMMS_CRR_COLL = 'comments_crr'
MILESTONES_COLL = 'milestones'
FILTS_COLL = 'filters'
ACCOUNT_FILTS_COLL = 'account_filters'
DTFO_COLL = 'dtfo'
DLF_COLL = 'dlf'
PIPE_COLL = 'pipe_dev'
DLF_FCST_COLL = 'dlf_fcst'
INSIGHT_COLL = 'insights'
INSIGHTS_HISTORY_COLL = 'insights_history'
ACTIONABLE_INSIGHTS = 'actionable_insights'
WB_CREATE_COLL = 'wb_creates'
PIPELINE_PROJECTION_COL = 'pipeline_projection'
LEADERBOARD_DATA = 'leaderboard_calculated_data'
ADAPTIVE_METRICS_DATA = 'adaptive_metrics_calculated_data'
TOTALS_CACHED = 'filter_totals'
GBM_DEALS = "gbm_deals_results"
NUDGE_STATS = "nudge_stats"
AI_FORECAST_DIFF = "ai_forecast_diff"
GBM_CRR_COLL = 'gbm_crr_results'
GBM_CRR_COLL_INACTIVE = 'gbm_crr_results_inactive'
PLAN_OVERVIEW = "plan_overview"
PLAN_OBJECTIVES = "plan_objectives"
MEDDIC_COLL = 'meddic'
DLF_PREV_TIMESTAMP = 'dlf_prev_timestamp'
SHARED_USERS_COLL = 'shared_users'
AUTH_TOKENS_COLL = 'auth_tokens'
MEDDIC_FILES_COLL = 'meddic_files'


# --special pivot snapshot api collections --
SNAPSHOT_CACHE_COLL = "accounts_snapshot_data"  # primary
SNAPSHOT_CACHE_PREV_COLL = "accounts_snapshot_data_prev"  # prev cache
SNAPSHOT_NODE_FORECAST_COLL = "accounts_node_level_forecast"  # primary
# -x-special pivot snapshot api collections -x-

ACCOUNTS_COLL = 'account_details'
DEALS_UE_FIELDS = "deals_ue_fields"
DEALS_STAGE_MAP = "deals_stage_map"
DEAL_CHANGES_CACHE = "deal_changes_cache"
PREMEETING_SCHEDULE_COL = 'premeeting_schedule'
FM_DATA = "fm_data"
INSIGHT_NUGGS= 'insight_nuggs'
DEALS_VLOOKUP = 'deals_vlookup'
ACCOUNTS_FILTERS_TOTAL_COLL = 'account_filter_totals'
ACCOUNTS_GRID_COLL = 'account_grid'

CURRENCY_CONV_COLL = 'currency_conversion_rates'
ACCOUNT_PLAN = 'account_plan'

PERIOD_OPP = 'period_opp'
ROLLUP_NODES = 'rollup_nodes'

fm_indexes = [IndexModel([('period', DESCENDING),
                          ('node', DESCENDING),
                          ('field', DESCENDING),
                          ('segment', DESCENDING),
                          ('timestamp', DESCENDING)], unique=True),
              IndexModel([('period', DESCENDING),
                          ('node', DESCENDING),
                          ('field', DESCENDING),
                          ('segment', DESCENDING)]),
              IndexModel([('period', DESCENDING),
                          ('how', DESCENDING)]),
              ]

FM_LATEST_COLL = 'fm_latest_dr'

fm_latest_indexes = [IndexModel([('period', DESCENDING),
                          ('node', DESCENDING),
                          ('field', DESCENDING),
                          ('segment', DESCENDING)], unique=True),
              IndexModel([('period', DESCENDING),
                          ('how', DESCENDING)]),
             ]


FM_LATEST_DATA_COLL = 'fm_latest_data'

FM_FORECAST_INSIGHTS_COLL = 'forecast_insights'

fm_forecast_insights_indexes = [IndexModel([
                                    ('timestamp', DESCENDING),
                                    ('period', DESCENDING),
                                    ('node', DESCENDING),
                                    ('segment', DESCENDING)], unique=True),
                                IndexModel([
                                    ('timestamp', DESCENDING),
                                    ('node', DESCENDING),
                                    ('segment', DESCENDING)]),
                                ]

FM_FORECAST_EXPLANATION_INSIGHTS_COLL = 'forecast_explanation_insights'

fm_forecast_explanation_insights_indexes = [IndexModel([
                                    ('timestamp', DESCENDING),
                                    ('period', DESCENDING),
                                    ('node', DESCENDING),
                                    ('segment', DESCENDING)], unique=True),
                                IndexModel([
                                    ('timestamp', DESCENDING),
                                    ('node', DESCENDING),
                                    ('segment', DESCENDING)]),
                                ]

SNAPSHOT_COLL = 'snapshot_data'

snapshot_indexes = [IndexModel([('period', DESCENDING),
                                ('node', DESCENDING),
                                ('segment', DESCENDING)], unique=True),
                    IndexModel([('period', DESCENDING),
                                ('node', DESCENDING),
                                ('segment', DESCENDING),
                                ('last_updated_time', DESCENDING)]),
                    ]

SNAPSHOT_HIST_COLL = 'snapshot_historical_data'

snapshot_hist_indexes = [IndexModel([('period', DESCENDING),
                                ('node', DESCENDING),
                                ('segment', DESCENDING),
                                ('as_of_date', DESCENDING)], unique=True),
                         IndexModel([('period', DESCENDING),
                                ('node', DESCENDING),
                                ('segment', DESCENDING),
                                ('as_of_date', DESCENDING),
                                ('last_updated_time', DESCENDING)]),
                         ]

PERFORMANCE_DASHBOARD_COLL = 'performance_dashboard'

performance_dashboard_indexes = [IndexModel([('period', DESCENDING),
                                          ('node', DESCENDING),
                                          ('field', DESCENDING),
                                          ('timestamp', DESCENDING)], unique=True),
                                IndexModel([('period', DESCENDING),
                                          ('node', DESCENDING),
                                          ('field', DESCENDING)])
                                 ]

ROLE_SUFFIX = 'roles'
SNAPSHOT_ROLE_COLL = '_'.join([SNAPSHOT_COLL, ROLE_SUFFIX])

snapshot_role_indexes = [IndexModel([('period', DESCENDING),
                                ('node', DESCENDING),
                                ('segment', DESCENDING),
                                ('role', DESCENDING)], unique=True),
                    IndexModel([('period', DESCENDING),
                                ('node', DESCENDING),
                                ('segment', DESCENDING),
                                ('role', DESCENDING),
                                ('last_updated_time', DESCENDING)]),
                    ]

SNAPSHOT_HIST_ROLE_COLL = '_'.join([SNAPSHOT_HIST_COLL, ROLE_SUFFIX])

snapshot_hist_role_indexes = [IndexModel([('period', DESCENDING),
                                          ('node', DESCENDING),
                                          ('segment', DESCENDING),
                                          ('role', DESCENDING),
                                          ('as_of_date', DESCENDING)], unique=True),
                              IndexModel([('period', DESCENDING),
                                          ('node', DESCENDING),
                                          ('segment', DESCENDING),
                                          ('role', DESCENDING),
                                          ('as_of_date', DESCENDING),
                                          ('last_updated_time', DESCENDING)]),
                              ]

MOBILE_SNAPSHOT_COLL = 'mobile_snapshot_data'

mobile_snapshot_indexes = [IndexModel([('period', DESCENDING),
                                       ('node', DESCENDING),
                                       ('segment', DESCENDING)], unique=True),
                           IndexModel([('period', DESCENDING),
                                       ('node', DESCENDING),
                                       ('segment', DESCENDING),
                                       ('last_updated_time', DESCENDING)]),
                           ]

MOBILE_SNAPSHOT_ROLE_COLL = MOBILE_SNAPSHOT_COLL + ROLE_SUFFIX

accounts_snapshot_indexes = [IndexModel([('period', DESCENDING),
                                         ('node', DESCENDING),
                                         ('segment_id', DESCENDING)], unique=True)]

NEXTQ_COLL = 'nextq_dashboard'

nextq_dashboard_indexes = [IndexModel([('node', DESCENDING),
                                           ('quarter', DESCENDING)]),
                               IndexModel([('node', DESCENDING),
                                           ('quarter', DESCENDING),
                                           ('timestamp', DESCENDING)], unique=True),
                               IndexModel([('node', DESCENDING),
                                           ('timestamp', DESCENDING)]),
                               ]

WATERFALL_COLL = 'waterfall'
#TODO: Commenting for now as Index we need to check which is optimised to use.
# waterfall_indexes = [IndexModel([('node', DESCENDING),
#                                            ('quarter', DESCENDING)]),
#                                IndexModel([('node', DESCENDING),
#                                            ('quarter', DESCENDING),
#                                            ('timestamp', DESCENDING)], unique=True),
#                                IndexModel([('node', DESCENDING),
#                                            ('timestamp', DESCENDING)]),
#                                ]

WATERFALL_HISTORY_COLL = 'waterfall_history'


WEEKLY_FORECAST_FM_COLL = 'weekly_forecast_fm_data'

weekly_forecast_fm_indexes = [IndexModel([('period', DESCENDING),
                                           ('node', DESCENDING),
                                           ('segment', DESCENDING),
                                           ('field', DESCENDING)
                                           ], unique=True)
                               ]

WEEKLY_FORECAST_TREND_COLL = 'weekly_forecast_trend_data'

WEEKLY_FORECAST_EXPORT_COLL = 'weekly_forecast_fm_export_data'

WEEKLY_EDW_DATA = 'weekly_edw_data'
WEEKLY_EDW_PROCESS_STATUS = 'weekly_edw_process_status'
WEEKLY_EDW_PROCESS_START_TIME = 'weekly_edw_process_start_time'
WEEKLY_FORECAST_EXPORT_ALL = 'weekly_forecast_export_all_{}'


FORECAST_SCHEDULE_COLL = 'fm_schedule'
FORECAST_UNLOCK_REQUESTS = 'fm_unlock_requests'
USER_LEVEL_SCHEDULE = 'user_schedule'
CRM_SCHEDULE = 'crm_schedule'
ADMIN_MAPPING = 'admin_mapping'
EXPORT_ALL = 'export_all'

export_all_indexes = [IndexModel([('export_date', DESCENDING)], unique=True)]


user_schedule_indexes = [IndexModel([('user_id', DESCENDING),
                                       ('node_id', DESCENDING)], unique=True)]

HIER_COLL = 'hierarchy'
DRILLDOWN_COLL = 'drilldowns'

HIER_LEADS_COLL = 'hierarchy_leads'
DRILLDOWN_LEADS_COLL = 'drilldowns_leads'

hier_indexes = [IndexModel([('node', DESCENDING),
                            ('parent', DESCENDING),
                            ('from', DESCENDING),
                            ('to', DESCENDING)], unique=True),
                IndexModel([('node', DESCENDING),
                            ('hidden_from', DESCENDING),
                            ('hidden_to', DESCENDING),
                            ('from', DESCENDING),
                            ('to', DESCENDING)]),
                IndexModel([('parent', DESCENDING),
                            ('hidden_from', DESCENDING),
                            ('hidden_to', DESCENDING),
                            ('from', DESCENDING),
                            ('to', DESCENDING)]),
                IndexModel([('from', DESCENDING),
                            ('to', DESCENDING)])
                ]

drilldown_indexes = [IndexModel([('node', DESCENDING),
                                ('parent', DESCENDING),
                                ('from', DESCENDING),
                                ('to', DESCENDING)], unique=True),
                    IndexModel([('node', DESCENDING),
                                ('hidden_from', DESCENDING),
                                ('hidden_to', DESCENDING),
                                ('from', DESCENDING),
                                ('to', DESCENDING)]),
                    IndexModel([('parent', DESCENDING),
                                ('hidden_from', DESCENDING),
                                ('hidden_to', DESCENDING),
                                ('from', DESCENDING),
                                ('to', DESCENDING)]),
                    IndexModel([('normal_segs', DESCENDING),
                                ('from', DESCENDING),
                                ('to', DESCENDING)]),
                    IndexModel([('from', DESCENDING),
                                ('to', DESCENDING)])
                    ]
HIER_SERVICE_INDEXES = {HIER_COLL: hier_indexes,
                        DRILLDOWN_COLL: drilldown_indexes,
                        HIER_LEADS_COLL: hier_indexes,
                        DRILLDOWN_LEADS_COLL: drilldown_indexes
                        }



gbm_crr_indexes = [IndexModel([('monthly_period', DESCENDING),
                               ('RPM_ID', DESCENDING)], unique=True),
                   IndexModel([('period', DESCENDING),
                               ('RPM_ID', DESCENDING)]),
                   IndexModel([('period', DESCENDING),
                               ('RPM_ID', DESCENDING),
                               ('AccountID', DESCENDING)]),
                   IndexModel([('BUYING_PROGRAM', DESCENDING)]),
                   IndexModel([('period', DESCENDING),
                               ('BUYING_PROGRAM', DESCENDING)]),
                   IndexModel([('monthly_period', DESCENDING),
                               ('BUYING_PROGRAM', DESCENDING)]),
                   IndexModel([('period', DESCENDING),
                               ('RPM_ID', DESCENDING),
                               ('OwnerID', DESCENDING)] ),
                   IndexModel([('monthly_period', DESCENDING),
                               ('RPM_ID', DESCENDING),
                               ('__id__', DESCENDING)]),
                    IndexModel([('monthly_period', DESCENDING),
                               ('__segs', DESCENDING),
                               ('forecast', DESCENDING)]),
                   IndexModel([('monthly_period', DESCENDING),
                               ('__segs', DESCENDING),
                               ('forecast', DESCENDING),
                               ('CRR_BAND_DESCR', DESCENDING)])]

deal_changes_cache_indexes = [IndexModel([('period', DESCENDING),
                                          ('node', DESCENDING),
                                          ('since', DESCENDING),
                                          ('segment', DESCENDING)], unique=True),
                              IndexModel([('period', DESCENDING),
                                          ('node', DESCENDING),
                                          ('since', DESCENDING),
                                          ('segment', DESCENDING),
                                          ('last_updated_time', DESCENDING)])]

accounts_node_level_forecast_indexes = [IndexModel([('period', DESCENDING),
                                                    ('node_key', DESCENDING)], unique=True)]

pipeline_projection_indexes = [IndexModel([('node', DESCENDING),
                                           ('quarter', DESCENDING)]),
                               IndexModel([('node', DESCENDING),
                                           ('quarter', DESCENDING),
                                           ('timestamp', DESCENDING)], unique=True),
                               IndexModel([('node', DESCENDING),
                                           ('timestamp', DESCENDING)]),
                               IndexModel([('timestamp', DESCENDING)]),
                               IndexModel([('quarter', DESCENDING),
                                           ('timestamp', DESCENDING)]),
                               ]

account_details_indexes = [IndexModel([('account_id', DESCENDING)], unique=True)]
meddic_indexes = [IndexModel([('opp_id', DESCENDING)], unique=True)]

shared_users_indexes = [IndexModel([('opp_id',DESCENDING)], unique=True)]
auth_tokens_indexes = [IndexModel([('token', DESCENDING)], unique=True),
                       IndexModel([('opp_id',DESCENDING),('email',DESCENDING)])]
meddic_file_indexes = [IndexModel([('opp_id',DESCENDING),('qId',DESCENDING)]),
                       IndexModel([('qId',DESCENDING),('opp_id',DESCENDING),('file_name',DESCENDING)]),
                       IndexModel([('s3_key',DESCENDING)],unique=True)]


plan_overview_indexes = [IndexModel([('account_id', DESCENDING)], unique=True)]

plan_objectives_indexes = [IndexModel([('account_id', DESCENDING)], unique=True)]

account_filter_totals_indexes = [IndexModel([('node', DESCENDING),('period', DESCENDING)], unique=True)]
account_grid_indexes = [IndexModel([('node', DESCENDING),('period', DESCENDING)], unique=True)]


deals_ci_indexes = [IndexModel([('period', DESCENDING),
                                ('opp_id', DESCENDING)], unique=True),
                    IndexModel([('period', DESCENDING),
                                ('opp_id', DESCENDING),
                                ('update_date', DESCENDING)]),
                    IndexModel([('period', DESCENDING),
                                ('close_period', DESCENDING),
                                ('hierarchy_list', DESCENDING),
                                ('update_date', DESCENDING)]),
                    IndexModel([('period', DESCENDING),
                                ('close_period', DESCENDING),
                                ('drilldown_list', DESCENDING),
                                ('update_date', DESCENDING)]),
                    IndexModel([('period', DESCENDING),
                                ('close_period', DESCENDING),
                                ('drilldown_list', DESCENDING),
                                ('is_deleted', DESCENDING)]),
                    IndexModel([('period', DESCENDING),
                                ('close_period', DESCENDING),
                                ('hierarchy_list', DESCENDING),
                                ('is_deleted', DESCENDING)]),
                    ]

deals_indexes = [IndexModel([('period', DESCENDING),
                             ('opp_id', DESCENDING)], unique=True),
                 IndexModel([('period', DESCENDING),
                             ('opp_id', DESCENDING),
                             ('update_date', DESCENDING)]),
                 IndexModel([('period', DESCENDING),
                             ('close_period', DESCENDING),
                             ('hierarchy_list', DESCENDING),
                             ('update_date', DESCENDING)]),
                 IndexModel([('period', DESCENDING),
                             ('close_period', DESCENDING),
                             ('drilldown_list', DESCENDING),
                             ('update_date', DESCENDING)]),
                 IndexModel([('period', DESCENDING),
                             ('close_period', DESCENDING),
                             ('drilldown_list', DESCENDING),
                             ('is_deleted', DESCENDING)]),
                 IndexModel([('period', DESCENDING),
                             ('close_period', DESCENDING),
                             ('hierarchy_list', DESCENDING),
                             ('is_deleted', DESCENDING)]),
                 IndexModel([('Amount', DESCENDING)]),
                 IndexModel([('drilldown_list', DESCENDING),('created_date_adj',DESCENDING)])]

wb_creates_indexes = [IndexModel([('period', DESCENDING),
                             ('opp_id', DESCENDING),
                             ('timestamp', DESCENDING)], unique=True),
                 IndexModel([('period', DESCENDING),
                             ('opp_id', DESCENDING),
                             ('node', DESCENDING)])]

deals_history_indexes = [IndexModel([('as_of', DESCENDING),
                             ('period', DESCENDING),
                             ('opp_id', DESCENDING)], unique=True),
                 IndexModel([('period', DESCENDING),
                             ('opp_id', DESCENDING),
                             ('update_date', DESCENDING)]),
                 IndexModel([('period', DESCENDING),
                             ('close_period', DESCENDING),
                             ('hierarchy_list', DESCENDING),
                             ('update_date', DESCENDING)]),
                 IndexModel([('period', DESCENDING),
                             ('close_period', DESCENDING),
                             ('drilldown_list', DESCENDING),
                             ('update_date', DESCENDING)])]

insights_indexes = [IndexModel([('opp_id', DESCENDING),
                             ('period', DESCENDING),
                             ('day_timestamp', DESCENDING)], unique=True),
                    IndexModel([('opp_id', DESCENDING),
                             ('day_timestamp', DESCENDING)])]

insights_history_indexes = [IndexModel([('type', DESCENDING),
                             ('period', DESCENDING),
                             ('as_of', DESCENDING),
                             ('opp_id', DESCENDING)], unique=True),
                            IndexModel([('opp_id', DESCENDING),
                                    ('as_of', DESCENDING)])]

actionable_insights_indexes = [IndexModel([('opp_id', DESCENDING),
                                           ('period', DESCENDING),
                                           ('node', DESCENDING)], unique=True),
                               IndexModel([('opp_id', DESCENDING),
                                           ('node', DESCENDING)]),
                               IndexModel([('opp_id', DESCENDING)])]

filter_totals_indexes = [IndexModel([('node', DESCENDING),
                                     ('period', DESCENDING),
                                     ('segment', DESCENDING)], unique=True),
                         IndexModel([('node', DESCENDING),
                                     ('period', DESCENDING)]),
                         IndexModel([('period', DESCENDING)]),
                         IndexModel([('period', DESCENDING),
                                     ('stale', DESCENDING)]),
                         ]

gbm_deals_results_indexes = [IndexModel([('opp_id', DESCENDING),
                                         ('timestamp', DESCENDING)], unique=True),
                             IndexModel([('timestamp', DESCENDING),
                                         ('drilldown_list', DESCENDING)]),
                             IndexModel([('timestamp', DESCENDING)])]

ai_forecast_diff_indexes = [IndexModel([('opp_id', DESCENDING),
                                        ('begin', DESCENDING),
                                        ('end', DESCENDING),
                                        ('node', DESCENDING),
                                        ('segment_id', DESCENDING)], unique=True),
                            IndexModel([('begin', DESCENDING),
                                        ('end', DESCENDING),
                                        ('node', DESCENDING),
                                        ('segment_id', DESCENDING)]),
                            IndexModel([('begin', DESCENDING),
                                        ('end', DESCENDING),
                                        ('node', DESCENDING),
                                        ('segment_id', DESCENDING),
                                        ('diff.trans_category', DESCENDING)])]

deals_migrator = {'uniqueness_fields': ['period', 'opp_id'],
                  'tiebreaker_fields': ['update_date']}
favs_indexes = [IndexModel([('period', DESCENDING),
                            ('opp_id', DESCENDING),
                            ('user', DESCENDING)], unique=True),
                IndexModel([('user', DESCENDING),
                            ('fav', DESCENDING)])]
comms_indexes = [IndexModel([('timestamp', DESCENDING),
                             ('opp_id', DESCENDING),
                             ('user', DESCENDING)], unique=True),
                 IndexModel([('opp_id', DESCENDING),
                             ('node', DESCENDING)]),
                 IndexModel([('opp_id', DESCENDING),
                             ('node', DESCENDING),
                             ('period', DESCENDING)])]

comms_crr_indexes = [IndexModel([('timestamp', DESCENDING),
                                 ('opp_id', DESCENDING),
                                 ('user', DESCENDING)], unique=True),
                     IndexModel([('opp_id', DESCENDING),
                                 ('node', DESCENDING)]),
                     IndexModel([('opp_id', DESCENDING),
                                 ('node', DESCENDING),
                                 ('period', DESCENDING)])]


filts_indexes = [IndexModel([('filter_id', DESCENDING)], unique=True),
                 IndexModel([('is_default', DESCENDING)]),
                 IndexModel([('is_open', DESCENDING)])]

account_filters_indexes = [IndexModel([('filter_id', DESCENDING)], unique=True),
                 IndexModel([('is_default', DESCENDING)]),
                 IndexModel([('is_open', DESCENDING)])]

dtfo_indexes = [IndexModel([('period', DESCENDING),
                            ('opp_id', DESCENDING),
                            ('close_period', DESCENDING),
                            ('as_of', DESCENDING)], unique=True),
                IndexModel([('period', DESCENDING),
                            ('opp_id', DESCENDING),
                            ('as_of', DESCENDING),
                            ('hierarchy_list_hash', DESCENDING)]),
                IndexModel([('period', DESCENDING),
                            ('opp_id', DESCENDING),
                            ('as_of', DESCENDING),
                            ('update_date', DESCENDING)]),
                IndexModel([('period', DESCENDING),
                            ('hierarchy_list', DESCENDING),
                            ('as_of', DESCENDING),
                            ('update_date', DESCENDING)])]


pipe_dev_indexes = [IndexModel([('period', DESCENDING),
                            ('as_of', DESCENDING),
                            ('segment',DESCENDING),
                            ('node', DESCENDING)], unique=True),
                    IndexModel([('period', DESCENDING),
                                ('as_of', DESCENDING),
                                ('node', DESCENDING)])]

dlf_indexes = [IndexModel([('period', DESCENDING),
                           ('opp_id', DESCENDING),
                           ('node', DESCENDING),
                           ('field', DESCENDING),
                           ('timestamp', DESCENDING)], unique=True),
               IndexModel([('period', DESCENDING),
                           ('opp_id', DESCENDING),
                           ('node', DESCENDING),
                           ('field', DESCENDING)])]

rollup_nodes_indexes = [IndexModel([('node', DESCENDING)], unique=True)]
dlf_fcst_indexes = [IndexModel([('period', DESCENDING),
                             ('opp_id', DESCENDING)], unique=True),
                 IndexModel([('period', DESCENDING),
                             ('close_period', DESCENDING),
                             ('hierarchy_list', DESCENDING),
                             ('update_date', DESCENDING)]),
                 IndexModel([('period', DESCENDING),
                             ('close_period', DESCENDING),
                             ('drilldown_list', DESCENDING),
                             ('update_date', DESCENDING)])]


leaderboard_indexes = [IndexModel([('period', DESCENDING),
                            ('node', DESCENDING)], unique=True),
                IndexModel([('node', DESCENDING),
                            ('period', DESCENDING)])]

adaptive_metrics_indexes = [IndexModel([('period', DESCENDING),
                                        ('node', DESCENDING)], unique=True),
                            IndexModel([('node', DESCENDING),
                                        ('period', DESCENDING)])]

mileston_indexes = [
    IndexModel([('opp_id', DESCENDING)]),
]

DEAL_SERVICE_INDEXES = {DEALS_COLL: deals_indexes,
                        NEW_DEALS_COLL: deals_indexes,
                        DEALS_CI_COLL: deals_ci_indexes,
                        AI_DEALS_COLL: deals_indexes,
                        DEALS_HISTORY_COLL: deals_history_indexes,
                        FAVS_COLL: favs_indexes,
                        COMMS_COLL: comms_indexes,
                        COMMS_CRR_COLL: comms_crr_indexes,
                        FILTS_COLL: filts_indexes,
                        ACCOUNT_FILTS_COLL:account_filters_indexes,
                        DTFO_COLL: dtfo_indexes,
                        PIPE_COLL: pipe_dev_indexes,
                        DLF_COLL: dlf_indexes,
                        DLF_FCST_COLL: dlf_fcst_indexes,
                        INSIGHT_COLL: insights_indexes,
                        INSIGHTS_HISTORY_COLL: insights_history_indexes,
                        WB_CREATE_COLL: wb_creates_indexes,
                        LEADERBOARD_DATA: leaderboard_indexes,
                        ADAPTIVE_METRICS_DATA: adaptive_metrics_indexes,
                        TOTALS_CACHED: filter_totals_indexes,
                        PIPELINE_PROJECTION_COL: pipeline_projection_indexes,
                        MILESTONES_COLL: mileston_indexes,
                        ACCOUNTS_COLL: account_details_indexes,
                        GBM_CRR_COLL: gbm_crr_indexes,
                        GBM_CRR_COLL_INACTIVE: gbm_crr_indexes,
                        PLAN_OVERVIEW: plan_overview_indexes,
                        PLAN_OBJECTIVES: plan_objectives_indexes,
                        MEDDIC_COLL: meddic_indexes,
                        ACCOUNTS_FILTERS_TOTAL_COLL:account_filter_totals_indexes,
                        ROLLUP_NODES: rollup_nodes_indexes,
                        ACCOUNTS_GRID_COLL:account_grid_indexes,
                        SHARED_USERS_COLL: shared_users_indexes,
                        AUTH_TOKENS_COLL: auth_tokens_indexes,
                        MEDDIC_FILES_COLL:meddic_file_indexes
                        }

DEAL_SERVICE_MIGRATORS = {DEALS_COLL: deals_migrator,
                          NEW_DEALS_COLL: deals_migrator,
                          AI_DEALS_COLL: deals_migrator
                          }


CRR_PIVOT = 'CRR'
