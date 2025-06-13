import copy
import logging
from collections import defaultdict
from datetime import timedelta

from aviso.settings import sec_context

from config.base_config import BaseConfig
from infra.filters import (FilterError, _valid_filter, fetch_filter,
                           fetch_many_filters, parse_filters)
from infra.rules import HIERARCHY_RULES, passes_configured_hierarchy_rules
from utils.common import cached_property
from utils.date_utils import datetime2epoch, epoch, get_bom, now
from utils.misc_utils import (CycleError, UndefinedError, dict_of_dicts_gen,
                              get_nested, merge_dicts, top_sort, try_float)

logger = logging.getLogger("gnana.%s" % __name__)

DEFAULT_ROLE = 'default_role'
DEFAULT_BEST_CASE_VALS = ["Best Case"]
DEFAULT_COMMIT_VALS = [
    "Commit",
    "Committed",
    "Forecasted",
    "commit",
    "Forecasted Upside",
    "True",
    "true",
    True,
]

DEFAULT_PIPELINE_VALS = ["Pipeline", "pipeline"]

DEFAULT_MOST_LIKELY_VALS = ["Most Likely", "most likely"]

DEFAULT_RENEWAL_VALS = [
    "Renewal",
    "renewal",
    "RENEWAL",
    "Renewals",
    "Recurring",
    "Resume",
    "subscription renewal",
    "support renewal",
    "Existing Customer - Maintenance renewal",
    "Existing customer - Subscription renewal",
    "Existing customer - subscription renewal",
    "Maintenance renewal",
    "Existing Customer \u2013 maintenance renewal",
    "Maintenance Renewal (MR)",
    "Existing Business",
    "delayed renewal",
    "Contract renewal",
    "contractual renewal",
    "Customer",
    "Support Renewal",
    "EC renewal",
]

DEFAULT_DLF_VALS = ["True", "true", True]

PULL_IN_LIST = [
    "__fav__",
    "OpportunityName",
    "OpportunityOwner",
    "win_prob",
    "pullin_prob",
    "CloseDate",
    "Amount",
    "Stage",
    "ForecastCategory",
    "__id__",
    "SFDCObject"
]

DEFAULT_DLF_FCST_COLL_SCHEMA = [
    "opp_id",
    "is_deleted",
    "period",
    "close_period",
    "drilldown_list",
    "hierarchy_list",
    "dlf.in_fcst",
    "update_date",
]

DEFAULT_DISPLAY_INSIGHTS_CARD = {
    "amount": [],
    "close_date": ["pushes", "suggested_push"],
    "stage": ["sharp_decline", "stage_dur", "stage_age"],
    "global": [
        "grouper",
        "other_group",
        "rare_group",
        "score_history",
        "close_date_exp",
    ],
    "score_explanation": [
        "upside_deal",
        "deal_amount_reco",
        "deal_speed",
        "scenario",
        "score_history_dip",
        "primary_competitor",
        "competitor_win_loss",
        "recency",
        "new_deal",
        "custom",
        "closing_soon_after_eoq",
        "high_leverage_moments",
        "risk_insights",
        "winscore_projections_upper",
        "winscore_projections_lower",
        "winscore_insights"
    ],
    "field_level": [
        "stale_deal",
        "cd_change",
        "anomaly",
        "amount_change",
        "no_amount",
        "recommit2",
        "stale_commit",
        "never_pushed",
        "engagement_grade",
        "yearold",
        "closedate",
        "past_closedate",
        "dlf_bad",
        "dlf_good",

    ],
    "in_fcst": ["dlf_change"],
}

DEFAULT_STANDARD_OPPMAP_MAP_TYPES = {
    "amount": "Amount",
    "count": "Count",
    "quartiles": "Quartiles",
}

DEFAULT_COVID_OPPMAP_MAP_TYPES = {"amount": "Amount", "count": "Count"}

DEFAULT_OPPMAP_JUDGE_TYPE_OPTION_LABELS = {
    "dlf": "DLF",
    "commit": "Commit",
    "most_likely": DEFAULT_MOST_LIKELY_VALS[0],
}

OWNER_INSIGHTS = ["owner_insight", "owner_bad",
                  "owner_amt", "owner_cd", "owner_dct", "owner_cmt"]

DEFAULT_MILESTONES_NEW = [
    {
        "name": "Lead Conversion",
        "color": "#f89685",
        "items": ['Convert lead to 5% probability']
    },
    {
        "name": "Account Engagement",
        "color": "#2faadc",
        "items": ["Develop customer interest in proceeding with conversations"]
    },
    {
        "name": "Qualification",
        "color": "#107e4e",
        "items": ['Completion of Disco call', 'BANT Qualification']
    },
    {
        "name": "Identify pain points and Metrics",
        "color": "#3d4689",
        "items": ["Identification of Pain Points", "Identification of Metrics"]
    },
    {
        "name": "Identify Champion",
        "color": "#46d62e",
        "items": ['Champion Identification']
    },
    {
        "name": "Identify your stakeholders",
        "color": "#ffc22b",
        "items": ['Identification of Economic Buyer', 'Identification of Decision Process',
                  'Identification of Decision Criteria']
    },
    {
        "name": "Approval from Executive board",
        "color": "#e1a612",
        "items": ['Schedule EB meeting', 'Business reviews']
    },
    {
        "name": "Legal Approval",
        "color": "#e03e28",
        "items": ['Legal Approval', 'Ready for signatures']
    },
    {
        "name": "Signatures",
        "color": "#6e77c2",
        "items": ["Signatures to be done by both the parties"]
    },
    {
        "name": "Final review",
        "color": "#025b8d",
        "items": ['Deal desk final review']
    }
]

DEFAULT_MILESTONES_RENEWAL = [
    {
        "name": "Renewal Generated",
        "color": "#f89685",
        "items": []
    },
    {
        "name": "Schedule Meeting with Customer",
        "color": "#2faadc",
        "items": ["Setup meeting with customer to discuss renewal"]
    },
    {
        "name": "Verbal Agreement",
        "color": "#107e4e",
        "items": ['Get verbal agreement to renew']
    },
    {
        "name": "Renewal Quote",
        "color": "#3d4689",
        "items": ["Meeting with EB/ Executive sponsor", "Begin negotiating propoal components",
                  "Complete the Business Case",
                  "Get the initial budget approved", "No churn/dollar churn amount agreement"]
    }
]

COLLABORATION_RECORDINGS_TAB_DEFAULT_OPTIONS =  [{
                                                    'key': 'All',
                                                    'label': 'All Recordings',
                                                    'default': False
                                                    },{
                                                    'key': 'SELF',
                                                    'label': 'My Recordings',
                                                    'default': True
                                                    }
                                                    # {
                                                    # 'key': 'TEAM',
                                                    # 'label': 'My Teams Recording'
                                                    # }
                                                ]


DEFAULT_MILESTONES = {
    'new': DEFAULT_MILESTONES_NEW,
    'renewal': DEFAULT_MILESTONES_RENEWAL
}


def _convert_state(state):
    if state == "True":
        return True
    elif state == "False":
        return False
    return state


def get_node_depth(node):
    pass


class FMConfig(BaseConfig):

    config_name = 'forecast_management'

    def __init__(self, config_json=None, test_mode=False, create_mode=False, debug=False, db=None):
        super(FMConfig, self).__init__(config_json, test_mode, create_mode, debug, db)
        # gotta initialize this up front because threading is dumb
        if not self.create_mode:
            self.fields
            self.segment_filters

    @cached_property
    def deal_config(self):
        return DealConfig(debug=self.debug, db=self.db)

    @cached_property
    def get_special_pivots(self):
        deal_config = self.deal_config
        if sec_context.name in deal_config.not_deals_tenant.get('tenant', []):
            return deal_config.not_deals_tenant.get('special_pivot', [])
        return []

    @cached_property
    def periods_config(self):
        from config.periods_config import PeriodsConfig
        return PeriodsConfig(debug=self.debug, db=self.db)

    @cached_property
    def hier_config(self):
        from config.hier_config import HierConfig
        return HierConfig(debug=self.debug, db=self.db)

    @cached_property
    def dummy_tenant(self):
        """
        tenant is using dummy data, is not hooked up to any gbm/etl
        """
        return self.config.get('dummy_tenant', False)

    @cached_property
    def month_to_qtr_rollup(self):
        """
        month to qtr rollup for ue fields
        """
        return self.config.get('month_to_qtr_rollup', False)

    @cached_property
    def is_custom_quarter_editable(self):
        """
        Is custom fields editable for quarter
        """
        return self.config.get('is_custom_quarter_editable', False)

    @cached_property
    def quarterly_high_low_fields(self):
        """
        quarterly high low fields
        """
        return self.config.get('quarterly_high_low_fields', [])

    @cached_property
    def quarterly_high_low_rollup_fields(self):
        """
        quarterly high low fields
        """
        return self.config.get('quarterly_high_low_rollup_fields', [])

    @cached_property
    def fm_data_fields(self):
        """
        fm data fields
        """
        return self.config.get('fm_data_fields', [])

    @cached_property
    def edw_fields(self):
        """
        fm data fields
        """
        return self.config.get('edw_fields', [])

    @cached_property
    def fm_history_fields(self):
        """
        fm history fields
        """
        return self.config.get('fm_history_fields', [])

    @cached_property
    def bootstrapped(self):
        """
        tenant has been bootstrapped and has initial data loaded into app
        """
        return self.config.get('bootstrapped', True)

    def monthly_period_realtime_update(self):
        """
        It will allow to have realtime uodate of the monthly period in forecast tab of the forecast app
        """
        return self.config.get('monthly_period_realtime_update',True)

    @cached_property
    def nav_config(self):
        """
        Fetch the tenant specific navigation config
        """
        default_config = {
            'dashboard': 'dashboard',
            'forecasts': 'forecasts',
            'deals': 'deals',
            'trend': 'trend',
            'newhome': 'newhome',
            'notifications': 'notifications',
            'selfservice': 'self service',
            'accounts': 'accounts relationship',
            'whatchanged': 'analytics',
            'reports': 'reports',
            'scenarios': 'scenarios',
            'collaboration': 'collaboration workspace',
            'admin': 'admin'
        }
        return self.config.get('nav_config', default_config)

    #
    # FM Fields
    #
    @cached_property
    def fields(self):
        """
        all fm fields

        Returns:
            dict -- {field: {field_dtls}}
        """
        _fields = merge_dicts(self.config.get('fields', {}), self._default_aviso_fields)
        return {field: self._render_field(field, _fields) for field in _fields}

    @cached_property
    def performance_dashboard_fields(self):
        """
        all performance_dashboard fields

        Returns:
            dict -- {field: {field_dtls}}
        """
        return self.config.get('performance_dashboard_fields', {'commit': ['Commit'], 'most likely': ['most_likely']})

    @cached_property
    def snapshot_fields(self):
        """
        fm fields that appear in fm snapshot

        Returns:
            set -- {fields}
        """
        return set([field for row_schema in self.config.get('schemas', {}).values()
                    for column in row_schema.values()
                    for field in column['fields']])

    @cached_property
    def aviso_fields(self):
        """
        fm fields generated from aviso predictions
        not configurable, come out of the box

        Returns:
            dict -- {field: {field_dtls}}
        """
        return {k: v for k, v in self.fields.items() if v['type'] == 'AI'}


    @cached_property
    def user_entered_fields(self):
        """
        fm fields that users can input data into

        Returns:
            dict -- {field: {field_dtls}}
        """
        return {k: v for k, v in self.fields.items() if v['type'] == 'UE'}

    @cached_property
    def child_sum_fields(self):
        """
        fm fields that are the sum of childrens fm fields

        Returns:
            dict -- {field: {field_dtls}}
        """
        return {k: v for k, v in self.fields.items() if v['type'] == 'CS'}

    @cached_property
    def deal_rollup_fields(self):
        """
        fm fields that are the sum of a deal amount field for deals matching a specified filter

        Returns:
            dict -- {field: {field_dtls}}
        """
        return {k: v for k, v in self.fields.items() if v['type'] == 'DR'}

    @cached_property
    def prnt_deal_rollup_fields(self):
        return {k: v for k, v in self.fields.items() if v['type'] == 'prnt_DR'}

    @cached_property
    def formula_fields(self):
        """
        fm fields that are the result of a specified function applied to other fm fields

        Returns:
            dict -- {field: {field_dtls}}
        """
        return {k: v for k, v in self.fields.items() if v['type'] == 'FN'}

    @cached_property
    def node_conditional_fields(self):
        """
        fm fields that are either a mgr field or a rep field depending on node

        Returns:
            dict -- {field: {field_dtls}}
        """
        return {k: v for k, v in self.fields.items() if v['type'] == 'NC'}

    @cached_property
    def plan_fields(self):
        """
        fm fields that are plan fields

        Returns:
            list -- [names of plan fields]
        """
        return self.config.get('plan_fields', [])

    @cached_property
    def ignore_recency_for_fields(self):
        """
        fm fields for which recency can be ignored for

        Returns:
            list -- [names of fields for which recency_window can be ignored.]
        """
        return self.config.get('ignore_recency_for_fields', [])

    @cached_property
    def show_raw_data_in_trend(self):
        """
        if we want to show raw data in trend incase of hist_field is present

        Returns:
            True/False
        """
        return self.config.get('show_raw_data_in_trend', False)

    @cached_property
    def comment_limit(self):
        """
        history comment limit for type UE fields

        Returns:
            integer -- max limit of history
        """
        return self.config.get('comment_limit', 10)


    @cached_property
    def linearity_field(self):
        """
        fm field that is linearity field
        NOTE: configuring this automatically turns this on in dashboard too, is that wanted?

        Returns:
            str -- name of linearity field
        """
        return self.config.get('linearity_field')


    @cached_property
    def won_field(self):
        """
        fields that is used as won field in trend module

        Returns:
            str -- field name
        """
        # TODO: validation
        return self.config.get('won_field')

    @cached_property
    def pullin_threshold(self):
        """
        fields that is used as won field in trend module

        Returns:
            str -- field name
        """
        # TODO: validation
        return self.config.get('pullin_threshold', 0.15)

    @cached_property
    def pullin_model(self):
        return self.config.get('pullin_model', False)

    @cached_property
    def trend_fields(self):
        """
        fm fields that appear in trend module

        Returns:
            dict -- {field: {field_dtls}}
        """
        # TODO: validation
        return merge_dicts(self.aviso_fields,
                           {field: self.fields[field] for field in self.config.get('trend_fields', {})})

    def get_trend_fields(self, node):
        custom_fields = {}
        for field, data in self.config.get('trend_fields', {}).items():
            if isinstance(data, str):
                custom_fields[field] = self.fields[field]
            else:
                field = get_nested(data, [node, 'field'], field)
                custom_fields[field] = self.fields[field]

        return merge_dicts(self.aviso_fields, custom_fields)


    def trend_labels(self, node):
        """
        mapping of fm field to ui display name for trend module

        Returns:
            dict -- {field: label}
        """
        custom_fields = {}
        for field, data in self.config.get('trend_fields', {}).items():
            if isinstance(data, str):
                custom_fields[field] = data
            else:
                field = get_nested(data, [node, 'field'], field)
                label = get_nested(data, [node, 'label'], data['default_label'])
                custom_fields[field] = label



        return custom_fields



    @cached_property
    def trend_field_attributes(self):
        """
        mapping of non plan trend field to order and color of display

        Returns:
            dict -- {field: {order:int, color:"hex"}}
        """
        return self.config.get('trend_field_attributes', {})

    @cached_property
    def leaderboard_options(self):
        return self.config.get('leaderboard_options', [])

    #
    # FM Segments
    #
    def get_segments(self, as_of, node, is_pivot_special=None):
        if is_pivot_special:
            return [segment for segment in self.special_pivot_segments_order
                    if passes_configured_hierarchy_rules(as_of, node,
                                                         self.special_pivot_segments[segment].get('rules', []))]
        else:
            return [segment for segment in self.segments_order
                    if passes_configured_hierarchy_rules(as_of, node, self.segments[segment].get('rules', []))]

    @cached_property
    def has_segments(self):
        """
        tenant has segments enabled

        Returns:
            bool
        """
        return len(self.segments) > 1

    @cached_property
    def alt_hiers(self):
        return self.config.get('alt_hiers', [])


    @cached_property
    def primary_segment(self):
        """
        primary segment to use in app

        Returns:
            str -- name of segment
        """
        return get_nested(self.config, ['segments', 'primary_segment'], 'all_deals')

    @cached_property
    def special_pivot_default_segment(self):
        """
        default segment to be displayed in UI on logging in
        If not defined in config, default to 'Breakdown by segment'

        Returns:
            str -- name of segment
        """
        return get_nested(self.config, ['crr_segments', 'default_segment'], 'breakdown')

    @cached_property
    def default_segment(self):
        """
        default segment to be displayed in UI on logging in
        If not defined in config, default to 'Breakdown by segment'

        Returns:
            str -- name of segment
        """
        return get_nested(self.config, ['segments', 'default_segment'], 'breakdown')

    @cached_property
    def special_pivot_segments(self):
        """
        mapping of special pivot segment to segment configuration

        Returns:
            dict -- {seg: {seg_dtls}}
        """
        return get_nested(self.config, ['crr_segments', 'map'], {'all_deals': {}})


    @cached_property
    def segments(self):
        """
        mapping of segment to segment configuration

        Returns:
            dict -- {seg: {seg_dtls}}
        """
        return get_nested(self.config, ['segments', 'map'], {'all_deals': {}})

    @cached_property
    def crr_segments(self):
        """
        mapping of segment to segment configuration

        Returns:
            dict -- {seg: {seg_dtls}}
        """
        return get_nested(self.config, ['crr_segments', 'map'], {'all_deals': {}})

    @cached_property
    def special_pivot_segments_order(self):
        """
                display order of segments for front end

                Returns:
                    list -- [segs]
                """
        return get_nested(self.config, ['crr_segments', 'display'], [])


    @cached_property
    def segments_order(self):
        """
        display order of segments for front end

        Returns:
            list -- [segs]
        """
        return get_nested(self.config, ['segments', 'display'], [])

    @cached_property
    def rollup_segments(self):
        """
        segments contributing to rollups

        Returns:
            list -- [segs]
        """
        default_rollup_segments = self.segments.keys()
        if len(default_rollup_segments) > 1 and self.rep_level_segment_editability:
            default_rollup_segments = list(set(self.segments.keys()) - {'all_deals'})
        return get_nested(self.config, ['segments', 'rollup_segments'], default_rollup_segments)

    @cached_property
    def rollup_ai_segments(self):
        """
        segments contributing to ai forecast

        Returns:
            bool -- False
        """
        return get_nested(self.config, ['segments', 'rollup_ai_segments'], False)

    @cached_property
    def count_override_segments(self):
        """
            segments which are overridden as count values

            Returns:
                bool -- False
        """
        count_override_segments = set([])
        for seg, seg_dict  in self.segments.items():
            if seg_dict.get('count_override', False):
                count_override_segments.add(seg)
        return count_override_segments

    @cached_property
    def percentage_override_segments(self):
        """
            segments which are overridden as count values

            Returns:
                bool -- False
        """
        percentage_override_segments = set([])
        for seg, seg_dict in self.segments.items():
            if seg_dict.get('percentage_override', False):
                percentage_override_segments.add(seg)
        return percentage_override_segments

    @cached_property
    def disable_editability_segment_field_map(self):
        """
            segments which are overridden as count values

            Returns:
                bool -- False
        """
        disable_editability_segment_field_map = {}
        for seg, seg_dict in self.segments.items():
            if seg_dict.get('disable_edit_fields', []):
                disable_editability_segment_field_map.update({
                    seg: seg_dict.get('disable_edit_fields', [])
                })
        return disable_editability_segment_field_map

    @cached_property
    def FN_segments(self):
        """
            segments which are overridden as count values

            Returns:
                bool -- False
        """
        FN_segments = {}
        for seg, seg_dict in self.segments.items():
            if seg_dict.get('segment_func', {}):
                FN_segments.update({
                    seg: seg_dict.get('segment_func', {})
                })
        return FN_segments


    @cached_property
    def fmrollup_batch_size(self):
        return self.config.get('fmrollup_batch_size', 1000)

    @cached_property
    def fmrollup_batch_size_prnt_dr(self):
        return self.config.get('fmrollup_batch_size_prnt_dr', 50)


    @cached_property
    def partially_segmented(self):
        """
        True if the tenant has segments only in some select nodes and not in others

        :return:
        """
        return get_nested(self.config, ['segments', 'partially_segmented'], False)

    @cached_property
    def config_based_segments_rollup(self):
        """
        config_based_segments rollup to all_deals

        Returns:
            bool -- True or False
        """
        return get_nested(self.config, ['segments', 'config_based_segments_rollup'], False)

    @cached_property
    def use_all_deals_for_past_data_rollup(self):
        """
        use all_deals sgement for  rollup in past quarter data

        Returns:
            bool -- True or False
        """
        return get_nested(self.config, ['segments', 'use_all_deals_for_past_data_rollup'], False)

    @cached_property
    def segment_map(self):
        """
        mapping of segment to component segments that contribute to it

        Returns:
            dict -- {'segment': [component segments]}
        """
        return {segment: dtls.get('components', [segment]) for segment, dtls in self.segments.items()}

    @cached_property
    def segment_filters(self):
        """
        mapping of segment to deals filter that defines is

        Returns:
            dict -- {'segment': {filter}}
        """
        seg_filts = {}
        for segment, dtls in self.segments.items():
            if dtls.get('filter'):
                # doing this a bit stupidly so we can still unit test
                seg_filts[segment] = fetch_filter([dtls['filter']], self.deal_config, db=self.db)
            else:
                seg_filts[segment] = {}
        return seg_filts

    @cached_property
    def crr_segment_filters(self):
        """
        mapping of segment to deals filter that defines is

        Returns:
            dict -- {'segment': {filter}}
        """
        seg_filts = {}
        for segment, dtls in self.crr_segments.items():
            if dtls.get('filter'):
                # doing this a bit stupidly so we can still unit test
                seg_filts[segment] = fetch_filter([dtls['filter']], self.deal_config, db=self.db)
            else:
                seg_filts[segment] = {}
        return seg_filts

    @cached_property
    def segment_amount_fields(self):
        """
        mapping of segment to amount field used for segment

        Returns:
            dict -- {'segment': amount field}
        """
        seg_amounts = {}
        for segment, dtls in self.segments.items():
            if 'sum_field' in dtls:
                seg_amounts[segment] = dtls['sum_field']
        return seg_amounts

    @cached_property
    def show_sepcial_pivot_segment_forecast(self):
        return get_nested(self.config, ['crr_segments', 'forecast', 'show_forecast'], False)

    @cached_property
    def is_rep_summary_editable(self):
        return get_nested(self.config, ['segments', 'is_rep_summary_editable'], False)

    @cached_property
    def show_segment_forecast(self):
        return get_nested(self.config, ['segments', 'forecast', 'show_forecast'], False)

    @cached_property
    def gbm_segment_forecast(self):
        # if we get forecast segment wise from gbm, enable it
        return get_nested(self.config, ['segments', 'forecast', 'gbm_segment_forecast'], False)

    @cached_property
    def special_pivot_segment_breakdown_label(self):
        return get_nested(self.config, ['crr_segments', 'segment_breakdown_label'], 'Breakdown By Segment')

    @cached_property
    def segment_breakdown_label(self):
        return get_nested(self.config, ['segments', 'segment_breakdown_label'], 'Breakdown By Segment')

    @cached_property
    def special_pivot_segment_amounts(self):
        return get_nested(self.config, ['crr_segments', 'forecast', 'amounts'], {})

    @cached_property
    def segment_amounts(self):
        return get_nested(self.config, ['segments', 'forecast', 'amounts'], {})

    @cached_property
    def segment_percentage_rollup(self):
        return get_nested(self.config, ['segments', 'segment_percentage_rollup'], False)

    #
    # FM Schema
    #
    def get_schema(self, as_of, node, user, is_top_row=False, is_leaf=False,period=None):
        """
        get name of fm schema for a data row in fm grid
        front end uses this to determine where fm data gets plugged into fm grid
        tries to get the first rule based schema that passes, falls back to defaults otherwise

        Arguments:
            as_of {int} -- epoch timestamp to get schema as of                      1556074024910
            node {str} -- hierarchy node                                            '0050000FLN2C9I2'
            user {str} -- user name                                                 'gnana'

        Keyword Arguments:
            is_top_row {bool} -- row is at top of fm grid (default: {False})
            is_leaf {bool} -- node is a leaf node (default: {False})

        Returns:
            str -- name of schema to use for node
        """
        for rules, schema_set in self.rule_based_schema_sets:
            if passes_configured_hierarchy_rules(as_of, node, rules,period=period):
                break
        else:
            if period and 'w' in period.lower():
                schema_set = self.default_weekly_schemas
            elif period and len(period) == 6 and 'q' not in period.lower():
                schema_set = self.default_monthly_schemas
            else:
                schema_set = self.default_schema_set

        if not is_top_row:
            return schema_set['grid_schema']
        if is_top_row and is_leaf:
            return schema_set['rep_schema']
        return schema_set['summary_schema']

    @cached_property
    def get_roles(self):
        default_role = [DEFAULT_ROLE]
        if self.has_roles:
            return self.config.get('role_schemas', {}).keys() + default_role
        return default_role

    @cached_property
    def has_roles(self):
        return len(self.config.get('role_schemas', {}).keys()) > 0

    def get_export_schema(self, as_of, node):
        """
        get name of export schema for a data row in fm grid
        Arguments:
            as_of {int} -- epoch timestamp to get schema as of                      1556074024910
            node {str} -- hierarchy node                                            '0050000FLN2C9I2'
            user {str} -- user name                                                 'gnana'

        Returns:
            str -- name of schema to use for node
        """
        for rules, schema_set in self.rule_based_export_schema_sets:
            if passes_configured_hierarchy_rules(as_of, node, rules):
                break
        else:
            schema_set = self.default_export_schema_set

        return schema_set['export_schema']

    def get_audit_trail_schema(self, as_of, node):
        """
        get name of audit trail schema for a data row in fm grid
        Arguments:
            as_of {int} -- epoch timestamp to get schema as of                      1556074024910
            node {str} -- hierarchy node                                            '0050000FLN2C9I2'
            user {str} -- user name                                                 'gnana'

        Returns:
            str -- name of schema to use for node
        """
        for rules, schema_set in self.rule_based_export_schema_sets:
            if passes_configured_hierarchy_rules(as_of, node, rules):
                break
        else:
            schema_set = self.default_audit_trail_schema_set

        return schema_set['audit_trail_schema']

    def get_schema_fields(self, schema, role=None):
        """
        fm fields that appear in schema

        Arguments:
            schema {str} -- schema name

        Returns:
            set -- {fields}
        """
        schema_fields = set()
        if self.config.get('persona_schemas'):
            personas = sec_context.get_current_user_personas()
            for persona in personas:
                schema_fields.update([field for column in get_nested(self.config, ['persona_schemas', persona, schema], {}).values()
                    for field in column['fields']])

        if self.config.get('role_schemas', {}):
            user_role = sec_context.get_current_user_role()
            if self.has_roles and role:
                user_role = role
            if user_role and self.config.get('role_schemas', {}).get(user_role):
                schema_fields = set([field for column in get_nested(self.config, ['role_schemas', user_role, schema], {}).values()
                                 for field in column['fields']])

        return schema_fields if schema_fields else set([field for column in get_nested(self.config, ['schemas', schema], {}).values()
                    for field in column['fields']])

    @cached_property
    def rule_based_export_schema_sets(self):
        """
        export schema sets associated with hierarchy based rules
        will use the first schema set that passes the hierarchy rules for a node
        so the list order is important

        Returns:
            list -- [([hierarchy rules], {schema type: schema name}), ]
        """
        return get_nested(self.config, ['export', 'rule_schema_sets'], [])

    @cached_property
    def rule_based_schema_sets(self):
        """
        fm schema sets associated with hierarchy based rules
        will use the first schema set that passes the hierarchy rules for a node
        so the list order is important

        Returns:
            list -- [([hierarchy rules], {schema type: schema name}), ]
        """
        return get_nested(self.config, ['display', 'rule_schema_sets'], [])


    @cached_property
    def rule_based_dashboard_schema_sets(self):
        """
        fm schema sets associated with hierarchy based rules
        will use the first schema set that passes the hierarchy rules for a node
        so the list order is important

        Returns:
            list -- [([hierarchy rules], {schema type: schema name}), ]
        """
        return get_nested(self.config, ['display', 'rule_based_dashboard_schema_sets'], [])

    @cached_property
    def default_schema_set(self):
        """
        default schema set

        Returns:
            dict -- {schema type: schema name}
        """
        return {'summary_schema': get_nested(self.config, ['display', 'summary_schema']),
                'grid_schema': get_nested(self.config, ['display', 'grid_schema']),
                'rep_schema': get_nested(self.config, ['display', 'rep_schema'])}

    @cached_property
    def default_weekly_schemas(self):
        return {'summary_schema': get_nested(self.config, ['display', 'weekly_summary_schema'],get_nested(self.config, ['display', 'summary_schema'])),
                'grid_schema': get_nested(self.config, ['display', 'weekly_grid_schema'],get_nested(self.config, ['display', 'grid_schema'])),
                'rep_schema': get_nested(self.config, ['display', 'weekly_rep_schema'],get_nested(self.config, ['display', 'rep_schema']))}

    @cached_property
    def default_monthly_schemas(self):
        return {'summary_schema': get_nested(self.config, ['display', 'monthly_summary_schema'],
                                             get_nested(self.config, ['display', 'summary_schema'])),
                'grid_schema': get_nested(self.config, ['display', 'monthly_grid_schema'],
                                          get_nested(self.config, ['display', 'grid_schema'])),
                'rep_schema': get_nested(self.config, ['display', 'monthly_rep_schema'],
                                         get_nested(self.config, ['display', 'rep_schema']))}

    @cached_property
    def default_export_schema_set(self):
        """
        default export schema set

        Returns:
            dict -- {schema type: schema name}
        """
        default_schema = get_nested(self.config, ['export', 'export_schema'])
        if not default_schema:
            default_schema = 'default'
        return {'export_schema': default_schema}

    @cached_property
    def default_audit_trail_schema_set(self):
        """
        default audit trail schema set

        Returns:
            dict -- {schema type: schema name}
        """
        default_schema = get_nested(self.config, ['export', 'audit_trail_schema'])
        if not default_schema:
            default_schema = 'default'
        return {'audit_trail_schema': default_schema}

    @cached_property
    def quarter_editable(self):
        return self.config.get('quarter_editable', False)

    @cached_property
    def export_month_data_at_quarter(self):
        return self.config.get('export_month_data_at_quarter', False)

    @cached_property
    def fields_to_show_for_month(self):
        return self.config.get('fields_to_show_for_month', [])

    @cached_property
    def component_periods_editable(self):
        return self.config.get('component_periods_editable', False)

    @cached_property
    def future_qtrs_editable_count(self):
        return self.config.get('future_qtrs_editable_count', 1)

    @cached_property
    def past_qtrs_editable_count(self):
        return self.config.get('past_qtrs_editable_count', 0)

    @cached_property
    def week_period_editability(self):
        """
            returns string with value 'W'.
            Could be set to 'Q' or 'M'.
            This config is the beginning period from where the weeks needs to be editable
        """
        week_period_editability = self.config.get('week_period_editability', 'W')
        return week_period_editability if week_period_editability in ['Q', 'M', 'W'] else 'W'

    @cached_property
    def is_summary_editable(self):
        return self.config.get('is_summary_editable', False)

    @cached_property
    def year_is_forecasted(self):
        return self.config.get('year_is_forecasted', False)

    @cached_property
    def snapshot_config(self):
        return self.config.get('snapshot_config', {u'allowed_periods': [],
                                                   u'chipotle': False,
                                                   u'daily': True,
                                                   u'future_qtr_run': {'chipotle': False,
                                                                       'daily': True},
                                                   u'future_qtrs_process_count': 2,
                                                   u'load_snapshot_stale_data': True,
                                                   u'past_qtrs_process_count': 2,
                                                   u'show_stale_limit': 1000000000,
                                                   u'yearly_view': True,
                                                   u'past_qtr_run': {'chipotle': False,
                                                                     'daily': True}})

    @cached_property
    def future_qtrs_prefetch_count(self):
        snapshot_config = self.snapshot_config
        future_qtrs_prefetch_count = 0
        if snapshot_config:
            future_qtrs_prefetch_count = snapshot_config.get("future_qtrs_process_count", 0)
        return future_qtrs_prefetch_count

    @cached_property
    def past_qtrs_prefetch_count(self):
        snapshot_config = self.snapshot_config
        past_qtrs_prefetch_count = 0
        if snapshot_config:
            past_qtrs_prefetch_count = snapshot_config.get("past_qtrs_process_count", 0)
        return past_qtrs_prefetch_count

    @cached_property
    def schemas(self):
        """
        defines how fm fields are displayed in the fm grid and if users can edit them
        comprised of many row schemas
        each row schema is has multiple columns
        each column can have up to 3 fm fields displayed in it

        Returns:
            dict -- {schema name: {schema dtls}}
        """
        try:
            is_admin = 'administrator' in sec_context.get_effective_user().roles.keys()
        except:
            is_admin = False
        schemas_set = []
        if self.config.get('persona_schemas', {}):
            personas = sec_context.get_current_user_personas()
            for persona in personas:
                schemas_set.append(
                    copy.deepcopy(self.config.get('persona_schemas', {}).get(persona, self.config.get('schemas', {}))))

        user_role = sec_context.get_current_user_role()
        if user_role and self.config.get('role_schemas', {}).get(user_role):
            schemas_set = [copy.deepcopy(self.config.get('role_schemas', {})).get(user_role)]

        if not schemas_set:
            schemas_set = [copy.deepcopy(self.config.get('schemas', {}))]
        res_schema = {}
        for schemas in schemas_set:
            for row_schema in schemas.values():
                for label, column in row_schema.items():
                    column['sort_by'] = column.get('sort_by', column['fields'][0])
                    fields, column['fields'] = column['fields'], []
                    editable_fields = []
                    for field in fields:
                        admin_editable_check=True
                        if field in self.config.get("admin_editable_fields", []): admin_editable_check=is_admin
                        field_dtls = {'field': field,
                                      'editable': ((field in self.user_entered_fields and not self.has_readonly_access) or (field in self.forecast_service_editable_fields)) and admin_editable_check,
                                      'format': self.fields[field].get('format', 'amount')}
                        # if field is a user sum field, and located below a user entered field, or a nc field
                        # users can roll value of user sum field up to user entered field
                        if field_dtls and field_dtls['editable']:
                            editable_fields.append(field)

                    for field in fields:
                        admin_editable_check=True
                        if field in self.config.get("admin_editable_fields", []): admin_editable_check=is_admin
                        field_dtls = {'field': field,
                                      'editable': ((field in self.user_entered_fields and not self.has_readonly_access) or (field in self.forecast_service_editable_fields)) and admin_editable_check,
                                      'format': self.fields[field].get('format', 'amount')}
                        if field in self.aviso_fields:
                            field_dtls['explainable'] = True

                        # if field is a user sum field, and located below a user entered field, or a nc field
                        # users can roll value of user sum field up to user entered field
                        # TODO: deprecate US
                        # in this case we are adding rollup_to field on the basis of index of the field and editable flag of the previous field
                        # but if the order is changed in the config, then this logic will not work

                        if self.fields[field]['type'] in ['US', 'CS', 'NC', 'FN'] and field_dtls and not field_dtls['editable']:
                            if editable_fields:
                                field_dtls.update({'rollup_to': editable_fields[0]})
                        # BEFORE:
                        # if field is deal rollup and located below a user entered field
                        # and those are the only two fields in the cell
                        # user can roll value of deal sum up to user entered field

                        # NOW:
                        # if field is deal rollup and located below or above a user entered field
                        # and those are the only two fields in the cell or even if there are more than 2 fields in the cell
                        # user can roll value of deal sum up to user entered field
                        elif self.fields[field]['type'] == 'DR' and len(fields) == 2 and field_dtls and not field_dtls['editable']:
                            if editable_fields:
                                field_dtls.update({'rollup_to': editable_fields[0]})
                        column['fields'].append(field_dtls)

        for schemas in schemas_set:
            for k,v in schemas.items():
                if k not in res_schema:
                    res_schema[k] = {}
                res_schema[k].update(v)

        return res_schema

    def role_schemas(self, role):
        """
        defines how fm fields are displayed in the fm grid and if users can edit them
        comprised of many row schemas
        each row schema is has multiple columns
        each column can have up to 3 fm fields displayed in it
        Returns:
            dict -- {schema name: {schema dtls}}
        """
        schemas_set = [copy.deepcopy(self.config.get('role_schemas', {})).get(role)]

        if not schemas_set:
            schemas_set = [copy.deepcopy(self.config.get('schemas', {}))]
        res_schema = {}
        for schemas in schemas_set:
            for row_schema in schemas.values():
                for label, column in row_schema.items():
                    column['sort_by'] = column.get('sort_by', column['fields'][0])
                    fields, column['fields'] = column['fields'], []
                    editable_fields = []
                    for field in fields:
                        field_dtls = {'field': field,
                                      'editable': (field in self.user_entered_fields and not self.has_readonly_access) or (field in self.forecast_service_editable_fields),
                                      'format': self.fields[field].get('format', 'amount')}
                        # if field is a user sum field, and located below a user entered field, or a nc field
                        # users can roll value of user sum field up to user entered field
                        if field_dtls and field_dtls['editable']:
                            editable_fields.append(field)

                    for field in fields:
                        field_dtls = {'field': field,
                                      'editable': (field in self.user_entered_fields and not self.has_readonly_access) or (field in self.forecast_service_editable_fields),
                                      'format': self.fields[field].get('format', 'amount')}
                        if field in self.aviso_fields:
                            field_dtls['explainable'] = True
                        # if field is a user sum field, and located below a user entered field, or a nc field
                        # users can roll value of user sum field up to user entered field
                        # TODO: deprecate US
                        # in this case we are adding rollup_to field on the basis of index of the field and editable flag of the previous field
                        # but if the order is changed in the config, then this logic will not work

                        if self.fields[field]['type'] in ['US', 'CS', 'NC', 'FN'] and field_dtls and not field_dtls['editable']:
                            if editable_fields:
                                field_dtls.update({'rollup_to': editable_fields[0]})
                        # if field is deal rollup and located below a user entered field
                        # and those are the only two fields in the cell
                        # user can roll value of deal sum up to user entered field
                        elif self.fields[field]['type'] == 'DR' and len(fields) == 2 and field_dtls and not field_dtls['editable']:
                            if editable_fields:
                                field_dtls.update({'rollup_to': editable_fields[0]})
                        column['fields'].append(field_dtls)

        for schemas in schemas_set:
            for k, v in schemas.items():
                if k not in res_schema:
                    res_schema[k] = {}
                res_schema[k].update(v)

        return res_schema

    def is_special_pivot_segmented(self, node):
        if node and node.split('#')[0] in self.get_special_pivots:
            return False

        return True

    def rendered_schemas(self, active_period=True, forecast_period=True, plan_is_editable=True, node=None,period=None):
        """
        fm schema, rendered for web app
        if not active period, editability is removed

        Keyword Arguments:
            active_period {bool} -- is period active (default: {True})
            forecast_period {bool} -- does period have an aviso forecast (default: {True})
            plan_is_editable {bool} -- are plan fields editable (default: {True})

        Returns:
           dict -- {schema name: {schema dtls}}
        """
        if active_period and forecast_period and plan_is_editable \
                and not (self.has_segments and self.is_special_pivot_segmented(node)):
            return self.schemas
        from utils.date_utils import epoch
        schemas_copy = copy.deepcopy(self.schemas)  # TODO: so bad, dont do this
        segment_schemas = {}
        node_pivot = node.split('#')[0] if node is not None else None
        hide_fm_edit_nodes = self.config.get('display', {}).get('hide_fm_edit', [])
        for row_name, row_schema in schemas_copy.items():
            forecast_columns = set()
            for label, column in row_schema.items():
                for field_dtls in column['fields']:
                    if not active_period:
                        field_dtls['editable'] = False
                        field_dtls.pop('rollup_to', None)
                    if not self.fields[field_dtls['field']].get('rollup_to'):
                        field_dtls.pop('rollup_to', None)
                    if field_dtls['field'] in self.plan_fields and not plan_is_editable:
                        field_dtls['editable'] = False
                    if not forecast_period and field_dtls['field'] in self.aviso_fields:
                        # remove ai fields from non forecast periods, cry
                        forecast_columns.add(label)
                    if self.has_readonly_access or node_pivot in hide_fm_edit_nodes:
                        field_dtls['editable'] = False
            for forecast_column in forecast_columns:
                row_schema.pop(forecast_column, None)
            # For some tenants, segments are enabled at one level and not enabled at some levels.
            # In these cases we call the 'schemas' API everytime we drilldown into a node and we get schema based on node
            condition = self.has_segments if not node else (len(self.get_segments(epoch().as_epoch(), node=node)) > 1)
            condition = condition and self.is_special_pivot_segmented(node)

            if condition:
                schema_segments = {'segment'}
                if self.config.get('show_hetero_segments', False): ##change this False before raising PR
                    schema_segments = set([segment for segment in self.segments.keys() if segment != 'all_deals'])
                for schema_segment in schema_segments:
                    segment_schemas['_'.join([row_name, schema_segment])] = copy.deepcopy(row_schema)
                    non_seg_schema = copy.deepcopy(row_schema)
                    schemas_copy[row_name] = non_seg_schema
                for label, column in segment_schemas.items():
                    if '_' in label:
                        schema_segment = label.split('_')[-1]
                    for k, v in column.items():
                        for field_dtls in v['fields']:
                            if field_dtls['field'] in self.aviso_fields:
                                field_dtls['explainable'] = False
                            if schema_segment in self.count_override_segments and field_dtls['format'] != 'percentage':
                                field_dtls['format'] = 'count'
                            if schema_segment in self.percentage_override_segments:
                                field_dtls['format'] = 'percentage'
                                field_dtls['editable'] = False
                            if schema_segment in self.FN_segments or \
                                    (schema_segment in self.disable_editability_segment_field_map and field_dtls[
                                        'field'] in
                                     self.disable_editability_segment_field_map[schema_segment]):
                                field_dtls['editable'] = False
                            if field_dtls['field'] in self.quarterly_high_low_fields and "Q" in period:
                                field_dtls['editable'] = True
                            if field_dtls['field'] not in self.quarterly_high_low_fields and "Q" in period and self.is_custom_quarter_editable:
                                field_dtls['editable'] = False

                for label, column in non_seg_schema.items():
                    for field_dtls in column['fields']:
                        if row_name != get_nested(self.config, ['display', 'rep_schema'], 'rep') or not self.is_rep_summary_editable:
                            field_dtls['editable'] = field_dtls['field'] in self.forecast_service_editable_fields and active_period
                            if not field_dtls['editable'] and not self.is_summary_editable:
                                field_dtls.pop('rollup_to', None)
        return merge_dicts(schemas_copy, segment_schemas)

    def rendered_mobile_schemas(self, active_period=True, forecast_period=True, plan_is_editable=True):
        """
        fm schema, rendered for mobile app
        if not active period, editability is removed

        Keyword Arguments:
            active_period {bool} -- is period active (default: {True})
            forecast_period {bool} -- does period have an aviso forecast (default: {True})
            plan_is_editable {bool} -- are plan fields editable (default: {True})

        Returns:
           dict -- {schema name: {schema dtls}}
        """
        # TODO: segments
        schemas_copy = copy.deepcopy(self.schemas)  # TODO: so bad, dont do this
        for profile, label, column in dict_of_dicts_gen(schemas_copy):
            column['sort_by'] = column.get('sort_by', column['fields'][0])
            fields, editable_field, rollup_to = [], None, None
            for i, field in enumerate(column['fields']):
                fields.append({'field': field['field'], 'format': field['format']})
                if active_period and field['editable'] and (field['field'] not in self.plan_fields or plan_is_editable):
                    editable_field = field['field']
                # TODO: deprecate US
                if self.fields[field['field']]['type'] in ['US', 'CS', 'NC'] and i and column['fields'][i - 1]['editable']:
                    rollup_to = field['field']
                elif self.fields[field['field']]['type'] == 'DR' and i and len(fields) == 2 and column['fields'][i - 1]['editable']:
                    rollup_to = field['field']
            column['fields'] = fields
            column['editable'] = editable_field is not None
            if editable_field:
                column['editable_field'] = editable_field
            if rollup_to:
                column['use_for_rollup'] = rollup_to
        return schemas_copy

    @cached_property
    def columns(self):
        """
        all fm columns across all schemas

        Returns:
            dict -- {column: column details}
        """
        return {column: column_dtls for row_schema in self.schemas.values()
                for column, column_dtls in row_schema.items()}

    def role_columns(self, role):
        return {column: column_dtls for row_schema in self.role_schemas(role).values()
                for column, column_dtls in row_schema.items()}

    @cached_property
    def column_order(self):
        """
        order that columns get displayed in front end

        Returns:
            list -- [column names]
        """
        user_role = sec_context.get_current_user_role()
        if user_role and self.config.get('role_schemas', {}).get(user_role):
            if user_role in ['renewal_rep', 'renewal_manager']:
                return get_nested(self.config, ['display', user_role+'_order'], sorted(self.columns.keys(), reverse=True))
            else:
                return get_nested(self.config, ['display', user_role+'_order'], sorted(self.columns.keys()))
        return get_nested(self.config, ['display', 'order'], sorted(self.columns.keys()))

    @cached_property
    def default_hidden_fields(self):
        """
        default_hidden_fields are columns hidden in front end

        Returns:
            list -- [column names]
        """
        user_role = sec_context.get_current_user_role()
        if user_role and self.config.get('role_schemas', {}).get(user_role):
            return get_nested(self.config, ['display', user_role+'_default_hidden_fields'], [])
        return get_nested(self.config, ['display', 'default_hidden_fields'], [])

    @cached_property
    def what_changed_columns(self):
        return get_nested(self.config, ['new_home_page', 'what_changed_columns'], [])

    @cached_property
    def has_readonly_access(self):
        user_det = sec_context.get_effective_user()
        if user_det and user_det.roles.get('user'):
            user_roles = user_det.roles['user']
            if isinstance(user_roles, dict) and 'read_only_access' in user_roles:
                return True
            for user_role in user_roles:
                if 'read_only_access' in user_role:
                    return True
        return False

    def rendered_column_order(self, schema, forecast_period=True):
        """
        column order rendered for front end

        Arguments:
            schema {str} -- schema name

        Keyword Arguments:
            forecast_period {bool} -- does period have an aviso forecast (default: {True})

        Returns:
            list -- [column names]
        """
        column_order = [x for x in self.column_order if x in self.schemas[schema]]
        if forecast_period:
            return column_order
        columns = {column for row_schema in self.schemas.values()
                   for column, column_dtls in row_schema.items()
                   if all(field_dtls['field'] not in self.aviso_fields.keys() for field_dtls in column_dtls['fields'])}
        return [x for x in column_order if x in columns]

    #
    # FM Status
    #
    @cached_property
    def fm_status_enabled(self):
        return get_nested(self.config, ['fm_status_enabled'], False)

    @cached_property
    def slack_config(self):
        return self.config.get('slack_config', None)

    @cached_property
    def forecast_update_nudge_config(self):
        """Finance Analyst/Manager/VP
           IT
           Sales Ops/Manager/VP"""
        config_params = {
            'roles_to_exclude': ['ceo', 'cfo', 'cro', 'finance_analyst', 'it', 'finance_manager',
                                 'finance_vp', 'sales_ops', 'sales_manager', 'sales_vp_director']
        }
        return get_nested(
            self.config,
            ['nudge_config', 'forecast_update_nudge_config'],
            config_params,
        )
    @cached_property
    def fm_status_fields(self):
        """
        Fields that are used for fm status
        It should be configureable, if it is not, use User Enter fields by default
        """
        fm_status_flds = get_nested(self.config, ['fm_status', 'fields'], None)
        # if fields not configured, use following default fields
        if not fm_status_flds:
            fm_status_flds = []
            flds = get_nested(self.config, ['fields'], {})
            for k, v in flds.items():
                if v.get('type', 'None') == 'UE' and k != 'planny':
                    fm_status_flds.append(k)
        return fm_status_flds

    @cached_property
    def fm_status_dr_fields(self):
        """
        Fields that are used for fm status
        It should be configurable, if it is not, use User Enter fields by default
        """
        fm_status_flds = get_nested(self.config, ['fm_status', 'fields'], [])
        # if fields not configured, use following default fields
        flds = get_nested(self.config, ['fields'], {})
        fm_status_flds_dr = []
        for k, v in flds.items():
            if v.get('type', 'None') in ['DR', 'prnt_DR'] and k in fm_status_flds:
                fm_status_flds_dr.append(k)
        return fm_status_flds_dr

    @cached_property
    def fm_status_fieldlabel(self):
        return get_nested(self.config, ['fm_status', 'field_label'], {})



    @cached_property
    def fm_status_combo(self):
        return get_nested(self.config, ['fm_status', 'field_combo'], {})

    '''
    Change validation rules so as to have status fields at all levels i.e. summary, grid and rep in config itself
    Grid level fields maybe NC (Ex: commitcondition - Lacework), Allow NC fields with source fields as CS/UE
    '''
    def _validate_fm_status_flds(self, fields):
        if not fields:
            return True, 'No config found'
        fm_fields = self.config['fields']
        for fld in fields:
            if fm_fields.get(fld)['type'] == 'NC' or fm_fields.get(fld)['type'] == 'FN':
                source_fields = fm_fields[fld]['source']
                for src_field in source_fields:
                    if fm_fields.get(src_field, {'type': None})['type'] not in ['UE', 'CS', 'DR', 'prnt_DR']:
                        return False, 'Field {} not valid. Needs to be User Enter/ Child Sum/DR fields'.format(src_field)
            elif fm_fields.get(fld, {'type': None})['type'] not in ['UE', 'CS', 'DR', 'prnt_DR']:
                return False, 'Field {} not valid. Needs to be User Enter/Child Sum/DR fields'.format(fld)
        return True, 'valid'

    def get_fm_status_timestamp(self, is_pivot_special=False):
        # get timestamp for fm status check
        if get_nested(self.config, ['fm_status', 'fixed_cutoff'], False):
            weekday, hour = get_nested(self.config, ['fm_status', 'fixed_cutoff'])
            # Making the cutoff for CRR pivots as a month
            if is_pivot_special:
                # Subtracting one day to reach the last day of the last month
                cutoff = get_bom(epoch(now().replace(hour=hour, minute=0, second=0))) - timedelta(days=1)
            else:
                cutoff = now().replace(hour=hour, minute=0, second=0) - timedelta(days=(now().weekday() - weekday) % 7)
                if cutoff > now():
                    cutoff -= timedelta(days=7)
                cutoff = epoch(cutoff)
            cutoff = cutoff.as_epoch()
        else:
            cutoff = datetime2epoch(now()) - (try_float(get_nested(self.config, ['fm_status', 'recency_cutoff'], 72) * 60 * 60 * 1000))
        return cutoff

    @cached_property
    def forecast_insights_config(self):
        return get_nested(self.config, ['forecast_insights_config'], {})

    @cached_property
    def status_nudge_enabled(self):
        return get_nested(self.config, ['status_nudge_enabled'], False)

    @cached_property
    def status_config(self):
        return get_nested(self.config, ['status_config'], {})

    @cached_property
    def require_all(self):
        return get_nested(self.config, ['fm_status', 'require_all'], False)

    @cached_property
    def fm_alert_rules(self):
        return get_nested(self.config, ['fm_alert_rules'], {})

    #
    # Dashboard
    #
    @cached_property
    def weekly_data(self):
        return get_nested(self.config, ['weekly_data'], default=[])

    @cached_property
    def dashboard_cards(self):
        return get_nested(self.config, ['dashboard', 'cards'])

    def dashboard_cards_fs(self, schema=None):
        if schema is None and not self.config.get('role_dashboard_schemas', {}) and \
                not self.config.get('persona_dashboard_schemas', {}):
            return get_nested(self.config, ['dashboard', 'cards'])
        schema_fields = []
        if self.config.get('persona_dashboard_schemas'):
            personas = sec_context.get_current_user_personas()
            if 'executive' in personas:
                personas = ['executive']
            for persona in personas:
                schema_fields += get_nested(self.config, ['persona_dashboard_schemas', persona, 'cards'], {})
            if schema_fields:
                schema_fields = {v['label']: v for v in schema_fields}.values()

        if self.config.get('role_dashboard_schemas', {}):
            user_role = sec_context.get_current_user_role()
            if user_role and self.config.get('role_dashboard_schemas', {}).get(user_role):
                schema_fields = get_nested(self.config, ['role_dashboard_schemas', user_role, 'cards'], {})

        schema_fields_default = get_nested(self.config, ['dashboard_schemas', schema, 'cards']) if schema else\
            get_nested(self.config, ['dashboard', 'cards'])

        return schema_fields if schema_fields else schema_fields_default

    def nq_dashboard_cards_fs(self, schema=None):
        if schema is None and not self.config.get('role_nq_dashboard_schemas', {}) and \
                not self.config.get('persona_nq_dashboard_schemas', {}):
            return get_nested(self.config, ['nq_dashboard', 'cards'])
        schema_fields = []
        if self.config.get('persona_nq_dashboard_schemas'):
            personas = sec_context.get_current_user_personas()
            if 'executive' in personas:
                personas = ['executive']
            for persona in personas:
                schema_fields += get_nested(self.config, ['persona_nq_dashboard_schemas', persona, 'cards'], {})
            if schema_fields:
                schema_fields = {v['label']: v for v in schema_fields}.values()

        if self.config.get('role_nq_dashboard_schemas', {}):
            user_role = sec_context.get_current_user_role()
            if user_role and self.config.get('role_nq_dashboard_schemas', {}).get(user_role):
                schema_fields = get_nested(self.config, ['role_nq_dashboard_schemas', user_role, 'cards'], {})

        schema_fields_default = get_nested(self.config, ['nq_dashboard_schemas', schema, 'cards']) if schema else\
            get_nested(self.config, ['nq_dashboard', 'cards'])

        return schema_fields if schema_fields else schema_fields_default

    @cached_property
    def default_dashboard_schema_set(self):
        """
        default dashbard schema set

        Returns:
            dict -- {schema type: schema name}
        """
        return {'mgr_dashboard_schema': get_nested(self.config, ['display', 'mgr_dashboard_schema'], None),
                'rep_dashboard_schema': get_nested(self.config, ['display', 'rep_dashboard_schema'], None)}

    @cached_property
    def default_nq_dashboard_schema_set(self):
        """
        default dashbard schema set

        Returns:
            dict -- {schema type: schema name}
        """
        return {'mgr_nq_dashboard_schema': get_nested(self.config, ['display', 'mgr_nq_dashboard_schema'], None),
                'rep_nq_dashboard_schema': get_nested(self.config, ['display', 'rep_nq_dashboard_schema'], None)}

    def get_dashboard_cards_schema(self, as_of, node, is_leaf=False, period=None):
            """
            get name of dashboard schema for a card in dashboard

            Arguments:
                as_of {int} -- epoch timestamp to get schema as of                      1556074024910
                node {str} -- hierarchy node                                            '0050000FLN2C9I2'
                user {str} -- user name                                                 'gnana'

            Keyword Arguments:
                is_top_row {bool} -- row is at top of fm grid (default: {False})
                is_leaf {bool} -- node is a leaf node (default: {False})

            Returns:
                str -- name of schema to use for node
            """
            for rules, schema_set in self.rule_based_dashboard_schema_sets:
                if passes_configured_hierarchy_rules(as_of, node, rules, period=period):
                    break
            else:
                schema_set = self.default_dashboard_schema_set
            if is_leaf:
                return schema_set['rep_dashboard_schema']
            return schema_set['mgr_dashboard_schema']

    def get_nq_dashboard_cards_schema(self, as_of, node, is_leaf=False, period=None):
            """
            get name of dashboard schema for a card in dashboard

            Arguments:
                as_of {int} -- epoch timestamp to get schema as of                      1556074024910
                node {str} -- hierarchy node                                            '0050000FLN2C9I2'
                user {str} -- user name                                                 'gnana'

            Keyword Arguments:
                is_top_row {bool} -- row is at top of fm grid (default: {False})
                is_leaf {bool} -- node is a leaf node (default: {False})

            Returns:
                str -- name of schema to use for node
            """
            for rules, schema_set in self.rule_based_dashboard_schema_sets:
                if passes_configured_hierarchy_rules(as_of, node, rules, period=period):
                    break
            else:
                schema_set = self.default_nq_dashboard_schema_set
            if is_leaf:
                return schema_set['rep_nq_dashboard_schema']
            return schema_set['mgr_nq_dashboard_schema']

    @cached_property
    def covid_labellings(self):
        return get_nested(self.config, ['dashboard', 'covid_labellings'], {})

    @cached_property
    def adaptive_metrics_cards(self):
        return get_nested(self.config, ['dashboard', 'adaptive_metrics'], {})

    @cached_property
    def booking_pipeline(self):
        return get_nested(self.config, ['dashboard', 'booking_pipeline'])

    @cached_property
    def dashboard_fields(self):
        fields = []
        for card in self.dashboard_cards:
            fields.extend([card['top_field'], card['bottom_field']])
            fields.extend([x['field'] for x in card.get('range_fields', [])])
            for row in card.get('rows', []):
                if 'field' in row:
                    fields.append(row['field'])
                if 'plan_field' in row:
                    fields.append(row['plan_field'])
                if 'booked_field' in row:
                    fields.append(row['booked_field'])
                if 'rollup_field' in row:
                    fields.append(row['rollup_field'])
            node_field_dicts = card.get('node_field_mapping', {})
            fields.extend([node_field_dicts.get(key) for key in node_field_dicts.keys()])
        return fields

    def dashboard_fields_fs(self, dashboard_cards):
        fields = []
        for card in dashboard_cards:
            fields.extend([card['top_field'], card['bottom_field']])
            fields.extend([x['field'] for x in card.get('range_fields', [])])
            for row in card.get('rows', []):
                if 'field' in row:
                    fields.append(row['field'])
                if 'plan_field' in row:
                    fields.append(row['plan_field'])
                if 'booked_field' in row:
                    fields.append(row['booked_field'])
                if 'rollup_field' in row:
                    fields.append(row['rollup_field'])
            node_field_dicts = card.get('node_field_mapping', {})
            fields.extend([node_field_dicts.get(key) for key in node_field_dicts.keys()])
        return fields


    def nq_dashboard_fields_fs(self, dashboard_cards):
        fields = []
        for card in dashboard_cards:
            fields.extend([card['top_field'], card['bottom_field']])
            fields.extend([x['field'] for x in card.get('range_fields', [])])
            for row in card.get('rows', []):
                if 'field' in row:
                    fields.append(row['field'])
                if 'plan_field' in row:
                    fields.append(row['plan_field'])
                if 'booked_field' in row:
                    fields.append(row['booked_field'])
                if 'rollup_field' in row:
                    fields.append(row['rollup_field'])
            node_field_dicts = card.get('node_field_mapping', {})
            fields.extend([node_field_dicts.get(key) for key in node_field_dicts.keys()])
        return fields

    @cached_property
    def adaptive_metrics_fields(self):
        fields = self.adaptive_metrics_cards.values()
        return fields

    @cached_property
    def next_quarter_label(self):
        return get_nested(self.config, ['dashboard', 'next_quarter_label'], 'Next Quarter Pipeline')

    @cached_property
    def current_quarter_fields(self):
        cq_fields = get_nested(self.config, ['dashboard', 'current_quarter_fields'], {})
        if self.linearity_field:
            cq_fields['linearity_target'] = self.linearity_field
        return cq_fields

    @cached_property
    def graph_y_axis_multiplier_in_pct(self):
        graph_y_axis_multiplier_value = get_nested(self.config, ['dashboard', 'graph_axis_multiplier_in_pct','y_axis'], 1)
        return graph_y_axis_multiplier_value

    @cached_property
    def graph_y_axis_multiplier_in_pct(self):
        graph_y_axis_multiplier_value = get_nested(self.config, ['dashboard', 'graph_axis_multiplier_in_pct','y_axis'], 1)
        return graph_y_axis_multiplier_value

    @property
    def next_quarter_fields(self):
        return {'aviso_forecast': 'tot_won_and_fcst',
                'run_rate': 'ubf',
                'pipeline': 'ep_epf',
                'pacing': 'unborn_existing',
                'pipe_target': 'nextq_pipe_target'
                }

    @cached_property
    def next_quarter_label(self):
        return get_nested(self.config, ['dashboard', 'next_quarter_label'], 'Next Quarter Pipeline')

    @cached_property
    def valid_status_fields_mapping(self):
        return get_nested(self.config, ['fm_status', 'valid_field_mapping'], {})

    @cached_property
    def auto_rollup(self):
        return get_nested(self.config, ['fm_status', 'auto_rollup'], False)

    @cached_property
    def gateway_schema(self):
        return self.config.get('gateway_schema', None)

    @cached_property
    def node_owner_mapping(self):
        return self.config.get('node_owner_mapping', False)

    @cached_property
    def hist_gateway_schema(self):
        return self.config.get('hist_gateway_schema', None)

    @cached_property
    def gateway_rep_schema(self):
        return self.config.get('gateway_rep_schema', None)

    @cached_property
    def gateway_weekly_schema(self):
        return self.config.get('gateway_weekly_schema', {})

    @cached_property
    def gateway_weekly_rep_schema(self):
        return self.config.get('gateway_weekly_rep_schema', {})

    @cached_property
    def weekly_forecast_field(self):
            return self.config.get('weekly_forecast_field', 'Weekly Commit')

    @cached_property
    def audit_trail_schema(self):
        audit_trail_schema = self.config.get('audit_trail_schema', [])
        for data in audit_trail_schema:
            if len(data) == 5:
                data.append(False)
        return audit_trail_schema

    @cached_property
    def export_day_of_start(self):
        return self.config.get('export_day_of_start',None)

    @cached_property
    def current_quarter_plan_label(self):
        return get_nested(self.config, ['dashboard', 'current_quarter_plan_label'], {})

    @cached_property
    def current_quarter_trend_label(self):
        return get_nested(self.config, ['dashboard', 'current_quarter_trend_label'], {})

    @cached_property
    def export_schema(self):
        """
        schema for fm grid csv export

        Returns:
            list -- [(label, fm field, record key, format)]
        """
        user_role = sec_context.get_current_user_role()
        default_schema = []
        for column in self.column_order:
            column_dtls = self.columns[column]
            for field in column_dtls['fields']:
                label = column_dtls['label'] + ' ' + self.fields[field['field']]['type']
                default_schema.append((label, field['field'], 'val', field['format']))

        if user_role and self.config.get('role_schemas', {}).get(user_role):
            return self.config.get(user_role+'_export_schema', default_schema)

        return self.config.get('export_schema', default_schema)

    @cached_property
    def export_pdf_fields(self):
        return self.config.get('export_pdf_fields', [])

    @cached_property
    def show_path_and_parent_fm_export(self):
        """
        an option for fm grid csv export

        Returns:
            bool
        """
        return self.config.get("show_path_and_parent_fm_export", False)

    @cached_property
    def show_leadership(self):
        """
        an option for fm grid csv export

        Returns:
            bool
        """
        return self.config.get("show_leadership", False)

    @cached_property
    def show_emails_in_export(self):
        """
        an option for fm grid csv export

        Returns:
            bool
        """
        return self.config.get("show_emails_in_export", False)

    @cached_property
    def show_path_and_parent_audit_export(self):
        """
        an option for audit trail csv export

        Returns:
            bool
        """
        return self.config.get("show_path_and_parent_audit_export", False)

    @cached_property
    def rep_level_segment_editability(self):
        """
        an option for setting rep level segment's editability

        Returns:
            bool
        """
        return self.config.get("rep_level_segment_editability", True)

    @cached_property
    def rep_segment_view_enabled(self):
        """
        an option for setting rep level segment's editability
        Returns:
            bool
        """
        return self.config.get("rep_segment_view_enabled", False)

    #
    # Validation
    #
    def validate(self, config):
        """
        validate config

        Arguments:
            config {dict} -- config dictionary

        Returns:
            tuple - bool(valid config), [error messages]
        """
        if not config:
            return True, 'no config provided'
        schemas = config.get('schemas', {})
        fields = merge_dicts(config.get('fields', {}), self._default_aviso_fields)
        fm_status_flds = get_nested(config, ['fm_status', 'fields'], None)
        display = config.get('display', {})
        segments = config.get('segments', {})
        past_qtrs_editable_count = config.get('past_qtrs_editable_count', 0)
        future_qtrs_editable_count = config.get('future_qtrs_editable_count', 0)
        good_schema, schema_msg = self._validate_schemas(schemas, fields, display)
        good_fields, field_msg = self._validate_fields(fields)
        good_dash, dash_msg = self._validate_dashboard(config.get('dashboard', {}), fields)
        good_plan, plan_msg = self._validate_plans(config.get('plan_fields', []), fields)
        good_lin, lin_msg = self._validate_linearity(config.get('linearity_field'), fields)
        good_sense, sense_msg = self._validate_sanity(schemas, fields, config)
        fm_status, status_msg = self._validate_fm_status_flds(fm_status_flds)
        good_segs, segs_msg = self._validate_segments(segments)
        good_export, export_msg = self._validate_export(config.get('export_schema', []), fields)
        user_entered_fields = [k for k, v in fields.items() if v['type'] == 'UE']
        DR_fields = [k for k, v in fields.items() if v['type'] == 'DR']
        FN_fields = [k for k, v in fields.items() if v['type'] == 'FN']
        good_audit_trail_export, audit_trail_msg = self._validate_audit_trail(config.get('audit_trail_schema', []),
                                                                              user_entered_fields, DR_fields, FN_fields)
        fields_to_fc_mapping = config.get('fields_to_forecast_category_name_map', {})
        good_fields_to_fc_mapping, fields_to_fc_mapping_msg = self._validate_fields_to_fc_mapping(fields_to_fc_mapping, fields)
        return good_schema and good_fields and good_dash and good_plan and good_lin and good_sense and fm_status and good_segs and good_export and \
               good_fields_to_fc_mapping and good_audit_trail_export, \
               [schema_msg, field_msg, dash_msg, plan_msg, lin_msg, sense_msg, segs_msg, export_msg, fields_to_fc_mapping_msg, audit_trail_msg]


    def _validate_schemas(self, schemas, fields, display):
        for profile, label, column in dict_of_dicts_gen(schemas):
            if 'label' not in column:
                return False, 'no label provided for {}'.format(profile)
            if 'fields' not in column:
                return False, 'no fields for {}'.format(profile)
            for field in column['fields']:
                if field not in fields:
                    return False, '{} not in fields for {}, fields: {}'.format(field, profile, fields)
        if not display:
            return True, ''
        for schema_type in ['summary_schema', 'rep_schema', 'grid_schema']:
            if schema_type not in display:
                return False, 'must have default {} in display config'.format(schema_type)
            if display[schema_type] not in schemas:
                return False, 'default display schema {} not in schemas'.format(display[schema_type])
        for rules, schema_set in display.get('rule_schema_sets', []):
            for complex_rule in rules:
                for rule, _ in complex_rule:
                    if rule not in HIERARCHY_RULES:
                        return False, 'rule: {} not in hierarchy rules: {}'.format(rule, HIERARCHY_RULES.keys())
            for schema_type in ['summary_schema', 'rep_schema', 'grid_schema']:
                if schema_type not in schema_set:
                    return False, 'must have default {} in display config'.format(schema_type)
                if schema_set[schema_type] not in schemas:
                    return False, 'rule based display schema {} not in schemas'.format(display[schema_type])
        columns = {col for schema in schemas.values() for col in schema.keys()}
        for col in display.get('order'):
            if col not in columns:
                return False, 'column: {} in display order but not in columns: {}'.format(col, list(columns))
        return True, ''

    def _validate_fields(self, fields):
        if not fields:
            return False, 'no fields provided'
        try:
            ordered_fields = top_sort(fields, ['source'])
        except UndefinedError as e:
            msg = 'undefined dependent for field: {}, dep: {}, config: {}'.format(e[0], e[1], fields)
            logger.warning(msg)
            return False, msg
        except CycleError as e:
            msg = 'cycle detected on field: {}, config: {}'.format(e, fields)
            logger.warning(msg)
            return False, msg

        validators = {'UE': self._validate_user_entered_field,
                      'US': self._validate_user_sum_field,
                      'DR': self._validate_deal_rollup_field,
                      'FN': self._validate_formula_field,
                      'AI': self._validate_ai_field,
                      'NC': self._validate_node_conditional_field,
                      'CS': self._validate_child_sum_field,
                      'prnt_DR': self._validate_prnt_DR_field
                      }

        for field in ordered_fields:
            try:
                validators[fields[field]['type']](field, fields)
            except AssertionError as e:
                logger.warning(e)
                return False, e
            except KeyError:
                msg = 'invalid field type on field: {}, config: {}'.format(field, fields[field])
                logger.warning(msg)
                return False, msg
            except FilterError:
                msg = 'invalid filter on field: {}, config: {}'.format(field, fields[field])
                logger.warning(msg)
                return False, msg

        return True, ''

    def _validate_user_entered_field(self, field, fields):
        return True

    def _validate_user_sum_field(self, field, fields):
        # TODO: deprecate this
        types = set()
        for source_field in fields[field]['source']:
            field_type = fields[source_field]['type']
            types.add(field_type)
            assert field_type in ['UE', 'US'], "source: {} is not in US or UE, config: {}".format(source_field, fields)
            assert len(types) == 1, "not allowed to mix source types, {}, {}".format(source_field, fields)
            if field_type == 'US':
                for src_field in fields[source_field]['source']:
                    assert fields[src_field]['type'] == 'UE', "source: {} is not UE, config: {}".format(
                        source_field, fields)

    def _validate_deal_rollup_field(self, field, fields):
        if 'filter' not in fields[field]:
            raise AssertionError('filter not in field: {}, dtls: {}'.format(field, fields[field]))
        if 'sum_field' not in fields[field]:
            raise AssertionError('sum_field not in field: {}, dtls: {}'.format(field, fields[field]))
        _valid_filter(fields[field]['filter'])

    def _validate_prnt_DR_field(self, field, fields):
        if 'filter' not in fields[field]:
            raise AssertionError('filter not in field: {}, dtls: {}'.format(field, fields[field]))
        if 'sum_field' not in fields[field]:
            raise AssertionError('sum_field not in field: {}, dtls: {}'.format(field, fields[field]))
        _valid_filter(fields[field]['filter'])

    def _validate_formula_field(self, field, fields):
        for source_field in fields[field]['source']:
            assert source_field in fields

        source = range(1, 10)
        try:
            eval(fields[field]['func'])
        except Exception as e:
            msg = 'cant eval function: {}, {}'.format(fields[field], e)
            raise AssertionError(msg)

        return True

    def _validate_ai_field(self, field, fields):
        return True

    def _validate_node_conditional_field(self, field, fields):
        if len(fields[field]['source']) != 2:
            raise AssertionError('conditional field: {} must have two source fields'.format(field, fields[field]))

    def _validate_child_sum_field(self, field, fields):
        # TODO: more thinking here ...
        source = fields[field]['source']
        if fields[source[0]]['type'] == 'CS':
            raise AssertionError(
                'child sum field: {} not allowed to be sum of other child sum'.format(field,
                                                                                      fields[field]))
        if fields[source[0]]['type'] == 'US':
            if fields[fields[source][0]['source'][0]] == 'US':
                raise AssertionError(
                    'child sum field: {} not allowed to be sum of grandkid sum'.format(field, fields[field]))
        return True

    def _validate_dashboard(self, dashboard, fields):
        for card in dashboard.get('cards', []):
            for required_field in ['top_field', 'bottom_field', 'label', 'format']:
                if required_field not in card:
                    return False, '{} field is required for: {}'.format(required_field, card)
            if card['top_field'] not in fields:
                return False, '{} not in fields {}, for {}'.format(card['top_field'], fields, card)
            if card['bottom_field'] not in fields:
                return False, '{} not in fields {}, for {}'.format(card['bottom_field'], fields, card)
            for range_dtls in card.get('range_fields', []):
                if range_dtls['field'] not in fields:
                    return False, '{} not in fields {}, for {}'.format(range_dtls['field'], fields, card)
            for row_dtls in card.get('rows', []):
                for field_type in ['field', 'plan_field', 'booked_field']:
                    if field_type in row_dtls and row_dtls[field_type] not in fields:
                        return False, '{} not in fields {}, for {}'.format(row_dtls[field_type], fields, card)
        for field in dashboard.get('current_quarter_fields', {}).values():
            if field not in fields:
                return False, '{} in dashboard current_quarter_fields but not in fm fields'.format(field)
        return True, ''

    def _validate_plans(self, plan_fields, fields):
        if not isinstance(plan_fields, list):
            return False, 'plan fields must be a list: {}'.format(plan_fields)
        for plan_field in plan_fields:
            if plan_field not in fields:
                return False, 'plan field {} not in fields: {}'.format(plan_field, fields)
            elif fields[plan_field]['type'] != 'UE':
                return False, 'plan field {} not editable: {}'.format(plan_field, fields[plan_field])
        return True, ''

    def _validate_linearity(self, linearity_field, fields):
        if not linearity_field:
            return True, ''
        if linearity_field not in fields:
            return False, 'linearity field {} not in fields: {}'.format(linearity_field, fields)
        elif fields[linearity_field]['type'] != 'UE':
            return False, 'plan field {} not editable: {}'.format(linearity_field, fields[linearity_field])
        return True, ''

    def _validate_export(self, export, fields):
        if isinstance(export, dict):
            for pivot in export:
                for i, (_, fields_, _, _) in enumerate(export[pivot]):
                    if not isinstance(fields_, list):
                        fields_ = [fields_]
                    for field in fields_:
                        if field not in fields:
                            return False, 'row: {} export field: {} not in fields: {} for {}'.format(i, field, fields, pivot)
        else:
            for i, (_, fields_, _, _) in enumerate(export):
                if not isinstance(fields_, list):
                    fields_ = [fields_]
                for field in fields_:
                    if field not in fields:
                        return False, 'row: {} export field: {} not in fields: {}'.format(i, field, fields)
        return True, ''

    def _validate_audit_trail(self, export, ue_fields, dr_fields, fn_fields):
        dr_field_count = 0
        if isinstance(export, dict):
            for pivot in export:
                for data in export[pivot]:
                    if len(data) == 5:
                        data.append(False)
                for i, (_, fields_, _, _, _, _) in enumerate(export[pivot]):
                    if not isinstance(fields_, list):
                        fields_ = [fields_]
                    for field in fields_:
                        if field in dr_fields:
                            dr_field_count += 1
                            if dr_field_count > 3:
                                return False, 'count of DR field in audit_trail field is greater than 3'
                            else:
                                continue
                        if field not in (ue_fields+fn_fields):
                            return False, 'row: {} audit_trail field: {} not in UE/FN fields: {} for {}'.format(i, field, ue_fields+fn_fields, pivot)
        else:
            for data in export:
                if len(data) == 5:
                    data.append(False)
            for i, (_, fields_, _, _, _, _) in enumerate(export):
                if not isinstance(fields_, list):
                    fields_ = [fields_]
                for field in fields_:
                    if field in dr_fields:
                        dr_field_count += 1
                        if dr_field_count > 3:
                            return False, 'count of DR field in audit_trail field is greater than 3'
                        else:
                            continue
                    if field not in (ue_fields+fn_fields):
                        return False, 'row: {} audit_trail field: {} not in UE/FN fields: {}'.format(i, field, ue_fields+fn_fields)
        return True, ''

    def _validate_sanity(self, schemas, fields,config):
        displayed_fields = {field for schema in schemas.values()
                            for dtls in schema.values()
                            for field in dtls['fields']}
        dependant_fields = {field for field_dtls in fields.values() for field in field_dtls.get('source', [])}
        dashboard = config.get('dashboard', {})
        for card in dashboard.get('cards', []):
            for field in ['top_field', 'bottom_field']:
                dependant_fields.add(card.get(field))
            dependant_fields |= {x['field'] for x in card.get('range_fields', [])}
        dependant_fields |= set(dashboard.get('current_quarter_fields', {}).values())

        for field in dashboard.get('adaptive_metrics', {}).values():
            dependant_fields.add(field)

        for field in config.get('gateway_schema', {}):
            dependant_fields.add(field)

        for field in config.get('specialist_gateway_schema', {}):
            dependant_fields.add(field)

        bad_fields = set()
        for field, field_dtls in fields.items():
            if field in displayed_fields:
                continue
            if field_dtls['type'] == 'DR' and field not in dependant_fields:
                bad_fields.add(field)
        if bad_fields:
            return False, 'deal rollup fields: {} not used anywhere in config'.format(list(bad_fields))
        return True, ''

    def _validate_segments(self, segments):
        if not segments:
            return True, ''
        for seg, seg_dtls in segments.get('map', {}).items():
            if 'label' not in seg_dtls:
                return False, 'label required for segment: {}'.format(seg)
            if 'filter' in seg_dtls:
                filter_id = seg_dtls['filter']
                filt = fetch_filter([filter_id], self.deal_config, db=self.db)
                if filt is None:
                    return False, f'{filter_id} filter id: {seg} for segment: ??? not in filters collection'
            elif 'components' not in seg_dtls:
                return False, 'either filter or components required for segment: {} config: {}'.format(seg, seg_dtls)

        for seg in segments.get('display', []):
            if seg not in segments.get('map', {}):
                return False, 'seg: {} not in map but in display order'.format(seg)
        return True, ''

    def _validate_fields_to_fc_mapping(self, fields_to_fc_map, fields):
        if not fields_to_fc_map:
            return True, ''
        for field, fc_name in fields_to_fc_map.items():
            if fc_name not in self.deal_config.custom_fc_ranks_default.keys():
                return False, "forecastcategory {} name is not correct".format(fc_name)
            if field not in fields.keys():
                return False, "Field {} is not correct".format(field)
        return True, ''

    def _render_field(self, field, fields):
        if fields[field]['type'] in ['DR', 'prnt_DR']:
            if fields[field]['sum_field'] in self.deal_config.dlf_fields:
                rendered_field = '.'.join(['dlf', fields[field]['sum_field'], '%(node)s', 'dlf_amt'])
            else:
                rendered_field = fields[field]['sum_field']
            fields[field]['crit_and_ops'] = (field, parse_filters(fields[field]['filter'], self.deal_config),
                                             [(fields[field]['sum_field'], rendered_field, fields[field]['op'] if fields[field].get('op') else '$sum')])
        if fields[field]['type'] not in ['US', 'CS']:  # TODO: deprecate US
            return fields[field]
        source = fields[field]['source']
        if fields[source[0]]['type'] in ['UE', 'NC', 'FN']:
            return fields[field]
        try:
            source = [fld for source_field in source for fld in fields[source_field]['source']]
        except KeyError:
            pass
        fields[field]['source'] = source
        fields[field]['grandkids'] = True
        return fields[field]

    @property
    def _default_aviso_fields(self):
        return {field: {'type': 'AI'} for field in ['ep_won',
                                                    'ep_won_and_fcst',
                                                    'ep_won_and_fcst_hi',
                                                    'ep_won_and_fcst_lo',
                                                    'exp_as_of',
                                                    'exp_ep_won_and_fcst',
                                                    'exp_np_won_and_fcst',
                                                    'exp_tot_won_and_fcst',
                                                    'np_won',
                                                    'np_won_and_fcst',
                                                    'np_won_and_fcst_hi',
                                                    'np_won_and_fcst_lo',
                                                    'tot_won',
                                                    'tot_won_and_fcst',
                                                    'tot_won_and_fcst_hi',
                                                    'tot_won_and_fcst_lo',
                                                    'ubf',
                                                    'ep_epf',
                                                    'unborn_existing',
                                                    'unborn_total',
                                                    'nextq_pipe_target']}

    @property
    def _default_segment(self):
        return {'all_deals': {'label': 'All Deals',
                              }}

    @cached_property
    def collaboration_recordings_tab(self):

        return {
            'enabled': get_nested(self.config, ['collaboration','recordings_tab_enabled'], True),
            'options': get_nested(self.config, ['collaboration','recordings_tab_options'], COLLABORATION_RECORDINGS_TAB_DEFAULT_OPTIONS)
        }

    @cached_property
    def default_transcribe_option(self):
        #OPTIONS: DEEPGRAM, AWS
        return get_nested(self.config, ['collaboration', 'default_transcribe_option'], "DEEPGRAM")

    @cached_property
    def disable_upload(self):
        return get_nested(self.config, ['collaboration', 'hide_upload_transcription'], False)

    @cached_property
    def default_emotions_display_config(self):

        return {
            'enabled': get_nested(self.config, ['collaboration','emotions_display_enabled'], False),
            'filtered_emotions': get_nested(self.config, ['collaboration','emotions_display_filters'], [])
        }

    # reporting related config

    @cached_property
    def get_format_m_to_k(self):
        """
        Returns:
            dict -- {field: [field, fc_name]}
        """
        return self.config.get('format_m_to_k', False)

    @cached_property
    def fields_to_forecast_category_name_mapping(self):
        """
        Returns:
            dict -- {field: [field, fc_name]}
        """
        return self.config.get('fields_to_forecast_category_name_map', {})

    @cached_property
    def forecast_service_editable_fields(self):
        return self.config.get('forecast_service_editable_fields', [])

    @cached_property
    def auto_deal_room_creation_config(self):

        return get_nested(self.config, ['collaboration','auto_deal_room_creation'], False)

    @cached_property
    def auto_deal_rooms_bacth_size(self):
        return get_nested(self.config, ['collaboration', 'auto_deal_rooms_bacth_size'], 100)

    @cached_property
    def auto_deal_rooms_for_current_period_only(self):
        return get_nested(self.config, ['collaboration', 'auto_deal_rooms_for_current_period_only'], False)

    @cached_property
    def get_moxtra_sdk_version(self):
        return get_nested(self.config, ['collaboration', 'sdk_version'], None)

    @cached_property
    def get_moxtra_private_url(self):
        return get_nested(self.config, ['collaboration', 'moxtra_private_url'], None)

    @cached_property
    def get_moxtra_public_url(self):
        return get_nested(self.config, ['collaboration', 'moxtra_public_url'], None)

    @cached_property
    def segments_dashboard(self):
        return get_nested(self.config, ['dashboard', 'segments_enabled'], False)

    @cached_property
    def deal_rooms_hierarchy_change_batch_size(self):

        return get_nested(self.config, ['collaboration', 'deal_rooms_hierarchy_change_batch_size'], 100)

    # CS-14453
    @cached_property
    def special_pivot_ceo_update_rollup_fields(self):
        """
        Returns list of fields for which we want to run the CrrRollupTask
        """
        return get_nested(self.config, ['special_pivot_ceo_update_rollup_fields'], [])

    @cached_property
    def read_from_latest_collection(self):
        """
        Returns True if read_from_latest_collection is set to true,
        In this case it will read from latest collection
        """
        return get_nested(self.config, ['read_from_latest_collection'], False)


    @cached_property
    def write_to_latest_collection(self):
        """
        Returns True if read_from_latest_collection is set to true,
        In this case it will read from latest collection
        """
        return get_nested(self.config, ['write_to_latest_collection'], False)

    @cached_property
    def snapshot_feature_enabled(self):
        """
        Returns True if snapshot_feature_enabled is set to true
        """
        return get_nested(self.config, ['snapshot_feature_enabled'], False)


    @cached_property
    def admin_node(self):
        return get_nested(self.config, ['admin_node'], 'Global#Global')

    @cached_property
    def forecast_window_default(self):
        """
        Returns True if snapshot_feature_enabled is set to true
        """
        return get_nested(self.config, ['forecast_window_default'], 'lock')

    @cached_property
    def read_from_fm_collection_for_year(self):
        """
        Returns True if read_from_latest_collection is set to true,
        In this case it will read from latest collection
        """
        return get_nested(self.config, ['read_from_fm_collection_for_year'], True)

    @cached_property
    def is_algolia_enabled(self):
        return  get_nested(self.config, ['algolia', 'is_enabled'], False)

    @cached_property
    def get_algolia_creds(self):
        return self.config.get('algolia', {})

    @cached_property
    def cached_export_all(self):
        """
        default export schema set

        Returns:
            dict -- {schema type: schema name}
        """
        cached_export = get_nested(self.config, ['export', 'cached_export_all'], False)
        return cached_export

    @cached_property
    def edw_display_timestamp_fields(self):
        """
        default export schema set

        Returns:
            dict -- {schema type: schema name}
        """
        edw_display_timestamp_fields = get_nested(self.config, ['display', 'edw_display_timestamp_fields'], [])
        return edw_display_timestamp_fields

    @cached_property
    def raise_auto_rollup_event(self):
        return self.config.get('raise_auto_rollup_event', True)

