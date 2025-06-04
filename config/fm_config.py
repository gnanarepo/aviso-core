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


class DealConfig(BaseConfig):
    config_name = "deals"

    @cached_property
    def rollup_for_writeback(self):
        return self.config.get('rollup_for_writeback', False)

    @cached_property
    def is_recommended_actions_enabled(self):
        return self.config.get('enable_recommended_actions_task', False)

    @cached_property
    def dummy_tenant(self):
        """
        tenant is using dummy data, is not hooked up to any gbm/etl
        """
        return self.config.get("dummy_tenant", False)

    @cached_property
    def hide_deal_grid_fields(self):
        """
        hide deal grid fields
        """
        return self.config.get("hide_deal_grid_fields", [])

    @cached_property
    def bootstrapped(self):
        """
        tenant has been bootstrapped and has initial data loaded into app
        """
        return self.config.get("bootstrapped", True)

    # gateway schema
    @cached_property
    def gateway_schema(self):
        return self.config.get("gateway_schema", {})

    @cached_property
    def is_aviso_tenant(self):
        return self.config.get("aviso_tenant", False)

    @cached_property
    def forecast_panel(self):
        return self.config.get("forecast_panel", {})

    @cached_property
    def ci_top_deals_panel(self):
        return self.config.get("ci_top_deals_panel", {})

    @cached_property
    def deal_alert_fields(self):
        return self.config.get("deal_alert_fields", [])

    @cached_property
    def deal_alert_on(self):
        return self.config.get("deal_alert_on", False)

    @cached_property
    def traffic_light_criteria(self):
        return self.config.get("traffic_light_criteria", False)

    @cached_property
    def disable_deal_details_tab(self):
        return self.config.get('disable_deal_details_tab', False)

    @cached_property
    def disable_deal_history_tab(self):
        return self.config.get('disable_deal_history_tab', False)

    @cached_property
    def deal_details_config(self):
        """
        Fetch the tenant specific navigation config
        """
        default_config = {
            'details': 'Details',
            'history': 'History',
            'relationships': 'Relationships',
            'interactions': 'Interactions',
            'deal_room': 'Deal Room',
            'to_do': 'TO DO\'s'
        }
        return self.config.get('deal_details_config', default_config)

    @cached_property
    def make_field_history_public(self):
        return self.config.get('make_field_history_public', [])

    @cached_property
    def not_deals_tenant(self):
        return self.config.get('not_deals_tenant', {})

    @cached_property
    def enable_email_tracking_nudge(self):
        return self.config.get('enable_email_tracking_nudge', False)

    @cached_property
    def special_pivot_month_closeout_day(self):
        return self.config.get("special_pivot_month_closeout_day", 3)

    @cached_property
    def crm_url(self):
        return self.config.get('crm_url')

    @cached_property
    def special_pivot_filters(self):
        from infra.filters import fetch_all_filters
        allowed_filters = {}
        filters = fetch_all_filters(self, grid_only=True, is_pivot_special=True)
        special_pivot_filters = self.config.get("special_pivot", {}).get("filters", [])
        for filt_id, filt in filters.items():
            if filt_id in special_pivot_filters:
                allowed_filters[filt_id] = filt
        return allowed_filters

    @cached_property
    def raw_crr_schema(self):
        if self.persona_schemas:
            # Getting the schema based on persona. If the user belongs to multiple personas, then merge the schemas
            personas = sec_context.get_current_user_personas()
            if not personas:
                return self.config.get("CRR_schema")
            if len(personas) == 1:
                return self.persona_schemas.get(personas[0], self.config.get("CRR_schema"))
            final_schema = {}
            for persona in personas:
                schema = self.persona_schemas.get(
                    persona, self.config.get("CRR_schema"))
                for k, v in schema.items():
                    if k not in final_schema:
                        final_schema[k] = v
                    else:
                        if type(v) == dict:
                            final_schema[k].update(v)
                        elif type(v) == list:
                            final_schema[k].extend(v)
                        else:
                            logger.error(
                                "This type of type is not supported in schemas of personas yet. please check"
                            )
                            raise Exception(
                                "This type of type is not supported in schemas of personas yet. please check"
                            )

            return final_schema
        user_role = sec_context.get_current_user_role()
        if user_role and self.crr_role_schemas.get(user_role, None):
            return self.crr_role_schemas.get(user_role, {})

        return self.config.get("CRR_schema")

    @cached_property
    def get_persona_schemas(self):
        if self.persona_schemas:
            personas = sec_context.get_current_user_personas()
            if not personas:
                return self.config.get("schema")
            if len(personas) == 1:
                return self.persona_schemas.get(personas[0], self.config.get("schema"))
            final_schema = {}
            personas = list(set(personas))
            for persona in personas:
                schema = self.persona_schemas.get(
                    persona, self.config.get("schema"))
                for k, v in schema.items():
                    if k not in final_schema:
                        final_schema[k] = v
                    else:
                        if type(v) == dict:
                            final_schema[k].update(v)
                        elif type(v) == list:
                            final_schema[k].extend(v)
                        else:
                            logger.error(
                                "This type of type is not supported in schemas of personas yet. please check"
                            )
                            raise Exception(
                                "This type of type is not supported in schemas of personas yet. please check"
                            )

            return final_schema

    @cached_property
    def get_user_role_schemas(self):
        user_role = sec_context.get_current_user_role()
        if user_role and self.role_schemas.get(user_role):
            return self.role_schemas.get(user_role, {})

    @cached_property
    def raw_schema(self):
        person_schema = self.get_persona_schemas
        if person_schema:
            return person_schema
        user_role_schema = self.get_user_role_schemas
        if user_role_schema:
            return user_role_schema
        return self.config.get("schema")

    #
    # Field Map
    #
    @cached_property
    def field_map(self):
        """
        mapping of standard aviso deal fields to their tenant specific field names

        Returns:
            dict -- {'amount': 'as_of_Amount', ...}
        """
        return self.config.get("field_map", {})

    @cached_property
    def add_poc_fields_to_indicator_report(self):
        """
        mapping of standard aviso deal fields to their tenant specific field names

        Returns:
            dict -- {'amount': 'as_of_Amount', ...}
        """
        return self.config.get("add_poc_fields_to_indicator_report", False)

    @cached_property
    def leading_indicator_ref_stages(self):
        """
        mapping of standard aviso deal fields to their tenant specific field names

        Returns:
            dict -- {'amount': 'as_of_Amount', ...}
        """
        return self.config.get("leading_indicator_ref_stages", ["Validate", "Stakeholder Alignment"])

    @cached_property
    def leading_indicator_poc_timestamp_fields(self):
        """
        mapping of standard aviso deal fields to their tenant specific field names

        Returns:
            dict -- {'amount': 'as_of_Amount', ...}
        """
        return self.config.get("leading_indicator_poc_timestamp_fields", ["POVStartDate", "POVEndDate"])

    @cached_property
    def leading_indicator_bva_fields(self):
        """
        mapping of standard aviso deal fields to their tenant specific field names

        Returns:
            dict -- {'amount': 'as_of_Amount', ...}
        """
        return self.config.get("leading_indicator_bva_fields",
                               ["BVA_Presented_to_Customer", "BVA_Presented_to_Customer_transition_timestamp"])

    @cached_property
    def stage_transition_timestamp(self):
        """
        mapping of standard aviso deal fields to their tenant specific field names

        Returns:
            dict -- {'amount': 'as_of_Amount', ...}
        """
        return self.config.get("stage_transition_timestamp", "Stage_transition_timestamp")

    @cached_property
    #
    # write back fields
    #
    def writeback_fields(self):
        crm_writable_fields = []
        for i in self.config["schema"]["deal"]:
            if "crm_writable" in i.keys():
                crm_writable_fields.append(self.config["schema"]["deal_fields"][i['field']])
        return crm_writable_fields

    #
    # Deal Alert Extended
    #
    @cached_property
    def deal_alerts_fields_extended(self):
        """
        Deal Alert Config for extended fields.
        """
        return self.config.get("deal_alerts_fields_extended", None)

    @cached_property
    def custom_manager_fc_ranks(self):
        return self.config.get("custom_manager_fc_ranks", {})

    @cached_property
    def custom_gvp_fc_ranks(self):
        return self.config.get("custom_gvp_fc_ranks", {})

    @cached_property
    def sankey_for_lacework(self):
        """
        Added sankey config for lacework to handle CS-8700
        This config will be used for only for lacework to load sankey even when deal results fails.
        """
        return self.config.get("sankey_for_lacework", False)

    @cached_property
    def high_leverage_moments(self):
        '''
        High leverage moments config is defined to populate high leverage deals insights.
        high_leverage_moments = {
                    'forecast_category_order': ['Pipeline', 'Upside', 'Commit', 'Closed'],
                    'stage_order': ['1-Validate', '2-Qualify', '3-Compete', '4-Negotiate', '5-Selected', '6-End user PO Issued', '8-Closed Won'],
                    'days': [7,14,21,28],
                    'hlm_threshold': 0.3
                }
        '''
        return self.config.get("high_leverage_moments", {})

    @cached_property
    def report_custom_fields_dict(self):
        """
        report_custom_fields for a tenant

        Returns:
            dict -- field name
        """
        return self.config.get("report_custom_fields", {})

    @cached_property
    def crr_amount_field(self):
        return get_nested(self.config, ["field_map", "crr_amount"]) or 'forecast'

    @cached_property
    def amount_field(self):
        """
        name of amount field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "amount"])

    @cached_property
    def news_deal_limit(self):
        """
        Fetches the news deal limit from the configuration.

        Returns:
            int | None -- The deal limit if configured, else None.
        """
        return get_nested(self.config, ["dashboard", "news", "deal_limit"])

    @cached_property
    def accountid_field(self):
        """
        name of account id field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "accountid"])

    @cached_property
    def meddicscore_field(self):
        """
        name of meddicscore field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "meddicscore"])

    @cached_property
    def amount_prev_wk_field(self):
        """
        name of amount field for tenant

        Returns:
            str -- field name
        """
        amount_field = get_nested(self.config, ["field_map", "amount"])
        amount_prev_wk_field = get_nested(self.config, ["field_map", "amount_prev_wk"])
        if not amount_prev_wk_field and amount_field:
            amount_prev_wk_field = amount_field + '_prev_wk'
        return amount_prev_wk_field

    @cached_property
    def crr_amount_prev_wk_field(self):
        """
        name of amount field for tenant

        Returns:
            str -- field name
        """
        amount_field = get_nested(self.config, ["field_map", "crr_amount"])
        amount_prev_wk_field = get_nested(self.config, ["field_map", "crr_amount_prev_wk"])
        if not amount_prev_wk_field and amount_field:
            amount_prev_wk_field = amount_field + '_prev_wk'
        return amount_prev_wk_field

    @cached_property
    def pivot_amount_fields(self):
        """
        map of amount field for tenant based on pivot

        Returns:
            dict -- pivot:amount_field_name
        """
        return self.config.get("pivot_amount_fields", None)

    @cached_property
    def close_date_field(self):
        """
        name of close date field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "close_date"])

    @cached_property
    def monthly_close_period_enrichment(self):
        return self.config.get("monthly_close_period_enrichment", False)

    @cached_property
    def export_close_date_field(self):
        """
        name of close date field for export (for jfrog)

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "export_close_date"])

    @cached_property
    def original_close_date_field(self):
        """
        name of the close date field which is not modified by rev scheduling code, etc

        Returns:
            str -- field name
        """
        return self.config.get("original_close_date_field", get_nested(self.config, ["field_map", "close_date"]))

    @cached_property
    def crr_groupby_field(self):
        """
        name of group by field for special pivot

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "CRR_BAND_DESCR"])

    @cached_property
    def crr_ceo_fields(self):
        return get_nested(self.config, ['crr_ceo_fields', []])

    @cached_property
    def stage_field(self):
        """
        name of stage field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "stage"])

    @cached_property
    def type_field(self):
        """
        name of stage field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "type"])

    @cached_property
    def stage_trans_field(self):
        """
        name of stage field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "stage_trans"])

    @cached_property
    def get_additional_ai_forecast_diff_fields(self):
        """
        return SFDCObject Config Key so as to get the CRM resource name of the deal.

        Returns:
            str -- SDFCObject Config key

        This config also used in csv export(CS-19586)
        """
        return self.config.get('additional_ai_forecast_diff_fields', [])

    @cached_property
    def forecast_category_field(self):
        """
        name of forecast category field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "forecast_category"])

    @cached_property
    def manager_forecast_category_field(self):
        """
        name of manager forecast category field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "manager_forecast_category"])

    @cached_property
    def gvp_forecast_category_field(self):
        """
        name of gvp forecast category field for tenant

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "gvp_forecast_category"])

    @cached_property
    def extend_deal_change_for(self):
        return self.config.get("extend_deal_change_for", [])

    @cached_property
    def oppmap_forecast_category_field(self):
        """
        name of oppmap forecast category field for tenant

        Returns:
            str -- field name
        """
        return get_nested(
            self.config,
            ["oppmap", "forecast_category"],
            get_nested(self.config, ["field_map", "forecast_category"]),
        )

    def oppmap_deal_type_grouping(self, type):
        """
        New/Renewal type values for oppmap

        Returns:
            str -- type value Ex: new
        """
        return get_nested(
            self.config,
            ["oppmap", "deal_type_grouping"],
            {
                "new": ["New"],
                "renewal": ["Renewal"],
                "cross_Sell/upsell/extensions": [
                    "Add-On",
                    "Add-On Business",
                    "Amendment",
                    "Existing Business",
                    "Upgrade",
                    "Upgrade or downgrade",
                ],
            },
        )[type]

    @cached_property
    def oppmap_deal_type_options(self):
        """
        deal type options for oppmap

        Returns:
            str -- type value Ex: new
        """
        return get_nested(
            self.config,
            ["oppmap", "oppmap_deal_type_options"],
            [
                ("all", "ALL"),
                ("new", "New"),
                ("renewal", "Renewal"),
                ("cross_Sell/upsell/extensions", "Cross Sell/Upsell/Extensions"),
            ],
        )

    """
    The score cutoff where a deal is considered risky"""

    @cached_property
    def opp_map_score_cutoff(self):
        return get_nested(self.config, ["opp_map", "score_cutoff"])

    @cached_property
    def owner_field(self):
        """
        Owner Id of the deal

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "owner_id"])

    @cached_property
    def owner_name_field(self):
        """
        Owner Name of Deal

        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "opp_owner"])

    @cached_property
    def owner_id_fields(self):
        """
        mapping of owner id fields to drilldown if drilldowns, else None

        Returns:
            list -- list of tuples of (owner id field, drilldown)
        """
        default_owner_id_fields = [(self.owner_field, None)]
        owner_id_fields = get_nested(
            self.config, ["owner_id_fields"], default_owner_id_fields
        )
        if isinstance(owner_id_fields, dict):
            logger.warning(
                "using old config format for owner_id_fields please switch")
            owner_id_fields = [(k, v) for k, v in owner_id_fields.items()]
        return owner_id_fields

    @cached_property
    def user_data_name(self):
        return self.config.get("UserData", "User")

    @cached_property
    def user_email_fld(self):
        return self.config.get("User_email_fld", "Email")

    @cached_property
    def user_level_persona_fields(self):
        if self.persona_schemas:
            # When persona schemas are enabled, we show user_level fields based on the fields of that particular persona.
            # So including the user_level_fields config in the schema itself
            personas = sec_context.get_current_user_personas()
            ret_val = {}
            for persona in personas:
                for k, v in (
                        self.persona_schemas.get(persona, {})
                                .get("user_level_fields", {})
                                .items()
                ):
                    if k not in ret_val:
                        ret_val[k] = v
                    else:
                        if type(v) == list:
                            ret_val[k].extend(v)
                        elif type(v) == dict:
                            ret_val[k].update(v)
                        else:
                            ret_val[k] = v
            return ret_val if ret_val else {}

    @cached_property
    def user_level_role_fields(self):
        user_role = sec_context.get_current_user_role()
        if user_role and self.role_schemas.get(user_role):
            return self.role_schemas.get(user_role).get("user_level_fields", {})

    @cached_property
    def user_level_fields(self):
        user_level_persona_fields = self.user_level_persona_fields
        user_level_role_fields = self.user_level_role_fields
        user_level_fields = copy.deepcopy(self.config.get("user_level_fields", {}))
        if user_level_persona_fields:
            for fld in user_level_persona_fields['fields']:
                if fld not in user_level_fields['fields']:
                    user_level_fields['fields'].append(fld)
            for fld in user_level_persona_fields['dlf_fields']:
                if fld not in user_level_fields['dlf_fields']:
                    user_level_fields['dlf_fields'].append(fld)

        if user_level_role_fields:
            for fld in user_level_role_fields['fields']:
                if fld not in user_level_fields['fields']:
                    user_level_fields['fields'].append(fld)
            for fld in user_level_role_fields['dlf_fields']:
                if fld not in user_level_fields['dlf_fields']:
                    user_level_fields['dlf_fields'].append(fld)
        logger.info("final user level fields {}".format(user_level_fields))

        return user_level_fields

    @cached_property
    def pivot_special_user_level_fields(self):
        if self.persona_schemas:
            # When persona schemas are enabled, we show user_level fields based on the fields of that particular persona.
            # So including the user_level_fields config in the schema itself
            personas = sec_context.get_current_user_personas()
            ret_val = {}
            for persona in personas:
                for k, v in (
                        self.persona_schemas.get(persona, {})
                                .get("pivot_special_user_level_fields", {})
                                .items()
                ):
                    if k not in ret_val:
                        ret_val[k] = v
                    else:
                        if type(v) == list:
                            ret_val[k].extend(v)
                        elif type(v) == dict:
                            ret_val[k].update(v)
                        else:
                            ret_val[k] = v
            return ret_val if ret_val else self.config.get("pivot_special_user_level_fields", {})

        user_role = sec_context.get_current_user_role()
        if user_role and self.role_schemas.get(user_role):
            return self.role_schemas.get(user_role).get("pivot_special_user_level_fields",
                                                        self.config.get("pivot_special_user_level_fields", {}))

        return self.config.get("pivot_special_user_level_fields", {})

    @cached_property
    def user_name_fld(self):
        return self.config.get("User_name_fld", "Name")

    @cached_property
    def restrict_lead_deals(self):
        """
         restrict_lead_deals - True if lead deals are supposed to be excluded from the reports_db
         lead deal identification - starts with 00Q
        """
        return self.config.get('restrict_lead_deals', False)

    #
    # Field Values
    #

    @cached_property
    def best_case_values(self):
        """
        values of forecast category field that make a deal be considered in best case
        optional: falls back to DEFAULT_BEST_CASE_VALS
        Returns:
            list -- [best case values]
        """
        return get_nested(
            self.config, ["field_values", "best_case"], DEFAULT_BEST_CASE_VALS
        )

    @cached_property
    def commit_values(self):
        """
        values of forecast category field that make a deal be considered in commit
        optional: falls back to DEFAULT_COMMIT_VALS

        Returns:
            list -- [commit values]
        """
        return get_nested(self.config, ["field_values", "commit"], DEFAULT_COMMIT_VALS)

    @cached_property
    def pipeline_values(self):
        """
        values of forecast category field that make a deal be considered in pipeline
        optional: falls back to DEFAULT_PIPELINE_VALS

        Returns:
            list -- [pipeline values]
        """
        return get_nested(
            self.config, ["field_values", "pipeline"], DEFAULT_PIPELINE_VALS
        )

    @cached_property
    def renewal_values(self):
        """
        Values of renewal type deal.
        optional: falls back to DEFAULT_RENEWAL_VALS

        Returns:
            list -- [renewal values]
        """
        return get_nested(
            self.config, ["field_values", "renewal"], DEFAULT_RENEWAL_VALS
        )

    @cached_property
    def most_likely_values(self):
        """
        values of forecast category field that make a deal be considered most likely
        optional: falls back to DEFAULT_MOST_LIKELY_VALS

        Returns:
            list -- [most likely values]
        """
        return get_nested(
            self.config, ["field_values",
                          "most_likely"], DEFAULT_MOST_LIKELY_VALS
        )

    @cached_property
    def dlf_values(self):
        """
        values of forecast category field that make a deal be considered in dlf
        optional: falls back to DEFAULT_DLF_VALS

        Returns:
            list -- [dlf values]
        """
        return get_nested(self.config, ["field_values", "dlf"], DEFAULT_DLF_VALS)

    #
    # Total Fields
    #
    @cached_property
    def special_pivot_total_fields(self):
        """
                deal amount fields to compute totals for in deals grid
                totals are unfiltered

                Returns:
                    list -- [(label, deal amount field, mongo operation)]
                """
        tot_fields = []
        defualt_totals = [('forecast', "$sum"), ("crr_in_fcst", "$sum")]
        label_map = {
            v: k for k, v in self.raw_crr_schema.get("deal_fields", {}).items()
        }
        for field_dtls in get_nested(
                self.config, ["totals", "crr_total_fields"], defualt_totals
        ):
            try:
                field, op = field_dtls
            except ValueError:
                (field,) = field_dtls
                op = "$sum"
            label = label_map.get(field, field)
            if 'ACT_CRR_value' in field:
                label = "ACT_CRR"
            if field in self.dlf_fields:
                field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
            tot_fields.append((label, field, op))
        return tot_fields

    @cached_property
    def special_pivot_subtotal_fields(self):
        """
        deal amount fields to compute subtotals for in deals grid
        subtotals are filtered

        Returns:
            list -- [(label, deal amount field, mongo operation)]
        """
        tot_fields = []
        default_subtotals = [('forecast', "$sum"), ("crr_in_fcst", "$sum")]
        label_map = {
            v: k for k, v in self.raw_crr_schema.get("deal_fields", {}).items()
        }
        for field_dtls in get_nested(
                self.config, ["totals", "crr_subtotal_fields"], default_subtotals
        ):
            try:
                field, op = field_dtls
            except ValueError:
                (field,) = field_dtls
                op = "$sum"
            label = label_map.get(field, field)
            if 'ACT_CRR_value' in field:
                label = "ACT_CRR"
            if field in self.dlf_fields:
                field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
            tot_fields.append((label, field, op))
        return tot_fields

    @cached_property
    def same_total_subtotals(self):
        """
            True if tenant requires totals and subtotals to be equal
            CS-13349 - AppAnnie
        """
        return self.config.get('same_total_subtotals', False)

    @cached_property
    def total_fields(self):
        """
        deal amount fields to compute totals for in deals grid
        totals are unfiltered

        Returns:
            list -- [(label, deal amount field, mongo operation)]
        """
        tot_fields = []
        defualt_totals = [(self.amount_field, "$sum"), ("in_fcst", "$sum")]
        label_map = {
            v: k for k, v in self.raw_schema.get("deal_fields", {}).items()
        }
        for field_dtls in get_nested(
                self.config, ["totals", "total_fields"], defualt_totals
        ):
            try:
                field, op = field_dtls
            except ValueError:
                (field,) = field_dtls
                op = "$sum"
            label = label_map.get(field, field)
            if field in self.dlf_fields:
                field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
            tot_fields.append((label, field, op))
        return tot_fields

    @cached_property
    def subtotal_fields(self):
        """
        deal amount fields to compute subtotals for in deals grid
        subtotals are filtered

        Returns:
            list -- [(label, deal amount field, mongo operation)]
        """
        tot_fields = []
        default_subtotals = [(self.amount_field, "$sum"), ("in_fcst", "$sum")]
        label_map = {
            v: k for k, v in self.raw_schema.get("deal_fields", {}).items()
        }
        for field_dtls in get_nested(
                self.config, ["totals", "subtotal_fields"], default_subtotals
        ):
            try:
                field, op = field_dtls
            except ValueError:
                (field,) = field_dtls
                op = "$sum"
            label = label_map.get(field, field)
            if field in self.dlf_fields:
                field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
            tot_fields.append((label, field, op))
        return tot_fields

    #
    # Display Schema
    #
    def get_field_format(self, field, pivot=None):
        try:
            if pivot in self.config.get('special_pivot', []):
                return next(
                    x["fmt"] for x in self.account_fields_config if x["field"] == field
                )
            else:
                return next(
                    x["fmt"] for x in self.deal_fields_config if x["field"] == field
                )
        except StopIteration:
            return None

    def get_field_label(self, field, pivot=None):
        try:
            if pivot in self.config.get('special_pivot', []):
                return next(
                    x["label"] for x in self.account_fields_config if x["field"] == field
                )
            else:
                return next(
                    x["label"] for x in self.deal_fields_config if x["field"] == field
                )
        except StopIteration:
            return field

    @cached_property
    def account_fields_config(self):
        """
        all accounts fields available to UI, with description of how to format + label them

        Returns:
            list -- [{'field': field, 'fmt': fmt, 'label': label}]
        """
        return self.raw_crr_schema.get("deal", [])

    @cached_property
    def deal_fields_config(self):
        """
        all deal fields available to UI, with description of how to format + label them

        Returns:
            list -- [{'field': field, 'fmt': fmt, 'label': label}]
        """
        return self.raw_schema.get("deal", [])

    @cached_property
    def trimmed_deal_fields_config(self):
        fields = {}
        for field_config in self.deal_fields_config:
            fields.update({field_config.get('field'): field_config})
        return fields.values()

    @cached_property
    def special_amounts(self):
        """
        Special Amount fields which will be useful to add special amount fields and

        Returns:
             list -- {'amount_field': "amount_label"}
        """
        return self.raw_schema.get("special_amounts", {})

    @cached_property
    def secondary_deal_fields(self):
        """
        secondary deal fields that dont appear in deals grid

        Returns:
            set -- {fields}
        """
        return {
            field_dtls["field"]
            for field_dtls in self.deal_fields_config
            if field_dtls.get("secondary")
        }

    def deal_ue_fields(self, pivot_schema="schema"):
        """
        user editable deal fields

        Returns:
            set -- {fields}
        """
        return [
            field_dtls["field"]
            for field_dtls in get_nested(self.config, [pivot_schema, "deal"], [])
            if field_dtls.get("user_edit")
        ]

    def pivot_secondary_deal_fields(self, pivot_schema="schema"):
        """
        pivot secondary deal fields that dont appear in deals grid

        Returns:
            set -- {fields}
        """
        return {
            field_dtls["field"]
            for field_dtls in get_nested(self.config, [pivot_schema, "deal"], [])
            if field_dtls.get("secondary")
        }

    @cached_property
    def primary_account_fields(self):
        """
        primary account fields that appears in accounts grid

        Returns:
            set -- {fields}
        """
        return {
            field_dtls["field"]
            for field_dtls in self.account_fields_config
            if field_dtls.get("primary")
        }

    @cached_property
    def primary_deal_fields(self):
        """
        primary deal fields that appears in deals grid

        Returns:
            set -- {fields}
        """
        return {
            field_dtls["field"]
            for field_dtls in self.deal_fields_config
            if field_dtls.get("primary")
        }

    @cached_property
    def gateway_deal_fields(self):
        """
        secondary deal fields that dont appear in deals grid

        Returns:
            set -- {fields}
        """
        return {
            field_dtls["field"]
            for field_dtls in self.deal_fields_config
            if field_dtls.get("gateway")
        }

    @cached_property
    def special_gateway_deal_fields(self):
        """
        secondary deal fields that dont appear in deals grid

        Returns:
            set -- {fields}
        """
        return get_nested(
            self.config, ["gateway_schema", "special_gateway_deal_fields"], {}
        )

    @cached_property
    def gateway_dlf_expanded(self):
        """
        boolean to activate expanded dlf fields with node level information

        Returns:
            boolean --
        """
        return get_nested(
            self.config, ["gateway_schema", "gateway_dlf_expanded"], False
        )

    @cached_property
    def gateway_dlf_expanded_field(self):
        """
        expanded dlf field with node level information in {label: field} format

        Returns:
            dict -- dlf fields with node level information
            default - {'dlfs': 'dlfs'}
        """
        return get_nested(
            self.config, ["gateway_schema", "gateway_dlf_expanded_field"], {'dlfs': 'dlfs'}
        )

    @cached_property
    def special_deal_fields(self):
        """
        deal fields that get called out in deal card

        Returns:
            list -- [(deal field, fe key, label, format)]
        """
        return self.raw_schema.get("special_deal_fields", [])

    def pivot_special_deal_fields(self, schema):
        """
        deal fields that get called out in deal card

        Returns:
            list -- [(deal field, fe key, label, format)]
        """
        return get_nested(self.config, [schema, "special_deal_fields"], [])

    @cached_property
    def default_hidden_fields(self):
        """
        deal fields that get called out in deal card

        Returns:
            list -- [(deal field, fe key, label, format)]
        """
        return self.raw_schema.get("default_hidden_fields", [])

    @cached_property
    def custom_layout_fields(self):
        """
        deal fields that get called out in deal card

        Returns:
            list -- [(deal field, fe key, label, format)]
        """
        return self.raw_schema.get("deal")

    @cached_property
    def deal_fields(self):
        """
        deal fields to display in deals grid mapped to their tenant specific field names

        Returns:
            dict -- {'OpportunityName': 'opp_name'}
        """

        return self._deal_fields()

    @cached_property
    def formula_driven_fields(self):
        """
        fields that are formula driven and formula

        Returns:
            dictionary where key is field_name and value have formula and source
            for e.g.:
            {"ACVProb": {"formula": "a * b"
            "source": {"a": "Amount",
                       "b": "Probability" }}}
        """
        return self.config.get("formula_driven_fields", {})

    def _deal_fields(self, gateway_call=False, pivot_schema=None, formula_driven_fields=[], segment=None):
        d_fields = {
            "alert": "alert",
            "dealalert": "dealalert",
        }  # HACK: get alert into deal ...

        secondary_deal_fields = self.secondary_deal_fields
        if pivot_schema:
            schema = self.config.get(pivot_schema)
            secondary_deal_fields = self.pivot_secondary_deal_fields(
                pivot_schema=pivot_schema
            )
        else:
            schema = self.raw_schema

        for label, field in schema.get("deal_fields", {}).items():
            if label in secondary_deal_fields:
                if (gateway_call and label in self.gateway_deal_fields) or label in formula_driven_fields:
                    pass
                else:
                    continue
            if field in self.dlf_fields:
                field = ".".join(["dlf", field])
            d_fields[label] = field
        if gateway_call:
            d_fields.update(self.special_gateway_deal_fields)
        # logger.info("deal fields %s" % d_fields)
        if "segment_schema" in self.config:
            segment_schema = self.config.get('segment_schema', {})
            if segment in segment_schema:
                deal_fields = segment_schema[segment]["deal_fields"]
                for key, value in deal_fields.items():
                    d_fields[key] = value

        return d_fields

    @cached_property
    def all_account_fields(self):
        return self.raw_crr_schema.get("deal_fields", {})

    def gateway_fields_from_schema(self, schema='schema'):
        schema = self.config.get(schema)
        deal_fields_config = schema.get("deal", [])

        gateway_deal_fields = []
        for field_dtls in deal_fields_config:
            if field_dtls.get('gateway'):
                field = field_dtls['field']
                if field in self.dlf_fields:
                    field = ".".join(["dlf", field])
                gateway_deal_fields.append([field_dtls['label'], field, field_dtls['fmt']])
        return gateway_deal_fields

    @cached_property
    def all_deal_fields(self):
        d_fields = {
            "alert": "alert",
            "dealalert": "dealalert",
        }  # HACK: get alert into deal ...
        for label, field in self.raw_schema.get("deal_fields", {}).items():
            if field in self.dlf_fields:
                field = ".".join(["dlf", field])
            d_fields[label] = field
        return d_fields

    @cached_property
    def filter_priority(self):
        return self.config.get("filter_priority", [])

    @cached_property
    def card_deal_fields(self):
        """
        deal fields to display in deal card mapped to their tenant specific field names
        optional: falls back to standard deal_fields

        Returns:
            dict -- {'OpportunityName': 'opp_name'}
        """
        d_fields = {
            "alert": "alert",
            "dealalert": "dealalert",
        }  # HACK: get alert into deal ...
        for label, field in self.raw_schema.get(
                "card_deal_fields", self.raw_schema.get("deal_fields", {})
        ).items():
            if field in self.dlf_fields:
                field = ".".join(["dlf", field])
            if field not in self.raw_schema.get(
                    "excluded_from_deal_card", ["__comment__"]
            ):
                d_fields[label] = field
        return d_fields

    def card_deal_fields_config(self):
        """
        all deal card fields available to UI be it combined/irrespective od deal grid, with description of how to format + label them

        Returns:
            list -- [{'field': field, 'fmt': fmt, 'label': label}]
        """
        return self.raw_schema.get('deal_card', []) if self.raw_schema.get('deal_card', []) else self.deal_fields_config

    def pivot_deal_fields_config(self, pivot_schema="schema"):
        """
        all pivot deal fields available to UI, with description of how to format + label them

        Returns:
            list -- [{'field': field, 'fmt': fmt, 'label': label}]
        """
        return get_nested(self.config, [pivot_schema, "deal"], [])

    def pivot_card_deal_fields(self, pivot_schema="schema"):
        """
        deal fields to display in deal card mapped to their tenant specific field names
        optional: falls back to standard deal_fields

        Returns:
            dict -- {'OpportunityName': 'opp_name'}
        """
        d_fields = {
            "alert": "alert",
            "dealalert": "dealalert",
        }  # HACK: get alert into deal ...
        for label, field in get_nested(
                self.config,
                [pivot_schema, "card_deal_fields"],
                get_nested(self.config, [pivot_schema, "deal_fields"], {}),
        ).items():
            if field in self.dlf_fields:
                field = ".".join(["dlf", field])
            if field not in get_nested(
                    self.config, [pivot_schema, "excluded_from_deal_card"], [
                        "__comment__"]
            ):
                d_fields[label] = field
        return d_fields

    def crr_card_fields(self):
        return self.config.get('crr_card_fields', {})

    def crr_card_special_fields(self):
        return self.config.get('crr_card_special_fields', {})

    def crr_card_graph_fields(self):
        return self.config.get('crr_card_graph_fields', {})

    @cached_property
    def export_hierarchy_fields(self):
        return self.config.get('export_hierarchy_fields', True)

    def export_deal_fields(self, schema="schema", special_pivot=False):
        """
        deal fields to display in deals export to their tenant specific field names
        optional: falls back to standard deal_fields

        Returns:
            list -- [(label, key, fmt) ... ]
        """
        d_fields = []
        pivot = schema.split('_')[0]
        id = '__id__' if pivot in self.config.get('not_deals_tenant', {}).get('special_pivot', []) else 'opp_id'
        if schema != 'schema' and schema in self.config:
            pivot_schema = self.config.get(schema)
            export_fields = pivot_schema.get("export_deal_fields", [])
            if export_fields:
                fields_order = [x[0] for x in export_fields]
                export_fields = {label: field for label, field in export_fields}
            else:
                fields_order = [x["field"] for x in pivot_schema.get("deal", [])]
                export_fields = pivot_schema.get("deal_fields", {})
        else:
            export_fields = self.raw_schema.get("export_deal_fields", [])
            if export_fields:
                fields_order = [x[0] for x in export_fields]
                export_fields = {label: field for label, field in export_fields}
            else:
                fields_order = [x["field"] for x in self.deal_fields_config]
                export_fields = self.raw_schema.get("deal_fields", {})

        for standard_field, db_field in export_fields.items():
            if standard_field[:2] == "__" and standard_field != '__comment__':
                continue
            label = self.get_field_label(standard_field, pivot=pivot)
            fmt = self.get_field_format(standard_field, pivot=pivot)
            if db_field in self.dlf_fields:
                db_field = ".".join(["dlf", db_field])
                d_fields.extend(
                    [
                        (label + " Status", standard_field, db_field, "dlf"),
                    ]
                )
                if self.dlf_mode.get(db_field.split(".")[-1], None) != "N":
                    d_fields.append(
                        (label, standard_field, db_field, "dlf_amount"))
            else:
                d_fields.append((label, standard_field, db_field, fmt))

        field_indices = {x: i for (i, x) in enumerate(fields_order)}
        # Sort based on standard field name.
        ordered_fields = sorted(
            [fld for fld in d_fields if fld[1] in fields_order],
            key=lambda x: field_indices[x[1]],
        )
        all_fields = ordered_fields + [
            fld for fld in d_fields if fld[1] not in fields_order
        ]
        if special_pivot:
            hierarchy_field_config = ('Hierarchy', '__segs', "list")
            return [("Id", id, "str"), hierarchy_field_config] + [(label, db_field, fmt) for
                                                                  (label, standard_field, db_field, fmt) in all_fields]
        if self.export_hierarchy_fields:
            hierarchy_field_config = ('Hierarchy', 'drilldown_list', "list")
            return [("Id", id, "str"), hierarchy_field_config] + [(label, db_field, fmt) for
                                                                  (label, standard_field, db_field, fmt) in all_fields]
        return [("Id", id, "str")] + [(label, db_field, fmt) for
                                      (label, standard_field, db_field, fmt) in all_fields]

    def export_pdf_deal_fields(self):
        export_pdf_fields = self.raw_schema.get("export_pdf_deal_fields", [])
        if not export_pdf_fields:
            return []
        export_fields = [label for label, _, _ in self.export_deal_fields()]
        return [label for label in export_pdf_fields if label in export_fields]

    @cached_property
    def reload_post_writeback(self):
        if 'reload_post_writeback' not in self.config.get('schema', {}):
            return None
        return self.config.get('schema').get('reload_post_writeback')

    @cached_property
    def export_deal_fields_format(self):
        if 'export_deal_fields_format' not in self.config.get('schema', {}):
            return None
        return self.config.get('schema').get('export_deal_fields_format')

    @cached_property
    def pull_in_deal_fields(self):
        """
        deal fields to display in pull in deals grid to their tenant specific field names
        optional: falls back to standard deal_fields

        Returns:
            dict -- {'OpportunityName': 'opp_name'}
        """
        d_fields = {"__fav__": "__fav__"}
        deal_fields_map = self.raw_schema.get("deal_fields", {})
        for label in PULL_IN_LIST:
            d_fields[label] = deal_fields_map.get(label, label)
        return d_fields

    @cached_property
    def pull_in_fields_order(self):
        """
        Force pull in deals columns to have an order

        Returns:
            dict -- {'label': labels_of_order,
                     'fields': fields_used_in_deals}
        """
        return {
            "label": PULL_IN_LIST,
            "fields": [self.pull_in_deal_fields[l] for l in PULL_IN_LIST],
        }

    @cached_property
    def opp_template(self):
        """
        template to make link to source crm system opportunity

        Returns:
            str -- url stub
        """
        try:
            return sec_context.details.get_config("forecast", "tenant", {}).get(
                "opportunity_link", "https://salesforce.com/{oppid}"
            )
        except AttributeError:
            return "https://salesforce.com/{oppid}"

    #
    # Filters
    #
    @cached_property
    def open_filter_criteria(self):
        """
        mongo db filter criteria for open deals

        Returns:
            dict -- {mongo db criteria}
        """
        return fetch_filter([self._open_filter_id], self, db=self.db)

    @cached_property
    def open_filter_raw(self):
        """
        aviso filter syntax criteria for open deal

        Returns:
            list -- [{'op': 'has', 'key': 'amt'}]
        """
        return fetch_filter([self._open_filter_id], self, filter_type="raw", db=self.db)

    def open_filter(self, deal):
        """
        check if a deal is open or not

        Arguments:
            deal {dict} -- deal record

        Returns:
            bool -- True if open, False if closed
        """
        return self._py_open_func(deal, None, None)

    def won_filter(self, deal):
        """
        check if a deal is won or not

        Arguments:
            deal {dict} -- deal record

        Returns:
            bool -- True if open, False if closed
        """
        return self._py_won_func(deal, None, None)

    def lost_filter(self, deal):
        """
        check if a deal is open or not

        Arguments:
            deal {dict} -- deal record

        Returns:
            bool -- True if open, False if closed
        """
        return self._py_lost_func(deal, None, None)

    @cached_property
    def opp_name_field(self):
        """
        name of opportunity name field for tenant
        Returns:
            str -- field name
        """
        return get_nested(self.config, ["field_map", "opp_name"])

    @cached_property
    def _open_filter_id(self):
        return get_nested(self.config, ["filters", "open_filter"])

    @cached_property
    def _favourites_filter_id(self):
        return get_nested(self.config, ["filters", "favourites_filter"], 'favourites')

    @cached_property
    def _py_open_func(self):
        return fetch_filter(
            [self._open_filter_id], self, filter_type="python", db=self.db
        )

    @cached_property
    def _py_won_func(self):
        return fetch_filter(
            [self._won_filter_id], self, filter_type="python", db=self.db
        )

    @cached_property
    def _py_lost_func(self):
        return fetch_filter(
            [self._lost_filter_id], self, filter_type="python", db=self.db
        )

    @cached_property
    def _all_filter_criteria(self):
        """
        mongo db filter criteria for won deals

        Returns:
        dict -- {mongo db criteria}
        """
        return fetch_filter([self._alldeals_filter_id], self, db=self.db)

    @cached_property
    def _alldeals_filter_id(self):
        return get_nested(self.config, ["filters", "all_filter"], 'All')

    @cached_property
    def multiple_filter_apply_or(self):
        return self.config.get("multiple_filter_apply_or", False)

    @cached_property
    def ai_driven_deals_buckets(self):
        default_expressions = {"Pullins": {"color": '#2bccff'},
                               "Aviso AI- Predicted Wins": {"expr": "win_prob_threshold < win_prob < 1.0",
                                                            "color": '#800080'}}
        return self.config.get("ai_driven_deals_buckets", default_expressions)

    @cached_property
    def default_currency(self):
        """
        Returns default currency set for the tenant to serve in notifications.
        """
        td = sec_context.details
        return td.get_config('forecast', 'tenant', {}).get('notifications_default_currency') or '$'

    # event-based nudge configs
    @cached_property
    def event_based_nudge_config(self):
        """
            ... Set nudge test_mode=True/False for testing purpose
            ... Set nudge enable=True/False to enable/disable event subscription
        """
        config_params = {
            "debug": False,
            "eb_score_hist_dip_nudge": {'test_mode': True, 'test_email': 'amit.khachane@aviso.com',
                                        'enable': False},
            "eb_scenario_nudge": {'test_mode': True, 'test_email': 'amit.khachane@aviso.com', 'enable': False},
            "eb_dlf_nudge": {'test_mode': True, 'test_email': 'amit.khachane@aviso.com', 'enable': False},
            "eb_pulledin_nudge": {'test_mode': True, 'test_email': 'amit.khachane@aviso.com', 'enable': False,
                                  "criteria": {'pulledin': True, 'terminal_fate': 'N/A'}},
        }
        return get_nested(
            self.config, ['nudge_config', 'event_based_nudge_config'],
            config_params
        )

    # Ringcentral Nudges Filters
    def nudge_filter_criteria(self, filter_id='Open Deals', root_node=None):
        """
        mongo db filter criteria for nudge deals
        Returns:
            dict -- {mongo db criteria}
        """
        # replicable_key = config.config.get('schema', {}).get('deal_fields', {}).get('Amount')
        config = DealConfig()
        if root_node is None:
            logger.exception("Root node not found, passing empty criteria")
            return {}
        criteria = fetch_filter([filter_id], config, root_node=root_node)
        return criteria

    # --x--Ringcentral Nudges Filters--x--

    #
    # DLF Config
    #
    @cached_property
    def primary_dlf_field(self):
        """
        main dlf field to use for other features like opp map and deal changes

        Returns:
            str -- dlf field name
        """
        try:
            return next(
                k for k, v in self.config.get("dlf", {}).items() if "primary" in v
            )
        except StopIteration:
            return None

    @cached_property
    def analytics_dlf_field(self):
        """
         dlf field to use in analytics field

        Returns:
            str -- dlf field name
        """
        try:
            return next(
                k
                for k, v in self.config.get("dlf", {}).items()
                if "use_in_pipeline_analytics" in v
            )
        except StopIteration:
            return None

    @cached_property
    def dlf_mode(self):
        """
        mapping of dlf field to dlf mode
        'N': no amount
        'O': optional amount

        Returns:
            dict -- {'in_fcst': 'N'}
        """
        return {
            field: field_config["mode"]
            for field, field_config in self.config.get("dlf", {}).items()
        }

    @cached_property
    def dlf(self):
        """
        return all dlf
        """
        return self.config.get("dlf", {})

    @cached_property
    def dlf_reports(self):
        return self.config.get("dlf_reports", False)

    @cached_property
    def top_deals_count(self):
        """
        return how many deals should be shown in top_deals section in dashboard
        """
        return self.config.get("top_deals_count", 30)

    @cached_property
    def dlf_fields(self):
        """
        all dlf fields

        Returns:
            list -- [dlf field]
        """
        return self.config.get("dlf", {}).keys()

    @cached_property
    def oppmap_dlf_field(self):
        return get_nested(
            self.config,
            ["oppmap", "dlf_field"],
            'in_fcst'
        )

    @cached_property
    def dlf_mismatch_default(self):
        """
        dlf_mismatch_default: True if we want to show mismatch wrt defaults
        """
        return self.config.get("dlf_mismatch_default", False)

    @cached_property
    def dlf_amount_field(self):
        """
        mapping of dlf field to deal amount field used to back dlf
        optional: falls back to tenants amount field

        Returns:
            dict -- {'in_fcst': 'amount'}
        """
        return {
            field: field_config.get("amount_field", self.amount_field)
            for field, field_config in self.config.get("dlf", {}).items()
        }

    @cached_property
    def change_dlf_with_writeback(self):
        """
        mapping of field name with it's values. During writeback if there is change in field name
        and value matches. DLF is toggled true

        Returns:
            dict -- {'ManagerForecastCategory':['Commit','Best Case','Closed Won']}
        """
        return self.config.get('change_dlf_with_writeback', [])

    @cached_property
    def dlf_secondary_amount_field(self):
        """
        mapping of dlf field to deal secondary amount field used to back dlf

        Returns:
            dict -- {'in_fcst': 'amount'}
        """
        return {
            field: field_config.get("secondary_amount_field", "")
            for field, field_config in self.config.get("dlf", {}).items()
        }

    @cached_property
    def multi_dlf(self):
        return get_nested(self.config, ["multi_dlf"], False)

    @cached_property
    def dlf_adorn_fields(self):
        """
        mapping of dlf field to extra deal fields to adorn each on each dlf to see state at time forecast was made

        Returns:
            dict -- {'in_fcst': {'win_prob': 'win_prob',}}
        """
        return {
            field: field_config.get(
                "adorn_field", self._default_dlf_adorn_fields)
            for field, field_config in self.config.get("dlf", {}).items()
        }

    @cached_property
    def dlf_crr_adorn_fields(self):
        """
        mapping of dlf field to extra deal fields to adorn each on each dlf to see state at time forecast was made

        Returns:
            dict -- {'in_fcst': {'win_prob': 'win_prob',}}
        """
        return {
            field: field_config.get(
                "adorn_field", self._default_crr_dlf_adorn_fields)
            for field, field_config in self.config.get("dlf", {}).items()
        }

    @cached_property
    def _default_dlf_adorn_fields(self):
        return {
            "score": "win_prob",
            "stage": self.stage_field,
            "forecastcategory": self.forecast_category_field,
            "raw_amt": self.amount_field,
            "closedate": self.close_date_field,
        }

    @cached_property
    def _default_crr_dlf_adorn_fields(self):
        return {
            "score": "win_prob",
            "stage": self.stage_field,
            "forecastcategory": self.forecast_category_field,
            "raw_amt": self.crr_amount_field,
            "closedate": self.close_date_field,
        }

    def deals_dlf_rendered_config(self, node):
        """
        dlf config rendered for consumption by front end

        Returns:
            dict -- {dlf config}
        """
        dlf_config = {}
        for field, dtls in self.config.get("dlf", {}).items():
            mode = dtls["mode"]
            dlf_config[field] = {
                "has_amt": mode != "N",
                "amt_editable": mode == "O",
                "option_editable": True,
            }
            try:
                if get_node_depth(node) >= dtls["hide_at_depth"]:
                    dlf_config[field]["hide"] = True
            except:
                pass
            if "options" in dtls:
                dlf_config[field]["options"] = dtls["options"]
            # TODO: hide at depth grossness
        return dlf_config

    @cached_property
    def dlf_rendered_config(self):
        """
        dlf config rendered for consumption by front end

        Returns:
            dict -- {dlf config}
        """
        dlf_config = {}
        for field, dtls in self.config.get("dlf", {}).items():
            mode = dtls["mode"]
            dlf_config[field] = {
                "has_amt": mode != "N",
                "amt_editable": mode == "O",
                "option_editable": True,
            }
            if "options" in dtls:
                dlf_config[field]["options"] = dtls["options"]
            # TODO: hide at depth grossness
        return dlf_config

    def dlf_locked_filter(self, deal, field):
        """
        check if a deal is locked in or out of forecast

        Arguments:
            deal {dict} -- deal record
            field {str} -- dlf field name

        Returns:
            bool -- True if locked in, False if locked out, None if not locked
        """
        for state, filter_func in self._dlf_py_locked_func.get(field, {}).items():
            if filter_func(deal, None, None):
                return state

    def ue_locked_filter(self, deal, field, pivot_schema='schema'):

        for state, filter_func in self._user_edit_dlf_py_locked_func(pivot_schema=pivot_schema).get(field,
                                                                                                    {}).items():
            if filter_func(deal, None, None):
                return state
        return False

    def dlf_default_filter(self, deal, field):
        """
        check if a deal if default to in our out of forecas

        Arguments:
            deal {dict} -- deal record
            field {str} -- dlf field name

        Returns:
            bool -- True if default in, False if default out
        """
        for state, filter_func in self._dlf_py_default_func.get(field, {}).items():
            if filter_func(deal, None, None):
                return state
        return self._dlf_default_values[field]

    @cached_property
    def favourites_filter_criteria(self):
        """
        mongo db filter criteria for favourite deals

        Returns:
            dict -- {mongo db criteria}
        """
        return fetch_filter([self._favourites_filter_id], self, db=self.db)

    @cached_property
    def _dlf_default_values(self):
        default_values = {}
        for field, field_config in self.config.get("dlf").items():
            try:
                default_values[field] = field_config["options"][0]["val"]
            except (KeyError, IndexError):
                default_values[field] = False
        return default_values

    @cached_property
    def _dlf_py_locked_func(self):
        return {
            field: {
                _convert_state(state): fetch_filter(
                    filter_ids, self, filter_type="python", db=self.db
                )
                for state, filter_ids in field_config.get(
                    "locked_filters", {}
                ).items()
            }
            for field, field_config in self.config.get("dlf").items()
        }

    def _user_edit_dlf_py_locked_func(self, pivot_schema=None):
        deal_fields = self.pivot_deal_fields_config(pivot_schema=pivot_schema)
        locked_filter_py = {}
        for deal_field in deal_fields:
            is_ue_field = deal_field.get('user_edit', False)
            if is_ue_field:
                locked_filters = deal_field.get('locked_filters', {})
                if locked_filters:
                    locked_filter_py.update({
                        deal_field['field']: {
                            _convert_state(state): fetch_filter(
                                filter_ids, self, filter_type="python", db=self.db
                            )
                            for state, filter_ids in locked_filters.items()
                        }
                    })

        return locked_filter_py

    @cached_property
    def _dlf_py_default_func(self):
        return {
            field: {
                _convert_state(state): fetch_filter(
                    filter_ids, self, filter_type="python", db=self.db
                )
                for state, filter_ids in field_config.get(
                    "default_filters", {}
                ).items()
            }
            for field, field_config in self.config.get("dlf").items()
        }

    #
    # Dashboard Config
    # Adaptive metrics config
    @cached_property
    def adaptive_metrics_categories(self):
        cats = {}
        am_categories = (
            self.config["dashboard"]
            .get("adaptive_metrics_categories", {})
            .get("categories", {})
        )

        for category in am_categories:
            cats[category] = []

            for (field_name, field_filter, field_tot_fields) in am_categories[category]:

                if "handler" in field_filter:
                    cats[category].append(
                        (field_name, field_filter, field_tot_fields))
                elif "get_ratio" in field_filter:
                    cats[category].append(
                        (field_name, field_filter, field_tot_fields))
                else:
                    try:
                        field = self.amount_field
                        label, op = field_tot_fields
                        if (
                                "amount_fields"
                                in self.config["dashboard"]["adaptive_metrics_categories"]
                        ):
                            if (
                                    category
                                    in self.config["dashboard"][
                                "adaptive_metrics_categories"
                            ]["amount_fields"]
                            ):
                                if (
                                        field_name
                                        in self.config["dashboard"][
                                    "adaptive_metrics_categories"
                                ]["amount_fields"][category]
                                ):
                                    field = self.config["dashboard"][
                                        "adaptive_metrics_categories"
                                    ]["amount_fields"][category][field_name]

                    except ValueError:
                        (field,) = field_tot_fields
                        op = "$sum"

                    cats[category].append(
                        (
                            field_name,
                            parse_filters(field_filter, self),
                            [[label, field, op]],
                        )
                    )

        return cats

    @cached_property
    def adaptive_metrics_additional_handler_filters(self):
        am_handlers = (
            self.config["dashboard"]
            .get("adaptive_metrics_categories", {})
            .get("additional_handler_filters", {})
        )
        for handler in am_handlers:
            am_handlers[handler] = parse_filters(am_handlers[handler], self)
        return am_handlers

    @cached_property
    def adaptive_metrics_additinal_info(self):
        cats = {}
        am_additinal_info = (
            self.config["dashboard"]
            .get("adaptive_metrics_categories", {})
            .get("additinal_info_categories", {})
        )

        for category in am_additinal_info:
            cats[category] = defaultdict(dict)
            for (field_name, additinal_info_filter) in am_additinal_info[category]:
                cats[category][field_name] = defaultdict(dict)
                if "h" in additinal_info_filter:
                    cats[category][field_name]["past_count"] = additinal_info_filter[
                        "h"
                    ]
                if "f" in additinal_info_filter:
                    cats[category][field_name]["future_count"] = additinal_info_filter[
                        "f"
                    ]
                if "d" in additinal_info_filter:
                    cats[category][field_name][
                        "difference_with"
                    ] = additinal_info_filter["d"]
                if "tt" in additinal_info_filter:
                    cats[category][field_name]["tooltip"] = {
                        "type": "text",
                        "text": additinal_info_filter["tt"],
                    }
                if "sl" in additinal_info_filter:
                    cats[category][field_name]["sublabel"] = additinal_info_filter["sl"]

        return cats

    @cached_property
    def adaptive_metrics_additional_filters(self):
        cats = {}
        am_additinal_info = (
            self.config["dashboard"]
            .get("adaptive_metrics_categories", {})
            .get("additional_filters", {})
        )

        for category in am_additinal_info:
            cats[category] = defaultdict(dict)
            for (field_name, additinal_info_filter) in am_additinal_info[category]:
                cats[category][field_name] = defaultdict(dict)
                if "close_date" in additinal_info_filter:
                    cats[category][field_name]["close_date_in"] = additinal_info_filter[
                        "close_date"
                    ]
                if "created_date" in additinal_info_filter:
                    cats[category][field_name]["created_date"] = additinal_info_filter[
                        "created_date"
                    ]

        return cats

    @cached_property
    def adaptive_metrics_views_order(self):
        order_info = (
            self.config["dashboard"]
            .get("adaptive_metrics_categories", {})
            .get("views_order", [])
        )
        return order_info

    @cached_property
    def adaptive_metrics_cache_level(self):
        cache_level = (
            self.config["dashboard"]
            .get("adaptive_metrics_cache_level", 2)
        )
        return cache_level

    @cached_property
    def leaderboard_cache_level(self):
        cache_level = (
            self.config["dashboard"]
            .get("leaderboard_cache_level", 2)
        )
        return cache_level

    @cached_property
    def adaptive_metrics_views_format(self):
        cats = {}
        format_info = (
            self.config["dashboard"]
            .get("adaptive_metrics_categories", {})
            .get("views_format", {})
        )
        for category in format_info:
            cats[category] = defaultdict(dict)
            if "fmt" in format_info[category]:
                cats[category]["format"] = format_info[category]["fmt"]
        return cats

    @cached_property
    def adaptive_metrics_close_date_aware(self):
        cats = {}
        close_date_aware_info = (
            self.config["dashboard"]
            .get("adaptive_metrics_categories", {})
            .get("close_date_aware", {})
        )
        for category in close_date_aware_info:
            cats[category] = []
            for field_name in close_date_aware_info[category]:
                cats[category].append(field_name)
        return cats

    # Coaching Leaderboard config
    @cached_property
    def coaching_leaderboard_categories(self):
        cats = {}
        cl_categories = (
            self.config["dashboard"]
            .get("coaching_leaderboard_categories", {})
            .get("categories", [])
        )

        # for category in cl_categories:
        #     cats[category] = []
        #
        #     for badge_name in cl_categories[category]:
        #         cats[category].append(badge_name)

        return cl_categories

    @cached_property
    def tenant_diff_node(self):
        cl_categories = ['lume.com', 'lumenbackup.com', 'netapp_pm.com', ]

        return cl_categories

    @cached_property
    def tenant_diff_owner(self):
        cl_categories = ['netapp.com']

        return cl_categories

    @cached_property
    def tenant_node_rename(self):
        cl_categories = ['cisco.com']

        return cl_categories

    @cached_property
    def pqr_data(self):
        """
        value for number of quarters to be considered for calculating time_threshold in deal_velocity badge in coaching
        leaderboard.
        """
        return self.config.get("dashboard", {}).get("pqr_data", {})

    @cached_property
    def deal_velocity_past_n_qtrs(self):
        """
        value for number of quarters to be considered for calculating time_threshold in deal_velocity badge in coaching
        leaderboard.
        """
        return self.config.get("dashboard", {}).get("deal_velocity_past_n_qtrs", 4)

    # Deals Config
    @cached_property
    def deal_categories(self):
        cats = []
        for cat_label, cat_filter, cat_tot_fields in get_nested(
                self.config, ["dashboard", "deal_categories", "categories"], []
        ):
            try:
                label, op = cat_tot_fields
                if self.config["dashboard"]["deal_categories"].get("amount_fields"):
                    field = self.config["dashboard"]["deal_categories"][
                        "amount_fields"
                    ][cat_label]
                else:
                    field = self.amount_field
            except ValueError:
                (field,) = cat_tot_fields
                op = "$sum"
            if field in self.dlf_fields:
                field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
            cats.append(
                (cat_label, parse_filters(
                    cat_filter, self), [[label, field, op]])
            )
        return cats

    @cached_property
    def totals_segmented_view(self):
        """
        if this is set to true for segmented tenant,
        subtotals and total will have same value on selection
        of particular segment deals
        """
        return self.config.get("totals_segmented_view", False)

    #
    # Dashboard Category sort fields
    #
    @cached_property
    def category_amount_fields(self):
        cat_amt_fields = {}
        if self.config["dashboard"]["deal_categories"].get("amount_fields"):
            for cat_label, cat_filter, cat_tot_fields in get_nested(
                    self.config, ["dashboard", "deal_categories", "categories"], []
            ):
                cat_amt_fields[cat_label] = self.config["dashboard"]["deal_categories"][
                    "amount_fields"
                ][cat_label]
            return cat_amt_fields
        else:
            return None

        #
        # covid Dashboard Config
        #

    @cached_property
    def covid_deal_categories(self):
        cats = []
        for cat_label, cat_filter, cat_tot_fields in get_nested(
                self.config, ["dashboard",
                              "covid_deal_categories", "categories"], []
        ):
            try:
                label, op = cat_tot_fields
                field = self.amount_field
            except ValueError:
                (field,) = cat_tot_fields
                op = "$sum"
            if field in self.dlf_fields:
                field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
            cats.append(
                (cat_label, parse_filters(
                    cat_filter, self), [[label, field, op]])
            )
        return cats

    @cached_property
    def how_it_changed(self):
        return self.config.get('new_home_page', {}).get('how_it_changed', [])

    @cached_property
    def pipeline_quality_categories(self):
        return self.config.get('new_home_page', {}).get('pipeline_quality_categories', [])

    @cached_property
    def deals_stage_map_cached(self):
        return self.config.get('deals_stage_map_cached', False)

    @cached_property
    def pipe_dev_gbm_fields(self):
        default_pipe_dev_gbm_fields = ['node', 'period', '__segs', 'forecast']
        return self.config.get('pipe_dev_gbm_fields', default_pipe_dev_gbm_fields)

    @cached_property
    def deal_changes_categories(self):
        cats = []
        for cat_label, cat_filter, cat_tot_fields in get_nested(
                self.config,
                ["dashboard", "deal_changes", "categories"],
                self._default_deal_changes["categories"],
        ):
            try:
                field, op = cat_tot_fields
            except ValueError:
                (field,) = cat_tot_fields
                op = "$sum"
            label = field
            if field in self.dlf_fields:
                field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
            cats.append(
                (
                    cat_label,
                    parse_filters(cat_filter, self, hier_aware=False),
                    [[label, field, op]],
                )
            )
        return cats

    @cached_property
    def deal_changes_categories_won(self):
        cats = []
        for cat_label, cat_filter, cat_tot_fields in get_nested(
                self.config,
                ["dashboard", "deal_changes", "categories"],
                self._default_deal_changes["categories"],
        ):
            if cat_label == "Won":
                try:
                    field, op = cat_tot_fields
                except ValueError:
                    (field,) = cat_tot_fields
                    op = "$sum"
                label = field
                if field in self.dlf_fields:
                    field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
                cats.append(
                    (
                        cat_label,
                        parse_filters(cat_filter, self, hier_aware=False),
                        [[label, field, op]],
                    )
                )
        return cats

    @cached_property
    def deal_changes_leaderboards(self):
        lbs = get_nested(
            self.config,
            ["dashboard", "deal_changes", "leaderboards"],
            self._default_deal_changes["leaderboards"],
        )
        for lb, lb_dtls in lbs.items():
            if "schema" not in lb_dtls:
                lb_dtls["schema"] = self.dashboard_deal_format
            if "total_name" not in lb_dtls:
                lb_dtls["total_name"] = "Amount"
            if "arrow" not in lb_dtls:
                lb_dtls["arrow"] = False
        return lbs

    @cached_property
    def deal_changes_leaderboards_label_performer_desc(self):
        return get_nested(
            self.config, ["dashboard", "deal_changes", "leaderboards", "desc"], "amount"
        )

    @cached_property
    def deal_changes_pipe_field(self):
        return get_nested(
            self.config, ["dashboard", "deal_changes",
                          "pipe_field"], "tot_won_and_fcst"
        )

    @cached_property
    def deal_changes_categories_order(self):
        return get_nested(
            self.config,
            ["dashboard", "deal_changes", "categories_order"],
            self._default_deal_changes["categories_order"],
        )

    @cached_property
    def deal_changes_leaderboards_order(self):
        return get_nested(
            self.config,
            ["dashboard", "deal_changes", "leaderboards_order"],
            self._default_deal_changes["leaderboards_order"],
        )

    @cached_property
    def deal_changes_default_key(self):
        return get_nested(
            self.config,
            ["dashboard", "deal_changes", "default_key"],
            self._default_deal_changes["default_key"],
        )

    @cached_property
    def account_categories(self):
        """
        for top account dashboard feature
        the filters + labels to split dealts out by

        Returns:
            list -- [(cat label, {cat filter}, [cat sum fields]) for each category]
        """
        cats = []
        for cat_label, cat_filter, cat_tot_fields in get_nested(
                self.config, ["dashboard", "accounts", "categories"]
        ):
            try:
                field, op = cat_tot_fields
            except ValueError:
                (field,) = cat_tot_fields
                op = "$sum"
            label = field
            if field in self.dlf_fields:
                field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
            cats.append(
                (
                    cat_label,
                    parse_filters(cat_filter, self, hier_aware=False),
                    [[label, field, op]],
                )
            )
        return cats

    @cached_property
    def account_fields_and_ops(self):
        """
        mapping from account category label to deal fields and operations to perform on them for top accounts

        Returns:
            dict -- {cat label: [(field label, db field name, db operation)]}
        """
        cat_field_map = defaultdict(list)
        for category, fields_dtls in get_nested(
                self.config, ["dashboard", "accounts", "fields"], {}
        ).items():
            for field_dtls in fields_dtls:
                field, op, _, label, _ = field_dtls
                if field in self.dlf_fields:
                    field = ".".join(["dlf", field, "%(node)s", "dlf_amt"])
                cat_field_map[category].append((field, field, op))
        return cat_field_map

    @cached_property
    def account_schema(self):
        """
        mapping from account category label to deal schema for category

        Returns:
           dict -- {cat label: {deal schema}}
        """
        cat_schemas = {}
        for category, fields_dtls in get_nested(
                self.config, ["dashboard", "accounts", "fields"], {}
        ).items():
            cat_schemas[category] = [
                {"fmt": fmt, "label": label, "field": field,
                 "is_opp_name": is_opp_name}
                for field, _, fmt, label, is_opp_name in fields_dtls
            ]
        return cat_schemas

    @cached_property
    def deals_schema(self):
        """
        mapping from deal category label to deal schema for category

        Returns:
            dict -- {cat label: {deal schema}}
        """
        cat_schemas = {}
        for category, fields_dtls in get_nested(
                self.config, ["dashboard", "deal_categories", "fields"], {}
        ).items():
            cat_schemas[category] = [
                {"fmt": fmt, "label": label, "field": field}
                for field, _, fmt, label in fields_dtls
            ]
        return cat_schemas

    @cached_property
    def account_group_fields(self):
        """
        deal fields to group by for top accounts feature

        Returns:
            list -- [db deal fields]
        """
        return get_nested(self.config, ["dashboard", "accounts", "group_fields"], [])

    @cached_property
    def leaderboard_previous_values(self):
        """
        previous values config for all leaderboards

        Returns:
            dict -- {cat label: {prev config schema}}
        """
        return get_nested(self.config, ["dashboard", "leaderboard_previous_values"], {})

    @cached_property
    def account_sort_fields(self):
        """
        deal fields to sort by for top accounts feature

        Returns:
            dict -- {cat label: [deal fields]}
        """
        sorts = get_nested(
            self.config, ["dashboard", "accounts", "sort_fields"], {})
        return {
            cat: [
                (field, 1) if isinstance(field, str) else field
                for field in fields
            ]
            for cat, fields in sorts.items()
        }

    @cached_property
    def deal_changes_categories_amounts(self):
        return get_nested(
            self.config,
            ["dashboard", "deal_changes", "categories_amounts"],
            self._default_deal_changes["categories_amounts"],
        )

    @cached_property
    def show_winscore_in_dashboard_top_deals(self):
        return get_nested(
            self.config,
            ["dashboard", 'show_winscore_in_dashboard_top_deals'], {})

    @cached_property
    def dashboard_deal_format(self):
        # Need a way to make it easier to the correct subset of fields
        forecast_cat_label = None
        forecast_cat_field = None
        for label, deal_field in self.raw_schema.get("deal_fields", {}).items():
            if deal_field == self.forecast_category_field:
                forecast_cat_field = label
                for x in self.deal_fields_config:
                    if x["field"] == label:
                        forecast_cat_label = x["label"]
                        break
                break
        return [
            {"fmt": "str", "label": "Opportunity Name", "field": "OpportunityName"},
            {"fmt": "str", "label": "Owner", "field": "OpportunityOwner"},
            {"fmt": "amount", "label": "Amount", "field": "Amount"},
            {"fmt": "excelDate", "label": "Close Date", "field": "CloseDate"},
            {"fmt": "str", "label": forecast_cat_label, "field": forecast_cat_field},
            {"field": "win_prob", "fmt": "prob", "label": "Aviso Score"},
        ]

    # TODO: BS How to change this to use covid fields
    @cached_property
    def covid_dashboard_deal_format(self):
        # Need a way to make it easier to the correct subset of fields
        forecast_cat_label = None
        forecast_cat_field = None
        for label, deal_field in self.raw_schema.get("deal_fields", {}).items():
            if deal_field == self.oppmap_forecast_category_field:
                forecast_cat_field = label
                for x in self.deal_fields_config:
                    if x["field"] == label:
                        forecast_cat_label = x["label"]
                        break
                break
        return [
            {"fmt": "str", "label": "Opportunity Name", "field": "OpportunityName"},
            {
                "field": "__covid__",
                "fmt": "",
                "label": self.covid_labellings.get("aviso_column", "Covid"),
            },
            {"fmt": "str", "label": "Owner", "field": "OpportunityOwner"},
            {"fmt": "str", "label": forecast_cat_label, "field": forecast_cat_field},
            {"fmt": "excelDate", "label": "Close Date", "field": "CloseDate"},
            {"fmt": "amount", "label": "Amount", "field": "Amount"},
        ]

    @cached_property
    def covid_labellings(self):
        return get_nested(self.config, ["dashboard", "covid_labellings"], {})

    @cached_property
    def activity_metrics_top_20_deals_filter(self):
        # deals filter to find top 20 deals for activity metrics graph in dashboard
        return get_nested(self.config, ['dashboard', 'activity_metrics', 'top_20_deals_filter'], {})

    @cached_property
    def week_on_week_filters(self):
        default_filters = {'commit': {'ManagerForecastCategory': {'$in': ['Commit']}},
                           'open_pipeline': {'as_of_StageTrans': {'$nin': ['1', '99']}}}
        return get_nested(self.config, ['week_on_week_filters'], default_filters)

    @cached_property
    def segment_amount(self):
        return get_nested(self.config, ["dashboard", "segment_amount"], {})

    @cached_property
    def activity_metrics_deal_filters(self):
        default_filter = [[u'Commit Deals',
                           [{u'key': self.forecast_category_field, u'op': u'in', u'val': [u'Commit']}],
                           [u'amount', u'$sum']],
                          [u'Most Likely Deals',
                           [{u'key': self.forecast_category_field, u'op': u'in', u'val': [u'Most Likely']}],
                           [u'amount', u'$sum']],
                          [u'Best Case Deals',
                           [{u'key': self.forecast_category_field, u'op': u'in', u'val': [u'Best Case']}],
                           [u'amount', u'$sum']]]
        cats = []
        for cat_label, cat_filter, cat_tot_fields in get_nested(self.config, ['new_home_page',
                                                                              'activity_metrics',
                                                                              'deal_filters'], default_filter):
            try:
                label, op = cat_tot_fields
                field = self.amount_field
            except ValueError:
                field, = cat_tot_fields
                op = '$sum'
            if field in self.dlf_fields:
                field = '.'.join(['dlf', field, '%(node)s', 'dlf_amt'])
            cats.append((cat_label, parse_filters(cat_filter, self, hier_aware=False), [[label, field, op]]))
        return cats

    @cached_property
    def engagement_grade(self):
        return get_nested(self.config, ['field_map', 'engagement_grade'], 'engagement_grade')

    @cached_property
    def oppmap_labellings(self):
        return get_nested(self.config, ["dashboard", "oppmap_labellings"], {})

    @cached_property
    def standard_oppmap_map_types(self):
        oppmap_types = get_nested(
            self.config,
            ["oppmap", "standard_oppmap_map_types"],
            DEFAULT_STANDARD_OPPMAP_MAP_TYPES,
        )
        oppmap_type_tuples = []
        for map_type in oppmap_types:
            oppmap_type_tuples.append((map_type, oppmap_types[map_type]))
        return oppmap_type_tuples

    @cached_property
    def covid_oppmap_map_types(self):
        oppmap_types = get_nested(
            self.config,
            ["oppmap", "covid_oppmap_map_types"],
            DEFAULT_COVID_OPPMAP_MAP_TYPES,
        )
        oppmap_type_tuples = []
        for map_type in oppmap_types:
            oppmap_type_tuples.append((map_type, oppmap_types[map_type]))
        return oppmap_type_tuples

    @cached_property
    def segment_field(self):
        return self.config.get("segment_field", None)

    # TODO: make it fuller if possible

    @cached_property
    def nudge_insight_facts(self):
        """
            config notebook - https://jupyter.aviso.com/user/amitk/notebooks/amitk/Ticket%20Specific/AV-11394.ipynb
        """
        return get_nested(
            self.config, ['insight_config', 'nudge_insight_facts'], {})

    @cached_property
    def insight_config(self):
        return self.config.get("insight_config", {})

    @cached_property
    def insight_task_config(self):
        return self.config.get('insight_task_config', {})

    @cached_property
    def custom_stage_ranks(self):
        return self.config.get("custom_stage_ranks", {})

    @cached_property
    def custom_fc_ranks(self):
        return self.config.get("custom_fc_ranks", {})

    @cached_property
    def custom_fc_ranks_default(self):
        dict_ = {"pipeline": 1, "upside": 2, "most likely": 3, "commit": 4}
        return self.config.get("custom_fc_ranks", dict_)

    @cached_property
    def close_date_pushes_flds(self):
        CLOSEDATE_FIELD_MAP = {
            "total_pushes": "close_date_total_pushes",
            "months_pushed": "close_date_months_pushed",
        }
        return self.config.get("close_date_pushes_flds", CLOSEDATE_FIELD_MAP)

    @cached_property
    def use_grouper_flag(self):
        return self.config.get("use_grouper_flag", False)

    @cached_property
    def weekly_report_dimensions(self):
        return self.config.get("weekly_report_dimensions", [])

    @cached_property
    def dimensions(self):
        return self.config.get("dimensions", [])

    @cached_property
    def fm_config(self):
        return self.config.get("fm_config", {})

    @cached_property
    def custom_fc_ranks(self):
        return self.config.get("custom_fc_ranks", {})

    @cached_property
    def bookingstimeline(self):
        return self.config.get("bookingstimeline", False)

    @cached_property
    def anomaly_config(self):
        return self.config.get("anomaly_config", {})

    @cached_property
    def stale_nudge_enable(self):
        return self.config.get("stale_nudge_enable", False)

    @cached_property
    def crm_hygiene_fld(self):
        return self.config.get("crm_hygiene_fld", [])

    @cached_property
    def past_closedate_enabled(self):
        return self.config.get("past_closedate_enabled", False)

    @cached_property
    def close_date_thresh(self):
        return self.config.get("close_date_thresh", 15)

    @cached_property
    def frequent_fld_nudge(self):
        return self.config.get("frequent_fld_nudge", False)

    @cached_property
    def frequent_fld(self):
        return self.config.get("frequent_fld", "NextStep")

    @cached_property
    def frequency_eoq_time(self):
        return self.config.get("frequency_eoq_time", 14)

    @cached_property
    def pipeline_nudge(self):
        return self.config.get("pipeline_nudge", False)

    @cached_property
    def pipeline_fields(self):
        fields = self.config.get("pipeline_fields", {})
        if fields:
            plan_field = fields.get("plan_field")
            booked_field = fields.get("booked_field")
            top_field = fields.get("top_field")
            rollup_field = fields.get("rollup_field")

            return {
                "plan_field": plan_field,
                "booked_field": booked_field,
                "top_field": top_field,
                "rollup_field": rollup_field,
            }
        else:
            return {}

    @cached_property
    def pipeline_ratio(self):
        return self.config.get("pipeline_ratio", 3)

    @cached_property
    def changed_deals_limit(self):
        return self.config.get("changed_deals_limit", 200)

    @cached_property
    def exclude_competitors(self):
        return self.config.get("exclude_competitors", ["no competition"])

    @cached_property
    def late_stg_thresh(self):
        return self.config.get("late_stg_thresh", 40.0)

    @cached_property
    def close_date_update(self):
        return self.config.get("close_date_update", False)

    @cached_property
    def update_thresh(self):
        return self.config.get("update_thresh", 30)

    @cached_property
    def close_date_thresh(self):
        return self.config.get("close_date_thresh", 30)

    @cached_property
    def manager_only_cd_no_update(self):
        return self.config.get("manager_only_cd_no_update", False)

    # @cached_property
    # def at_risk_deals_enabled(self):
    #     return self.config.get("at_risk_deals_enabled", False)

    # @cached_property
    # def low_pipeline_threshold(self):
    #     return self.config.get('low_pipeline_threshold', 15.0)

    @cached_property
    def low_netxq_pipeline_nudge(self):
        return self.config.get("low_netxq_pipeline_nudge", False)

    @cached_property
    def commit_not_in_dlf(self):
        return self.config.get("commit_not_in_dlf_nudge", False)

    @cached_property
    def competitor_nudge_enabled(self):
        return self.config.get("competitor_nudge_enabled", False)

    # AV-996
    @cached_property
    def stale_nudge_thresh(self):
        return self.config.get("stale_nudge_thresh", 40)

    @cached_property
    def stale_nudge_config(self):
        config_params = {
            'deal_filter_id': 'Filter_Closedate_Stage',
            'deal_cnt': None,
            'close_within_days': 10,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            'nudge_heading': '',
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'stale_nudge_config'],
            config_params
        )

    @cached_property
    def at_risk_nudge_config(self):
        config_params = {
            'deal_cnt': None,
            'send_to_rep': True,
            'senf_to_mgr': True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            'send_only_to': [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'at_risk_nudge_config'],
            config_params
        )

    # @cached_property
    # def score_history_dip_nudge(self):
    #     return self.config.get("score_history_dip_nudge", False)

    # @cached_property
    # def manager_only_score_drop(self):
    #     return self.config.get("manager_only_score_drop", False)

    @cached_property
    def score_hist_dip_nudge_config(self):
        config_params = {
            'threshold': 10.0,
            'deal_filter_id': '',
            'deal_cnt': None,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'score_hist_dip_nudge_config'],
            config_params
        )

    # @cached_property
    # def competitor_nudge_config(self):
    #     return self.config.get("competitor_nudge_config", {})
    @cached_property
    def competitor_nudge_config(self):
        config_params = {
            'competitor_field': 'Competitor',
            'group_by_field': 'Type',
            'deal_filter_id': 'Competitor Deals',
            'deal_cnt': None,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            'send_only_to': [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'competitor_nudge_config'],
            config_params
        )

    @cached_property
    def past_closedate_nudge_config(self):
        config_params = {
            'deal_filter_id': 'Filter_Closedate',
            'extra_param': '',
            'full_year_deals_view_cta': True,
            'deal_cnt': None,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            "nudge_heading": "",
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'past_closedate_nudge_config'],
            config_params
        )

    @cached_property
    def highamount_change_nudge_config(self):
        config_params = {
            'deal_filter_id': '',
            'deal_cnt': None,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'highamount_change_nudge_config'],
            config_params
        )

    @cached_property
    def tenant_agg_metrics_nudge_config(self):
        config_params = {
            'deal_cnt': None,
            'deal_filter_id': 'Open Deals',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'tenant_agg_metrics_nudge_config'],
            config_params
        )

    @cached_property
    def scenario_nudge_config(self):
        config_params = {
            'threshold': 20,
            'list_of_stages': [],
            'projection_days': 7,
            'deal_filter_id': 'scenario_deals',
            'deal_cnt': None,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'scenario_nudge_config'],
            config_params
        )

    @cached_property
    def upside_deal_stats_nudge_config(self):
        config_params = {
            'industry_fld': 'Industry',
            'filter_types': ['Renewal', 'Renewals'],
            'threshold': 60,
            'win_prob_treshold': 40,
            # additional
            'deal_filter_id': 'oppmap/amount/commit/upside',
            'deal_cnt': None,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'upside_deal_stats_nudge_config'],
            config_params
        )

    @cached_property
    def pullin_deals_nudge_config(self):
        config_params = {
            'deal_filter_id': '/pull-ins',
            'deal_cnt': None,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'pullin_deals_nudge_config'],
            config_params
        )

    @cached_property
    def rep_closing_metrics_config(self):
        config_params = {
            'threshold': 30.0,  # Percentage threshold
            'deal_filter_id': 'Open Deals',
            'deal_cnt': None,
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'rep_closing_metrics_config'],
            config_params
        )

    @cached_property
    def cd_no_update_config(self):
        config_params = {
            'recommended_stage': '',  # default stage
            'deal_filter_id': 'cd_no_update',
            'deal_cnt': None,
            'send_to_managers_only': False,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            'send_only_to': [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'cd_no_update_config'],
            config_params
        )

    @cached_property
    def non_commit_config(self):
        config_params = {
            'notif_gap': 7,
            'deal_filter_id': 'non_commit_fast',
            'deal_cnt': None,
            'send_to_mgr': True,
            'send_to_rep': True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'non_commit_config'],
            config_params
        )

    @cached_property
    def make_or_break_nudge_config(self):
        config_params = {
            'deal_filter_id': 'Open Deals',
            'deal_cnt': None,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'make_or_break_nudge_config'],
            config_params
        )

    @cached_property
    def drop_in_engagement_grade_nudge_config(self):
        config_params = {
            'deal_cnt': None,
            'deal_filter_id': '',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'drop_in_engagement_grade_nudge_config'],
            config_params
        )

    @cached_property
    def anomaly_nudge_config(self):
        config_params = {
            'deal_filter_id': '',
            'deal_cnt': None,
            'send_to_mgr': True,
            'send_to_rep': True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            'send_only_to': [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'anomaly_nudge_config'],
            config_params
        )

    @cached_property
    def outquater_pipline_nudge_config(self):
        config_params = {
            'deal_filter_id': 'Open Deals',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'outquater_pipline_nudge_config'],
            config_params
        )

    @cached_property
    def pipline_nudge_config(self):
        config_params = {
            'deal_filter_id': 'Open Deals',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'pipline_nudge_config'],
            config_params
        )

    @cached_property
    def low_pipeline_nudge_config(self):
        config_params = {
            'nextq_coverage_ratio': 4,
            'deal_filter_id': 'nextq_open_deals',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'low_pipeline_nudge_config'],
            config_params
        )

    @cached_property
    def late_stage_conversion_ratio_nudge_config(self):
        config_params = {
            'upside_deal_filter': '/oppmap/amount/commit/upside',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'late_stage_conversion_ratio_nudge_config'],
            config_params
        )

    @cached_property
    def upsell_recommendation_nudge_config(self):
        config_params = {
            'filter_specific_nodes': False,
            'deal_filter_id': 'Open Deals',
            'allowed_specific_nodes': {},
            'prohibited_roles': [],
            'prohibited_emails': [],
            'etl_line_item_name': 'OpportunityLineItem',
            'product_field': 'ProductName'
        }
        return get_nested(
            self.config, ['nudge_config', 'upsell_recommendation_nudge_config'],
            config_params
        )

    @cached_property
    def past_deals_based_alert_nudge_config(self):
        config_params = {
            'filter_specific_nodes': False,
            'deal_filter_id': 'Open Deals',
            'allowed_specific_nodes': {},
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'past_deals_based_alert_nudge_config'],
            config_params
        )

    # @cached_property
    # def booking_accuracy_manager_only(self):
    #     return self.config.get("booking_accuracy_manager_only", False)
    @cached_property
    def deal_amount_recommendation_nudge_config(self):
        config_params = {
            'deal_filter_id': '',
            'deal_cnt': None,
            'send_to_managers_only': False,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'deal_amount_recommendation_nudge_config'],
            config_params
        )

    @cached_property
    def booking_accuracy_nudge_config(self):
        config_params = {
            'deal_filter_id': '',
            'deal_cnt': None,
            'gap': 7,
            "send_to_mgr": True,
            "send_to_rep": False,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'booking_accuracy_nudge_config'],
            config_params
        )

    @cached_property
    def discount_nudge_config(self):
        config_params = {
            'deal_filter_id': '',
            'deal_cnt': None,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'discount_nudge_config'],
            config_params
        )

    @cached_property
    def market_basket_nudge_config(self):
        config_params = {
            'confidence': 50.0,
            'deal_filter_id': '',
            'deal_cnt': None,
            'send_to_managers': False,
            'filter_hierarchy': False,
            'allowed_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'market_basket_nudge_config'],
            config_params
        )

    # forecast_dip_nudge
    @cached_property
    def forecast_dip_enabled(self):
        return self.config.get("forecast_dip_enabled", False)

    @cached_property
    def forecast_dip_thresh(self):
        return self.config.get("forecast_dip_thresh", 10)

    @cached_property
    def forecast_dip_nudge_config(self):
        config_params = {
            'deal_filter_id': '',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            'notif_gap': 7,
            "internal_heading": "",
            'send_only_to': [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'forecast_dip_nudge_config'],
            config_params
        )

    @cached_property
    def conversion_rate_nudge(self):
        config_params = {
            'upside_deal_filter': '/oppmap/amount/commit/upside',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            'send_only_to': [],
            'prohibited_roles': [],
            'prohibited_emails': [],
            'commit_fld': 'commit'
        }
        return get_nested(
            self.config, ['nudge_config', 'conversion_rate_nudge'],
            config_params
        )

    @cached_property
    def pace_value_dip_enabled(self):
        return self.config.get("pace_value_dip_enabled", False)

    # @cached_property
    # def pace_value_dip_thresh(self):
    #     return self.config.get("pace_value_dip_thresh", -5)
    @cached_property
    def pace_value_dip_nudge_config(self):
        config_params = {
            'notif_gap': 7,
            'pace_value_dip_thresh': -5,
            'no_reps': None,
            'deal_filter_id': '',
            'filter_specific_nodes': False,
            'allowed_specific_nodes': {},
            "send_only_to": [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'pace_value_dip_nudge_config'],
            config_params
        )

    @cached_property
    def dlf_news_nudge_config(self):
        config_params = {
            'up_thresh': .20,
            'down_thresh': .15,
            'no_reps': None,
            'deal_filter_id': '',
            'filter_hierarchy': False,
            'allowed_nodes': {},
            'send_only_to': [],
            'prohibited_roles': [],
            'prohibited_emails': []
        }
        return get_nested(
            self.config, ['nudge_config', 'dlf_news_nudge_config'],
            config_params
        )

    # --AV-996--

    # Favorite Deals Nudge
    @cached_property
    def favorite_deals_nudge_config(self):
        config_params = {
            "since": 'yest',  # bow, bom
            "allowed_users": ["amit.khachane@aviso.com"],
            "attributes": '',
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config, ["nudge_config",
                          "favorite_deals_nudge_config"], config_params
        )

    @cached_property
    def deal_delta_alert_config(self):
        """Configurations for Deal Delta Alert Nudge (launchdarkly daily digest)"""
        config_params = {
            "since": 7,  # bow, bom
            "allowed_users": ["amit.khachane@aviso.com"],
            "attributes": '',
            "filter_id": 'Favorites',
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config, ["nudge_config", "favorite_deals_nudge_config"], config_params
        )

    @cached_property
    def meeting_nextsteps_nudge_config(self):
        """Configurations for Pre-Meeting Nudge"""
        config_params = {
            "allowed_users": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
            "send_only_to": [],
            "debug": True,
            "save_notifications": True,
        }
        return get_nested(
            self.config, ["nudge_config", "meeting_nextsteps_nudge_config"], config_params)

    @cached_property
    def unclassified_meetings_nudge_config(self):
        """Configurations for Unclassified Meetings Nudge"""
        config_params = {
            "targeted_roles": [],
            "prohibited_usersids": [],
            "send_only_to_usersids": [],

        }
        return get_nested(
            self.config, ["nudge_config", "unclassified_meetings_nudge_config"], config_params)

    # Jfrog Nudge
    @cached_property
    def pushed_out_deals_nudge_config(self):
        config_params = {
            "threshold": 1,
            "deal_cnt": None,
            "send_to_mgr": True,
            "send_to_rep": True,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config, ["nudge_config",
                          "pushed_out_deals_nudge"], config_params
        )

    @cached_property
    def dashboard_updates_nudge_config(self):
        """Configurations for Dashboard Updates Nudge"""
        config_params = {
            "targeted_roles": [],
            "prohibited_usersids": [],
            "send_only_to_usersids": [],
        }
        return get_nested(
            self.config, ["nudge_config", "dashboard_updates_nudge_config"], config_params)

    # RingCentral Nudges
    @cached_property
    def best_case_nudge_config(self):
        config_params = {
            "threshold": 50,
            "deal_cnt": None,
            "send_to_mgr": True,
            "send_to_rep": True,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config, ["nudge_config",
                          "best_case_nudge_config"], config_params
        )

    @cached_property
    def commit_nudge_config(self):
        config_params = {
            "threshold": 55,
            "deal_cnt": None,
            "send_to_mgr": True,
            "send_to_rep": True,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config, ["nudge_config", "commit_nudge_config"], config_params
        )

    @cached_property
    def yearold_nudge_config(self):
        config_params = {
            "early_stages_thresh": [],
            "send_to_mgr": True,
            "send_to_rep": True,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
            'title_stages': "",
        }
        return get_nested(
            self.config, ["nudge_config",
                          "yearold_nudge_config"], config_params
        )

    @cached_property
    def yearold_rep_nudge_config(self):
        config_params = {
            "early_stages_thresh": [],
            "deal_cnt": None,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
            "title_stages": "",
        }
        return get_nested(
            self.config, ["nudge_config",
                          "yearold_rep_nudge_config"], config_params
        )

    @cached_property
    def stagnant_manager_nudge_config(self):
        config_params = {
            "threshold": 15,
            "send_to_mgr": True,
            "send_to_rep": True,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config,
            ["nudge_config", "stagnant_manager_nudge_config"],
            config_params,
        )

    @cached_property
    def stagnant_rep_nudge_config(self):
        config_params = {
            "threshold": 15,
            "deal_cnt": None,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config, ["nudge_config",
                          "stagnant_rep_nudge_config"], config_params
        )

    @cached_property
    def highvalue_manager_nudge_config(self):
        config_params = {
            "stage_threshold": 50,
            "threshold": 100000,
            "send_to_mgr": True,
            "send_to_rep": True,
            "filter_hierarchy": False,
            "filter_id": "highvalue_deals_100k",
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config,
            ["nudge_config", "highvalue_manager_nudge_config"],
            config_params,
        )

    @cached_property
    def highvalue_rep_nudge_config(self):
        config_params = {
            "stage_threshold": 50,
            "threshold": 100000,
            "deal_cnt": None,
            "filter_hierarchy": False,
            "filter_id": "highvalue_deals_100k",
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config, ["nudge_config",
                          "highvalue_rep_nudge_config"], config_params
        )

    @cached_property
    def closedate_manager_nudge_config(self):
        config_params = {
            "threshold": 15,
            "send_to_mgr": True,
            "send_to_rep": True,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config,
            ["nudge_config", "closedate_manager_nudge_config"],
            config_params,
        )

    @cached_property
    def closedate_rep_nudge_config(self):
        config_params = {
            "threshold": 15,
            "deal_cnt": None,
            "filter_hierarchy": False,
            "nudge_heading": "",
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config, ["nudge_config",
                          "closedate_rep_nudge_config"], config_params
        )

    @cached_property
    def mismatch_manager_nudge_config(self):
        config_params = {
            "commit_stage_thresh": 55,
            "bestcase_stage_thresh": 50,
            "send_to_mgr": True,
            "send_to_rep": True,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config,
            ["nudge_config", "mismatch_manager_nudge_config"],
            config_params,
        )

    @cached_property
    def mismatch_rep_nudge_config(self):
        config_params = {
            "commit_stage_thresh": 55,
            "bestcase_stage_thresh": 50,
            "deal_cnt": None,
            "filter_hierarchy": False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config,
            ["nudge_config", "mismatch_rep_nudge_config"],
            config_params,
        )

    @cached_property
    def potential_manager_nudge_config(self):
        config_params = {
            'arr_threshold': 150000,
            'tcv_threshold': 800000,
            "send_to_mgr": True,
            "send_to_rep": True,
            'filter_hierarchy': False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config,
            ['nudge_config', 'potential_manager_nudge_config'],
            config_params,
        )

    @cached_property
    def potential_rep_nudge_config(self):
        config_params = {
            'arr_threshold': 150000,
            'tcv_threshold': 800000,
            'deal_cnt': None,
            'filter_hierarchy': False,
            "allowed_nodes": {},
            "send_only_to": [],
            "prohibited_roles": [],
            "prohibited_emails": [],
        }
        return get_nested(
            self.config,
            ['nudge_config', 'potential_rep_nudge_config'],
            config_params,
        )

    # --x--RingCentral Nudges--x--

    @cached_property
    def competitor_win_loss_config(self):
        return self.config.get("competitor_win_loss_config", {})

    @cached_property
    def scenario_nudge_enabled(self):
        return self.config.get("scenario_nudge_enabled", False)

    # @cached_property
    # def manager_only_high_amount_change(self):
    #     return self.config.get("manager_only_high_amount_change", False)

    @cached_property
    def high_risk_thresh(self):
        return self.config.get("high_risk_thresh", 20)

    @cached_property
    def cutoff_change(self):
        return self.config.get("high_risk_amount_change_cutoff", 10.0)

    @cached_property
    def high_risk_deals_enabled(self):
        return self.config.get("high_risk_deals_enabled", False)

    @cached_property
    def booking_accuracy_enable(self):
        return self.config.get("booking_accuracy_enable", False)

    @cached_property
    def tenant_aggregate_metrics(self):
        return self.config.get("tenant_aggregate_metrics", False)

    @cached_property
    def non_commit_enabled(self):
        return self.config.get("non_commit_enabled", False)

    @cached_property
    def non_commit_manager_only(self):
        return self.config.get("non_commit_manager_only", False)

    @cached_property
    def non_commit_dlf_enabled(self):
        return self.config.get("non_commit_dlf_enabled", False)

    # groupby_fields_map for mobile api
    @cached_property
    def groupby_fields_map(self):
        return self.config.get("groupby_fields_map", {})

    # ADDITION OF CONFIGURATIONS TO RESTRICT EMAIL SENDING TO MANAGERS ONLY
    @cached_property
    def manager_only_past_closedate(self):
        return self.config.get("manager_only_past_closedate", False)

    @cached_property
    def discount_nudge_enabled(self):
        return self.config.get("discount_nudge_enabled", False)

    @cached_property
    def nudge_config(self):
        return self.config.get("nudge_config", {})

    @cached_property
    def crm_hygiene_fld(self):
        return self.config.get("crm_hygiene_fld", [])

    @cached_property
    def won_filter_criteria(self):
        """
        mongo db filter criteria for won deals

        Returns:
        dict -- {mongo db criteria}
        """
        return fetch_filter([self._won_filter_id], self, db=self.db)

    @cached_property
    def _won_filter_id(self):
        return get_nested(self.config, ["filters", "won_filter"])

    def lost_filter_criteria(self):

        return fetch_filter([self._lost_filter_id], self, db=self.db)

    @cached_property
    def _lost_filter_id(self):
        return get_nested(self.config, ["filters", "lost_filter"])

    def alldeal_filter_criteria(self):

        return fetch_filter([self._alldeal_filter_id], self, db=self.db)

    @cached_property
    def _alldeal_filter_id(self):
        return get_nested(self.config, ["filters", "all_filter"])

    def pushout_deals_filter_criteria(self):

        return fetch_filter([self._pushout_deals_filter_id], self, db=self.db)

    @cached_property
    def _pushout_deals_filter_id(self):
        return get_nested(self.config, ["filters", "pushout_deals"])

    def commit_filter_criteria(self):

        return fetch_filter([self._commit_filter_id], self, db=self.db)

    @cached_property
    def _commit_filter_id(self):
        return get_nested(self.config, ["filters", "Commit"], 'Commit')

    @cached_property
    def created_date_field(self):
        """
        name of created date field for tenant
        Returns:
        str -- field name
        """
        return get_nested(self.config, ["field_map", "created_date"])

    @cached_property
    def manager_only_next_step_nudge(self):
        return self.config.get("manager_only_next_step_nudge", False)

    @cached_property
    def manager_only_commit_no_dlf(self):
        return self.config.get("manager_only_commit_no_dlf", False)

    @cached_property
    def persona_schemas(self):
        return self.config.get("persona_schemas")

    @cached_property
    def role_schemas(self):
        return self.config.get("role_schemas", {})

    @cached_property
    def crr_role_schemas(self):
        return self.config.get("crr_role_schemas", {})

    #
    # Complex Fields aka the please dont configure this fields
    #
    @cached_property
    def period_aware_fields(self):
        """
        fields that are period aware

        Returns:
            list -- [please no]
        """
        return get_nested(self.config, ["complex_fields", "period_aware_fields"], [])

    @cached_property
    def hier_aware_fields(self):
        """
        fields that are hierarchy aware (split fields)

        Returns:
            list -- [seriously, dont]
        """
        gbm_hier_aware_fields = [
            "forecast"
        ]  # TODO: @logan what else is always hier aware in gbm

        tenant_hier_aware_fields = get_nested(
            self.config, ["complex_fields", "hier_aware_fields"], []
        )

        dtfo_hier_aware_fields = []

        if self.amount_field in tenant_hier_aware_fields:
            dtfo_hier_aware_fields = [
                "won_amount_diff",
                "lost_amount_diff",
                "amt",
                "stg",
            ]

        return (
                get_nested(self.config, ["complex_fields",
                                         "hier_aware_fields"], [])
                + gbm_hier_aware_fields
                + dtfo_hier_aware_fields
        )

    @cached_property
    def revenue_fields(self):
        """
        fields that are revenue based

        Returns:
            list -- [i beg of you]
        """
        return get_nested(self.config, ["complex_fields", "revenue_fields"], [])

    @cached_property
    def active_field(self):
        return get_nested(
            self.config, ["complex_fields", "active_field"], "active_amount"
        )

    @cached_property
    def home_page_weekly_metrics_schema(self):
        conf = {"columns": ["activity", "Commit Deals", "Best Case Deals", "Most Likely Deals"],
                "schema": {"activity": {"type": "string",
                                        "label": "Activity"},
                           "Commit Deals": {"type": "cell-block",
                                            "label": "Commit"},
                           "Best Case Deals": {"type": "cell-block",
                                               "label": "Best Case"},
                           "Most Likely Deals": {"type": "cell-block",
                                                 "label": "Most Likely"}}}
        return get_nested(self.config, ['new_home_page', 'activity_metrics', 'schema'], conf)

    @cached_property
    def deals_to_hide(self):
        """
        Deals with values that need to be hidden in UI
        :return:
        dict -- {'as_of_Stage':'Dummy'}
        """
        return self.config.get("deals_to_hide")

    @cached_property
    def account_relationships_config(self):
        return self.config.get("account_relationships")

    @cached_property
    def account_dashboard_config(self):
        return self.config.get("account_dashboard")

    @cached_property
    def has_wiz_metrics(self):
        return self.config.get("has_wiz_metrics")

    @cached_property
    def owner_email_field(self):
        return self.field_map.get("owner_email", 'OwnerEmail')

    @cached_property
    def conditional_writeback(self):
        return self.config.get("conditional_writeback")

    @cached_property
    def vlookup_writeback_fields(self):
        return self.config.get('vlookup_fields', {})

    @cached_property
    def has_wiz_metrics(self):
        return self.config.get("has_wiz_metrics")

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
            return True, ["no config provided"]

        good_field_map, field_map_message = self._validate_field_map(
            config.get("field_map", {})
        )
        good_dlf, dlf_message = self._validate_dlf(config.get("dlf", {}))
        good_complex, complex_message = self._validate_complex(
            config.get("complex_fields", {})
        )
        good_totals, total_message = self._validate_totals(
            config.get("totals", {}))
        good_schema, schema_message = self._validate_schema(
            config.get("schema", {}))
        good_filters, filters_message = self._validate_filters(
            config.get("filters", {})
        )
        good_dashboard, dashboard_message = self._validate_dashboard(
            config.get("dashboard", {})
        )

        all_good = all(
            [
                good_field_map,
                good_dlf,
                good_complex,
                good_totals,
                good_schema,
                good_filters,
            ]
        )
        all_messages = [
            field_map_message,
            dlf_message,
            complex_message,
            total_message,
            schema_message,
            filters_message,
        ]
        return all_good, [msg for msg in all_messages if msg]

    def _validate_field_map(self, field_map):
        if not field_map:
            return False, "no field map provided"
        for required_field in [
            "amount",
            "stage",
            "forecast_category",
            "close_date",
            "owner_id",
            "stage_trans",
        ]:
            if required_field not in field_map:
                return False, "{} field not in map".format(required_field)

        return True, ""

    def _validate_dlf(self, dlf_config):
        if not dlf_config:
            return True, ""

        for field, field_config in dlf_config.items():
            if "mode" not in field_config:
                return False, "no mode provided for {}, config: {}".format(
                    field, field_config
                )
            # TODO: is amount seed field required?

            for dlf_filter in ["locked_filters", "default_filters"]:
                for state, filter_ids in field_config.get(dlf_filter, {}).items():
                    filt = fetch_filter(filter_ids, self, db=self.db)
                    if filt is None:
                        return (
                            False,
                            "{} filter id: {} for {} not in filters collection".format(
                                dlf_filter, state, filter_ids
                            ),
                        )

        return True, ""

    def _validate_complex(self, complex_config):
        return True, ""  # TODO: this

    def _validate_totals(self, total_config):
        # TODO: this
        return True, ""

    def _validate_schema(self, schema_config):
        deal_schema = schema_config.get("deal", [])
        fields = set()
        for field_dtls in deal_schema:
            if "field" not in field_dtls:
                return False, "no field provided for {}".format(field_dtls)
            if "label" not in field_dtls:
                return False, "no label provided for {}".format(field_dtls)
            if "fmt" not in field_dtls:
                return False, "no format provided for {}".format(field_dtls)
            if (
                    "crm_writable" in field_dtls
                    and field_dtls["field"] in self.hier_aware_fields
            ):
                return (
                    False,
                    "no writeback allowed on hierarchy split fields {}".format(
                        field_dtls
                    ),
                )
            fields.add(field_dtls["field"])
        for field in schema_config.get("deal_fields", {}).keys():
            if "__" not in field and field not in fields:
                return (
                    False,
                    "field {} in deal fields not configured in deal schema: {}".format(
                        field, deal_schema
                    ),
                )
        for optional_schema in ["card_deal_fields", "pull_in_deal_fields"]:
            for field in schema_config.get(optional_schema, {}).keys():
                if "__" not in field and field not in fields:
                    return (
                        False,
                        "field {} in {} not configured in deal schema: {}".format(
                            field, optional_schema, deal_schema
                        ),
                    )
        return True, ""

    def _validate_filters(self, filters):
        filter_ids = filters.values()
        filter_results = fetch_many_filters(
            [[filt_id] for filt_id in filter_ids], self, db=self.db
        )
        for filter_id in filter_ids:
            name, filt = filter_results[tuple([filter_id])]
            if not name:
                return False, "filter id: {} not in filters collection".format(
                    filter_id
                )
        return True, ""

    def _validate_dashboard(self, dashboard):
        # TODO: me
        return True, ""

    #
    # Default Configurations
    #
    @cached_property
    def _default_deal_changes(self):
        return {
            "leaderboards": {
                "Amount Changes": {
                    "categories": ["Increase", "Decrease"],
                    "arrow": True,
                },
                "Biggest Movers": {
                    "categories": ["Upgraded", "Downgraded"],
                    "total_name": "Forecast Impact",
                    "arrow": True,
                },
                "Close Date Changes": {"categories": ["Pulled In", "Pushed Out"]},
                "Forecast Category Changes": {
                    "categories": ["Committed", "Decommitted"]
                },
                "Pipeline Changes": {"categories": ["New", "Won", "Lost"]},
            },
            "categories": [
                [
                    "Committed",
                    [{"key": "comm"}, {"key": "actv"},
                     {'key': 'pushedout', 'negate': True}],
                    [self.amount_field, "$sum"],
                ],
                [
                    "Decommitted",
                    [{"key": "decomm"}, {"key": "actv"},
                     {'key': 'pushedout', 'negate': True}],
                    [self.amount_field, "$sum"],
                ],
                ["New", [{"key": "new_since_as_of"},
                         {'key': 'pushedout', 'negate': True}],
                 [self.amount_field, "$sum"]],
                [
                    "Won",
                    [
                        {
                            "key": ["won_amount_diff", "%(node)s"],
                            "op": "nested_in_range",
                            "val": [0, None, True],
                        },
                        {"key": self.stage_trans_field,
                         "op": "in", "val": ["99"]},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    ["won_amount_diff", "$sum"],
                ]
                if self.amount_field in self.hier_aware_fields
                else [
                    "Won",
                    [
                        {
                            "key": "won_amount_diff",
                            "op": "range",
                            "val": [0, None, True],
                        },
                        {"key": self.stage_trans_field,
                         "op": "in", "val": ["99"]},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    ["won_amount_diff", "$sum"],
                ],
                [
                    "Lost",
                    [
                        {
                            "key": ["lost_amount_diff", "%(node)s"],
                            "op": "nested_in_range",
                            "val": [0, None, True],
                        },
                        {"key": self.stage_trans_field,
                         "op": "in", "val": ["-1"]},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    ["lost_amount_diff", "$sum"],
                ]
                if self.amount_field in self.hier_aware_fields
                else [
                    "Lost",
                    [
                        {
                            "key": "lost_amount_diff",
                            "op": "range",
                            "val": [0, None, True],
                        },
                        {"key": self.stage_trans_field,
                         "op": "in", "val": ["-1"]},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    ["lost_amount_diff", "$sum"],
                ],
                [
                    "Increase",
                    [
                        {
                            "key": ["amt", "%(node)s"],
                            "op": "nested_in_range",
                            "val": [0, None, True],
                        },
                        {"key": "actv"},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    [self.amount_field, "$sum"],
                ]
                if self.amount_field in self.hier_aware_fields
                else [
                    "Increase",
                    [
                        {"key": "amt", "op": "range", "val": [0, None, True]},
                        {"key": "actv"},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    [self.amount_field, "$sum"],
                ],
                [
                    "Decrease",
                    [
                        {
                            "key": ["amt", "%(node)s"],
                            "op": "nested_in_range",
                            "val": [None, 0, True, True],
                        },
                        {"key": "actv"},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    [self.amount_field, "$sum"],
                ]
                if self.amount_field in self.hier_aware_fields
                else [
                    "Decrease",
                    [
                        {"key": "amt", "op": "range",
                         "val": [None, 0, True, True]},
                        {"key": "actv"},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    [self.amount_field, "$sum"],
                ],
                [
                    "Upgraded",
                    [
                        {
                            "key": ["fcst", "%(node)s"],
                            "op": "nested_in_range",
                            "val": [0, None, True, True],
                        },
                        {"key": "actv"},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    [self.amount_field, "$sum"],
                ],
                [
                    "Downgraded",
                    [
                        {
                            "key": ["fcst", "%(node)s"],
                            "op": "nested_in_range",
                            "val": [None, 0, True, True],
                        },
                        {"key": "actv"},
                        {'key': 'pushedout', 'negate': True}
                    ],
                    [self.amount_field, "$sum"],
                ],
                [
                    "Pulled In",
                    [{"key": "pulledin"}, {"key": "actv"}],
                    [self.amount_field, "$sum"],
                ],
                [
                    "Pushed Out",
                    [{"key": "pushedout"}, {"key": "actv"}],
                    [self.amount_field, "$sum"],
                ],
            ],
            "categories_order": [
                ["Previous Pipe", ["Pipe"]],
                ["Current Pipe", ["Pipe"]],
                ["New", ["New"]],
                ["Won", ["Won"]],
                ["Lost", ["Lost"]],
                ["Date Changes", ["Pulled In", "Pushed Out"]],
                ["Amount Changes", ["Increase", "Decrease"]],
                ["Commit Changes", ["Committed", "Decommitted"]],
                ["Aviso Forecast Changes", ["Upgraded", "Downgraded"]],
            ],
            "default_key": "Close Date Changes",
            "leaderboards_order": [
                {
                    "i": "cal",
                    "key": "Close Date Changes",
                    "label": "Close Date Changes",
                },
                {"i": "bars", "key": "Amount Changes", "label": "Amount Changes"},
                {
                    "i": "therm",
                    "key": "Forecast Category Changes",
                    "label": "Forecast Category Changes",
                },
                {"i": "flag", "key": "Pipeline Changes",
                 "label": "Pipeline Changes"},
                {"i": "brain", "key": "Biggest Movers", "label": "Biggest Movers"},
            ],
            "categories_amounts": {
                "Won": "won_amount_diff",
                "Lost": "lost_amount_diff",
            },
        }

    @cached_property
    def categories_order_labels(self):
        return get_nested(
            self.config,
            ["dashboard", "categories_order_labels"],
            self._default_categories_order_labels,
        )

    @cached_property
    def _default_categories_order_labels(self):
        return {
            "Previous Pipe": "Previous Pipe",
            "Current Pipe": "Current Pipe",
            "New": "New",
            "Won": "Won",
            "Lost": "Lost",
            "Date Changes": "Date Changes",
            "Amount Changes": "Amount Changes",
            "Commit Changes": "Commit Changes",
            "Aviso Forecast Changes": "Aviso Forecast Changes",
        }

    @cached_property
    def default_milestones(self):
        default_milestones = self.config.get('default_milestones', None)
        if default_milestones is None:
            self.config['default_milestones'] = DEFAULT_MILESTONES
        return self.config.get('default_milestones', {})

    @cached_property
    def default_milestones_stages(self):
        return self.config.get('default_milestones_stages', {})

    @cached_property
    def default_judg_type(self):
        return get_nested(self.config, ["oppmap", "default_judg_type"], "commit")

    @cached_property
    def show_deal_type(self):
        return get_nested(self.config, ["oppmap", "show_deal_type"], False)

    @cached_property
    def default_deal_type(self):
        return get_nested(self.config, ["oppmap", "default_deal_type"], "all")

    @cached_property
    def oppmap_judg_type_options(self):
        return get_nested(
            self.config,
            ["oppmap", "oppmap_judg_type_options"],
            ["commit", "dlf", "most_likely"],
        )

    @cached_property
    def oppmap_judg_type_option_labels(self):
        return get_nested(
            self.config,
            ["oppmap", "oppmap_judg_type_option_labels"],
            DEFAULT_OPPMAP_JUDGE_TYPE_OPTION_LABELS,
        )

    @cached_property
    def alt_amount_val_for_na(self):
        return get_nested(self.config, ["amount_field_checks", "alt_amount_val"], 0.0)

    @cached_property
    def amount_val_check_enabled(self):
        return get_nested(
            self.config, ["amount_field_checks",
                          "amount_val_chk_for_fld"], False
        )

    @cached_property
    def versioned_hierarchy(self):
        from config.hier_config import HierConfig

        hier_config = HierConfig()

        return hier_config.versioned_hierarchy

    # Return the dlf_fcst default collection schema along with the additional configured fields(if configured),
    # else return None(This will not create the data for the collection)
    @cached_property
    def dlf_fcst_coll_schema(self):
        dlf_fcst_schema = []
        dlf_fcst_schema.extend(DEFAULT_DLF_FCST_COLL_SCHEMA)
        dlf_fcst_schema_additional_fields = get_nested(
            self.config, ["dlf_fcst_coll_additional_fields"], None
        )
        # Add the amount field dynamically
        dlf_fcst_schema.append(self.amount_field)

        if dlf_fcst_schema_additional_fields:
            dlf_fcst_schema.extend(dlf_fcst_schema_additional_fields)

        return dlf_fcst_schema

    def amount_field_by_pivot(self, node):
        if node:
            pivot = node.split("#")[0]
            if self.pivot_amount_fields is not None:
                return self.pivot_amount_fields.get(pivot, self.amount_field)
        return self.amount_field

    @cached_property
    def custom_fc_ranks_ext(self):
        dict_ = self.custom_fc_ranks_default
        if "Commit" in dict_:
            dict_["commit"] = dict_["Commit"]
        if "Most Likely" in dict_:
            dict_["most_likely"] = dict_["Most Likely"]

        return dict_

    @cached_property
    def display_insights_card(self):
        return get_nested(
            self.config,
            ["win_score_insights_card", "display_insights_card"],
            DEFAULT_DISPLAY_INSIGHTS_CARD,
        )

    @cached_property
    def winscore_graph_options(self):
        return {
            'change_in_stagetrans': 'Change in StageTrans',
        }

    @cached_property
    def count_recs_to_display(self):
        return get_nested(
            self.config, ["win_score_insights_card",
                          "count_recs_to_display"], 5
        )

    @cached_property
    def oppds_fieldmap(self):
        return self.config.get("oppds_fieldmap", {})

    @cached_property
    def demo_report_config_enabled(self):
        return self.config.get("demo_report_config_enabled", False)

    @cached_property
    def uip_user_fields(self):
        return get_nested(
            self.config,
            ["uip_fields", "account"],
            {"fields": ["Email"], "ref_field": "Email"},
        )

    @cached_property
    def uip_account_fields(self):
        return get_nested(
            self.config,
            ["uip_fields", "account"],
            {
                "fields": ["LeanData__LD_EmailDomains__c"],
                "reference": "LeanData__LD_EmailDomains__c",
            },
        )

    @cached_property
    def custom_dtfo_fields(self):
        return get_nested(self.config, ["dashboard", "custom_dtfo_fields_to_add"], [])

    @cached_property
    def filter_totals_config(self):
        return self.config.get('filter_totals_config', {
            'enabled': True,
            'daily': True,
            'chipotle': True
        })

    @cached_property
    def deal_changes_totals_config(self):
        return self.config.get('deal_changes_totals_config', {})

    @cached_property
    def run_insights_batch_size(self):
        return self.config.get('run_insights_batch_size', {})

    @cached_property
    def run_gbm_crr_batch_size(self):
        return self.config.get('run_gbm_crr_batch_size', {})

    @cached_property
    def future_qtrs_prefetch_count(self):
        filter_totals_config = self.filter_totals_config
        future_qtrs_prefetch_count = 0
        if filter_totals_config:
            future_qtrs_prefetch_count = filter_totals_config.get("future_qtrs_process_count", 0)
        return future_qtrs_prefetch_count

    @cached_property
    def past_qtrs_prefetch_count(self):
        filter_totals_config = self.filter_totals_config
        past_qtrs_prefetch_count = 0
        if filter_totals_config:
            past_qtrs_prefetch_count = filter_totals_config.get("past_qtrs_process_count", 0)
        return past_qtrs_prefetch_count

    @cached_property
    def no_update_on_writeback_fields(self):
        """
        fields for which we don't need to run filter_totals or snapshot task on update from ui.

        Returns:
            list
        """

        return self.config.get('no_update_on_writeback_fields', ["Manager Comments",
                                                                 "__comments__",
                                                                 "__comment__",
                                                                 "comments",
                                                                 "NextStep",
                                                                 "NextSteps",
                                                                 "latest_NextStepsquestions",
                                                                 "latest_Problems",
                                                                 "ManagerNotes",
                                                                 "ProServCommentsNotes",
                                                                 "latest_CoachingNotes",
                                                                 "LastStep",
                                                                 "LegalNotes",
                                                                 "Manager_Comments",
                                                                 "Mgr Notes",
                                                                 "MgrNextSteps",
                                                                 "TAPNextSteps",
                                                                 "CustomNextStep",
                                                                 "Next_Steps",
                                                                 "Next Step",
                                                                 "ProServCommentsNotes",
                                                                 "ForecastNotes",
                                                                 "ManagerForecastNotes",
                                                                 "SalesDirectorNotes",
                                                                 "SlipNotes",
                                                                 "SEDirectorNotes",
                                                                 "Notes",
                                                                 "CoachingNotes",
                                                                 "CSMNotes",
                                                                 "latest_CoachingNotes"])

    @cached_property
    def weekly_fm(self):
        return self.config.get("weekly_fm", False)

    @cached_property
    def yearly_sfdc_view(self):
        return self.config.get("yearly_sfdc_view", True)

    @cached_property
    def insight_actions_mapping(self):
        """
         Serves config to DeepLink API
         insight_actions_mapping = {'scenario': {'component': 'filter_id'},
                                    'risk': {'opp_map_link': '/*/oppmap/amount/commit/upside/standard/all'}
                                   }
        """
        return self.config.get('insight_actions_mapping', {})

    @cached_property
    def enhanced_waterfall_chart(self):
        return self.config.get("enhanced_waterfall_chart", False)

    @cached_property
    def enabled_file_parsed_load_run(self):
        return self.config.get("enabled_file_parsed_load_run", False)


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
        '''
            returns string with value 'W'.
            Could be set to 'Q' or 'M'.
            This config is the beginning period from where the weeks needs to be editable
        '''
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
                return False, e.message
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

