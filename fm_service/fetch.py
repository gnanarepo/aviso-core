import itertools
import logging

from aviso.framework.views import GnanaView
from aviso.settings import sec_context

from config.fm_config import DealConfig
from fm_service import FMView, node_can_access_field, period_is_valid
from infra.read import (fetch_boundry, fetch_eligible_nodes_for_segment,
                        fetch_users_nodes_and_root_nodes, get_current_period,
                        get_period_as_of, get_period_begin_end,
                        get_period_range, get_time_context)
from utils.date_utils import epoch, get_eod, get_nextq_mnem_safe
from utils.misc_utils import index_of, try_int
from utils.mongo_reader import get_snapshot_window_feature_data

logger = logging.getLogger('gnana.%s' % __name__)


class FMFetch(FMView):
    http_method_names = ['get']
    restrict_to_roles = GnanaView.Role.All_Roles

    validators = [period_is_valid, node_can_access_field]

    def get(self, request, *args, **kwargs):
        try:
            req_param = request.GET
        except:
            req_param = request

        return self._get_metadata(req_param, *args, **kwargs)

    def _get_metadata(self, request, *args, **kwargs):
        self.api_request = request.get('api_request', None)
        self.period = request.get('period', get_current_period(self.config.periods_config))
        self.nodes_having_changes = request.get('nodes_having_changes', [])
        self.node = request.get('node')
        self.segment_id = request.get('segment_id')

        # -- used in inherited classes, not to remove --
        deal_config = DealConfig()
        self.is_pivot_special = False
        if sec_context.name in deal_config.not_deals_tenant.get('tenant', []) and self.node:
            if self.node.split('#')[0] in deal_config.not_deals_tenant.get('special_pivot', []):
                self.is_pivot_special = True
        # -x- used in inherited classes, not to remove -x-
        self.original_period = self.period

        # Commenting as part of CS-19462, please revert if needed
        # if self.is_pivot_special and 'Q' in self.period:
        #     period_config = PeriodsConfig()
        #     available_periods = get_available_quarters_and_months(period_config)
        #     quarter_months = available_periods.get(self.period) or []
        #     if quarter_months:
        #         last_month = max(quarter_months)
        #         self.period = last_month

        self.time_context = get_time_context(self.period, config=None, quarter_editable=self.config.quarter_editable)
        self.original_time_context = get_time_context(self.original_period, config=None, quarter_editable=self.config.quarter_editable)
        self.next_period = get_nextq_mnem_safe(
            self.time_context.deal_period if not isinstance(self.time_context.deal_period,
                                                            list) else self.time_context.fm_period)
        self.next_time_context = get_time_context(self.next_period, quarter_editable=self.config.quarter_editable)

        self.status_node = request.get('status_node')
        # fetch takes one field, fetch many/fetch history can take multiple
        self.field = request.get('field')

        # wherever we use getlist we need to check api_request as we are using this method for task Class also
        self.fields = request.get('fields') if self.api_request else request.getlist('field')
        self.room_id = request.get('roomId')
        # fetch takes one segment, fetch many/fetch history can take multiple
        self.segment = request.get('segment', self.config.primary_segment)
        segments = self.config.special_pivot_segments if self.is_pivot_special else self.config.segments
        self.segments = request.get('segment', segments) if self.api_request else request.getlist('segment', segments)
        # fetch history takes multiple timestamps, others take one
        self.timestamp = try_int(request.get('timestamp'), None)
        if self.timestamp:
            self.timestamp = get_eod(epoch(self.timestamp)).as_epoch()

        timestamps = get_period_range(self.period, range_type='days')
        self.timestamps = request.get('timestamp', timestamps) if self.api_request else request.getlist('timestamp', timestamps)
        self.as_of = try_int(self.timestamp) if self.timestamp else get_period_as_of(self.period)
        if not self.node:
            user = sec_context.get_effective_user()
            user_nodes, root_nodes = fetch_users_nodes_and_root_nodes(user, self.as_of, drilldown=True, period=self.period)
            self.node = user_nodes[0]['node']

        # HACK: replace start of day timestamp with as_of to get values entered mid day
        as_of_idx = index_of(self.as_of, self.timestamps)
        if as_of_idx < len(self.timestamps) - 1:
            self.timestamps[as_of_idx + 1] = self.as_of

        self.week_timestamps = get_period_range(self.period, range_type='weeks')
        self.eligible_nodes_for_segs = {}

        self.is_versioned = sec_context.details.get_config(category='micro_app',
                                          config_name='hierarchy').get('versioned',False)
        self.boundary_dates = None
        if self.is_versioned:
            if self.period:
                as_of = get_period_as_of(self.period)
                from_date, _ = fetch_boundry(as_of, drilldown=True)
                _, to_date = get_period_begin_end(self.period)
            else:
                from_date = to_date = self.as_of
            self.boundary_dates = (from_date, to_date)

        segments = self.config.special_pivot_segments if self.is_pivot_special else self.config.segments
        for segment in segments:
            if segment == self.config.primary_segment:
                continue
            self.eligible_nodes_for_segs[segment] = [dd['node'] for dd in
                                                     fetch_eligible_nodes_for_segment(self.time_context.now_timestamp,
                                                                                      segment, period=self.period, boundary_dates=self.boundary_dates)]
        self.fm_recs = {}


    def get_field_value(self, period, node, field, segment, timestamp, key="val",**kwargs):
        week_start = kwargs.get("start")
        week_end = kwargs.get("end")
        from tasks.hierarchy.hierarchy_utils import hier_switch
        if self.config.fields[field].get("alt_hier"):
            alt_hier_flds = self.config.fields[field]['source']
            hiers = itertools.chain(*[alt_hier['hiers'] for alt_hier in [self.config.alt_hiers]])
            alt_nodes = list({hier_switch(hnode, *hier) for hier in hiers for hnode in [node]})
            source1 = self.fm_recs[(period, node, alt_hier_flds[0], segment, timestamp)][key]
            alt_nodes.remove(node)
            try:
                source2 = self.fm_recs[(period, alt_nodes[0], alt_hier_flds[1], segment, timestamp)][key]
                source = [source1, source2]
                val = eval(self.config.fields[field]['func'])
            except:
                val = 0
        else:
            if week_start and week_end:
                val = self._get_value_for_timestamp_range(period, node, field, segment,(week_start, week_end),key=key)
            else:
                val = self.fm_recs.get((period, node, field, segment, timestamp), {}).get(key, None)
        return val

    def _get_value_for_timestamp_range(self, period, node, field, segment, timestamp_range, key):
        """
        Retrieve 'val' from self.fm_recs based on a range of timestamps.

        Args:
            period (str): The period (e.g., '2025Q4').
            node (str): The node identifier.
            field (str): The field name.
            segment (str): The segment name.
            timestamp_range (tuple): A tuple specifying the start and end of the timestamp range (start, end).
            key (str): The key to retrieve from the dictionary.

        Returns:
            The value associated with the key within the timestamp range, or None if not found.
        """
        start, end = timestamp_range
        for rec_key, rec_value in self.fm_recs.items():
            # Unpack the key tuple
            try:
                rec_period, rec_node, rec_field, rec_segment, rec_timestamp = rec_key
            except:
                continue
            # Check if the key matches the desired attributes and the timestamp falls within the range
            if (
                rec_period == period and
                rec_node == node and
                rec_field == field and
                rec_segment == segment and
                start <= rec_timestamp <= end
            ):
                return rec_value.get(key, None)
        return None

    def _get_snapshot_feature_data(self):
        return get_snapshot_window_feature_data(self.node, self.config)

    def node_email_mapping(self):
        from domainmodel.app import User

        db_to_use = User.get_db()
        users = db_to_use.findDocuments(User.getCollectionName(), {})
        user_mapping = {}
        for item in users:
            if not isinstance(item, dict) or 'object' not in item or 'email' not in item['object']:
                continue  # Skip invalid user entries
            user_list = item['object']['roles']['user']
            email = item['object']['email']
            if '@' not in email:
                continue  # Skip if email is malformed
            username, domain = email.split('@')

            if domain == "aviso.com" or domain == "administrative.domain":
                continue
            for user in user_list:
                if 'results' not in user:
                    continue
                node = user[1]  # Ensure this is correct, adjust if needed
                if not node:
                    continue  # Skip if node is missing or empty
                # Initialize self.user_mapping[node] as a list if not already done
                if node not in user_mapping:
                    user_mapping[node] = []

                if email not in user_mapping[node]:
                    user_mapping[node].append(email)
        return user_mapping
