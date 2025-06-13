import logging

from aviso.framework.views import GnanaView
from aviso.settings import sec_context

from config import DealConfig, FMConfig
from infra.read import (fetch_boundry, fetch_eligible_nodes_for_segment,
                        fetch_users_nodes_and_root_nodes, get_current_period,
                        get_period_as_of, get_period_begin_end,
                        get_period_range, get_time_context)
from utils.common import cached_property
from utils.date_utils import epoch, get_eod, get_nextq_mnem_safe
from utils.misc_utils import index_of, try_int

logger = logging.getLogger('gnana.%s' % __name__)


class FMFetch:

    @cached_property
    def config(self):
        return FMConfig()

    @cached_property
    def deal_config(self):
        return DealConfig()

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
