import logging

from aviso.settings import sec_context

from utils.common import cached_property
from .base_config import BaseConfig


from utils.misc_utils import get_nested


logger = logging.getLogger('gnana.%s' % __name__)


class HierConfig(BaseConfig):

    config_name = 'hierarchy'

    @cached_property
    def segment_rules_updated(self):
        return self.config.get('segment_rules_updated', False)

    @cached_property
    def dummy_tenant(self):
        """
        tenant is using dummy data, is not hooked up to any gbm/etl
        """
        return self.config.get('dummy_tenant', False)

    @cached_property
    def bootstrapped(self):
        """
        tenant has been bootstrapped and has initial data loaded into app
        """
        return self.config.get('bootstrapped', True)
    
    @cached_property
    def bootstrapped_drilldown(self):
        """
        tenant has been bootstrapped and has drilldown data loaded into app
        """
        return self.config.get('bootstrapped_drilldown', True)

    @cached_property
    def versioned_hierarchy(self):
        """
        hierarchy is versioned
        periods will display data using hierarchy as of that period
        unversioned hierarchies have one version for all of time
        changes to unversioned hierarchies will impact data in previous periods

        Returns:
            bool
        """
        return self.config.get('versioned', False)

    @cached_property
    def sort_system_nodes(self):
        """
        push system nodes to the end of the hierarchy
        Returns:
            bool
        """
        return self.config.get('sort_system_nodes', False)

    @cached_property
    def logger_draw_tree(self):
        """
        Enable or disable the draw tree function.
        Default: Enabled

        Returns:
            bool
        """
        return self.config.get('logger_draw_tree',True)

    #
    # Hierarchy Configuration
    #
    @cached_property
    def hierarchy_builder(self):
        """
        type of hierarchy builder to use to make hierarchy from source data for syncing

        Returns:
            str -- type of hier builder
        """
        # TODO: assert etl connection exists, assert hier builder exists
        return self.config.get('hierarchy_builder')

    @cached_property
    def forced_roots(self):
        """
        hierarchy node to set as root node

        Returns:
            list -- [nodes]
        """
        return self.config.get('forced_roots')

    #
    # Drilldown Configuration
    #
    @cached_property
    def drilldown_builders(self):
        """
        mapping from drilldown to type of drilldown builder to use to make drilldown

        Returns:
            dict -- {drilldown: drilldown builder}
        """
        return {drilldown: dd_dtls['drilldown_builder']
                for drilldown, dd_dtls in self.config.get('drilldowns', {}).iteritems()}

    @cached_property
    def drilldown_labels(self):
        """
        mapping from drilldown to label

        Returns:
            dict -- {drilldown: label}
        """
        return {drilldown: dd_dtls.get('label', drilldown)
                for drilldown, dd_dtls in self.config.get('drilldowns', {}).iteritems()}

    @cached_property
    def drilldown_deal_fields(self):
        """
        mapping from drilldown to deal fields used in drilldown

        Returns:
            dict -- {drilldown: [deal fields]}
        """
        return {drilldown: dd_dtls.get('deal_fields', [])
                for drilldown, dd_dtls in self.config.get('drilldowns', {}).iteritems()}

    @cached_property
    def deal_only_drilldowns(self):
        """
        drilldowns that are only based on deal attributes, no hierarchy included

        Returns:
            set -- {drilldowns}
        """
        return {drilldown for drilldown, dd_dtls in self.config.get('drilldowns', {}).iteritems()
                if dd_dtls['drilldown_builder'] == 'deal_only'}

    @cached_property
    def deal_partial_hier_drilldowns(self):
        """
        drilldowns that are based on deal attributes, and a select number of levels from hierarchy

        Returns:
            dict -- {drilldowns: number of hierarchy levels to keep}
        """
        return {drilldown: dd_dtls['num_levels'] for drilldown, dd_dtls in self.config.get('drilldowns', {}).iteritems()
                if dd_dtls['drilldown_builder'] == 'deal_partial_hier'}

    @cached_property
    def hier_display_order(self):
        """
        display order for hierarchies in ui

        Returns:
           list -- [root node ids]
        """
        return self.config.get('hierarchy_display_order', [])

    @cached_property
    def pivot_and_root_node_mapping(self):
        """
        pivot and root node mapping

        Returns:
           dict -- {pivot1: root_node_of_pivot1,
                    pivot2: root_node_of_pivot2}
        """
        return self.config.get('pivot_map', {})

    @cached_property
    def hierarchy_editable(self):
        if self.hierarchy_builder == 'collab_fcst':
            return False
        if all(dd_dtls['drilldown_builder'] == 'deal_only' for dd_dtls in self.config.get('drilldowns', {}).values()):
            return False
        return True

    #
    # Validation
    #
    def validate(self, config):
        dummy_tenant = config.get('dummy_tenant', False)
        hier_builder = config.get('hierarchy_builder')
        good_hier_builder, hier_builder_msg = self._validate_hierarchy_builder(hier_builder, dummy_tenant)
        good_drilldowns, drilldowns_msg = self._validate_drilldowns(config.get('drilldowns', {}), dummy_tenant)
        return good_hier_builder and good_drilldowns, [hier_builder_msg, drilldowns_msg]

    def _validate_hierarchy_builder(self, hierarchy_builder, dummy_tenant):
        from ..tasks import HIERARCHY_BUILDERS  # TODO: deal with this circular import ...
        if not hierarchy_builder and dummy_tenant:
            return True, ''
        if hierarchy_builder not in HIERARCHY_BUILDERS:
            return False, 'hierarchy builder: {} not in available hierarchy builders: {}'.format(hierarchy_builder, sorted(HIERARCHY_BUILDERS.keys()))
        if hierarchy_builder in ['collab_fcst'] and not sec_context.get_microservice_config('etl_data_service'):
            # TODO: if we ever build other syncers, check them here
            return False, 'not allowed to have syncing hierarchy without an etl service connection'
        return True, ''

    def _validate_drilldowns(self, drilldowns, dummy_tenant):
        from ..tasks import DRILLDOWN_BUILDERS  # TODO: deal with this circular import ...
        gbm_svc = sec_context.get_microservice_config('gbm_service')
        if gbm_svc:
            #TODO: Understanding how domainmodel is working
            from domainmodel.datameta import Dataset
            ds = Dataset.getByNameAndStage('OppDS', full_config=True).get_as_map()
            gbm_dd_config = get_nested(ds, ['models', 'common', 'config', 'viewgen_config'], {})
            uipfields = set(get_nested(ds, ['params', 'general', 'uipfield'], []))

        for dd, dd_dtls in drilldowns.iteritems():
            dd_builder = dd_dtls.get('drilldown_builder')
            if dd_builder not in DRILLDOWN_BUILDERS:
                return False, 'drilldown builder: {} not in available drilldown builders: {}'.format(dd_builder, sorted(DRILLDOWN_BUILDERS.keys()))
            if dd_builder in ['deal_top', 'deal_bottom', 'deal_only', 'deal_partial_hier']:
                deal_fields = dd_dtls.get('deal_fields')
                if not deal_fields:
                    return False, 'no deal fields for deal based drilldown: {}, {}'.format(dd, dd_dtls)
                if not dummy_tenant and not gbm_svc:
                    return False, 'not allowed to have deal based hierarchy without a gbm service'
                if dummy_tenant:
                    continue
                gbm_deal_fields = _get_deal_fields(gbm_dd_config, dd)
                if deal_fields != gbm_deal_fields:
                    return False, 'deal fields in gbm and hier svc config must match. gbm: {}, hier: {}'.format(gbm_deal_fields, deal_fields)
                not_in_uip = set(deal_fields) - uipfields
                if not_in_uip:
                    return False, 'deal fields must all be in uipfields, missing from uip: {}'.format(list(not_in_uip))
            if dd_builder == 'deal_partial_hier':
                if 'num_levels' not in dd_dtls:
                    return False, 'must configure number of hierarchy levels to include for {}'.format(dd)
        return True, ''

def _get_deal_fields(gbm_dd_cfg, dd):
    gbm_node_key = get_nested(gbm_dd_cfg, ['drilldown_config', dd, 'node'], '')
    return get_nested(gbm_dd_cfg, ['node_config', gbm_node_key, 'fields'], [])
