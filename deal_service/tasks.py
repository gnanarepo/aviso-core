import logging

from config import DealConfig
from config import PeriodsConfig
from infra.read import fetch_ancestors, get_available_quarters_and_months
from utils.common import cached_property
from utils.misc_utils import try_values

from ..tasks import BaseTask

logger = logging.getLogger('gnana.%s' % __name__)


class DealsTask(BaseTask):

    def adorn_hierarchy(self, deal, as_of):
        """
        get deals node hierarchy and drilldown hierarchy

        Arguments:
            deal {dict} -- deal object
            as_of {int} -- epoch timestamp to get hierarchy as of

        Returns:
            tuple -- ([hierarchy ancestors], [drilldown ancestors])
        """
        owner_ids = {(owner_id, drilldown)
                     for owner_field, drilldown in self.config.owner_id_fields if drilldown and owner_field
                     for owner_id in try_values(deal, owner_field) if owner_id}

        hierarchy_list = [hier_node for owner_id, _ in owner_ids
                          for hier_node in self.get_hierarchy_ancestors(as_of, owner_id)]

        drilldown_list = []
        # NOTE: should only be reading drilldown in testing cases
        deal_segs = deal.pop('__segs', deal.get('drilldown_list', []))
        for owner_id, drilldown in owner_ids:
            for owner_node in [x for x in deal_segs if (owner_id in x or 'not_in_hier' in x) and drilldown in x]:
                drilldown_list.extend(self.get_drilldown_ancestors(as_of, owner_node))

        try:
            # this log is added to check discrepencies at load task with gbm results
            # we are populationg opp_id key in deal dictionary in load task before calling adorn_hierarchy
            if len(drilldown_list) == 0:
                logger.info("deal_segs {} owner_ids {} for opp_id {}".format(deal_segs, owner_ids, deal['opp_id']))
        except:
            pass

        return list(set(hierarchy_list)), drilldown_list

    def get_hierarchy_ancestors(self, as_of, owner_id):
        try:
            return self._hier_ancestors[as_of].get(owner_id, [owner_id])
        except KeyError:
            self._hier_ancestors[as_of] = {node['node']: node.get('ancestors', []) + [node['node']]
                                           for node in fetch_ancestors(as_of, drilldown=False, include_hidden=True) if
                                           'node' in node.keys()}
            return self._hier_ancestors[as_of].get(owner_id, [owner_id])

    def get_drilldown_ancestors(self, as_of, owner_node=None):
        try:
            if not owner_node:
                return self._dd_ancestors[as_of]
            return self._dd_ancestors[as_of].get(owner_node, [owner_node])
        except KeyError:
            self._dd_ancestors[as_of] = {node['node']: node.get('ancestors', []) + [node['node']]
                                         for node in fetch_ancestors(as_of, drilldown=True, include_hidden=True) if
                                         'node' in node.keys()}
            if not owner_node:
                return self._dd_ancestors[as_of]
            return self._dd_ancestors[as_of].get(owner_node, [owner_node])

    @cached_property
    def period_config(self):
        return PeriodsConfig()

    @cached_property
    def quarters_and_months(self):
        return get_available_quarters_and_months(self.period_config)

    @cached_property
    def _hier_ancestors(self):
        return {}

    @cached_property
    def _dd_ancestors(self):
        return {}

    @cached_property
    def config(self):
        return DealConfig()

    @cached_property
    def period(self):
        raise NotImplementedError
