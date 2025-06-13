import logging
from itertools import groupby
from operator import itemgetter

from config.deal_config import DealConfig
from config.periods_config import PeriodsConfig
from infra.read import (fetch_ancestors, get_available_quarters_and_months,
                        get_period_boundaries, get_period_boundaries_monthly,
                        get_period_boundaries_weekly)
from utils.common import cached_property
from utils.misc_utils import get_nested, merge_nested_dicts, try_values

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

    def adorn_close_period(self, close_date):
        """
        get period deal closes in

        Arguments:
            close_date {float} -- close date in xldate format

        Returns:
            str -- period mnemonic
        """
        try:
            return next(mnem for (beg, end, mnem) in self.period_boundaries if beg <= close_date <= end)
        except:
            return 'N/A'

    def adorn_weekly_period(self, close_date):
        """
        get period deal closes in

        Arguments:
            close_date {float} -- close date in xldate format

        Returns:
            str -- period mnemonic
        """
        try:
            return next(mnem for (beg, end, mnem) in self.period_boundaries_weekly if beg <= close_date <= end)
        except:
            return 'N/A'

    def adorn_monthly_period(self, close_date):
        """
        get period deal closes in

        Arguments:
            close_date {float} -- close date in xldate format

        Returns:
            str -- period mnemonic
        """
        try:
            return next(mnem for (beg, end, mnem) in self.period_boundaries_monthly if beg <= close_date <= end)
        except:
            return 'N/A'

    def fetch_month_enumeration(self, close_date):
        monthly_period = self.adorn_monthly_period(close_date)
        if 'M' in monthly_period:
            num_str = monthly_period.split('M')[1]  # Get the part after 'M'
            number = int(num_str)
            result = number % 3
            if result == 0:
                result = 3
            return result
        else:
            return 'N/A'

    def split_out_deals(self, deal):
        """
        resplit deals :(
        elasticsearch cant handle split deals
        so were gonna blow out all the splits into individual records
        truly the dumbest fucking thing ive ever done ... today

        Arguments:
            deal {dict} -- deal object

        Returns:
            list -- [deal dicts]
        """
        if self.config.hier_aware_fields:
            deals = []
            for node_group in self.group_nodes(deal, deal.pop('__segs', [])):
                deals.append(
                    merge_nested_dicts({k: v if k not in self.config.hier_aware_fields else v.get(node_group[0])
                                        for k, v in deal.iteritems()},
                                       {'__segs': node_group}))
            return deals
        return [deal]

    def group_nodes(self, deal, nodes):
        """
        group nodes with matching split values

        Arguments:
            deal {dict} -- deal object
            nodes {list} -- list of nodes

        Returns:
            list -- list of lists of grouped nodes
        """
        nvals = [{'vals': {field: get_nested(deal, [field, node]) for field in self.config.hier_aware_fields},
                  'node': node} for node in nodes]
        return [[x['node'] for x in v] for _, v in groupby(sorted(nvals), key=itemgetter('vals'))]

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

    def get_quarter_of_month(self, month):
        if "Q" in month:
            return month
        else:
            for quarter, months in self.quarters_and_months.iteritems():
                if month in months:
                    return quarter
        return self.period

    @cached_property
    def _hier_ancestors(self):
        return {}

    @cached_property
    def _dd_ancestors(self):
        return {}

    @cached_property
    def period_boundaries(self):
        return get_period_boundaries(self.period)

    @cached_property
    def period_boundaries_weekly(self):
        return get_period_boundaries_weekly(self.period)

    @cached_property
    def period_boundaries_monthly(self):
        return get_period_boundaries_monthly(self.period)

    @cached_property
    def config(self):
        return DealConfig()

    @cached_property
    def period(self):
        raise NotImplementedError
