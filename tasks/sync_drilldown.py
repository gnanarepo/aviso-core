import logging

from aviso.settings import sec_context
from pymongo import UpdateOne

from config import HierConfig
from config import FMConfig
from config import DRILLDOWN_BUILDERS
from config import PeriodsConfig
from infra.constants import DRILLDOWN_COLL
from infra.read import (fetch_descendant_ids, fetch_hidden_nodes,
                        fetch_node_to_parent_mapping_and_labels,
                        fetch_top_level_nodes, get_as_of_dates,
                        get_current_period, get_now, get_period_as_of,
                        get_period_begin_end)
from infra.rules import passes_configured_hierarchy_rules
from tasks import BaseTask
from tasks.hierarchy import draw_tree
from utils.date_utils import epoch, monthly_periods, prev_period
from utils.misc_utils import is_lead_service, try_index
from utils.mongo_writer import (bulk_write, create_many_nodes, create_node,
                                hide_node, label_node, move_node, unhide_node,
                                update_nodes_with_valid_to_timestamp)

logger = logging.getLogger('gnana.%s' % __name__)

DRILLDOWN_PIVOT = 'Leads'

class SyncDrilldown(BaseTask):
    """
    Sync Drilldown
    sync drilldown hierarchy from source system, and apply changes to app drilldowns

    Arguments:
        period {str} -- period mnemonic                                         '2020Q2'

    Optional Arguments:
        timestamp {int} -- epoch timestamp to save records as_of                1556074024910
                            if None, uses latest app time (default: {None})
    """

    def __init__(self, *args, **kwargs):
        self.config = HierConfig()
        self.period = kwargs.get('period', get_current_period())
        self.service = kwargs.get('service', None)
        if self.config.bootstrapped_drilldown or is_lead_service(self.service):
            self.syncer = _SyncDrilldown(*args, **kwargs)
        else:
            self.syncer = _BootstrapDrilldown(*args, **kwargs)

    def process(self):
        self.syncer.process()
        if self.config.segment_rules_updated and not is_lead_service(self.service):
            self.update_segment_details()

    def update_segment_details(self):
        fm_config = FMConfig()
        segments = fm_config.segments
        drilldown_collection = sec_context.tenant_db[DRILLDOWN_COLL]
        nodes = list(fetch_top_level_nodes(self.syncer.timestamp, levels=20, period=self.syncer.period))
        records = []
        for node in nodes:
            normal_segs = []
            for segment in segments:
                rules = fm_config.segments[segment].get('rules', [])
                if passes_configured_hierarchy_rules(self.syncer.timestamp, node, rules, exclude_display_specific=True):
                    normal_segs.append(segment)

            if self.config.versioned_hierarchy:
                if PeriodsConfig().monthly_fm:  # updating segment details for monthly segments tenants
                    for i in list(monthly_periods(self.period)):
                        start = epoch(i.begin).as_epoch()
                        records.append(UpdateOne({'node': node,
                                                  'from': start},
                                                 {'$set': {'normal_segs': normal_segs}}))
                else:
                    records.append(UpdateOne({'node': node,
                                              'from': self.syncer.periods_for_mnemonic[self.syncer.period][0]},
                                             {'$set': {'normal_segs': normal_segs}}))
            else:
                records.append(UpdateOne({'node': node},
                                         {'$set': {'normal_segs': normal_segs}}))

        try:
            bulk_write(records, drilldown_collection)
            self.config.update_config({'segment_rules_updated': False})
        except Exception as e:
            logger.error("Segment Deatails Update failed with %s", e)

    def persist(self):
        self.syncer.persist()
        if FMConfig().has_segments:
            self.update_segment_details()


class _SyncDrilldown(BaseTask):
    """
    Sync Drilldown
    sync drilldown hierarchy from source system, and apply changes to app drilldowns

    Arguments:
        period {str} -- period mnemonic                                         '2020Q2'

    Optional Arguments:
        timestamp {int} -- epoch timestamp to save records as_of                1556074024910
                            if None, uses latest app time (default: {None})
    """

    def __init__(self, period, timestamp=None, deal_count=None, shuffle=False, seed=None, **kwargs):
        self.period = period
        self.timestamp = get_period_as_of(self.period, timestamp)
        self.prev_timestamp = None
        self.config = HierConfig(debug=True)
        self.as_to = None
        self.now_timestamp = get_now().as_epoch()
        # Any nodes that has a "from" timestamp before this will be updated with a specific "to" timestamp
        self.timestamp_for_updating_to = None

        if self.config.versioned_hierarchy:
            self.periods_for_mnemonic = {}
            if 'Q' in self.period and not sec_context.details.get_flag('prd_svc', 'pure_quarter', False):
                for i in list(monthly_periods(self.period)):
                    start = epoch(i.begin).as_epoch()
                    end = epoch(i.end).as_epoch()
                    end = None if start <= self.now_timestamp <= end else end
                    self.periods_for_mnemonic[i[0]] = (start, end)
                    if not end:
                        self.timestamp_for_updating_to = start
                        break
            else:
                start, end = get_period_begin_end(self.period)
                end = None if start <= self.now_timestamp <= end else end
                self.periods_for_mnemonic[self.period] = (start, end)
                if not end:
                    self.timestamp_for_updating_to = start

        self.deleted_nodes = set()
        self.created_nodes = {}
        self.moved_nodes = {}
        self.relabled_nodes = {}
        self.hidden_reps = set()
        self.hidden_nodes = set()

        self.deleted_nodes_versioned = {}
        self.created_nodes_versioned = {}
        self.moved_nodes_versioned = {}
        self.relabeled_nodes_versioned = {}
        self.hidden_reps_versioned = {}
        self.hidden_nodes_versioned = {}
        self.resurrected_nodes_versioned = {}
        self.curr_node_details_versioned = {}
        self.new_node_details_versioned = {}

        # only used for dummy tenant data generation
        self.deal_count = deal_count
        self.shuffle = shuffle
        self.seed = seed

        self.service = kwargs.get('service', None)

    def process(self):

        if self.config.versioned_hierarchy:
            self.process_versioned()
            return

        logger.info('syncing drilldown for for: %s', self.period)

        self.curr_node_to_parent, self.curr_labels = fetch_node_to_parent_mapping_and_labels(self.timestamp,
                                                                                             include_hidden=False,
                                                                                             drilldown=True,
                                                                                             period=self.period,
                                                                                             service=self.service)
        hidden_by_sync, hidden_by_user, hidden_by_rule = set(), set(), set()
        for hidden_node in fetch_hidden_nodes(self.timestamp, drilldown=True, period=self.period, service=self.service):
            if hidden_node['how'] == 'dd_sync_hide':
                hidden_by_sync.add(hidden_node['node'])
            elif hidden_node['how'] == 'dd_sync_hide_rep':
                hidden_by_rule.add(hidden_node['node'])
            else:
                hidden_by_user.add(hidden_node['node'])
        already_hidden_nodes = hidden_by_sync | hidden_by_rule | hidden_by_user

        self.new_node_to_parent, self.new_labels = self.build_new_drilldowns()

        if self.config.debug and self.config.logger_draw_tree:
            logger.info(draw_tree(self.curr_node_to_parent, self.curr_labels))
            logger.info(draw_tree(self.new_node_to_parent, self.new_labels))

        curr_nodes = set(self.curr_node_to_parent)
        new_nodes = set(self.new_node_to_parent)
        hide_nodes = set()

        self.deleted_nodes = curr_nodes - new_nodes

        if self.config.deal_only_drilldowns:
            # for tenants that dont want hierarchy nodes to be visible in the app
            # but for our internal purposes, we still want them to exist in the drilldown hierarchy
            dd_nodes = {node: parent for node, parent in self.new_node_to_parent.iteritems()
                        if node.split('#')[0] in self.config.deal_only_drilldowns}
            hide_nodes |= (set(dd_nodes.keys()) - set(dd_nodes.values()))

        for partial_drilldown, num_hier_levels in self.config.deal_partial_hier_drilldowns.iteritems():
            # for tenants that want some limited number of hierarchy levels to be visible in the app
            # but for our internal purposes, we still want them to exist in the drilldown hierarchy
            used_nodes = set(fetch_descendant_ids(self.timestamp, None, levels=num_hier_levels, drilldown=False,
                                                  service=self.service))
            used_nodes.add('!')
            hide_nodes |= {node for node in self.new_node_to_parent.iterkeys()
                           if node.split('#')[0] in partial_drilldown
                           and node.split('#')[-1] not in used_nodes}

        self.hidden_reps = hide_nodes - hidden_by_rule

        # create nodes present in new tree, but dont recreate nodes we have hidden
        self.created_nodes = {node: {"parent": parent, "hidden": True if parent in already_hidden_nodes else False}
                              for node, parent in self.new_node_to_parent.iteritems()
                              if node not in curr_nodes and node not in already_hidden_nodes
                              }

        # resurrect nodes that were hidden by earlier drilldown sync, leave manually/rep rule hidden nodes alone
        self.resurrected_nodes = {node for node, _ in self.new_node_to_parent.iteritems()
                                  if node not in curr_nodes
                                  and node in hidden_by_sync}

        # move nodes that have had a parent change from old tree
        self.moved_nodes = {node: {"parent": parent, "hidden": True if parent in already_hidden_nodes else False}
                            for node, parent in self.new_node_to_parent.iteritems()
                            if node in self.curr_node_to_parent
                            and parent != self.curr_node_to_parent[node]}

        # relabel nodes that have had a label change from old tree
        self.relabeled_nodes = {node: label for node, label in self.new_labels.iteritems()
                                if node in self.curr_node_to_parent
                                and label != self.curr_labels[node]}

    def process_versioned(self):
        ## Update the previous nodes with valid "to" timestamp if there is a latest version
        if self.timestamp_for_updating_to:
            ## This is to make sure that only the latest version has "to" as None
            update_nodes_with_valid_to_timestamp(self.timestamp_for_updating_to, drilldown=True)

        for prd in self.periods_for_mnemonic:
            logger.info('syncing drilldown for for: %s', prd)
            if 'Q' in prd:
                prev_prd = prev_period(period_type='Q')[0]
                self.prev_timestamp = get_as_of_dates(prev_prd)['boq']['ep']
            else:
                prev_prd = str(int(prd[:4]) - 1) + str(int(prd[4:]) + 11) if int(prd[4:]) - 1 == 0 else str(
                    int(prd) - 1)
                self.prev_timestamp = get_as_of_dates(prev_prd)['bom']['ep']

            self.curr_node_to_parent, self.curr_labels = fetch_node_to_parent_mapping_and_labels(self.timestamp,
                                                                                                 include_hidden=False,
                                                                                                 drilldown=True,
                                                                                                 period=prd,
                                                                                                 service=self.service)
            self.curr_node_details_versioned[prd] = {'curr_node_to_parent': self.curr_node_to_parent,
                                                     'curr_labels': self.curr_labels}
            hidden_by_sync, hidden_by_user, hidden_by_rule = set(), set(), set()
            for hidden_node in fetch_hidden_nodes(self.timestamp, drilldown=True, period=prd, service=self.service):
                if hidden_node['how'] == 'dd_sync_hide':
                    hidden_by_sync.add(hidden_node['node'])
                elif hidden_node['how'] == 'dd_sync_hide_rep':
                    hidden_by_rule.add(hidden_node['node'])
                else:
                    hidden_by_user.add(hidden_node['node'])
            if not self.curr_node_to_parent:
                prev_hidden_by_sync, prev_hidden_by_user, prev_hidden_by_rule = set(), set(), set()
                for hidden_node in fetch_hidden_nodes(self.prev_timestamp, drilldown=True, period=prev_prd,
                                                      service=self.service):
                    if hidden_node['how'] == 'dd_sync_hide':
                        prev_hidden_by_sync.add(hidden_node['node'])
                    elif hidden_node['how'] == 'dd_sync_hide_rep':
                        prev_hidden_by_rule.add(hidden_node['node'])
                    else:
                        prev_hidden_by_user.add(hidden_node['node'])
                prev_already_hidden_nodes = prev_hidden_by_sync | prev_hidden_by_rule | prev_hidden_by_user
                hidden_by_sync.update(prev_hidden_by_sync)
                hidden_by_rule.update(prev_hidden_by_rule)
                hidden_by_user.update(prev_hidden_by_user)
            already_hidden_nodes = hidden_by_sync | hidden_by_rule | hidden_by_user

            self.new_node_to_parent, self.new_labels = self.build_new_drilldowns(prd)
            self.new_node_details_versioned[prd] = {'new_node_to_parent': self.new_node_to_parent,
                                                    'new_labels': self.new_labels}

            if self.config.debug and self.config.logger_draw_tree:
                logger.info(draw_tree(self.curr_node_to_parent, self.curr_labels))
                logger.info(draw_tree(self.new_node_to_parent, self.new_labels))

            curr_nodes = set(self.curr_node_to_parent)
            new_nodes = set(self.new_node_to_parent)
            hide_nodes = set()

            self.deleted_nodes_versioned[prd] = curr_nodes - new_nodes

            if self.config.deal_only_drilldowns:
                # for tenants that dont want hierarchy nodes to be visible in the app
                # but for our internal purposes, we still want them to exist in the drilldown hierarchy
                dd_nodes = {node: parent for node, parent in self.new_node_to_parent.iteritems()
                            if node.split('#')[0] in self.config.deal_only_drilldowns}
                hide_nodes |= (set(dd_nodes.keys()) - set(dd_nodes.values()))

            for partial_drilldown, num_hier_levels in self.config.deal_partial_hier_drilldowns.iteritems():
                # for tenants that want some limited number of hierarchy levels to be visible in the app
                # but for our internal purposes, we still want them to exist in the drilldown hierarchy
                used_nodes = set(
                    fetch_descendant_ids(self.timestamp, None, levels=num_hier_levels, drilldown=False, period=prd,
                                         service=self.service))
                used_nodes.add('!')
                hide_nodes |= {node for node in self.new_node_to_parent.iterkeys()
                               if node.split('#')[0] in partial_drilldown
                               and node.split('#')[-1] not in used_nodes}

            self.hidden_reps_versioned[prd] = hide_nodes - hidden_by_rule

            # create nodes present in new tree, but dont recreate nodes we have hidden
            self.created_nodes_versioned[prd] = {
                node: {"parent": parent, "hidden": True if parent in already_hidden_nodes else False}
                for node, parent in self.new_node_to_parent.iteritems()
                if node not in curr_nodes and node not in already_hidden_nodes
                }

            # resurrect nodes that were hidden by earlier drilldown sync, leave manually/rep rule hidden nodes alone
            self.resurrected_nodes_versioned[prd] = {node for node, _ in self.new_node_to_parent.iteritems()
                                                     if node not in curr_nodes
                                                     and node in hidden_by_sync}

            # move nodes that have had a parent change from old tree
            self.moved_nodes_versioned[prd] = {
                node: {"parent": parent, "hidden": True if parent in already_hidden_nodes else False}
                for node, parent in self.new_node_to_parent.iteritems()
                if node in self.curr_node_to_parent
                and parent != self.curr_node_to_parent[node]}

            # relabel nodes that have had a label change from old tree
            self.relabeled_nodes_versioned[prd] = {node: label for node, label in
                                                   self.new_labels.iteritems()
                                                   if node in self.curr_node_to_parent
                                                   and label != self.curr_labels[node]}

    def persist(self):

        if self.config.versioned_hierarchy:
            self.persist_versioned()
            return

        if not self.new_node_to_parent:
            logger.warn('no drilldown records persisted')
            self.return_value = {'success': False, 'error': 'no hierarchy'}
            return

        logger.info("""changing drilldown for: %s,
                       deleting: %s, sample delete: %s,
                       creating: %s, sample create: %s,
                       resurrecting: %s, sample resurrect: %s,
                       moving: %s, sample move: %s""",
                    self.period,
                    len(self.deleted_nodes), next(iter(self.deleted_nodes)) if self.deleted_nodes else None,
                    len(self.created_nodes), try_index(self.created_nodes.items(), 0),
                    len(self.resurrected_nodes), next(iter(self.resurrected_nodes)) if self.resurrected_nodes else None,
                    len(self.moved_nodes), try_index(self.moved_nodes.items(), 0), )

        # TODO: these need to have bulk ops
        for node, parent in self.created_nodes.items():
            create_node(parent["parent"],
                        node,
                        self.new_labels[node],
                        self.timestamp,
                        self.config,
                        signature='dd_sync_create',
                        drilldown=True,
                        as_to=self.as_to,
                        service=self.service)
            if parent["hidden"]:
                hide_node(node,
                          self.timestamp,
                          self.config,
                          signature='admin_auto_hide',
                          hide_descendants=False,
                          drilldown=True,
                          as_to=self.as_to,
                          service=self.service)

        for node, parent in self.moved_nodes.iteritems():
            move_node(parent["parent"],
                      node,
                      self.timestamp,
                      self.config,
                      signature='dd_sync_move',
                      ignore_cycles=self.config.dummy_tenant,
                      drilldown=True,
                      as_to=self.as_to,
                      service=self.service)
            if parent["hidden"]:
                hide_node(node,
                          self.timestamp,
                          self.config,
                          signature='admin_auto_hide',
                          hide_descendants=False,
                          drilldown=True,
                          as_to=self.as_to,
                          service=self.service)

        for node, label in self.relabeled_nodes.iteritems():
            label_node(node,
                       label,
                       self.config,
                       signature='dd_sync_label',
                       drilldown=True,
                       as_to=self.as_to,
                       service=self.service)

        for node in self.resurrected_nodes:
            unhide_node(node,
                        self.timestamp,
                        self.config,
                        signature='dd_sync_unhide',
                        drilldown=True,
                        as_to=self.as_to,
                        service=self.service)

        for node in self.deleted_nodes:
            hide_node(node,
                      self.timestamp,
                      self.config,
                      signature='dd_sync_hide',
                      hide_descendants=False,
                      drilldown=True,
                      as_to=self.as_to,
                      service=self.service)

        for node in self.hidden_reps:
            hide_node(node,
                      self.timestamp,
                      self.config,
                      signature='dd_sync_hide_rep',
                      hide_descendants=False,
                      drilldown=True,
                      as_to=self.as_to,
                      service=self.service)

        try:
            if (len(self.deleted_nodes) or len(self.created_nodes)
                    or len(self.resurrected_nodes) or len(self.moved_nodes)):
                fm_config = FMConfig()
                if fm_config.snapshot_feature_enabled:
                    nodes_having_changes = self.deleted_nodes|set(self.created_nodes.keys())|self.resurrected_nodes|set(self.moved_nodes.keys())
                    from fm_service.forecast_schedule import FMSchedule
                    fm_schedule_class = FMSchedule(self.period, list(nodes_having_changes))
                    fm_schedule_class.update_fm_schedule()
        except Exception as e:
            logger.exception(e)



    def persist_versioned(self):
        # if not self.new_node_to_parent:
        #     logger.warn('no drilldown records persisted')
        #     self.return_value = {'success': False, 'error': 'no hierarchy'}
        #     return
        for prd in self.periods_for_mnemonic:
            logger.info("""changing drilldown for: %s,
                        deleting: %s, sample delete: %s,
                        creating: %s, sample create: %s,
                        resurrecting: %s, sample resurrect: %s,
                        moving: %s, sample move: %s""",
                        prd,
                        len(self.deleted_nodes_versioned[prd]),
                        next(iter(self.deleted_nodes_versioned[prd])) if self.deleted_nodes_versioned[prd] else None,
                        len(self.created_nodes_versioned[prd]), try_index(self.created_nodes_versioned[prd].items(), 0),
                        len(self.resurrected_nodes_versioned[prd]),
                        next(iter(self.resurrected_nodes_versioned[prd])) if self.resurrected_nodes_versioned[
                            prd] else None,
                        len(self.moved_nodes_versioned[prd]), try_index(self.moved_nodes_versioned[prd].items(), 0), )

            if len(self.created_nodes_versioned[prd]):
                pass

            prd_dtls = self.periods_for_mnemonic[prd]
            # TODO: these need to have bulk ops
            for node, parent in self.created_nodes_versioned[prd].iteritems():
                create_node(parent["parent"],
                            node,
                            self.new_node_details_versioned[prd]['new_labels'][node],
                            prd_dtls[0],
                            self.config,
                            signature='dd_sync_create',
                            drilldown=True,
                            as_to=prd_dtls[1],
                            service=self.service)
                if parent["hidden"]:
                    hide_node(node,
                              prd_dtls[0],
                              self.config,
                              signature='admin_auto_hide',
                              hide_descendants=False,
                              drilldown=True,
                              as_to=prd_dtls[1],
                              service=self.service)

            for node, parent in self.moved_nodes_versioned[prd].iteritems():
                move_node(parent["parent"],
                          node,
                          prd_dtls[0],
                          self.config,
                          signature='dd_sync_move',
                          ignore_cycles=self.config.dummy_tenant,
                          drilldown=True,
                          as_to=prd_dtls[1],
                          service=self.service)
                if parent["hidden"]:
                    hide_node(node,
                              prd_dtls[0],
                              self.config,
                              signature='admin_auto_hide',
                              hide_descendants=False,
                              drilldown=True,
                              as_to=prd_dtls[1],
                              service=self.service)

            for node, label in self.relabeled_nodes_versioned[prd].iteritems():
                label_node(node,
                           label,
                           self.config,
                           signature='dd_sync_label',
                           drilldown=True,
                           as_to=prd_dtls[1],
                           service=self.service)

            for node in self.resurrected_nodes_versioned[prd]:
                unhide_node(node,
                            prd_dtls[0],
                            self.config,
                            signature='dd_sync_unhide',
                            drilldown=True,
                            as_to=prd_dtls[1],
                            service=self.service)

            for node in self.deleted_nodes_versioned[prd]:
                hide_node(node,
                          prd_dtls[0],
                          self.config,
                          signature='dd_sync_hide',
                          hide_descendants=False,
                          drilldown=True,
                          as_to=prd_dtls[1],
                          service=self.service)

            for node in self.hidden_reps_versioned[prd]:
                hide_node(node,
                          prd_dtls[0],
                          self.config,
                          signature='dd_sync_hide_rep',
                          hide_descendants=False,
                          drilldown=True,
                          as_to=prd_dtls[1],
                          service=self.service)
        try:
            if (len(self.deleted_nodes_versioned[prd]) or len(self.created_nodes_versioned[prd])\
                    or len(self.resurrected_nodes_versioned[prd]) or len(self.moved_nodes_versioned[prd])):
                fm_config = FMConfig()
                nodes_having_changes = self.deleted_nodes_versioned[prd]|set(self.created_nodes_versioned[prd].keys())|self.resurrected_nodes_versioned[prd]|set(self.moved_nodes_versioned[prd].keys())
                if fm_config.snapshot_feature_enabled:
                    from fm_service.forecast_schedule import FMSchedule
                    fm_schedule_class = FMSchedule(self.period, nodes_having_changes)
                    fm_schedule_class.update_fm_schedule()
        except Exception as e:
            logger.exception(e)

    def build_new_drilldowns(self, period=None):
        """
        builds new drilldowns from whatever source system tenant uses

        Returns:
            tuple -- ({node: parent}, {node: label})
        """
        if is_lead_service(self.service):
            node_to_parent, labels = {}, {}
            drilldown = DRILLDOWN_PIVOT
            drilldown_label = DRILLDOWN_PIVOT
            hier_builder = DRILLDOWN_BUILDERS['hier_only']
            if hier_builder:
                dd_ntp, dd_l = hier_builder(self.period if not period else period, drilldown, self.deal_count,
                                            self.seed, drilldown_label=drilldown_label).build_hierarchy(self.service)
                node_to_parent.update(dd_ntp)
                labels.update(dd_l)
            return node_to_parent, labels

        node_to_parent, labels = {}, {}
        for drilldown, dd_builder in self.config.drilldown_builders.iteritems():
            try:
                hier_builder = DRILLDOWN_BUILDERS[dd_builder]
            except KeyError:
                hier_builder = DRILLDOWN_BUILDERS['flat_hier']
            if hier_builder:
                dd_ntp, dd_l = hier_builder(self.period if not period else period, drilldown, self.deal_count,
                                            self.seed).build_hierarchy()
                node_to_parent.update(dd_ntp)
                labels.update(dd_l)

        return node_to_parent, labels


class _BootstrapDrilldown(BaseTask):
    """
    Bootstrap Drilldown
    create a new drilldown from hierarchy and drilldown configuration
    """

    def __init__(self, period, timestamp=None, deal_count=None, shuffle=False, seed=None, **kwargs):
        self.period = period
        self.timestamp = get_period_as_of(self.period, timestamp)
        self.config = HierConfig(debug=True)

        self.hidden_reps = set()

        # only used for dummy tenant data generation
        self.deal_count = deal_count
        self.shuffle = shuffle
        self.seed = seed

    def process(self):
        logger.info('bootstrapping drilldown for for: %s', self.period)

        self.new_node_to_parent, self.new_labels = self.build_new_drilldowns()

        if self.config.debug and self.config.logger_draw_tree:
            logger.info(draw_tree(self.new_node_to_parent, self.new_labels))

        hide_nodes = set()

        if self.config.deal_only_drilldowns:
            # for tenants that dont want hierarchy nodes to be visible in the app
            # but for our internal purposes, we still want them to exist in the drilldown hierarchy
            dd_nodes = {node: parent for node, parent in self.new_node_to_parent.iteritems()
                        if node.split('#')[0] in self.config.deal_only_drilldowns}
            hide_nodes |= (set(dd_nodes.keys()) - set(dd_nodes.values()))

        for partial_drilldown, num_hier_levels in self.config.deal_partial_hier_drilldowns.iteritems():
            # for tenants that want some limited number of hierarchy levels to be visible in the app
            # but for our internal purposes, we still want them to exist in the drilldown hierarchy
            used_nodes = set(fetch_descendant_ids(self.timestamp, None, levels=num_hier_levels, drilldown=False))
            used_nodes.add('!')
            hide_nodes |= {node for node in self.new_node_to_parent.iterkeys()
                           if node.split('#')[0] in partial_drilldown
                           and node.split('#')[-1] not in used_nodes}

        self.hidden_reps = hide_nodes

    def persist(self):
        if not self.new_node_to_parent:
            logger.warn('no drilldown records persisted')
            self.return_value = {'success': False, 'error': 'no hierarchy'}
            return

        logger.info("""creating drilldowns for: %s,
                       sample drilldown: %s,
                       """,
                    self.period,
                    try_index(self.new_node_to_parent.items(), 0))

        create_many_nodes(self.new_node_to_parent,
                          self.new_labels,
                          0,
                          self.config,
                          signature='bootstrap_drilldowns',
                          drilldown=True)

        for node in self.hidden_reps:
            hide_node(node,
                      self.timestamp,
                      self.config,
                      signature='dd_sync_hide_rep',
                      hide_descendants=False,
                      drilldown=True)

    def build_new_drilldowns(self):
        """
        builds new drilldowns from whatever source system tenant uses

        Returns:
            tuple -- ({node: parent}, {node: label})
        """
        node_to_parent, labels = {}, {}
        for drilldown, dd_builder in self.config.drilldown_builders.iteritems():
            try:
                hier_builder = DRILLDOWN_BUILDERS[dd_builder]
            except KeyError:
                hier_builder = DRILLDOWN_BUILDERS['flat_hier']
            if hier_builder:
                dd_ntp, dd_l = hier_builder(self.period, drilldown, self.deal_count, self.seed).build_hierarchy()
                node_to_parent.update(dd_ntp)
                labels.update(dd_l)

        return node_to_parent, labels