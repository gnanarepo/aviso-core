import logging

from aviso.settings import sec_context

from config import HierConfig
from config import HIERARCHY_BUILDERS, write_hierarchy_to_gbm
from infra.read import (fetch_hidden_nodes,
                        fetch_node_to_parent_mapping_and_labels,
                        get_period_as_of, get_period_begin_end)
from tasks import BaseTask
from tasks.hierarchy import (draw_tree, graft_pruned_tree, make_new_hierarchy,
                             make_pruned_tree, make_valid_tree)
from utils.misc_utils import is_lead_service, try_index
from utils.mongo_writer import (create_many_nodes, create_node, hide_node,
                                label_node, move_node, unhide_node)

# logger = logging.getLogger('aviso-core.%s' % __name__)


class HierSyncTask:

    def execute(self, period):
        try:
            sync_obj = Sync(period=period)
            sync_obj.process()
            sync_obj.persist()
            result = sync_obj.return_value
            return {'success': True, 'result': result}
        except Exception as e:
            # logger.exception(e)
            return {'success': False,
                    'error_msg': e}


class Sync(BaseTask):
    """
    Sync
    sync hierarchy from crm system, and apply changes to app hierarchy

    Arguments:
        period {str} -- period mnemonic                                         '2020Q2'

    Optional Arguments:
        timestamp {int} -- epoch timestamp to save records as_of                1556074024910
                            if None, uses latest app time (default: {None})
    """
    def __init__(self, *args, **kwargs):
        config = HierConfig()
        if config.bootstrapped:
            self.syncer = _Sync(*args, **kwargs)
        else:
            self.syncer = _Bootstrap(*args, **kwargs)

    def process(self):
        self.syncer.process()

    def persist(self):
        self.syncer.persist()


class _Sync:
    """
    Sync
    sync hierarchy from crm system, and apply changes to app hierarchy
    """
    def __init__(self, period, timestamp=None, shuffle=False, seed=None, chaos=None, **kwargs):
        self.period = period
        self.timestamp = get_period_as_of(self.period, timestamp)
        self.config = HierConfig()
        self.as_to = None

        if self.config.versioned_hierarchy:
            self.timestamp, self.as_to = get_period_begin_end(self.period)

        self.deleted_nodes = set()
        self.created_nodes = {}
        self.moved_nodes = {}
        self.relabled_nodes = {}
        self.resurrected_nodes = set()

        # only used for dummy tenant data generation
        self.shuffle = shuffle
        self.seed = seed
        self.chaos = chaos

        self.service = kwargs.get('service', None)

    def process(self):
        # logger.info('syncing hierarchy for for: %s', self.period)

        self.curr_node_to_parent, self.curr_labels = fetch_node_to_parent_mapping_and_labels(self.timestamp,
                                                                                             include_hidden=False,
                                                                                             drilldown=False,
                                                                                             service=self.service)
        self.hidden_node_to_parent, hidden_nodes = {}, set()
        for hidden_node in fetch_hidden_nodes(self.timestamp, drilldown=False, signature='hier_sync_hide', period=self.period, service=self.service):
            hidden_nodes.add(hidden_node['node'])
            self.hidden_node_to_parent[hidden_node['node']] = hidden_node['parent']

        self.new_node_to_parent, self.new_labels = self.build_new_hierarchy()

        if self.config.debug:
            pass
            # logger.info(draw_tree(self.curr_node_to_parent, self.curr_labels))
            # logger.info(draw_tree(self.new_node_to_parent, self.new_labels))

        curr_nodes = set(self.curr_node_to_parent)
        new_nodes = set(self.new_node_to_parent)

        self.deleted_nodes = curr_nodes - new_nodes
        self.created_nodes = {node: parent for node, parent in self.new_node_to_parent.items()
                              if node not in curr_nodes and node not in hidden_nodes}
        self.resurrected_nodes = {node for node, _ in self.new_node_to_parent.items()
                                  if node not in curr_nodes and node in hidden_nodes}
        self.moved_nodes = {node: parent for node, parent in self.new_node_to_parent.items()
                            if node in self.curr_node_to_parent and parent != self.curr_node_to_parent[node]}
        self.relabeled_nodes = {node: label for node, label in self.new_labels.items()
                                if node not in self.created_nodes
                                and node not in hidden_nodes
                                and label != self.curr_labels[node]}
        # TODO: serious cycle checking ...

    def persist(self):
        if not self.new_node_to_parent:
            # logger.warning('no hierarchy records persisted')
            self.return_value = {'success': False, 'error': 'no hierarchy'}
            return

        # logger.info("""changing hierarchy for: %s,
        #                deleting: %s, sample delete: %s,
        #                creating: %s, sample create: %s,
        #                moving: %s, sample move: %s""",
        #             self.period,
        #             len(self.deleted_nodes), next(iter(self.deleted_nodes)) if self.deleted_nodes else None,
        #             len(self.created_nodes),  try_index(self.created_nodes.items(), 0),
        #             len(self.moved_nodes), try_index(self.moved_nodes.items(), 0),)
        # TODO: these need to have bulk ops
        for node in self.deleted_nodes:
            hide_node(node,
                      self.timestamp,
                      self.config,
                      signature='hier_sync_hide',
                      hide_descendants=False,
                      service=self.service)

        for node, parent in self.created_nodes.items():
            create_node(parent,
                        node,
                        self.new_labels[node],
                        self.timestamp,
                        self.config,
                        signature='hier_sync_create',
                        service=self.service)

        for node, parent in self.moved_nodes.items():
            move_node(parent,
                      node,
                      self.timestamp,
                      self.config,
                      signature='hier_sync_move',
                      ignore_cycles=self.config.dummy_tenant, service=self.service)

        for node, label in self.relabeled_nodes.items():
            label_node(node,
                       label,
                       self.config,
                       signature='hier_sync_label', service=self.service)

        for node in self.resurrected_nodes:
            unhide_node(node,
                        self.timestamp,
                        self.config,
                        signature='hier_sync_unhide', service=self.service)

        if not self.service:
            write_hierarchy_to_gbm(self.config)

    def build_new_hierarchy(self):
        """
        builds new hierarchy from whatever source system tenant uses

        Returns:
            tuple -- ({node: parent}, {node: label})
        """
        if self.config.dummy_tenant:
            if self.shuffle:
                return make_valid_tree(self.curr_node_to_parent, self.seed), self.curr_labels
            elif self.chaos == 'prune':
                return make_pruned_tree(self.curr_node_to_parent, self.seed), self.curr_labels
            elif self.chaos == 'restore':
                return graft_pruned_tree(self.curr_node_to_parent, self.hidden_node_to_parent), self.curr_labels
            return self.curr_node_to_parent, self.curr_labels

        if self.config.hierarchy_builder == 'upload' and not is_lead_service(self.service):
            return self.curr_node_to_parent, self.curr_labels

        try:
            hier_builder = HIERARCHY_BUILDERS[self.config.hierarchy_builder]
        except KeyError:
            hier_builder = HIERARCHY_BUILDERS['flat_hier']

        if is_lead_service(self.service):
            hier_builder = HIERARCHY_BUILDERS['lead_hierarchy']

        if hier_builder:
            return hier_builder(self.period).build_hierarchy()

        return {}, {}


class _Bootstrap:
    """
    Bootstrap Hierarchy
    create a new hierarchy from scratch from crm system
    """
    def __init__(self, period, rep_count=None, seed=100, alt_hier=False, **kwargs):
        self.period = period
        self.timestamp = 0
        self.config = HierConfig()

        # only used for dummy tenant data generation
        self.rep_count = rep_count
        self.seed = seed
        self.alt_hier = alt_hier

        self.new_node_to_parent = {}
        self.labels = {}

    def process(self):
        # logger.info('bootstrapping hierarchy for for: %s', self.period)

        self.new_node_to_parent, self.labels = self.build_new_hierarchy()

    def persist(self):
        # logger.info('persisting %s hierarchy for: %s, sample node: %s',
        #             len(self.new_node_to_parent),
        #             self.period,
        #             try_index(self.new_node_to_parent.items(), 0))
        if not self.new_node_to_parent:
            # logger.warning('no hierarchy records persisted')
            # self.return_value = {'success': False, 'error': 'no hierarchy'}
            return

        create_many_nodes(self.new_node_to_parent,
                          self.labels,
                          self.timestamp,
                          self.config,
                          signature='bootstrap_nodes')
        write_hierarchy_to_gbm(self.config)

    def build_new_hierarchy(self):
        if self.config.dummy_tenant:
            return make_new_hierarchy(self.rep_count, self.seed, self.alt_hier)
        try:
            hier_builder = HIERARCHY_BUILDERS[self.config.hierarchy_builder]
        except KeyError:
            hier_builder = HIERARCHY_BUILDERS['flat_hier']
        if hier_builder:
            return hier_builder(self.period).build_hierarchy()

        return {}, {}
