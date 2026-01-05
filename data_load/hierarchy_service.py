import logging
import random

from aviso.settings import sec_context
# Try new location first, fallback to compatibility module
try:
    from utils.date_utils import epoch
except ImportError:
    from aviso.utils.dateUtils import epoch
from django.utils.functional import cached_property

logger = logging.getLogger('gnana.%s' % __name__)

HIER_COLL = 'hierarchy'


def merge_dicts(*dictionaries):
    """
    combine dictionaries together
    """
    result = {}
    for dictionary in dictionaries:
        result.update(dictionary)
    return result


class CoolerHierarchyService:
    '''
    New version of the hierarchy service, compatible with the new app.

    Arguments:
        dd_config {} - ?
        drilldowns - ?
    '''
    # TODO remove
    cool_version = True

    def __init__(self, config, asof=None):
        # TODO maybe use the caches
        self.ancestor_cache = {}
        self.paths_cache = {}
        self.period_cache = {}

        # we only use the leaf field if the owner isn't defined by the splits
        self.leaf_field = config.get('leaf_field', 'as_of_OwnerID')
        self.closedate_field = config.get('closedate_field', 'as_of_CloseDate_adj')
        self.asof = epoch(asof).as_epoch() if asof is not None else epoch().as_epoch()
        self.hier_with_ancestors = self.load_ancestors_from_db(self.asof)
        random.seed(13579)

        # we set the func up so we can save a little time on lookups
        if config.get('versioned', False):
            logger.info(f'Hierarchy service - running in versioned mode, hierarchy asof {self.asof}')
            self.timestamp_func = self.versioned_ts
        else:
            logger.info(f'Hierarchy service - running in unversioned mode, hierarchy asof {self.asof}')
            self.timestamp_func = self.unversioned_ts

    @cached_property
    def unborn_fields(self):
        return {self.leaf_field}

    @cached_property
    def drilldown_fields(self):
        # a list of stringified fields to use in the drilldowns list
        return tuple([str(i) for i in range(self.max_depth)])

    @cached_property
    def groupby_fields(self):
        output = {}
        for i in range(self.max_depth):
            output[str(i)] = tuple([str(k) for k in range(i + 1)])
        return output

    @cached_property
    def max_depth(self):
        # since we got rid of [not_in_hier, node], max depth ok to be 1
        try:
            max_len = max(len(x) for x in self.hier_with_ancestors.values())
        except ValueError:
            max_len = 0
        # +1 is for yourself
        return max_len + 1

    def get_segs(self, record, split_ids):
        # grab ts outside the loop even if we don't use it
        ts = self.timestamp_func(record)
        # no split mode
        if split_ids is None:
            leaf = record[self.leaf_field]
            return {'dummy': self.get_ancestors(leaf, ts)}
        # assuming there were splits
        output = {}
        for split_id in split_ids:
            try:
                if isinstance(record[self.leaf_field], dict):
                    leaf = record[self.leaf_field][split_id]
                else:
                    leaf = record[self.leaf_field]
                raw_ancestors = self.get_ancestors(leaf, ts)
            except (TypeError, KeyError):
                raw_ancestors = ['unmapped']
            output[split_id] = raw_ancestors
        return output

    def get_paths(self, record, split_ids):
        # grab ts outside the loop even if we don't use it
        ts = self.timestamp_func(record)
        # no split mode
        if split_ids is None:
            leaf = record[self.leaf_field]
            raw_ancestors = self.get_ancestors(leaf, ts)
            ancestors = raw_ancestors + [None] * (self.max_depth - len(raw_ancestors))
            return {'dummy': tuple(ancestors)}
        # assuming there were splits
        output = {}
        for split_id in split_ids:
            try:
                if isinstance(record[self.leaf_field], dict):
                    leaf = record[self.leaf_field][split_id]
                else:
                    leaf = record[self.leaf_field]
                raw_ancestors = self.get_ancestors(leaf, ts)
            # for unborn, split ids isn't always
            except (TypeError, KeyError):
                raw_ancestors = ['unmapped']
            output[split_id] = tuple(raw_ancestors) + (None,) * (self.max_depth - len(raw_ancestors))
        return output

    def versioned_ts(self, record):
        # TODO this is the old logic, probably shouldn't be this way, especially if we fix
        # the dumb key bullshit
        return epoch(record[self.closedate_field]).as_epoch() if 'won_amount' in record else None

    def unversioned_ts(self, record):
        # passthrough for unversioned so we can be a tad more efficient
        return None

    def get_ancestors(self, node, ts=None):
        try:
            if ts is None:
                return self.hier_with_ancestors[node] + [node]
            else:
                # returns a generator, so we gotta do a little wonkiness, might as well
                # make it match the same access pattern, should only return one thing though
                anc_dict = {rec['node']: rec['ancestors'] for rec in self.fetch_ancestors(as_of=ts, nodes=[node])}
                return anc_dict[node] + [node]
        except KeyError:
            # this would be a lot easier if we consolidated where we handled nulls
            return ['unmapped'] if node == 'N/A' else ['not_in_hier']

    def load_ancestors_from_db(self, asof):
        # TODO: figure out actual timestamps
        return {node['node']: node['ancestors'] for node in self.fetch_ancestors(asof)}  # , db=db)}

    def fetch_ancestors(self,
                        as_of,
                        nodes=None,
                        include_hidden=False,
                        include_children=False,
                        # db=None,
                        ):
        """
        fetch ancestors of many hierarchy nodes
        Arguments:
            as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        Keyword Arguments:
            nodes {list} -- nodes to find ancestors for, if None grabs all          ['0050000FLN2C9I2', ]
                            (default: {None})
            include_hidden {bool} -- if True, include hidden nodes
                                     (default: {False})
            include_children {bool} -- if True, fetch children of nodes in nodes
                                (default: {False})
            db {object} -- instance of tenant_db (default: {None})
                           if None, will create one
        Returns:
            generator -- generator of ({node: node, parent: parent, ancestors: [ancestors]})
        """
        hier_collection = sec_context.tenant_db[HIER_COLL]
        # hier_collection = gnana_db.db[HIER_COLL]
        criteria = {'$and': [
            {'$or': [{'from': None},
                     {'from': {'$lte': as_of}}]},
            {'$or': [{'to': None},
                     {'to': {'$gt': as_of}}]}
        ]}
        if not include_hidden:
            criteria['hidden'] = {'$ne': True}
        if nodes:
            node_criteria = {'node': {'$in': list(nodes)}}
            if include_children:
                node_criteria = {'$or': [node_criteria, {'parent': {'$in': list(nodes)}}]}
            match = merge_dicts(criteria, node_criteria)
        else:
            match = criteria
        lookup = {'from': HIER_COLL,
                  'startWith': '$parent',
                  'connectFromField': 'parent',
                  'connectToField': 'node',
                  'depthField': 'level',
                  'restrictSearchWithMatch': criteria,
                  'as': 'ancestors'}
        project = {'node': 1,
                   'parent': 1,
                   'label': 1,
                   '_id': 0,
                   'ancestors': {'$map': {'input': '$ancestors',
                                          'as': 'ancestor',
                                          'in': {'node': '$$ancestor.node',
                                                 'level': '$$ancestor.level'}}}}
        pipeline = [{'$match': match},
                    {'$graphLookup': lookup},
                    {'$project': project}]
        return self._anc_yielder(hier_collection.aggregate(pipeline))

    def _anc_yielder(self, nodes):
        for node in nodes:
            node['ancestors'] = [x['node'] for x in
                                 sorted(node['ancestors'], key=lambda x: x.get('level'), reverse=True)]
            yield node
