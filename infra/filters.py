import re
import threading
import uuid
from collections import namedtuple
from copy import deepcopy

from aviso.settings import sec_context

from utils.date_utils import epoch, rng_from_prd_str
from utils.misc_utils import get_nested, inrange, try_float, try_index

from .constants import AUDIT_COLL, FILTS_COLL


NEGATION_MAP = {'$gt': '$lte', '$gte': '$lt', '$lte': '$gt', '$lt': '$gte'}
FAKE_CONFIG = namedtuple('fakeconfig', ['hier_aware_fields', 'period_aware_fields'])
FILTER_CACHE = {}

def fetch_filter(filter_ids,
                 config,
                 filter_type='mongo',
                 db=None,
                 cache=None,
                 root_node=None,
                 get_custom_limit=False,
                 collection=FILTS_COLL,
                 is_pivot_special=False
                 ):
    """
    fetch a composed db filter from filter ids

    Arguments:
        filter_ids {list} -- filter ids to compose into a single AND filter      ['id1', 'id2']
        config {DealsConfig} -- instance of DealsConfig                          ...?

    Keyword Arguments:
        filter_type {str} -- flag to switch return signature (default: {'mongo'})
                             if mongo, returns mongo db filter criteria
                             else, returns python func(deal, favs, dlf)
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one
                       (pass in to avoid overhead when fetching many times)
        cache {dict} -- dict to hold filters fetched (default: {None})
                        (used to memoize fetching many filters)
        root_node {str} -- if root_node provided, add root_node in _make_filter
                           if filter_type=mongo and hier_aware_field

    Returns:
        if 'mongo' filter_type
            dict -- mongo db filter criteria
        if 'python' filter_type
            lambda -- python function with func(deals, favs, dlf) signature
        if 'raw' filter_type
            list -- raw filter language nonsense
    """
    filters_collection = db[collection] if db else sec_context.tenant_db[collection]

    if filter_ids:
        criteria = {'filter_id': {'$in': filter_ids}}
    else:
        criteria = {'is_default': True}

    filters = list(filters_collection.find(criteria, {'desc': 1, 'name': 1,
                                                      'custom_limit': 1}))
    if config.multiple_filter_apply_or:
       filt = fetch_filter_OR_condition(filters,
                                         config,
                                         filter_type,
                                         cache,
                                         filter_ids,
                                         root_node=root_node,
                                        is_pivot_special=is_pivot_special)
    else:
       filt = fetch_filter_AND_condition(filters,
                                          config,
                                          filter_type,
                                          cache,
                                          filter_ids,
                                          root_node=root_node,
                                         is_pivot_special=is_pivot_special)
    if get_custom_limit:
        custom_limit = None
        for filt_ in filters:
            custom_limit = filt_.get("custom_limit", None)
            if custom_limit:
                break
        return filt, custom_limit

    return filt


def fetch_filter_AND_condition(filters,
                               config,
                               filter_type,
                               cache,
                               filter_ids,
                               root_node=None,
                               is_pivot_special=False):
    # AND condition to be applied on choosing multiple filters
    names = []
    raw_filters = []
    for filt in filters:
        names.append(filt['name'])
        for desc in filt['desc']:
            raw_filters.append(desc)

    filt = parse_filters(raw_filters, config, filter_type, root_node=root_node, is_pivot_special=is_pivot_special) if filter_type != 'raw' else raw_filters

    if cache is not None:
        cache[tuple(filter_ids)] = (', '.join(names), filt)
    return filt


def fetch_filter_OR_condition(filters,
                               config,
                               filter_type,
                               cache,
                               filter_ids,
                               root_node=None,
                              is_pivot_special=False):
    # OR condition to be applied on choosing multiple filters
    names = []
    final_filters = {'$or': []}
    final_raw_filters = []
    for filt in filters:
        names.append(filt['name'])
        raw_filters = []
        for desc in filt['desc']:
            raw_filters.append(desc)
            final_raw_filters.append(desc)
        if filter_type == 'mongo':
            final_filters['$or'].append(parse_filters(raw_filters, config, filter_type, root_node=root_node,
                                                      is_pivot_special=is_pivot_special))
    if not final_filters['$or']:
        del final_filters['$or']
    if filter_type == 'raw':
        final_filters = final_raw_filters
    elif filter_type == 'python':
        final_filters = parse_filters(final_raw_filters, config, filter_type)

    if cache is not None:
        cache[tuple(filter_ids)] = (', '.join(names), final_filters)

    return final_filters


def fetch_many_filters(list_of_filter_ids,
                       config,
                       filter_type='mongo',
                       db=None,
                       ):
    """
    fetch multiple db filters from many filter ids

    Arguments:
        list_of_filter_ids {list} -- list of lists of filter ids                 [['id1', 'id2'], ..., ['idn']]
        config {DealsConfig} -- instance of DealsConfig                          ...?

    Keyword Arguments:
        filter_type {str} -- flag to switch return signature (default: {'mongo'})
                             if mongo, returns mongo db filter criteria
                             else, returns python func(deal, favs, dlf)
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one
                       (pass in to avoid overhead when fetching many times)

    Returns:
        dict -- {(filt ids): (filter name, mongo db filter criteria OR python func)}
    """
    # TODO: theres actually a good bit of non i/o work in fetch_filters
    # so threading is probably the wrong call here, but being lazy for now
    db = db or sec_context.tenant_db
    threads, cache = [], {}

    for filter_ids in list_of_filter_ids:
        t = threading.Thread(target=fetch_filter,
                             args=(filter_ids,
                                   config,
                                   filter_type,
                                   db,
                                   cache))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    return cache


def fetch_all_filters(config,
                      grid_only=False,
                      filter_type='mongo',
                      db=None,
                      get_custom_limit=False,
                      collection=FILTS_COLL,
                      is_pivot_special=False,
                      segment_id=None
                      ):
    """
    fetch all the filters from the db

    Arguments:
        config {DealsConfig} -- instance of DealsConfig                          ...?

    Keyword Arguments:
        grid_only {bool} -- only return filters that display in deals grid
                            (default: {False})
        filter_type {str} -- flag to switch return signature (default: {'mongo'})
                             if mongo, returns mongo db filter criteria
                             else, returns python func(deal, favs, dlf)
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one
                       (pass in to avoid overhead when fetching many times)

    Returns:
        dict -- {(filt ids): (filter name, mongo db filter criteria OR python func)}
    """
    all_filters = {}
    filters_collection = db[collection] if db else sec_context.tenant_db[collection]
    criteria = {'in_grid': True} if grid_only else {}
    if segment_id:
        criteria.update({'segment_id': segment_id})
    criteria["$or"] = [{"user_level": {"$exists": False}}, {"user_level": False}]
    user_level_criteria = {"user_level": True, "user": sec_context.login_user_name, "$or": [{"deleted": {"$exists": False}}, {"deleted": False}]}
    final_criteria = {"$or": [criteria, user_level_criteria]}
    filters = filters_collection.find(final_criteria, {'desc': 1, 'name': 1, 'filter_id': 1, "tenant_level":1, "user_level":1, "group_name":1, 'custom_limit': 1})
    for filt in filters:
        org_filt = deepcopy(filt)
        tenant_level = False if filt.get("user_level") else True
        if get_custom_limit:
            all_filters[filt['filter_id']] = (
            filt['name'], parse_filters(filt['desc'], config, filter_type, is_pivot_special=is_pivot_special), org_filt['desc'], tenant_level,
            filt.get("custom_limit", None))
        else:
            all_filters[filt['filter_id']] = (filt['name'], parse_filters(filt['desc'], config, filter_type, is_pivot_special=is_pivot_special), org_filt['desc'], tenant_level)

    return all_filters


def parse_filters(filters, config, filter_type='mongo', safe_mode=True, hier_aware=True, root_node=None,
                  is_pivot_special=False):
    """
    convert aviso filter definition into either a mongo db filter OR a python lambda function

    Arguments:
        filters {list} -- some sort of iterable. could be a list,             [{?}, ?]
                           list of lists, dict, dict of dict, etc
        config {DealsConfig} -- instance of DealsConfig                          ...?

    Keyword Arguments:
        filter_type {str} -- flag to switch return signature (default: {'mongo'})
                             if mongo, returns mongo db filter criteria
        safe_mode {bool} -- flag to safely parge invalid filters to empty filters
                            (default: {True})
        hier_aware {bool} -- flag to make filters hierarchy aware
                             (default: {True}) should be true for deals filters
                             but false for dtfo filters :((

    Returns:
        if 'mongo' filter_type
            dict -- mongo db filter criteria
        else
            lambda -- python function with func(deals, favs, dlf) signature
    """
    # single filter format:
    # {'key': 'key', 'op': 'op', 'val': 'val'}
    # OR filter format:
    # This checks if (stage 50 or 60 commit) OR (stage 90 or 99).
    # {'$or':
    #  [
    #      [{'key': 'ForecastCategory', 'op': 'in', 'val': ['Commit']},
    #       {'key': 'Stage', 'op': 'in', 'val': ['50','60']}
    #       ],
    #      [{'key': 'Stage', 'op': 'in', 'val': ['90','99']},
    #       ],
    #  ]}
    if isinstance(filters, dict) and ('key' in filters or '!or' in filters):
        filters = [filters]

    # deprecated multi filter format: (keys are vestigal)
    # {'name': {'key': 'key', 'op': 'op', 'val': 'val'},
    #  'other_name': {'key': 'key', 'op': 'op', 'val': 'val'}}
    elif isinstance(filters, dict) and '!or' not in filters:
        filters = filters.values()

    # multi filter format:
    # [{'key': 'key', 'op': 'op', 'val': 'val'},
    #  {'key': 'key', 'op': 'op', 'val': 'val'}]
    if filter_type == 'mongo':
        return _parse_mongo_filters(filters, config, safe_mode, hier_aware, root_node=root_node,
                                    is_pivot_special=is_pivot_special)
    return _parse_python_filters(filters, config, safe_mode, hier_aware)


def _parse_mongo_filters(filters, config, safe_mode, hier_aware, root_node=None, is_pivot_special=False):
    mongo_filter = {}

    for fltr in filters:
        if not fltr:
            continue

        if '!or' in fltr:
            # need to recursively parse the individual filters in the OR
            mongo_ors = {'$or': []}
            for or_group in fltr['!or']:
                mongo_or_filt = _parse_mongo_filters(or_group, config, safe_mode, hier_aware, root_node=root_node)
                mongo_ors['$or'].append(mongo_or_filt)
            mongo_filter = mongo_ors if not mongo_filter else {'$and': [mongo_filter, mongo_ors]}
        elif "or" in fltr:
            # need to recursively parse the individual filters in the OR
            mongo_ors = {'$or': []}
            for or_group in fltr['or']:
                mongo_or_filt = _parse_mongo_filters([or_group], config, safe_mode, hier_aware, root_node=root_node)
                mongo_ors['$or'].append(mongo_or_filt)
            mongo_filter = mongo_ors if not mongo_filter else {'$and': [mongo_filter, mongo_ors]}
        else:
            mongo_single_flt = _make_filter(fltr, config, 'mongo', safe_mode, hier_aware, root_node=root_node,
                                            is_pivot_special=is_pivot_special)
            mongo_filter = mongo_single_flt if not mongo_filter else {'$and': [mongo_filter, mongo_single_flt]}

    return mongo_filter


def _parse_python_filters(filters, config, safe_mode, hier_aware):
    python_filters = []

    for fltr in filters:
        if not fltr:
            continue

        if '!or' in fltr:
            def python_ors(deal, favs, dlf): return any(_parse_python_filters(orr, config, safe_mode, hier_aware)(deal, favs, dlf)
                                                        for orr in fltr['!or'])
            python_filters.append(python_ors)
        else:
            python_filter = _make_filter(fltr, config, 'python', safe_mode, hier_aware)
            if python_filter:
                python_filters.append(python_filter)
    if python_filters:
        return lambda deal, favs, dlf: all(py_filter(deal, favs, dlf) for py_filter in python_filters)
    return lambda deal, favs, dlf: False


def _make_filter(fltr, config, filter_type='mongo', safe_mode=True, hier_aware=True, root_node=None, is_pivot_special=False):
    op = fltr.get('op', 'bool')

    if fltr.get('key') in config.period_aware_fields:
        fltr['key'] = '{}.%(period)s'.format(fltr['key'])
    if hier_aware and fltr.get('key') in config.hier_aware_fields:
        if root_node is None:
            fltr['key'] = '{}.%(node)s'.format(fltr['key'])
        else:
            fltr['key'] = '{}.{}'.format(fltr['key'], root_node)

    if is_pivot_special:
        fltr['is_pivot_special'] = True

    if safe_mode:
        try:
            return OP_MAP[op](fltr, filter_type)
        except KeyError:
            return {} if filter_type == 'mongo' else lambda deal, favs, dlf: False
    return OP_MAP[op](fltr, filter_type)


def _in_filter(fltr, filter_type):
    key = fltr.get('key')
    value = fltr.get('val')
    negate = fltr.get('negate', False)

    if filter_type == 'mongo':
        mongo_op = '$in' if not negate else '$nin'
        return {key: {mongo_op: value}}

    # sometimes possible that given key does not exist. On safer side check that 1st.
    if not negate:
        return lambda deal, favs, dlf: False if deal.get(key) is None else deal.get(key) in value

    return lambda deal, favs, dlf: False if deal.get(key) is None else deal.get(key) not in value


def _nested_in_filter(fltr, filter_type):
    key = fltr.get('key')
    value = fltr.get('val')
    negate = fltr.get('negate', False)

    if filter_type == 'mongo':
        mongo_op = '$in' if not negate else '$nin'
        return {'.'.join(key): {mongo_op: value}}

    if not negate:
        return lambda deal, favs, dlf: get_nested(deal, key) in value

    return lambda deal, favs, dlf: get_nested(deal, key) not in value


def _bool_filter(fltr, filter_type):
    key = fltr.get('key')
    negate = fltr.get('negate', False)

    if filter_type == 'mongo':
        mongo_op = '$eq' if not negate else '$ne'
        return {key: {mongo_op: True}}

    if not negate:
        return lambda deal, favs, dlf: bool(deal.get(key))

    return lambda deal, favs, dlf: not bool(deal.get(key))


def _has_filter(fltr, filter_type):
    key = fltr.get('key')
    negate = fltr.get('negate', False)

    if filter_type == 'mongo':
        mongo_bool = True if not negate else False
        return {key: {'$exists': mongo_bool}}

    if not negate:
        return lambda deal, favs, dlf: deal.get(key) is not None

    return lambda deal, favs, dlf: key not in deal


def _range_filter(fltr, filter_type):
    key = fltr.get('key')
    value = fltr.get('val')
    negate = fltr.get('negate', False)

    if filter_type == 'mongo':
        query = {}
        if value[0] is not None:
            great_op = '$gt' if try_index(value, 2) else '$gte'
            great_op = great_op if not negate else NEGATION_MAP[great_op]
            query[great_op] = value[0]
        if value[1] is not None:
            less_op = '$lt' if try_index(value, 3) else '$lte'
            less_op = less_op if not negate else NEGATION_MAP[less_op]
            query[less_op] = value[1]
        return {key: query}

    valuetuple = tuple(value)
    if not negate:
        return lambda deal, favs, dlf: inrange(try_float(deal.get(key)), valuetuple)

    return lambda deal, favs, dlf: not inrange(try_float(deal.get(key)), valuetuple)


def _favs_filter(fltr, filter_type):
    key = fltr.get('key')
    negate = fltr.get('negate', False)
    is_pivot_special = fltr.get('is_pivot_special', False)

    if filter_type == 'mongo':
        mongo_op = '$in' if not negate else '$nin'
        favs_key = 'opp_id' if not is_pivot_special else 'RPM_ID'
        return {favs_key: {mongo_op: '%(favs)s'}}

    if not negate:
        return lambda deal, favs, dlf: favs is not None and deal.get(key) in favs

    return lambda deal, favs, dlf: favs is not None and deal.get(key) not in favs

def _search_filter(fltr, filter_type):
    key = fltr.get('key')
    values = fltr.get('val')
    negate = fltr.get('negate', False)
    if filter_type == 'mongo':
        if negate:
            query = {"$nor": [{key: {"$regex": val, "$options": "i"}} for val in values]}
        else:
            query = {"$or": [{key : {"$regex" : val , "$options" : "i"}} for val in values]}
        return query
    return {}

def _dlf_range_filter(fltr, filter_type):
    key = fltr.get('key')
    node = fltr.get('node')
    value_key = fltr.get('val')
    value = fltr.get('match_vals')
    negate = fltr.get('negate', False)
    if filter_type == 'mongo':
        dlf_cat, dlf_key = value_key.split(".")
        if node is not None:
            mongo_key = "dlf.%s.%s.%s" % (dlf_cat, node, dlf_key)
        else:
            mongo_key = "dlf.{}.%(node)s.{}".format(dlf_cat, dlf_key)
        query = {}
        if value[0] is not None:
            great_op = '$gt' if try_index(value, 2) else '$gte'
            great_op = great_op if not negate else NEGATION_MAP[great_op]
            query[great_op] = value[0]
        if value[1] is not None:
            less_op = '$lt' if try_index(value, 3) else '$lte'
            less_op = less_op if not negate else NEGATION_MAP[less_op]
            query[less_op] = value[1]
        return {mongo_key: query}

    valuetuple = tuple(value)
    if not negate:
        return lambda deal, favs, dlf: inrange(try_float(deal.get(key)), valuetuple)

    return lambda deal, favs, dlf: not inrange(try_float(deal.get(key)), valuetuple)

def _dlf_filter(fltr, filter_type):
    key = fltr.get('key')
    value = fltr.get('val')
    negate = fltr.get('negate', False)
    node = fltr.get('node')
    ignore = fltr.get('ignore_above')
    match_vals = fltr.get('match_vals')
    match_vals = match_vals if match_vals else [True]

    if filter_type == 'mongo':
        mongo_op = '$in' if not negate else '$nin'
        if node is not None:
            return {'dlf.{}.{}.state'.format(value, node): {mongo_op: match_vals}}
        else:
            return {'dlf.{}.%(node)s.state'.format(value): {mongo_op: match_vals}}

    if not negate:
        return lambda deal, favs, dlf: (dlf is not None and
                                        dlf.get_latest_oride(deal.get(key), value, deal,
                                                             node=node, ignore_above=ignore).state in match_vals)
    return lambda deal, favs, dlf: (dlf is not None and
                                    dlf.get_latest_oride(deal.get(key), value, deal,
                                                         node=node, ignore_above=ignore).state not in match_vals)


def _mismatch_filter(fltr, filter_type):
    key = fltr.get('key')
    value = fltr.get('val')
    negate = fltr.get('negate', False)
    node = fltr.get('node')
    ignore = fltr.get('ignore_above')

    if filter_type == 'mongo':
        mongo_op = '$in' if not negate else '$nin'
        if isinstance(value, (list, tuple)):
            if node is not None:
                return {"$or": [{'dlf.{}.{}.info'.format(val, node): {mongo_op: ['M']}} for val in value]}
            else:
                return {"$or": [{'dlf.{}.%(node)s.info'.format(val): {mongo_op: ['M']}} for val in value]}
        else:
            if node is not None:
                return {'dlf.{}.{}.info'.format(value, node): {mongo_op: ['M']}}
            else:
                return {'dlf.{}.%(node)s.info'.format(value): {mongo_op: ['M']}}

    if not negate:
        return lambda deal, favs, dlf: (dlf is not None and
                                        dlf.get_latest_oride(deal.get(key), value, deal,
                                                             node=node, ignore_above=ignore).info == 'M')

    return lambda deal, favs, dlf: (dlf is not None and
                                    dlf.get_latest_oride(deal.get(key), value, deal,
                                                         node=node, ignore_above=ignore).info != 'M')


def _date_in_range_filter(fltr,
                          filter_type):
    key = fltr.get('key')
    if fltr.get("range"):
        value_ = fltr.get('range')
    else:
        value_ = fltr.get('val')
    value = [epoch(value_[0]).as_xldate(), epoch(value_[1]).as_xldate()]
    negate = fltr.get('negate', False)

    if filter_type == 'mongo':
        query = {}
        if value[0] is not None:
            great_op = '$gt' if try_index(value, 2) else '$gte'
            great_op = great_op if not negate else NEGATION_MAP[great_op]
            query[great_op] = value[0]
        if value[1] is not None:
            less_op = '$lt' if try_index(value, 3) else '$lte'
            less_op = less_op if not negate else NEGATION_MAP[less_op]
            query[less_op] = value[1]
        return {key: query}

def _date_in_filter(fltr, filter_type):
    key = fltr.get('key')
    value = fltr.get('val')
    negate = fltr.get('negate', False)
    filter_format = fltr.get('type', 'xldate')
    # if filter_type == 'mongo':
    custom_filter = {}
    if 'CUSTOM' in value:
        custom_filter = _date_in_range_filter(fltr, filter_type)
        if len(value) == 1:
            return custom_filter
        index = value.index('CUSTOM')
        value.pop(index)
    option_values = ['ALL'] if 'ALL' in value else value
    range_tuples = [tuple(rng_from_prd_str(prd, filter_format, user_level=True)) for prd in option_values]
    if filter_type == 'mongo':
        first_iteration = True
        query = {}
        for r_tuple in range_tuples:
            rquery = {}
            if r_tuple[0] is not None:
                great_op = '$gt' if try_index(r_tuple, 2) else '$gte'
                great_op = great_op if not negate else NEGATION_MAP[great_op]
                rquery[great_op] = r_tuple[0]
            if r_tuple[1] is not None:
                less_op = '$lt' if try_index(r_tuple, 3) else '$lte'
                less_op = less_op if not negate else NEGATION_MAP[less_op]
                rquery[less_op] = r_tuple[1]
            if rquery:
                if first_iteration:
                    query["$or"] = []
                    first_iteration = False
                query["$or"].append({key: rquery})
        if custom_filter:
            query["$or"].append(custom_filter)
        return query
    # range_tuples = [tuple(rng_from_prd_str(prd, 'xldate')) for prd in value]   --> Commented this code as usage is not there.
    # if not negate:
    #     return lambda deal, favs, dlf: any(inrange(deal.get(key), range_tuple)
    #                                        for range_tuple in range_tuples)
    #
    # return lambda deal, favs, dlf: not any(inrange(deal.get(key), range_tuple)
    #                                        for range_tuple in range_tuples)

    return {}


def _range_match_filter(fltr, filter_type):
    key = fltr.get('key')
    value = fltr.get('val')
    rng = fltr.get('rng')
    negate = fltr.get('negate', False)

    if filter_type == 'mongo':
        return {}  # TODO: implement me

    if isinstance(rng, list):
        beg, end = rng
        if not negate:
            return lambda deal, favs, dlf: deal.get(key, [])[beg:end] == value
        return lambda deal, favs, dlf: deal.get(key, [])[beg:end] != value

    if not negate:
        return lambda deal, favs, dlf: try_index(deal.get(key, []), rng) == value
    return lambda deal, favs, dlf: try_index(deal.get(key, []), rng) != value


def _range_func_filter(fltr, filter_type):
    key = fltr.get('key')
    # value = fltr.get('val')
    # rng = fltr.get('rng')
    negate = fltr.get('negate', False)

    comparator_map = {
        '<': '$lt',
        '<=': '$lte',
        '>': '$gt',
        '>=': '$gte',
    }
    import copy
    if len(fltr['val'])>0:
        temp = []
        for val in fltr['val']:
            temp_filter = {'val': [val], 'key': fltr['key'], 'op': fltr['op']}
            temp.append(temp_filter)
        fltr = copy.deepcopy(temp)

    if filter_type == 'mongo':
        queries=[]
        for i in range(len(fltr)):
            query = {}
            key_filter = {}
            if fltr[i]['val'] is not None:
                value = fltr[i]['val']
                if isinstance(value[0], str) or isinstance(value[0], str):
                    if value[0][0] in comparator_map:
                        if '=' in value[0]:
                            op = comparator_map[value[0][0]+'=']
                            num = value[0][2:]
                        else:
                            op = comparator_map[value[0][0]]
                            num = value[0][1:]
                        op = op if not negate else NEGATION_MAP[op]
                        key_filter[op] = float(num)/100
                elif isinstance(value[0], list):
                    great_op = "$gte"
                    key_filter[great_op] = float(value[0][0])/100
                    less_op = "$lt"
                    key_filter[less_op] = float(value[0][1])/100
            query[key] = key_filter
            queries.append(query)

        if len(queries)>1:
            return {'$or': queries}
        else:
            return queries[0]

        # return {key:queries}  # TODO: implement me

    # if not negate:
    #         return lambda deal, favs, dlf: rng(deal.get(key, [False, 0, False, 0])) == value

    # return lambda deal, favs, dlf: rng(deal.get(key, [False, 0, False, 0])) != value


def _nested_in_range_filter(fltr, filter_type):
    key = fltr.get('key')
    value = fltr.get('val')
    negate = fltr.get('negate', False)

    if filter_type == 'mongo':
        query = {}
        if value[0] is not None:
            great_op = '$gt' if try_index(value, 2) else '$gte'
            great_op = great_op if not negate else NEGATION_MAP[great_op]
            query[great_op] = value[0]
        if value[1] is not None:
            less_op = '$lt' if try_index(value, 3) else '$lte'
            less_op = less_op if not negate else NEGATION_MAP[less_op]
            query[less_op] = value[1]
        return {'.'.join(key): query}

    valuetuple = tuple(value)
    if not negate:
        return lambda deal, favs, dlf: inrange(try_float(get_nested(deal, key)), valuetuple)

    return lambda deal, favs, dlf: not inrange(try_float(get_nested(deal, key)), valuetuple)


OP_MAP = {'in': _in_filter,
          'nested_in': _nested_in_filter,
          'bool': _bool_filter,
          'has': _has_filter,
          'range': _range_filter,
          'favs': _favs_filter,
          'dlf': _dlf_filter,
          'dlf_range': _dlf_range_filter,
          'in_fcst': _dlf_filter,  # NOTE: for backwards compatibility
          'mismatch': _mismatch_filter,
          'dateIn': _date_in_filter,
          "dateInRange": _date_in_range_filter,
          'range_match': _range_match_filter,
          'range_func': _range_func_filter,
          'nested_in_range': _nested_in_range_filter,
          'search': _search_filter}


def _valid_filter(desc):
    if not desc:
        return True
    try:
        fake_config = FAKE_CONFIG([], [])
        py_filt = parse_filters(desc, fake_config, filter_type='python', safe_mode=False)
    except KeyError:
        raise FilterError('filter operation doesnt exist, filter: {}, ops: {}'.format(desc, OP_MAP.keys()))

    try:
        py_filt({}, None, None)
    except Exception as e:
        raise FilterError('cant evaluate filter: {}, error: {}'.format(desc, e))
    return True


class FilterError(Exception):
    pass
