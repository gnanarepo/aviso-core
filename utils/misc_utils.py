from datetime import datetime
from itertools import chain, combinations

from aviso.settings import sec_context
from operator import lt, gt, le, ge, itemgetter

range_lambdas = {}

CURR_TABLE = {'USD':'$', 'CAD': '$', 'GBP':u'\xa3','JPY':u'\xa5',
              'CNY': u'\xa5', 'EUR':u'\u20ac','INR':u'\u20b9','KRW':u'\u20a9','RAND':'R'
            }

MONTH_NAMES = ['January', 'Feburary', 'March', 'April', 'May', 'June',
               'July', 'August', 'September', 'October', 'November', 'December']

class CycleError(Exception):
    pass


class UndefinedError(Exception):
    pass


class BootstrapError(Exception):
    pass


def get_nested(input_dict, nesting, default=None):
    """
    get value (or dict) from nested dict by specifying the levels to go through
    """
    if not nesting:
        return input_dict
    try:

        for level in nesting[:-1]:
            input_dict = input_dict.get(level, {})

        return input_dict.get(nesting[-1], default)
    except AttributeError:
        # Not a completely nested dictionary
        return default

def set_nested(input_dict, nesting, value):
    """
    set value (or dict) in nested dict by specifying the path to traverse
    """
    if not nesting:
        input_dict.clear()
        return
    curr_dict = input_dict
    for level in nesting[:-1]:
        try:
            curr_dict = curr_dict[level]
        except KeyError:
            curr_dict[level] = {}
            curr_dict = curr_dict[level]
    curr_dict[nesting[-1]] = value

def pop_nested(input_dict, nesting, default=None):
    """
    pop value (or dict) from nested dict by specifying the levels to go through
    """
    if not nesting:
        return input_dict
    for level in nesting[:-1]:
        input_dict = input_dict.get(level, {})
    return input_dict.pop(nesting[-1], default)

def try_int(x, default=None):
    try:
        return int(x)
    except:
        return default

def powerset(iterable):
    """ Given an iterable, return a new iterable of all subsets."""
    xs = list(iterable)
    return chain.from_iterable(combinations(xs, n)
                               for n in range(len(xs) + 1))

def trimsort(iterable, distinct, key=None):
    """
    sort an iterable by key, and keep only the first unique item by distinct
    iterable = [{'a': 1, 'b': 2}, {'a': 1, 'b': 3}, {'a': 2, 'b': 4}, {'b': 5}]
    trimsort(iterable, 'a') --> [{'a': 1, 'b': 2}, {'a': 2, 'b': 4}]
    """
    sorted_iter = sorted(iterable, key=key)
    seen = set()
    for item in sorted_iter:
        try:
            distinct_val = itemgetter(distinct)(item)
        except (KeyError, IndexError):
            continue
        if distinct_val not in seen:
            yield item
        seen.add(distinct_val)

def format_val(val, fmt, c=None, ep_cache=None):
    """Format a value for display according to our standard conventions."""

    c = sec_context.details.get_config(category='forecast', config_name='tenant').get(
        'default_cur_format', 'USD')

    if fmt == 'amount':
        val = try_float(val)
        abs_val = abs(val)
        curr = CURR_TABLE.get(c, '$')
        neg_prefix = u'-' if val != abs_val else u''
        for (exp, letter) in [(9, 'B'), (6, 'M'), (3, 'K')]:
            if abs_val >= 10 ** exp:
                return neg_prefix + u'{}{:0,.1f}{}'.format(curr, abs_val / 10 ** exp, letter)
        else:
            return neg_prefix + u'{}{:0,.1f}'.format(curr, abs_val)
    elif fmt == 'longAmount':
        val = try_int(val)
        abs_val = abs(val)
        curr = CURR_TABLE.get(c, '$')
        neg_prefix = u'-' if val != abs_val else u''
        return neg_prefix + u'{}{:,}'.format(curr, val)
    elif fmt == 'excelDate':
        try:
            try:
                return ep_cache[val]
            except:
                from utils.date_utils import epoch
                ret_val = datetime.strftime(epoch(try_float(val, 'N/A')).as_datetime(), '%b %d')
                if ep_cache is not None:
                    ep_cache[val] = ret_val
                return ret_val
        except:
            return 'N/A'
    elif fmt == 'stringDate':
        try:
            from utils.date_utils import epoch
            return datetime.strftime(epoch(val).as_datetime(), '%Y-%m-%d')
        except:
            return 'N/A'
    elif fmt == 'none':
        return val
    elif fmt == 'calMonth':
        yyyy, mm = val[:4], int(val[4:])
        return '{month} {year}'.format(month=MONTH_NAMES[mm-1], year=yyyy)
    elif fmt == 'yyyymmdd':
        try:
            yyyy, mm, dd = int(val[:4]), int(val[4:6]), int(val[6:8])
            return '{month}/{day}/{year}'.format(month=mm,
                                                 day=dd,
                                                 year=yyyy)
        except:
            return 'N/A'
    elif fmt == 'percentage':
        val = try_float(val) * 100
        abs_val = abs(val)
        neg_prefix = '-' if val != abs_val else ''
        new_val = neg_prefix + '{:0,.2f}'.format(abs_val)
        return str(new_val) + "%"
    elif fmt == 'count':
        if val==None: val=0
        return int(val)
    else:
        return "'%s'" % val


def try_float(maybe_float, default=0.0):
    try:
        return float(maybe_float)
    except:
        return default

def try_index(listy, index, default=None):
    try:
        return listy[index]
    except:
        return default


def is_lead_service(service):
    if service and service=='leads':
        return True
    return False


def merge_dicts(*dictionaries):
    """
    combine dictionaries together
    """
    result = {}
    for dictionary in dictionaries:
        result.update(dictionary)
    return result

def iter_chunks(my_list, batch_size=5):
    start = 0
    more = True
    while more:
        end = start + batch_size
        if end < len(my_list):
            yield my_list[start:end]
            start = end
        else:
            more = False
            yield my_list[start:]

def flatten_to_list(*args, **kwargs):
    """
    takes arguments that may be strings or lists or lists of lists and returns flattened list
    ex:
    combine_to_list(1, 2, 3, [4, 5], 6, [[[7],8], 9])
    [1, 2, 3, 4, 5, 6, 7, 8, 9]
    """
    flat_list = kwargs.get('flat_list', [])
    for arg in args:
        if isinstance(arg, list):
            flatten_to_list(*arg, flat_list=flat_list)
        else:
            flat_list.append(arg)
    return flat_list

def contextualize_field(field, config, node=None, period=None, hier_aware=True):
    if hier_aware and field in config.hier_aware_fields:
        field = field + '.%(node)s'
    return field % {'node': node, 'period': period}

def use_df_for_dlf_rollup():
    tenant = sec_context.details
    return tenant.get_flag('deal_rollup', 'use_df_for_dlf_rollup', False)

def inrange(val, range_tuple):
    """Check if a value is in a range. A range tuple looks like
    (50,60,True,False) means 50<x<=60.
    Nones in the first 2 places mean -inf and +inf respectively.
    """
    range_tuple = tuple(range_tuple)
    try:
        return range_lambdas[range_tuple](val)
    except KeyError:
        pass
    try:
        left_op = gt if range_tuple[2] else ge
    except IndexError:
        left_op = gt
    try:
        right_op = lt if range_tuple[3] else le
    except IndexError:
        right_op = lt
    try:
        if range_tuple[1] is None:
            range_lambdas[range_tuple] = lambda x: left_op(x, range_tuple[0])
        else:
            range_lambdas[range_tuple] = lambda x: left_op(x, range_tuple[0]) and right_op(x, range_tuple[1])
        return range_lambdas[range_tuple](val)
    except IndexError:
        return False

def recursive_dict_iter(nest_dict):
    for k, v in nest_dict.items():
        if isinstance(v, dict):
            for k2, v2 in recursive_dict_iter(v):
                yield [k] + k2, v2
        else:
            yield [k], v

def get_nested_with_placeholder(input_dict, nesting, placeholder_dict, default=None):
    """
    get value (or dict) from nested dict by specifying the levels to go through
    """
    if not nesting:
        return input_dict
    try:

        for level in nesting[:-1]:
            input_dict = input_dict.get(level % placeholder_dict, {})

        return input_dict.get(nesting[-1], default)
    except AttributeError:
        # Not a completely nested dictionary
        return default

def use_dlf_fcst_coll_for_rollups():
    tenant = sec_context.details
    return tenant.get_flag('deal_rollup', 'use_dlf_fcst_coll', False)

def get_node_depth(node):
    """returns depth of node from top level"""
    # TODO: figure out for repeating nodes
    #if "*" not in node:
    #    return 0
    if node.endswith("Global") and "||" in node:
        return len(node.split("||"))
    elif "Global" not in node and "|" in node and "*" in node:
        return 1+node.split("|")[-1].count("*")
    elif "Global" not in node and "*" in node and "#" in node:
        return 1+node.split("#")[-1].count("*")
    elif "Global" not in node and "|" in node:
        return len(node.split("|"))
    elif "Global" not in node and "#" in node and "|" not in node and "*" not in node:
        from infra.read import check_children
        return len(node.split("#")) if check_children(node, drilldown=True) == 0 else 1
    else:
        return 0

def dict_of_dicts_gen(dct):
    for w, x in dct.items():
        for y, z in x.items():
            yield (w, y, z)

def top_sort(dag,
             nesting=[],
             ):
    """
    sort a DAG in topological order

    Arguments:
        dag {dict} -- dictionary representation of a DAG

    Keyword Arguments:
        nesting {list} -- path to source if the source in dag isnt the dictionary values
                          (default: {[]})

    Example:
        >>> dag = {'a': {'source': ['b', 'c']},
                   'b': {'source': ['d']},
                   'c': {'source': ['d']},
                   'd': {'source': []}}
        >>> top_sort(dag, ['source'])
            ['d', 'b', 'c', 'a']

    Raises:
        UndefinedError: source in DAG isnt defined
        CycleError: DAG has a cycle

    Returns:
        list -- [sorted keys of DAG]
    """
    items, states, order = set(dag), {}, []

    def dfs(item):
        states[item] = 'temp'
        for dependent in get_nested(dag, [item] + nesting, []):
            if dependent not in dag:
                raise UndefinedError(item, dependent)

            state = states.get(dependent)
            if state == 'perm':
                continue
            if state == 'temp':
                raise CycleError(dependent)

            items.discard(dependent)
            dfs(dependent)

        order.append(item)
        states[item] = 'perm'

    while items:
        dfs(items.pop())

    return order

def index_of(val, array):
    """
    returns index in presorted array where val would be inserted (to left)
    """
    low, high = 0, len(array) - 1
    while low <= high:
        mid = (high + low) // 2
        if array[mid] == val:
            return mid
        elif val < array[mid]:
            high = mid - 1
        else:
            low = mid + 1
    return low - 1 if low else low

def prune_pfx(fld):
    """ Helper function to prune the time-slicing prefix from field name"""
    if fld.startswith('latest_'):
        return fld[7:]
    elif fld.startswith('as_of_fv_'):
        return fld[9:]
    elif fld.startswith('as_of_'):
        return fld[6:]
    elif fld.startswith('frozen_'):
        return fld[7:]
    else:
        return fld
