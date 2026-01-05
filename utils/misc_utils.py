import logging
import time
from datetime import datetime
from itertools import chain, combinations

from aviso.settings import sec_context
from operator import lt, gt, le, ge, itemgetter

logger = logging.getLogger('aviso-core.%s' % __name__)
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
                from ..utils.date_utils import epoch
                ret_val = datetime.strftime(epoch(try_float(val, 'N/A')).as_datetime(), '%b %d')
                if ep_cache is not None:
                    ep_cache[val] = ret_val
                return ret_val
        except:
            return 'N/A'
    elif fmt == 'stringDate':
        try:
            from ..utils.date_utils import epoch
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
        if val is None: val=0
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

def merge_nested_dicts(*dicts):
    """
    merge many nested dictionaries together
    dicts later in list take precedence on overlaps
    """
    dicts = list(dicts)
    if len(dicts) == 1:
        return dicts.pop()
    while len(dicts) > 2:
        merge_nested_dicts(*dicts[-2:])
        dicts.pop()
    for key in dicts[-1]:
        if key in dicts[-2] and isinstance(dicts[-2][key], dict) and isinstance(dicts[-1][key], dict):
            merge_nested_dicts(dicts[-2][key], dicts[-1][key])
        else:
            dicts[-2][key] = dicts[-1][key]
    return dicts[-2]

def try_values(dicty, key):
    try:
        return dicty[key].values()
    except:
        return [dicty.get(key, key)]

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
            yield w, y, z

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

def update_rtfm_flag(td, last_exec_time=None, fastlane_sync_until=None, chipotle_last_exec_time=None, eoq_time=None, period=None, status='active'):
    t_fl = td.get_flag('molecule_status', 'rtfm')
    if last_exec_time:
        from ..utils.date_utils import epoch
        t_fl['last_execution_time'] = last_exec_time
        t_fl['ui_display_time'] = epoch().as_epoch()#Just to know when the chipotle was completed
        logger.info("UI time updated as %s" %t_fl['ui_display_time'])
    if fastlane_sync_until:
        t_fl['fastlane_sync_until'] = fastlane_sync_until
    if chipotle_last_exec_time:
        t_fl['chipotle_last_execution_time'] = chipotle_last_exec_time
    if eoq_time and period:
        t_fl[period+'_eoq_time'] = eoq_time
        if not t_fl[period+'_eoq_time']:
            t_fl[period+'_eoq_time'] = {}
        t_fl[period+'_eoq_time'] = eoq_time
    t_fl['status'] = status
    logger.info("Updating the rtfm flag : %s " % t_fl)
    td.set_flag('molecule_status', 'rtfm', t_fl)

def update_stats(params, status='started', event_type='all_caches', etl_time=None):
    from aviso.framework import tracer
    from aviso.settings import CNAME
    from gnana.events import EventBus
    EVENT_BUS = EventBus()
    if params.get('etl_time'):
        if 'start_time' not in params:
            params['start_time'] = int(time.time() * 1000)
        payload_data = {'service': CNAME,
                        'stack': CNAME,
                        'parent_task': 'caches',
                        'tenant': sec_context.name,
                        'run_type': params.get('run_type', 'daily'),
                        'etl_time': etl_time if etl_time else params.get('etl_time'),
                        'main_id': params.get('etl_time'),
                        'event_type': event_type,
                        'times': {'start_time' : params['start_time']},
                        'time_taken': 0,
                        'status': status,
                        'trace_id': tracer.trace}
        if status != 'started':
            payload_data['time_taken'] = (time.time() * 1000 - params['start_time']) / 1000
        if event_type == 'all_caches' and status in ['finished', 'skipped', 'veto']:
            payload_data['end_time'] = time.time() * 1000
            tdetails = sec_context.details
            if status in ['finished', 'veto']:
                mol_stat = tdetails.get_flag('molecule_status', 'rtfm')
                payload_data['UI_time'] = mol_stat.get('last_execution_time')
        if status == 'failed':
            payload_data['end_time'] = time.time() * 1000
        logger.info("payload data %s ", payload_data)
        EVENT_BUS.publish("$Stats", **payload_data)
    else:
        logger.info("stats were not published")

def _get_daily_trace_date():
    try:
        return sec_context.details.get_flag('molecule', 'daily_last_execution_date', 0)
    except:
        return None

def _set_daily_trace_date(value):
    try:
        return sec_context.details.set_flag('molecule', 'daily_last_execution_date', value)
    except:
        return None


def _get_snapshot_trace_datetime():
    try:
        return sec_context.details.get_flag('molecule', 'snapshot_trace_execution_date', 0)
    except:
        return None

def _set_snapshot_trace_datetime(value):
    try:
        return sec_context.details.set_flag('molecule', 'snapshot_trace_execution_date', value)
    except:
        return None

def check_trace(params, trace_name='chipotle_trace', report='all_caches'):
    retry = 0
    from aviso.framework import tracer
    t = sec_context.details
    caches_trace = None
    etl_time = params.get('etl_time')
    run_type = params.get('run_type', 'chipotle')
    is_eoq_run = params.get("is_eoq_run", False)
    if is_eoq_run:
        return True, tracer.trace
    wait_time = t.get_flag('chipotle', 'wait_time_min', 10)
    if run_type == 'daily':
        wait_time = 60
    while  retry <= wait_time:
        caches_trace = t.get_flag('molecule', trace_name, 'finished')
        if caches_trace in ['finished', tracer.trace]:
            t.set_flag('molecule', trace_name, tracer.trace)
            break
        elif trace_name == 'daily_trace':
            from ..utils.date_utils import epoch
            as_of = epoch().as_datetime()
            as_of_date = as_of.strftime("%Y-%m-%d")
            daily_run_date = _get_daily_trace_date()
            if not daily_run_date or (daily_run_date and daily_run_date < as_of_date):
                _set_daily_trace_date(as_of_date)
                t.set_flag('molecule', trace_name, tracer.trace)
                break
        elif trace_name == 'snapshot_trace':
            from ..utils.date_utils import epoch
            as_of = epoch().as_datetime()
            as_of_date = as_of.strftime("%Y-%m-%d")
            snapshot_trace_run_datetime = _get_snapshot_trace_datetime()
            if snapshot_trace_run_datetime:
                snapshot_run_date = epoch(_get_snapshot_trace_datetime()).as_datetime().strftime("%Y-%m-%d")
                if not snapshot_run_date or (snapshot_run_date and snapshot_run_date < as_of_date):
                    _set_snapshot_trace_datetime(epoch().as_epoch())
                    t.set_flag('molecule', trace_name, tracer.trace)
                    break
            else:
                _set_snapshot_trace_datetime(epoch().as_epoch())
                t.set_flag('molecule', trace_name, tracer.trace)
                break

        logger.info("%s are running by other tasks %s waiting for 1 min", report, caches_trace)
        time.sleep(60)
        retry += 1
    else:
        update_stats(params, 'skipped', report, etl_time)
        logger.error("waited for %s min, %s will taken care by next event, %s is %s", wait_time, report, trace_name, caches_trace)
        return False, caches_trace
    return True, tracer.trace

def update_trace(trace_name='chipotle_trace', trace_id=None, force_update=False):
    from aviso.framework import tracer
    t = sec_context.details
    if force_update:
        t.set_flag('molecule', trace_name, 'finished')
        return
    if not trace_id:
        trace_id = tracer.trace
    if t.get_flag('molecule', trace_name, 'finished') == trace_id:
        t.set_flag('molecule', trace_name, 'finished')
        if trace_name == 'snapshot_trace':
            from ..utils.date_utils import epoch
            _set_snapshot_trace_datetime(epoch().as_epoch())

def describe_trend(val):
    if val in [0, None]:
        return 'no_arrow', 'no difference to', 'ok'
    if val < 0:
        return 'down_arrow', 'below', 'bad'
    elif val > 0:
        return 'up_arrow', 'above', 'good'
    return 'no_arrow', 'no difference to', 'ok'

def mongify_field(field, no_replace_quote=False):
    if isinstance(field, str):
        return field.replace('.', '|') if no_replace_quote else field.replace('.', '|').replace("'", "#")
    if isinstance(field, int):
        return field
    if type(field) is list:
        return [mongify_field(x, no_replace_quote) for x in field]
    if type(field) is tuple:
        return tuple(mongify_field(x, no_replace_quote) for x in field)
    if type(field) is set:
        return {mongify_field(x, no_replace_quote) for x in field}

def unmongify_field(field):
    # return field.replace("#", "'").replace('|', '.')
    if isinstance(field, str):
        return field.replace("#", "'").replace('|', '.')
    if isinstance(field, int):
        return field
    if type(field) is list:
        return [unmongify_field(x) for x in field]
    if type(field) is tuple:
        return tuple(unmongify_field(x) for x in field)
    if type(field) is set:
        return {unmongify_field(x) for x in field}

def mongify_dict(dct):
    copy = {}
    for k, v in dct.iteritems():
        if type(v) is dict:
            copy.update({mongify_field(k): mongify_dict(v)})
        else:
            copy.update({mongify_field(k): mongify_field(v)})
    return copy

def unmongify_dict(d):
    copy = {}
    for k, v in d.iteritems():
        if type(v) is dict:
            copy.update({unmongify_field(k): unmongify_dict(v)})
        else:
            copy.update({unmongify_field(k): unmongify_field(v)})
    return copy

def create_nested(input_dict, attr_list):
    """
    create a value from nested input dict by specifying the levels to go through and write it to output dict
    """
    if not attr_list:
        return None
    try:


        val = get_nested(input_dict, attr_list)

        dict_by_level = val
        for key in reversed(attr_list):
            dict_by_level = {key: dict_by_level}

        return dict_by_level
    except AttributeError:
        # Not a completely nested dictionary
        return None

def adorn_dlf_fcst(deal, dlf_fcst_coll_schema):
    """
    Create deal level dlf_fcst collection required information
    """
    try:
        dlf_fcst_record = {'criteria': {'period': deal['period'],
                                        'opp_id': deal['opp_id']},
                                        'set': {}}
        for attr in dlf_fcst_coll_schema:
            attr_as_list = attr.split('.')
            # TODO: The limitation with the below code is if there is are 2 nestings i.e, dlf.in_fcst and dlf.fcst_flag,
            # the dlf.in_fcst will be removed and dlf.fcst_flag will be added
            if len(attr_as_list) > 0:
                dlf_fcst_record['set']= dict(dlf_fcst_record['set'], **create_nested( deal, attr_as_list))
            else:
                dlf_fcst_record['set'][attr_as_list[0]] = deal[attr_as_list[0]]

        return dlf_fcst_record
    except:
        return None

def get_last_execution_time():
    try:
        return sec_context.details.get_flag('molecule_status', 'rtfm', {}).get('chipotle_last_execution_time', 0)
    except:
        return 0
