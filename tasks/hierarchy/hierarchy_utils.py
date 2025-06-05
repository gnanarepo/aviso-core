import logging
from collections import namedtuple
from itertools import chain, zip_longest, repeat

from aviso.settings import sec_context
from utils.cache_utils import memcacheable
from utils.misc_utils import prune_pfx, try_float
from utils.time_series_utils import slice_timeseries

logger = logging.getLogger("gnana." + __name__)

NodeName = namedtuple('NodeName', ['seg', 'segtype', 'full_name', 'display_name'])


def construct_child_nodes(parent, name, dd_flds, is_rep=False):
    """
    gen exp of child nodes, will pad out repeated nodes to leaf level if is_rep is True
    """
    try:
        drilldowns = next(x for x in dd_flds if parent.startswith(x[0]))
    except StopIteration:
        drilldowns = dd_flds[0]
    try:

        if parent == '.~::~summary':
            back = []
        else:
            back = parent.split('~::~')[1].split('~')
        child_hier_levels = len(back) + 1
        if not is_rep:
            child_front = chain(drilldowns)
            child_back = chain(back, repeat(name))
            yield '~::~'.join(['~'.join(next(child_front) for x in range(child_hier_levels)),
                               '~'.join(next(child_back) for x in range(child_hier_levels))])
        else:
            for level in range(child_hier_levels, len(drilldowns) + 1):
                child_front = chain(drilldowns)
                child_back = chain(back, repeat(name))
                yield '~::~'.join(['~'.join(next(child_front) for x in range(level)),
                                   '~'.join(next(child_back) for x in range(level))])
    except IndexError:
        logger.warning('scary node being added, parent: %s, name: %s', parent, name)
        yield '~::~'.join([parent, name])


def pad_to_rep_level(node, hier_flds):
    """
    repeat a node to rep level of heirarchy
    """
    back = node.split('~::~')[1].split('~')
    name = back[-1]
    rep_levels = len(hier_flds)
    node_front = chain(hier_flds)
    node_back = chain(back, repeat(name))
    return '~::~'.join(['~'.join(next(node_front) for x in range(rep_levels)),
                        '~'.join(next(node_back) for x in range(rep_levels))
                        ])


@memcacheable('hierarchy_fields')
def get_hierarchy_fields():
    # disgusting hack to match  custom user ds levels
    #TODO: Need discussion: Understanding how domainmodel is working
    from domainmodel.datameta import Dataset, UIPIterator
    ds = Dataset.getByNameAndStage('CustomUserDS')
    if not ds:
        logger.warning('no CustomUserDS found, using default fields')
        return ['Level_0', 'Level_1', 'frozen_Owner']
    for idx, record in enumerate(UIPIterator(ds, {}, None)):
        if idx >= 1:
            break
        rec = record.featMap
        rec.pop('UIPCreatedDate')
        return sorted(rec.keys())


def get_ownerID_mapping(as_of=None):
    owner_dict = {}
    # TODO: Need discussion: Understanding how domainmodel is working
    from domainmodel.datameta import Dataset, UIPIterator
    ds = Dataset.getByNameAndStage('CustomUserDS')
    if not ds:
        return owner_dict
    if not as_of:
        as_of = float('inf')
    uip_recs = list(UIPIterator(ds, {}, None))
    if not len(uip_recs):
        raise Exception('Cannot build OwnerID mapping. No CustomUserDS records were found.')
    for record in uip_recs:
        rec = record.featMap
        rec.pop('UIPCreatedDate', None)
        node_string = '~'.join([slice_timeseries([t for t, _ in rec[key]],
                                                 [v for _, v in rec[key]],
                                                 as_of,
                                                 use_fv=True) for key in sorted(rec.keys())])
        owner_dict[record.ID] = node_string
        owner_dict[node_string] = record.ID

    return owner_dict


def get_all_owner_ids():
    owner_ids = set()
    # TODO: Need discussion: Understanding how domainmodel is working
    from domainmodel.datameta import Dataset, UIPIterator
    ds = Dataset.getByNameAndStage('CustomUserDS')
    if not ds:
        return owner_ids
    for record in UIPIterator(ds, {}, None):
        owner_ids.add(record.ID)
    return owner_ids


def make_valid_crm_id(crm_id):
    # TODO: make this not dumb
    if len(crm_id) == 18 and crm_id[:3] == '005':
        # This is a 18 character SFDC ID. We only want first 15 chars.
        return crm_id[:15]
    if (len(crm_id) < 15) or (crm_id[:3] != '005') or (crm_id in get_all_owner_ids()):
        # Can't make a valid ID.
        return None
    return crm_id


def explode_node(node, user_id=None):
    """ Explodes a self-describing node identifier into a dictionary with fields and vals."""
    ks, vs = [x.split('~') for x in node.split('~::~')]

    # dont include new mongo/aryaka style top hierarchy levels
    vs = chain([v for i, v in enumerate(vs) if i or try_float(ks[i][-1], None) is not None], repeat(vs[-1]))

    try:
        hier_levels = get_hierarchy_fields()
    except:
        hier_levels = ks

    return dict(zip(hier_levels, vs))


def trim_node(node):
    ks, vs = node.split('~::~')
    back_parts = vs.split('~')
    return '~::~'.join([ks, ''])


def node_values(node, hier_flds):
    _, vs = node.split('~::~')
    vs = vs.split('~')
    return [x[0] for x in (zip_longest(vs, hier_flds, fillvalue=vs[-1]))]


# TODO: review and make sure this handles all required cases
def in_hierarchy(hierarchy, node, root_dim=None):
    """
    checks if node is in hierarchy
    hierarchy is first part of root dimension
    in_hierarchy('as_of_OverlayOwner','as_of_OverlayOwner~frozen_Owner~::~Sandra Boyd~Austin Brannan') -> True
    """
    if not hierarchy:
        return True
    elif node.startswith(hierarchy):
        return True
    elif node == '.~::~summary':
        return root_dim is None or hierarchy == root_dim
    elif node.startswith('.'):
        return node.endswith(hierarchy)
    return False


def hier_switch(node, replacement, begin, end, length):
    """
    try to switch node to new hierarchy
    """
    try:
        front, back = node.split(begin)
        back_parts = back.split(end)
        back = end.join([replacement] + back_parts[length:])
        return begin.join([front, back])
    except Exception as e:
        logger.warning(e)
        return None


def check_parallel_nodes(nodes):
    """
    check if nodes are same in parallel hierarchies
    """
    if len(nodes) == 1:
        return True
    ancestors = set()
    hier_flds = get_hierarchy_fields()
    for node in nodes:
        ks, vs = node.split('~::~')
        ks, vs = ks.split('~'), vs.split('~')
        ancestors.add('~'.join(vs[i] for i, k in enumerate(ks) if prune_pfx(k) in hier_flds))
    return len(ancestors) == 1


def switch_hierarchy(origin_node, move_node):
    move_ks, move_vs = [x.split('~') for x in move_node.split('~::~')]
    origin_ks, origin_vs = [x.split('~') for x in origin_node.split('~::~')]
    try:
        hier_flds = [prune_pfx(x) for x in get_hierarchy_fields() + ['Owner']]
    except:
        hier_flds = max(move_ks, origin_ks)

    move_hier_parts = '~'.join(move_vs[i] for i, k in enumerate(move_ks) if prune_pfx(k) in hier_flds)
    origin_hier_parts = '~'.join(origin_vs[i] for i, k in enumerate(origin_ks) if prune_pfx(k) not in hier_flds)

    return '~::~'.join(['~'.join(move_ks), '~'.join([origin_hier_parts, move_hier_parts])])


def same_hierarchy(node, other_node):
    hier_flds = [prune_pfx(x) for x in get_hierarchy_fields() + ['Owner']]
    node_ks, node_vs = [x.split('~') for x in node.split('~::~')]
    other_ks, other_vs = [x.split('~') for x in other_node.split('~::~')]
    node_hier_parts = '~'.join(node_vs[i] for i, k in enumerate(node_ks) if prune_pfx(k) not in hier_flds)
    other_hier_parts = '~'.join(other_vs[i] for i, k in enumerate(other_ks) if prune_pfx(k) not in hier_flds)
    return node_hier_parts == other_hier_parts


def check_privilege(user, node):
    if node is None:
        return True
    try:
        user_nodes = get_user_permissions(user, 'results')
    except:
        user_nodes = ['*']
    node_pfx, node_sfx = node.split('~::~')
    return any((user_node == '*' or visible(user_node, node_sfx, node_pfx)) for user_node in user_nodes)


def visible(from_node, node_sfx, node_pfx=''):
    """Returns true if node is visible from from_node.
    Eg, If I'm at fL_0~::~EMEA, then EMEA~Benelux is visible to me.
    """
    try:
        return (node_sfx is None or from_node == '*' or
                from_node.endswith('summary') or
                (node_sfx.startswith(from_node.split('~::~')[1]) and (not node_pfx or
                                                                      node_pfx.startswith(from_node.split('~::~')[0]))))
    except IndexError:
        # there are several incompatible user configurations floating around
        # preferred: user.roles is a dict with key 'results' which is another dict, the keys of which are nodes
        # bad: user.roles is a list of lists, one of those lists starts with 'results' and the other items are nodes
        # bad: user.roles is a dict with key 'results' which is another dict, the keys of which are node prefixes
        # logger.warn('Cant evaluate visibility for node sfx: %s, for user node: %s, trying for node pfx: %s', node_sfx, from_node, node_pfx)
        user = sec_context.get_effective_user()
        # logger.warn('User details for unevaluable user" %s',  user.roles)
        return from_node in node_pfx
    except AttributeError:
        # if, eg. from_node is None
        return False


def can_see(from_node, target_node):
    """ Returns true if node is visible from from_node.
    TODO: Combine with .
    Eg, If I'm at fL_0~::~EMEA, then fL_0~fL_1~::~EMEA~Benelux is visible to me.
    """
    return (target_node is None or
            from_node.endswith('summary') or
            target_node.split('~::~')[1].startswith(from_node.split('~::~')[1]))


def is_leaf(node, wrd=None):
    """Approximates checkings whether a node is a leaf ."""
    if not wrd:
        wrd = get_wrd()
    return node.split('~::~')[0] in wrd


def should_hide(node):
    """
    returns true if node has __ in name and should be hidden
    """
    _, v = node.split('~::~')
    v_parts = v.split('~')
    if '__' in v_parts[-1]:
        return True
    return False


def node_and_parent(k, v):
    """Returns node and parent node"""
    v_parts = v.split('~')
    if v_parts[0] == 'summary':
        # Fake parent of '.~::~summary'
        parent_node = u'root~::~root'
    elif len(v_parts) == 1:
        parent_node = u'.~::~summary'
    else:
        parent_node = '~'.join(k.split('~')[:-1]) + '~::~' + '~'.join(v_parts[:-1])
    node = k + '~::~' + v
    return node, parent_node


def get_parent(node):
    """Given a node, return (my_node, parents_node)"""
    k, v = node.split('~::~')
    v_parts = v.split('~')

    if v_parts[0] == 'summary':
        # Fake parent of '.~::~summary'
        parent_node = u'root~::~root'
    elif len(v_parts) == 1:
        parent_node = u'.~::~summary'
    else:
        parent_node = '~'.join(k.split('~')[:-1]) + '~::~' + '~'.join(v_parts[:-1])

    return parent_node


# TODO this is disgusting
# TODO ask team for help to make this more better :)
def get_toplevel_parent(node):
    """Given a node, return (my_node, parents_node)"""
    k, v = node.split('~::~')
    v_parts = v.split('~')

    if v_parts[0] == 'summary':
        # Fake parent of '.~::~summary'
        parent_node = u'root~::~root'
    elif len(v_parts) == 1:
        parent_node = u'.~::~summary'
    else:
        rev = v_parts[:-1][::-1]

        if len(rev) != 1:
            gen = (i for i, val in enumerate(rev) if val != rev[i + 1] and rev[i + 1])
            idx = next(gen) + 1
            parent_node = '~'.join(k.split('~')[:-idx]) + '~::~' + '~'.join(v_parts[:-idx])
        else:
            parent_node = '~'.join(k.split('~')[:-1]) + '~::~' + '~'.join(v_parts[:-1])

    return parent_node


def get_ancestors(node, root_dim=None):
    """Returns ancestors of node"""
    ancestors = ['.~::~summary']
    if node == '.~::~summary':
        return ancestors
    seg_and_seg_val = node.split('~::~')
    try:
        flds, vals = [x.split('~') for x in seg_and_seg_val]
    except ValueError as e:
        raise Exception('Invalid leaf: %s. Msg: %s' % (seg_and_seg_val, e))
    return ancestors + ['{}~::~{}'.format('~'.join(flds[0:i + 1]), '~'.join(vals[0:i + 1]))
                        for i, _fld in enumerate(flds)]


def get_depth(node):
    """returns depth of node from top level"""
    # TODO: figure out for repeating nodes
    if node == '.~::~summary':
        return 0
    return 1 + node.split('~::~')[-1].count("~")


def level_diff(node1, node2):
    return get_depth(node1) - get_depth(node2)


def get_name(node, disp_map=None, global_mode=False):
    segs, name = node.rsplit('~', 1)
    if disp_map:
        name = disp_map.get(name, name)
    if not global_mode:
        return name
    elif node == '.~::~summary':
        return 'Global'
    return name


def make_display_name(node, display_mappings):
    # TODO: retire get_name, replace with this
    try:
        seg, segtype = node.split('~::~')
        mapped_segs = [display_mappings.get(node, node) for node in segtype.split('~')]
        full_name = '~'.join(mapped_segs)
        display_name = mapped_segs[-1]
    except ValueError:
        logger.warning('messed up node: %s', node)
        seg, segtype, full_name, display_name = node, node, node, node
    return NodeName(seg, segtype, full_name, display_name)


@memcacheable('weekly_report_dimensions')
def get_wrd():
    # TODO: Need discussion: Understanding how domainmodel is working
    from domainmodel.datameta import Dataset
    """
    get weekly report dimensions
    """
    try:
        return Dataset.getByNameAndStage(name='OppDS').params['general']['weekly_report_dimensions']
    except Exception as e:
        logger.warning(e)
        return ['frozen_Level_0~frozen_Level_1~frozen_Owner']


def get_drilldowns(node, hier_flds):
    try:
        return next(x for x in hier_flds if node.startswith(x[0]))
    except StopIteration:
        return hier_flds[0]


def get_user_permissions(user, app_section):
    # there are several user configurations floating around
    roles = user.roles['user']
    if isinstance(roles, dict):
        # preferred: user.roles is a dict with key 'results' which is another dict
        # the keys of which are nodes
        # TODO: handle this bad case
        # bad: user.roles is a dict with key 'results' which is another dict
        # the keys of which are node prefixes
        return roles.get(app_section, {}).keys()
    elif isinstance(roles, list):
        # bad: user.roles is a list of lists, one of those lists starts with 'results'
        # and the other items are nodes
        return next(x for x in roles if x[0] == app_section)[1:]


def get_root_dim(user):
    """
    get root dimension for a user, fall back to first weekly report dimension if wrongly configured
    """
    user_nodes = get_user_permissions(user, 'results')
    if '*' not in user_nodes:
        top_node = max(user_nodes, key=lambda x: (x.count('~'), len(x)))
        return top_node.split('~')[0]
    wrds = get_wrd()
    return wrds[0].split('~')[0]


def get_root_dims():
    return [wrd.split('~')[0] for wrd in get_wrd()]


def get_default_root_dim():
    wrds = get_wrd()
    return wrds[0].split('~')[0]


def move_permissions(node_mapping, debug=False):
    # TODO: Need discussion: Understanding how domainmodel is working
    from domainmodel.app import User
    for u in User.getAll():
        needs_update = False
        try:
            old_nodes = get_user_permissions(u, 'results')
        except Exception as e:
            logger.warning('bad user record: %s', u)
            old_nodes = []
        for old_node in old_nodes:
            if old_node in node_mapping:
                new_node = node_mapping[old_node]
                try:
                    # permissions is a list of:
                    # [app_section, node for read access, can write, can delegate]
                    perms = u.roles['user']['results'].pop(old_node)
                    perms[1] = new_node
                    u.roles['user']['results'][new_node] = perms
                except KeyError:
                    perms_idx, nodes = next((i, x) for i, x in enumerate(u.roles['user']) if x[0] == 'results')
                    node_idx = next(i for i, x in enumerate(nodes) if x == old_node)
                    nodes[node_idx] = new_node
                    u.roles['user'][perms_idx] = nodes
                needs_update = True
        if needs_update:
            if debug:
                logger.info('user permissions: %s', u)
            u.save()


def get_user_id(node, id_mappings, hier_flds=None):
    if hier_flds:
        drilldowns = get_drilldowns(node, hier_flds)
        node = pad_to_rep_level(node, drilldowns)
    node_end = node.split('~::~')[1]
    user_id = id_mappings.get(node_end)
    if user_id:
        return user_id, node_end
    # HACK: for mongo/aryaka the top level doesnt count..
    node_end = '~'.join(node_end.split('~')[1:])
    user_id = id_mappings.get(node_end, 'none')
    return user_id, node_end


def hier_level(node):
    return len(node.split('~::~')[0].split('~'))
