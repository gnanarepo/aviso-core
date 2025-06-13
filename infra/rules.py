from infra.read import fetch_ancestors, fetch_children, fetch_descendants


def passes_configured_hierarchy_rules(as_of,
                           node,
                           rules,
                           drilldown=True,
                           db=None,
                           exclude_display_specific=False,
                           period=None,
                           boundary_dates=None):
    """
    check if a node passes some rules

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'
        rules {list} -- list of tuples of (rule, {rule_dtls})                   [('is_ancestor_of', {'related_nodes': ['00E0000FLN2C9I2']}),]

    Keyword Arguments:
        drilldown {bool} -- if True, check drilldown nodes instead of hierarchy
                            (default: {True})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if all rules pass
    """
    for complex_rule in rules:
        complex_rule_res = []
        for rule in complex_rule:
            rule, rule_dtls = rule
            if rule_dtls.get('display_specific', False) and exclude_display_specific:
                complex_rule_res.append(True)
                continue

            if rule_dtls.get('weekly_schema') and (not period or 'w' not in period.lower()):
                continue

            if rule_dtls.get('monthly_schema') and (not period or len(period) != 6 or 'q' in period.lower()):
                continue

            passes_rule = HIERARCHY_RULES[rule](as_of,
                                                node,
                                                rule_dtls.get('related_nodes', []),
                                                drilldown,
                                                db, period=period,boundary_dates=boundary_dates)
            if rule_dtls.get('negated', False):
                passes_rule = not passes_rule
            complex_rule_res.append(passes_rule)

        if not any(complex_rule_res):
            return False

    return True


def is_ancestor_of(as_of,
                   node,
                   related_nodes,
                   drilldown=True,
                   db=None,
                   period=None,
                   boundary_dates=None):
    """
    check if node is an ancestor of any of related_nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'
        related_nodes {list} -- nodes to check relationship with                ['00E0000FLN2C9I2']

    Keyword Arguments:
        drilldown {bool} -- if True, check drilldown nodes instead of hierarchy
                            (default: {True})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if node is an ancestor of a related_node
    """
    ancestors = fetch_ancestors(as_of, related_nodes, drilldown=drilldown, db=db, period=period,boundary_dates=boundary_dates).next()['ancestors']
    return node in ancestors


def is_descendant_of(as_of,
                     node,
                     related_nodes,
                     drilldown=True,
                     db=None,
                     period=None,
                     boundary_dates=None):
    """
    check if node is a descendant of any of related_nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'
        related_nodes {list} -- nodes to check relationship with                ['00E0000FLN2C9I2']

    Keyword Arguments:
        drilldown {bool} -- if True, check drilldown nodes instead of hierarchy
                            (default: {True})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if node is a descendant of a related_node
    """
    ancestors = fetch_ancestors(as_of, [node], drilldown=drilldown, db=db, period=period,boundary_dates=boundary_dates).next()['ancestors']
    return any(related_node in ancestors for related_node in related_nodes)

def is_descendant_of_inclusive(as_of,
                     node,
                     related_nodes,
                     drilldown=True,
                     db=None,
                     period=None,
                     boundary_dates=None):
    """
    check if node is a descendant of any of related_nodes (Inclusive of the parent)

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'
        related_nodes {list} -- nodes to check relationship with                ['00E0000FLN2C9I2']

    Keyword Arguments:
        drilldown {bool} -- if True, check drilldown nodes instead of hierarchy
                            (default: {True})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if node is a descendant of a related_node
    """
    ancestors = fetch_ancestors(as_of, [node], drilldown=drilldown, db=db, period=period,boundary_dates=boundary_dates).next().get('ancestors', []) + [node]
    return any(related_node in ancestors for related_node in related_nodes)


def is_leaf(as_of,
            node,
            _,
            drilldown=True,
            db=None,
            period=None,
            boundary_dates=None):
    """
    check if a node is a leaf node

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'

    Keyword Arguments:
        drilldown {bool} -- if True, check drilldown nodes instead of hierarchy
                            (default: {True})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if node is a leaf
    """
    children = list(fetch_children(as_of, [node], drilldown=drilldown, db=db, period=period,boundary_dates=boundary_dates))
    return not children


def is_first_line_manager(as_of,
                          node,
                          _,
                          drilldown=True,
                          db=None,
                          period=None,
                          boundary_dates=None):
    """
    check if a node is a first line manager (parent of only leaf nodes)

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'

    Keyword Arguments:
        drilldown {bool} -- if True, check drilldown nodes instead of hierarchy
                            (default: {True})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if node is a flm
    """
    try:
        _, grandchildren = fetch_descendants(as_of, [node], levels=2, drilldown=drilldown, db=db, period=period,boundary_dates=boundary_dates).next()['descendants']
        return not grandchildren
    except StopIteration:
        return False

def is_node_equal(as_of,
            node,
            related_nodes,
            drilldown=True,
            db=None,
            period=None,
            boundary_dates=None):
    """
    check if a node is in the configured related_nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'

    Keyword Arguments:
        drilldown {bool} -- if True, check drilldown nodes instead of hierarchy
                            (default: {True})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if node is a leaf
    """
    return node in related_nodes


HIERARCHY_RULES = {
    'is_ancestor_of': is_ancestor_of,
    'is_descendant_of': is_descendant_of,
    'is_descendant_of_inclusive' : is_descendant_of_inclusive,
    'is_leaf': is_leaf,
    'is_first_line_manager': is_first_line_manager,
    'is_node_equal': is_node_equal
}