import logging

from utils.common import cached_property

logger = logging.getLogger('gnana.%s' % __name__)


class HierNodeService:
    '''
    Hierarchy only version of the node service.
    Service is mostly passthroughs of the hierarchy + master root node
    '''

    def __init__(self, config):
        pass

    @property
    def unborn_fields(self):
        return set()

    def add_node_fields(self, hier_fields):
        return hier_fields

    def add_node_groups(self, hier_groups):
        return hier_groups

    def amend_nodes(self, record, node_dict, dd_key):
        return {split_id: {f'{dd_key}#{node}' for node in nodes + ['!']} for split_id, nodes in node_dict.items()}

    def amend_paths(self, record, path_dict):
        return path_dict

    def format_unborn_node(self, dd_key, drilldown_fields, group_vals, n_hier):
        # for the master root
        if drilldown_fields is None:
            return f'!.{dd_key}#!'
        # for everything else
        hier = group_vals if len(drilldown_fields) == 1 else group_vals[-1]
        return f'!.{dd_key}#{hier}'


class TopNodeService:
    '''
    Deal top version of the node service.
    Given a hierarchy with a single root, it will insert the deal fields between the single root
        the rest of the hierarchy in the drilldown tree
    '''

    def __init__(self, config):
        # import here due to circular stuff
        from utils.result_utils import add_prefix
        self.fields = {add_prefix(field, 'as_of_'): field for field in config['fields']}

    @cached_property
    def unborn_fields(self):
        return set(self.fields)

    def add_node_fields(self, hier_fields):
        return (hier_fields[0],) + tuple(self.fields) + hier_fields[1:]

    def add_node_groups(self, hier_groups):
        # first the hier_root level
        output = {'0': ('0',)}
        # then add the fields
        for i, field in enumerate(self.fields):
            output[field] = ('0',) + tuple(self.fields)[:i + 1]
        for lvl, vals in hier_groups.items():
            if lvl == '0':
                continue
            output[lvl] = ('0',) + tuple(self.fields) + vals[1:]
        return output

    def generate_values(self, record, node_dict):
        output = {}
        for split_id in node_dict:
            split_vals = []
            for field in self.fields:
                # this should cover when the field is lookup based
                # sometimes the field is missing apparently
                try:
                    if isinstance(record[field], dict):
                        split_vals.append(record[field][split_id])
                    else:
                        split_vals.append(record[field])
                except KeyError:
                    # this worries me, cause it could mask other issues, but it is the best way to do it
                    split_vals.append('N/A')
            output[split_id] = [f'{self.fields[field]}|{val}' for field, val in zip(self.fields, split_vals)]
        return output

    def amend_nodes(self, record, node_dict, dd_key):
        output = {}
        group_ref = self.generate_values(record, node_dict)
        for split_id, nodes in node_dict.items():
            hier_root = nodes[0]
            # if your root is not_in_hier/unmapped, you're directly under !
            if hier_root in ['not_in_hier', 'unmapped']:
                output[split_id] = {f'{dd_key}##{hier_root}', f'{dd_key}##!'}
                continue
            # otherwise, stick the fields between your root and the rest of your hierarchy tree
            # add the dd_root
            output[split_id] = {f'{dd_key}##!'}
            # full fields for hier_root -> leaf
            full_group = '||'.join(group_ref[split_id])
            output[split_id] |= {f'{dd_key}#{full_group}#{node}' for node in nodes}
            # diminishing fields + root
            group_vals = ['||'.join(group_ref[split_id][:i]) for i in range(len(self.fields) + 1)]
            output[split_id] |= {f'{dd_key}#{group_val}#{hier_root}' for group_val in group_vals}
        return output

    def amend_paths(self, record, path_dict):
        group_ref = self.generate_values(record, path_dict)
        output = {}
        for split_id, path in path_dict.items():
            hier_root = path[0]
            # if your root is not_in_hier/unmapped, you get that root + all nulls
            # TODO: maybe it's ok to be nones here
            if hier_root in ['not_in_hier', 'unmapped']:
                output[split_id] = (hier_root,) + ('nullval',) * (len(self.fields) + len(path) - 1)
                continue
            output[split_id] = (hier_root,) + tuple(group_ref[split_id]) + path[1:]
        return output

    def format_unborn_node(self, dd_key, drilldown_fields, group_vals, n_hier):
        # no drilldown fields -> summary level
        if not drilldown_fields:
            return f'!.{dd_key}##!'
        # if only one, group_vals is the hierarchy root or not_in_hier/unmapped
        elif len(drilldown_fields) == 1:
            return f'!.{dd_key}##{group_vals}'
        # if we're in the "field" section, group_vals[0] is hier_root, everything else is fields
        if 1 < len(drilldown_fields) <= len(self.fields) + 1:
            hier_root = group_vals[0]
            group_str = '||'.join(group_vals[1:])
            return f'!.{dd_key}#{group_str}#{hier_root}'
        # else, full group str + however much hier
        else:
            hier_node = group_vals[-1]
            group_str = '||'.join(group_vals[1:len(self.fields) + 1])
            return f'!.{dd_key}#{group_str}#{hier_node}'


class BotNodeService:
    '''
    Deal bot version of the node service.
    Given a hierarchy with a single root, it will append the deal fields to the bottom of the
        hierarchy (with any padding required).
    '''

    def __init__(self, config):
        from utils.result_utils import add_prefix
        self.fields = {add_prefix(field, 'as_of_'): field for field in config['fields']}

    @cached_property
    def unborn_fields(self):
        return set(self.fields)

    def add_node_fields(self, hier_fields):
        return hier_fields + tuple(self.fields)

    def add_node_groups(self, hier_groups):
        output = {lvl: vals for lvl, vals in hier_groups.items()}
        max_group = hier_groups[max(hier_groups, key=lambda x: len(hier_groups[x]))]
        for i, field in enumerate(self.fields):
            output[field] = max_group + tuple(list(self.fields)[:i + 1])
        return output

    def generate_values(self, record, node_dict):
        output = {}
        for split_id in node_dict:
            split_vals = []
            for field in self.fields:
                try:
                    if isinstance(record[field], dict):
                        split_vals.append(record[field][split_id])
                    else:
                        split_vals.append(record[field])
                except KeyError:
                    split_vals.append('N/A')
            output[split_id] = [f'{self.fields[field]}|{val}' for field, val in zip(self.fields, split_vals)]
        return output

    def amend_nodes(self, record, node_dict, dd_key):
        output = {}
        group_ref = self.generate_values(record, node_dict)
        for split_id, nodes in node_dict.items():
            hier_root = nodes[0]
            if hier_root in ['not_in_hier', 'unmapped']:
                output[split_id] = {f'{dd_key}##{hier_root}', f'{dd_key}##!'}
                continue
            output[split_id] = {f'{dd_key}##{node}' for node in nodes + ['!']}
            group_vals = ['||'.join(group_ref[split_id][:i]) for i in range(1, len(self.fields) + 1)]
            output[split_id] |= {f'{dd_key}#{group_val}#{nodes[-1]}' for group_val in group_vals}
        return output

    def amend_paths(self, record, path_dict):
        output = {}
        group_ref = self.generate_values(record, path_dict)
        # TODO: can't be (x, y, None, 'a|b')
        for split_id, path in path_dict.items():
            hier_root = path[0]
            if hier_root in ['not_in_hier', 'unmapped']:
                output[split_id] = (hier_root,) + ('nullval',) * (len(self.fields) + len(path) - 1)
                continue
            fixed_path = tuple(['nullval' if node is None else node for node in path])
            output[split_id] = fixed_path + tuple(group_ref[split_id])
        return output

    def format_unborn_node(self, dd_key, drilldown_fields, group_vals, n_hier):
        if not drilldown_fields:
            return f'!.{dd_key}##!'
        elif len(drilldown_fields) == 1:
            return f'!.{dd_key}##{group_vals}'
        elif 1 < len(drilldown_fields) <= n_hier:
            return f'!.{dd_key}##{group_vals[-1]}'
        else:
            hier_node = next(node for node in group_vals[n_hier - 1::-1] if node != 'nullval')
            group_str = '||'.join(group_vals[n_hier:])
            return f'!.{dd_key}#{group_str}#{hier_node}'


def gimme_node_service(config):
    node_type = config.get('type')
    if node_type is None:
        return HierNodeService(config)
    elif node_type == 'top':
        return TopNodeService(config)
    elif node_type == 'bot':
        return BotNodeService(config)
    else:
        logger.error('Unsupported node type')
        raise Exception('Please configure with supported type')
