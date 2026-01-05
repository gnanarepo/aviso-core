
def get_drilldowns(tenant_name, stack, viewgen_config):
    from ..data_load.tenants import fa_connection_strings
    fa_connection_string = fa_connection_strings(stack, tenant_name)
    from pymongo import MongoClient
    client = MongoClient(fa_connection_string)
    db = client[tenant_name.split('.')[0]+'_cache_'+stack]
    coll = db['drilldowns']
    import time
    as_of = int(time.time()) * 1000
    criteria = {'$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}
    drilldowns = list(coll.find(criteria, {'_id': 0}))
    parents = {record['node']: record['parent'] for record in drilldowns}
    all_nodes = {node: [node] for node in list(parents.keys())}
    pivots = list(set([node.split('#')[0] for node in parents]))
    for node in all_nodes:
        parent = parents.get(all_nodes[node][0])
        while parent is not None:
            all_nodes[node] = [parent] + all_nodes[node]
            parent = parents.get(all_nodes[node][0])
    for pivot in pivots:
        try:
            if viewgen_config['node_config'][viewgen_config['drilldown_config'][pivot]['node']].get('type') == 'top':
                delimiter = '##'
            else:
                delimiter = '#'
        except:
            delimiter = '#'
        if pivot + delimiter + "not_in_hier" not in all_nodes:
            all_nodes[pivot + delimiter + "not_in_hier"] = [pivot + delimiter + "!",
                                                            pivot + delimiter + "not_in_hier"]
    return all_nodes

def get_dd_list(viewgen_config, values, drilldowns, prune_prefix):
    drilldown_list = []
    split_fields = {}
    for split_pivot, split_pivot_config in viewgen_config['split_config'].items():
        for split_name, split_dtls in split_pivot_config.items():
            if type(split_dtls) != list:
                split_dtls = [split_dtls]
            for split_dtls_dict in split_dtls:
                for fld in split_dtls_dict['num_fields']:
                    if fld not in split_fields:
                        split_fields[fld] = {}
                for fld in split_dtls_dict['str_fields']:
                    if fld not in split_fields:
                        split_fields[fld] = {}
    for pivot, dd_dtls in viewgen_config['drilldown_config'].items():
        leaf_field = viewgen_config['hier_config'][dd_dtls['hier']].get('leaf_field', 'as_of_OwnerID')
        node_config = viewgen_config['node_config'][dd_dtls['node']]
        split_config = viewgen_config['split_config'].get(dd_dtls['split'], {})
        if split_config:
            val = check_val(values.get(prune_pfx(leaf_field, prune_prefix), {'unmapped': 'unmapped'}))
            val = val if val and type(val) == dict else {'unmapped': 'unmapped'}
            for identifier, leaf in val.items():
                for split_name, split_dtls in split_config.items():
                    if node_config.get('type') == 'top':
                        middle = []
                        for field in node_config['fields']:
                            fld_val = check_val(values.get(prune_pfx(field, prune_prefix), {'unmapped': 'N/A'}))
                            if type(fld_val) == dict:
                                fld_val = fld_val.get(identifier, 'N/A')
                            middle.append(field + '|' + fld_val)
                        middle = '||'.join(middle)
                        leaf_val = pivot + '#' + middle + '#' + leaf
                        curr_drilldowns = drilldowns.get(leaf_val, drilldowns[pivot + '##not_in_hier'])
                    else:
                        leaf_val = pivot + '#' + leaf
                        curr_drilldowns = drilldowns.get(leaf_val, drilldowns[pivot + '#not_in_hier'])
                    for drilldown in curr_drilldowns:
                        if drilldown not in drilldown_list:
                            drilldown_list.append(drilldown)
                    if type(split_dtls) != list:
                        split_dtls = [split_dtls]
                    for split_dtls_dict in split_dtls:
                        ratios = check_val(values.get(prune_pfx(split_dtls_dict['ratio_field'], prune_prefix), {}))
                        ratios = ratios if type(ratios) == dict else {}
                        ratios['unmapped'] = 1.0
                        for fld in split_dtls_dict['num_fields']:
                            fld_val = check_val(values.get(prune_pfx(fld, prune_prefix), 0.0))
                            for drilldown in curr_drilldowns:
                                if drilldown not in split_fields[fld]:
                                    split_fields[fld][drilldown] = 0.0
                                split_fields[fld][drilldown] += (ratios.get(identifier, 0.0) * fld_val)
                        for fld in split_dtls_dict['str_fields']:
                            fld_val = check_val(values.get(prune_pfx(fld, prune_prefix), 'N/A'))
                            fld_val = fld_val.get(identifier, 'N/A') if type(fld_val) == dict else fld_val
                            for drilldown in curr_drilldowns:
                                if drilldown not in split_fields[fld]:
                                    split_fields[fld][drilldown] = []
                                if fld_val not in split_fields[fld][drilldown]:
                                    split_fields[fld][drilldown].append(fld_val)
            for fld in split_fields:
                for drilldown, fld_val in split_fields[fld].items():
                    if type(fld_val) == list:
                        split_fields[fld][drilldown] = ' / '.join(fld_val)
        else:
            val = check_val(values.get(prune_pfx(leaf_field, prune_prefix), 'N/A'))
            if node_config.get('type') == 'top':
                middle = []
                for field in node_config['fields']:
                    fld_val = check_val(values.get(prune_pfx(field, prune_prefix), 'N/A'))
                    middle.append(field + '|' + fld_val)
                middle = '||'.join(middle)
                leaf_val = pivot + '#' + middle + '#' + val
                curr_drilldowns = drilldowns.get(leaf_val, drilldowns[pivot + '##not_in_hier'])
            else:
                leaf_val = pivot + '#' + val
                curr_drilldowns = drilldowns.get(leaf_val, drilldowns[pivot + '#not_in_hier'])
            for drilldown in curr_drilldowns:
                if drilldown not in drilldown_list:
                    drilldown_list.append(drilldown)
            for fld in split_fields:
                for drilldown in curr_drilldowns:
                    if drilldown not in split_fields[fld]:
                        split_fields[fld][drilldown] = check_val(values.get(prune_pfx(fld, prune_prefix), 'N/A'))
    return drilldown_list, split_fields

def prune_pfx(fld, prune_prefix):
    if not prune_prefix:
        return fld[6:] if fld.startswith('as_of_') and not fld.startswith('as_of_fv_') else fld
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

def check_val(val):
    from json import loads
    try:
        val = loads(val)
    except:
        pass
    return val
