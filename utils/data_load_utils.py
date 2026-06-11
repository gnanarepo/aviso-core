from aviso.settings import sec_context

def get_drilldowns(tenant_name, stack, viewgen_config):
    # from gbm_apis.data_load.tenants import fa_connection_strings
    # fa_connection_string = fa_connection_strings(stack, tenant_name)
    # from pymongo import MongoClient
    # client = MongoClient(fa_connection_string)
    db = sec_context.tenant_db
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
            if viewgen_config['node_config'][viewgen_config['drilldown_config'][pivot]['node']].get('type') in ['top', 'bot']:
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
    
    # --- 1. Split Config Processing (Initialization) ---
    for split_pivot, split_pivot_config in viewgen_config.get('split_config', {}).items():
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

                    # ### ADDED FOR RATIOS: Initialize the ratio dictionary ###
                    ratio_key = fld + '_ratios'
                    if ratio_key not in split_fields:
                        split_fields[ratio_key] = {}

    # --- 2. Drilldown Processing ---
    for pivot, dd_dtls in viewgen_config['drilldown_config'].items():
        leaf_field = viewgen_config['hier_config'][dd_dtls['hier']].get('leaf_field', 'as_of_OwnerID')
        node_config = viewgen_config['node_config'][dd_dtls['node']]
        split_config = viewgen_config.get('split_config', {}).get(dd_dtls['split'], {})

        if split_config:
            # Get the leaf values (the split identifiers)
            val = check_val(values.get(prune_pfx(leaf_field, prune_prefix), {'unmapped': 'unmapped'}))
            if val == 'N/A' or not val:
                val = {'unmapped': 'unmapped'}
            elif not isinstance(val, dict):
                val = {val: val}

            leaf_field_val_keys = list(val.keys()) if isinstance(val, dict) else []
            for identifier, leaf in val.items():
                for split_name, split_dtls in split_config.items():
                    # --- Determine Drilldown Path (Hierarchy) ---
                    if node_config.get('type') in ('top', 'bot'):
                        middle = []
                        for field in node_config['fields']:
                            fld_val = check_val(values.get(prune_pfx(field, prune_prefix), {'unmapped': 'unmapped'}))
                            if type(fld_val) == dict:
                                fld_val = fld_val.get(identifier, 'unmapped')
                            middle.append(field + '|' + fld_val)
                        middle = '||'.join(middle)
                        leaf_val = pivot + '#' + middle + '#' + leaf
                        # Uses .get with explicit default list to prevent KeyError
                        #curr_drilldowns = drilldowns.get(leaf_val, drilldowns.get(pivot + '##not_in_hier', []))
                        curr_drilldowns = drilldowns.get(leaf_val, [pivot + '##unmapped', pivot + '##!']
                                                 if (leaf == 'N/A' or leaf == 'unmapped') else drilldowns.get(pivot + '##not_in_hier', []))
                    else:
                        leaf_val = pivot + '#' + leaf
                        # Uses .get with explicit default list to prevent KeyError
                        #curr_drilldowns = drilldowns.get(leaf_val, drilldowns.get(pivot + '#not_in_hier', []))
                        curr_drilldowns = drilldowns.get(leaf_val, [pivot + '#unmapped', pivot + '#!']
                                                 if (leaf == 'N/A' or leaf == 'unmapped') else drilldowns.get(pivot + '#not_in_hier', []))

                    # Update main drilldown list
                    for drilldown in curr_drilldowns:
                        if drilldown not in drilldown_list:
                            drilldown_list.append(drilldown)

                    if type(split_dtls) != list:
                        split_dtls = [split_dtls]

                    # --- Process Values and Ratios ---
                    for split_dtls_dict in split_dtls:
                        # Get ratios for this split
                        ratios = check_val(
                            values.get(prune_pfx(split_dtls_dict['ratio_field'], prune_prefix), {})
                        )
                        if not ratios or ratios == 'N/A':
                            ratios = {'unmapped': 1.0}
                            
                        # Current ratio for this specific identifier
                        if isinstance(ratios, dict):
                            current_ratio_val = ratios.get(identifier, 1.0)
                        else:
                            current_ratio_val = 1.0

                        # 1. Numeric Fields (Weighted Sum)
                        for fld in split_dtls_dict['num_fields']:
                            fld_val = check_val(values.get(prune_pfx(fld, prune_prefix), None))
                            # Preserve old GBM null behavior
                            if fld_val in [None, 'N/A', '']:
                                fld_val = None

                            for drilldown in curr_drilldowns:
                                if drilldown not in split_fields[fld]:
                                    split_fields[fld][drilldown] = None
                                
                                if fld_val is not None:
                                    existing_val = split_fields[fld][drilldown] or 0.0
                                    split_fields[fld][drilldown] = (
                                        existing_val + (current_ratio_val * fld_val)
                                    )
                                # split_fields[fld][drilldown] += (current_ratio_val * fld_val)

                            ## OLD GBM Record Matching for TopMost(Global#!) and BottomMost(Global#unmapped!)
                            ratio_field_keys = (
                                list(ratios.keys())
                                if isinstance(ratios, dict)
                                else []
                            )

                            for ratio_key in ratio_field_keys:
                                if (
                                    ratio_key not in leaf_field_val_keys and ratio_key != 'unmapped'
                                    and pivot + '##not_in_hier' not in curr_drilldowns
                                    and pivot + '#not_in_hier' not in curr_drilldowns 
                                    and node_config.get('type') not in ('top', 'bot')
                                ):
                                    extra_drilldowns = []

                                    unmapped_node = pivot + '#unmapped'
                                    pivot_node = pivot + '#!'

                                    # Avoid duplicate additions
                                    if unmapped_node not in curr_drilldowns:
                                        extra_drilldowns.append(unmapped_node)

                                    # OLD GBM parity:
                                    # unmapped branch ALSO contributes to #!
                                    if pivot_node not in curr_drilldowns:
                                        extra_drilldowns.append(pivot_node)

                                    extra_ratio_val = ratios.get(ratio_key, 1.0)

                                    if fld_val in [None, '', 'N/A']:
                                        continue

                                    for ed in extra_drilldowns:
                                        if ed not in split_fields[fld]:
                                            split_fields[fld][ed] = None

                                        existing_val = split_fields[fld][ed]

                                        if existing_val is None:
                                            existing_val = 0.0

                                        split_fields[fld][ed] = (
                                            existing_val + (extra_ratio_val * fld_val)
                                        )

                                        if ed not in drilldown_list:
                                            drilldown_list.append(ed)
                        
                        
                        # 2. String Fields (Concatenation + Ratio Aggregation)
                        for fld in split_dtls_dict['str_fields']:
                            fld_val = check_val(values.get(prune_pfx(fld, prune_prefix), 'N/A'))
                            fld_val = fld_val.get(identifier, 'N/A') if type(fld_val) == dict else fld_val  

                            ratio_key = fld + '_ratios'

                            for drilldown in curr_drilldowns:
                                # String Concatenation Logic
                                if drilldown not in split_fields[fld]:
                                    split_fields[fld][drilldown] = []
                                if fld_val not in split_fields[fld][drilldown]:
                                    split_fields[fld][drilldown].append(fld_val)

                                # ### ADDED FOR RATIOS: Aggregate Ratios per split value ###
                                if drilldown not in split_fields[ratio_key]:
                                    split_fields[ratio_key][drilldown] = {}

                                # Sum ratios for this specific string value (fld_val) on this node
                                current_ratio_sum = split_fields[ratio_key][drilldown].get(fld_val, 0.0)
                                split_fields[ratio_key][drilldown][fld_val] = current_ratio_sum + current_ratio_val

                            ratio_field_keys = (
                                list(ratios.keys())
                                if isinstance(ratios, dict)
                                else []
                            )

                            for ratio_field_key in ratio_field_keys:
                                if (
                                    ratio_field_key not in leaf_field_val_keys
                                    and ratio_field_key != 'unmapped'
                                    and pivot + '##not_in_hier' not in curr_drilldowns
                                    and pivot + '#not_in_hier' not in curr_drilldowns
                                    and node_config.get('type') not in ('top', 'bot')
                                ):

                                    extra_drilldowns = []

                                    unmapped_node = pivot + '#unmapped'
                                    pivot_node = pivot + '#!'

                                    # Avoid duplicate additions
                                    if unmapped_node not in curr_drilldowns:
                                        extra_drilldowns.append(unmapped_node)

                                    # OLD GBM parity:
                                    # unmapped branch ALSO contributes to #!
                                    if pivot_node not in curr_drilldowns:
                                        extra_drilldowns.append(pivot_node)
                                    
                                    extra_ratio_val = ratios.get(ratio_field_key, 1.0)

                                    if fld_val in [None, '']:
                                        continue

                                    for ed in extra_drilldowns:

                                        # -------- STRING FIELD --------
                                        if ed not in split_fields[fld]:
                                            split_fields[fld][ed] = []

                                        if fld_val not in split_fields[fld][ed]:
                                            split_fields[fld][ed].append(fld_val)

                                        # -------- RATIO FIELD --------
                                        if ed not in split_fields[ratio_key]:
                                            split_fields[ratio_key][ed] = {}

                                        existing_ratio = split_fields[ratio_key][ed].get(fld_val, 0.0)

                                        split_fields[ratio_key][ed][fld_val] = (
                                            existing_ratio + extra_ratio_val
                                        )

                                        if ed not in drilldown_list:
                                            drilldown_list.append(ed)

            # --- Finalize Fields (Convert Lists to Strings) ---
            for fld in split_fields:
                # We skip the ratio dictionaries here, as they should remain dicts
                if fld.endswith('_ratios'):
                    continue

                for drilldown, fld_val in split_fields[fld].items():
                    if type(fld_val) == list:
                        split_fields[fld][drilldown] = ' / '.join(fld_val)
        else:
            # --- Non-Split Logic (Default Handling) ---
            val = check_val(values.get(prune_pfx(leaf_field, prune_prefix), 'N/A'))
            if node_config.get('type') in ('top', 'bot'):
                middle = []
                for field in node_config['fields']:
                    fld_val = check_val(values.get(prune_pfx(field, prune_prefix), 'N/A'))
                    middle.append(field + '|' + fld_val)
                middle = '||'.join(middle)
                leaf_val = pivot + '#' + middle + '#' + val
                #curr_drilldowns = drilldowns.get(leaf_val, drilldowns.get(pivot + '##not_in_hier', []))
                curr_drilldowns = drilldowns.get(leaf_val, [pivot + '##unmapped', pivot + '##!']
                                                 if val == 'N/A' else drilldowns.get(pivot + '##not_in_hier', []))
            else:
                leaf_val = pivot + '#' + val
                #curr_drilldowns = drilldowns.get(leaf_val, drilldowns.get(pivot + '#not_in_hier', []))
                curr_drilldowns = drilldowns.get(leaf_val, [pivot + '#unmapped', pivot + '#!']
                                                 if val == 'N/A' else drilldowns.get(pivot + '#not_in_hier', []))

            for drilldown in curr_drilldowns:
                if drilldown not in drilldown_list:
                    drilldown_list.append(drilldown)

            for fld in split_fields:
                # Skip ratios in non-split logic (or handle if necessary, but usually empty/N/A)
                if fld.endswith('_ratios'):
                    continue

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
