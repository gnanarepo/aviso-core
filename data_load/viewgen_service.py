from data_load.hierarchy_service import CoolerHierarchyService
from data_load.node_service import gimme_node_service
from data_load.splitter_service import CoolerSplitterService
from utils.common import cached_property


class CoolerViewGeneratorService:
    '''
    New viewgen
    The way it works now, is you have a config defining a reference service for each type of service we need
    (split/hier/group). Then in the main drilldown_config, you provide a key, and what service from above you want
    to use. Basically everything is now on a per drilldown basis

    '''

    def __init__(self, perspective=None, hier_asof=None, ):
        from domainmodel.datameta import Dataset
        ds = Dataset.getByNameAndStage('OppDS', None)
        self.viewgen_config = ds.models['common'].config.get('viewgen_config', {})

        # TODO until we get done debugging stuff
        # self.viewgen_config = config0
        # self.viewgen_config = config1
        # self.viewgen_config = config2
        # self.viewgen_config = config3
        # self.viewgen_config = config4
        # self.viewgen_config = config5

        # TODO: at some point we should validate the config to throw grumpy errors if incorrect
        # self.validate_config()

        # set up reference services
        split_config = self.viewgen_config.get('split_config', {})
        self.split_svcs = {split_key: CoolerSplitterService(config) for split_key, config in split_config.items()}

        # TODO prob get rid of .get cause i don't think this is optional
        hier_config = self.viewgen_config.get('hier_config', {})
        self.hier_svcs = {hier_key: CoolerHierarchyService(config, asof=hier_asof) for hier_key, config in
                          hier_config.items()}

        # TODO same as above i think
        node_config = self.viewgen_config.get('node_config', {})
        self.node_svcs = {node_key: gimme_node_service(config) for node_key, config in node_config.items()}

        # set up the drilldowns with their respective reference services
        dd_config = self.viewgen_config['drilldown_config']
        self.drilldowns = {}
        for dd_key, dd_cfg in dd_config.items():
            self.drilldowns[dd_key] = {
                'hier': self.hier_svcs.get(dd_cfg['hier']),
                'node': self.node_svcs.get(dd_cfg['node']),
                'split': self.split_svcs.get(dd_cfg['split']),
            }

        # deal with the perspective if there is one
        self.perspective = perspective

    @cached_property
    def drilldown_keys(self):
        # a list of the drilldown keys specified
        return list(self.drilldowns.keys())

    @cached_property
    def primary_pivot(self):
        # returns primary pivot if available in config else returns 0th entry of drilldown keys
        return self.viewgen_config.get('primary_pivot', self.drilldown_keys[0])

    @cached_property
    def unborn_fields(self):
        # a list of the fields required by the unborn model
        output = set()
        for dd_key, dd_svcs in self.drilldowns.items():
            output |= {self.viewgen_config['drilldown_config'][dd_key]['unborn_field']}
            output |= dd_svcs['split'].unborn_fields if dd_svcs['split'] is not None else set()
            output |= dd_svcs['hier'].unborn_fields | dd_svcs['node'].unborn_fields
        return output

    @cached_property
    def unborn_amount_fields(self):
        return {dd_key: config['unborn_field'] for dd_key, config in self.viewgen_config['drilldown_config'].items()}

    @cached_property
    def v2_split_fields(self):
        # a set of the unborn split fields for v2
        output = set()
        for dd_key, dd_svcs in self.drilldowns.items():
            output |= dd_svcs['split'].unborn_fields if dd_svcs['split'] is not None else set()
            output |= dd_svcs['hier'].unborn_fields | dd_svcs['node'].unborn_fields
        return output

    @cached_property
    def v2_amount_fields(self):
        # a set of the unborn amount fields for v2
        output = set()
        for dd_key, dd_svcs in self.drilldowns.items():
            output |= {self.viewgen_config['drilldown_config'][dd_key]['unborn_field']}
        return output

    @cached_property
    def drilldown_fields(self):
        # a reference of dd key to field indices of the hierarchy
        output = {}
        for dd_key, dd_svcs in self.drilldowns.items():
            hier_fields = dd_svcs['hier'].drilldown_fields
            output[dd_key] = dd_svcs['node'].add_node_fields(hier_fields)
        return output

    @cached_property
    def level_groupby_fields(self):
        # a version of the drilldown_fields with extra levels for unborn groupbys
        output = {}
        for dd_key, dd_svcs in self.drilldowns.items():
            # get hier fields, then add the grouper fields
            hier_grps = dd_svcs['hier'].groupby_fields
            output[dd_key] = dd_svcs['node'].add_node_groups(hier_grps)
            # add an empty tuple for top level summary for each drilldown
            output[dd_key][dd_key] = tuple()
        return output

    @cached_property
    def all_split_fields(self):
        # the res fields are always in dict form no matter what, for now, NEED COPY
        # but everything else depends on the split config
        output = CoolerSplitterService.res_fields.copy()
        for split_svc in self.split_svcs.values():
            output |= split_svc.all_fields
        return output

    def get_views(self, raw_record):
        '''
        Takes a flat version of a record, and returns a split version of the record with the appropriate
            segs attached.
        NOTE: This mutates the original record (because copying is inefficient), this should be safe,
            but something to be aware of

        Arguments:
            record {dict} - The flat version of the deal to be split. Has the 'res' and 'uip' keys for
                the results portion and uip portion respectively

        Output:
            {dict} - A singular dictionary for the whole record, with any split fields being sub dictionaries,
                keyed by the appropriate seg, also with a new key "__segs" that returns a list of all the
                segments involved in the deal
            ex:
            {
                'oppid': '0123ABC',
                'stage': 'Fake stage',
                'amount': {
                    'dd_key#node0': 100,
                    'dd_key#node1': 200,
                    'dd_key#node2': 300,
                    }
                '__segs': ['dd_key#node0', 'dd_key#node1', 'dd_key#node2'],
            }
        '''
        # step one, unnest the record
        record = raw_record.pop('res')
        record.update(raw_record['uip'])

        all_segs = set()
        update_dict = {field: {} for field in self.all_split_fields if field in record}
        # we're now doing this on a per drilldown basis
        for dd_key, dd_svcs in self.drilldowns.items():
            dd_split = dd_svcs['split']
            dd_hier = dd_svcs['hier']
            dd_node = dd_svcs['node']

            if dd_split is not None:
                split_dict, split_ids = dd_split.create_splits(record)
            else:
                split_dict, split_ids = CoolerSplitterService.dummy_splits(record)

            hier_segs = dd_hier.get_segs(record, split_ids)
            node_segs = dd_node.amend_nodes(record, hier_segs, dd_key)
            for nodes in node_segs.values():
                all_segs |= nodes

            split_info = CoolerSplitterService.dummy_info if dd_split is None else dd_split.split_info
            final_dict = self.resolve_splits(split_dict, split_info, node_segs)

            for field, field_dict in final_dict.items():
                for node, val in field_dict.items():
                    update_dict[field][node] = val
        record.update(update_dict)
        record['__segs'] = list(all_segs)
        if self.perspective is not None and self.perspective not in all_segs:
            return
        return record

    def resolve_splits(self, split_dict, split_info, node_dict):
        output = {fld: {} for fld in split_dict}
        all_segs = set()
        num_fields = set(split_dict) & split_info['num']
        str_fields = set(split_dict) & split_info['str']
        for split_id, nodes in node_dict.items():
            for node in nodes:
                for field in num_fields:
                    output[field][node] = self.try_add(output[field], split_dict[field], split_id, node)
                for field in str_fields:
                    output[field][node] = self.try_concat(output[field], split_dict[field], split_id, node)
        return output

    def try_add(self, out_dict, split_vals, split_id, node):
        try:
            return out_dict.get(node, 0) + split_vals[split_id]
        except:
            return None

    def try_concat(self, out_dict, split_vals, split_id, node):
        try:
            if out_dict[node]:
                return out_dict[node] + ' / ' + split_vals[split_id] if split_vals[split_id] and split_vals[
                    split_id] not in out_dict[node].split(' / ') else out_dict[node]
            else:
                return split_vals[split_id]
        except:
            return split_vals[split_id]

    def format_output(self, record, disp_map):
        output = {disp_map.get(key, key): val for key, val in record.items()}
        if self.perspective is None:
            return output
        for key, val in output.items():
            if isinstance(val, dict) and self.perspective in val:
                output[key] = val[self.perspective]
        output['__segs'] = [self.perspective]
        return output

    def split_unborn(self, record):
        '''
        Takes a crafted version of the record with the fields required to split + the relevant amount fields
        and returns list of tuples representing each split of the deal and it's corresponding hierarchy

        Arguments
            record {dict} - A dict of the relevant split fields (ratio_fld/split_fld) + the amount fields to split

        Output
            {list of tuples} - Each tuple contains the drilldown key, the split amount relevant to that drilldown
                and the padded hierarchy path required for use in the unborn
        '''
        output = []
        for dd_key, dd_svcs in self.drilldowns.items():
            dd_split = dd_svcs['split']
            dd_hier = dd_svcs['hier']
            dd_node = dd_svcs['node']

            # returns a dict of split fields all indexed by split_id
            if dd_split is not None:
                split_dict, split_ids = dd_split.create_unborn_splits(record, self.unborn_amount_fields[dd_key])
            else:
                split_dict, split_ids = CoolerSplitterService.dummy_unborn_splits(record,
                                                                                  self.unborn_amount_fields[dd_key])
            # given split_ids, makes a reference to hier nodes,
            # then appends any groupby/field info
            hier_paths = dd_hier.get_paths(record, split_ids)
            node_paths = dd_node.amend_paths(record, hier_paths)
            output += self.resolve_unborn(split_dict, node_paths, dd_key)
        return output

    def resolve_unborn(self, split_amounts, path_dict, dd_key):
        return [(dd_key, split_amounts[split_id], path) for split_id, path in path_dict.items()]

    def format_unborn_node(self, dd_key, drilldown_fields, group_vals):
        # top level summary
        dd_hier = self.drilldowns[dd_key]['hier']
        dd_node = self.drilldowns[dd_key]['node']
        return dd_node.format_unborn_node(dd_key, drilldown_fields, group_vals, dd_hier.max_depth)

    def validate_config(self):
        # ideally we'd validate it at set time
        # but we might have to settle for validating it afterwards
        return

    # TODO: These are just here so code doesn't break
    @property
    def foreign_fields(self):
        return set()
