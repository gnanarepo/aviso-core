import copy
from ..deal_result.hierarchy_service import HierarchyService
from aviso.framework.views import check_dimension_values
# from ..deal_result.result_Utils import add_prefix
from aviso.utils.misc_utils import try_float
from django.utils.functional import cached_property

def add_prefix(aFeature, default_prefix='latest_'):
    # Eswar claims this optimization gives a 5% performance improvement in model runtime.
    # Be careful to choose prefixes that don't invalidate this optimization.
    if aFeature[0] == 'a':
        if(aFeature[:9] == 'as_of_fv_' or
                aFeature[:11] == 'atbegin_fv_' or
                aFeature[:13] == 'athorizon_fv_' or
                aFeature[:6] == 'as_of_' or
                aFeature[:8] == 'atbegin_' or
                aFeature[:10] == 'athorizon_'):
            return aFeature
    else:
        if aFeature[:7] in ['latest_', 'frozen_']:
            return aFeature
    return default_prefix + aFeature

class SplitterService(object):

    '''
    Splitter Service
    The splitter service takes as its input a dictionary
    holding the dimensions (which can have dictionaries containing
    split information) and results and uses the splits config to output a
    list of dictionaries containing the splits. This service will be used by the unborn forecast
    model to split uip objects and by the load module to split up the results.
    '''

    def __init__(self, config):
        self._uip_prefix = "uip__"
        if config.get("uip_only", False):
            config = SplitterService.get_uip_only_config(config)
            self._uip_prefix = ""
        self.split_defs = config.get("split_defs", [])
        self.enabled = config.get("enabled", True)
        self.merge_delim = config.get("merge_delim", "~")
        self.amt_fld_merge_infos = {}
        self.all_dd_flds = {}
        for splt_def in self.split_defs:
            splt_key_fld = splt_def["split_key_fld"]
            self.all_dd_flds.update(splt_def.get("dep_dds", {}))
            for ratio_fld_dtls in splt_def["ratio_flds"].values():
                for out_amt_fld in ratio_fld_dtls["amt_flds"]:
                    if out_amt_fld not in self.amt_fld_merge_infos:
                        merge_info = {"key_flds": set()}
                        self.amt_fld_merge_infos[out_amt_fld] = merge_info
                    else:
                        merge_info = self.amt_fld_merge_infos[out_amt_fld]
                    merge_info["key_flds"].add(splt_key_fld)

    @property
    def source_fields(self):
        try:
            return self._splitter_src_flds
        except:
            self._splitter_src_flds = set()
            for split_def in self.split_defs:
                len_uip_prefix = len(self._uip_prefix)
                self._splitter_src_flds |= set([in_fld[len_uip_prefix:]
                                                for in_fld in split_def.get("dep_dds").values()
                                                if in_fld.startswith(self._uip_prefix)])
                for ratio_fld, ratio_fld_dtls in split_def["ratio_flds"].items():
                    if ratio_fld.startswith(self._uip_prefix):
                        self._splitter_src_flds.add(ratio_fld[len_uip_prefix:])
                    self._splitter_src_flds |= set([x[len_uip_prefix:] for x in ratio_fld_dtls["amt_flds"].values()
                                                    if x.startswith(self._uip_prefix)])
        return self._splitter_src_flds

    @staticmethod
    def get_uip_only_config(splitter_config):
        split_defs = []
        for split_def in splitter_config.get("split_defs", []):
            new_split_def = {
                "split_key_fld": split_def["split_key_fld"].split("__", 1)[1],
                "dep_dds": {
                    out_fld.split("__", 1)[1]: in_fld.split("__", 1)[1]
                    for out_fld, in_fld in split_def.get("dep_dds", {}).items()},
                "ratio_flds": {},
            }
            for ratio_fld, ratio_fld_dtls in split_def["ratio_flds"].items():
                new_split_def["ratio_flds"].update({
                    ratio_fld.split("__", 1)[1]: {"amt_flds": {
                        out_fld.split("__", 1)[1]: in_fld.split("__", 1)[1]
                        for out_fld, in_fld in ratio_fld_dtls["amt_flds"].items()
                        if in_fld.split("__", 1)[0] == "uip"}}})
            split_defs.append(new_split_def)
        return {"enabled": splitter_config.get("enabled", False),
                "split_defs": split_defs}

    @staticmethod
    def _get_val(obj, fld):
        parts = fld.split("__", 1)
        if len(parts) == 2:
            return obj[parts[0]][parts[1]]
        else:
            return obj[fld]

    @staticmethod
    def _try_get_val(obj, fld, default_val="N/A"):
        try:
            return SplitterService._get_val(obj, fld)
        except:
            return default_val

    @staticmethod
    def _set_val(obj, fld, val):
        parts = fld.split("__", 1)
        if len(parts) == 2:
            obj[parts[0]][parts[1]] = val
        else:
            obj[fld] = val

    @staticmethod
    def _set_vals(obj, fld_val_map):
        for fld, val in fld_val_map.items():
            SplitterService._set_val(obj, fld, val)

    @staticmethod
    def _has_val(obj, fld):
        try:
            SplitterService._get_val(obj, fld)
        except KeyError:
            return False
        return True

    @staticmethod
    def _get_copy(obj):
        """ deepcopy is too expensive, so do a one level copy... probably good enough!!! """
        return {k: copy.copy(v) for k, v in obj.items()}

    def get_splits(self, obj):
        """ """
        if not self.enabled:
            return [obj]

        splt_objs = [obj]
        for splt_def in self.split_defs:
            ratio_flds = splt_def["ratio_flds"]
            splt_key_fld = splt_def["split_key_fld"]
            dd_vals = {out_fld: SplitterService._get_val(obj, in_fld)
                       for out_fld, in_fld in splt_def.get("dep_dds", {}).items()}
            dd_vals = {k: v if isinstance(v, dict) else {} for k, v in dd_vals.items()}
            ratios = {}

            for ratio_fld in ratio_flds:
                ratio_vals = SplitterService._get_val(obj, ratio_fld)
                if isinstance(ratio_vals, dict):
                    for splt_key, ratio in ratio_vals.items():
                        try:
                            ratios[splt_key][ratio_fld] = ratio
                        except KeyError:
                            ratios[splt_key] = {ratio_fld: ratio}

            # Make sure that every ratio field has an explicit ratio for every split key.
            for splt_key, ratio_dtls in ratios.items():
                ratio_dtls.update({ratio_fld: 0.0 for ratio_fld
                                   in ratio_flds if ratio_fld not in ratio_dtls})

            new_splt_objs = []
            # got over all existing splits and split them further with values of new split
            for splt_obj in splt_objs:
                for splt_key, ratio_dtls in ratios.items():
                    # deep copy is too expensive so hoping this is enough!
                    new_splt_obj = SplitterService._get_copy(splt_obj)
                    for ratio_fld, ratio in ratio_dtls.items():
                        SplitterService._set_vals(
                            new_splt_obj,
                            {out_amt_fld: (ratio * try_float(SplitterService._get_val(splt_obj, in_amt_fld)))
                             for out_amt_fld, in_amt_fld in ratio_flds[ratio_fld]["amt_flds"].items()
                             if SplitterService._has_val(splt_obj, in_amt_fld)})
                    SplitterService._set_val(new_splt_obj, splt_key_fld, splt_key)
                    SplitterService._set_vals(
                        new_splt_obj,
                        {fld: val.get(splt_key, "N/A") for fld, val in dd_vals.items()})
                    new_splt_objs.append(new_splt_obj)
                if not ratios:
                    # If split ratios are empty, we will make a single split of the deal for the full amount or 0
                    # (depending on config). Also make sure dep dd fields are N/A.
                    new_splt_obj = SplitterService._get_copy(splt_obj)
                    SplitterService._set_val(new_splt_obj, splt_key_fld, "N/A")
                    SplitterService._set_vals(
                        new_splt_obj,
                        {fld: "N/A" for fld in dd_vals if not SplitterService._has_val(new_splt_obj, fld)})
                    new_splt_objs.append(new_splt_obj)
            if new_splt_objs:
                splt_objs = new_splt_objs
        return splt_objs

    def get_merge(self, obj_list, single_view=False):
        if single_view and len(obj_list) == 1:
            return obj_list[0]
        ret_obj = SplitterService._get_copy(obj_list[0])
        if not self.enabled or len(obj_list) == 1:
            return ret_obj
        SplitterService._set_vals(ret_obj,
                                  {out_fld: self.merge_delim.join(
                                      sorted(set([str(SplitterService._try_get_val(obj, out_fld))
                                                  for obj in obj_list])))
                                      for out_fld in self.all_dd_flds})
        all_split_key_vals = {}
        for amt_fld, merge_info in self.amt_fld_merge_infos.items():
            if SplitterService._has_val(ret_obj, amt_fld):
                splt_amts = {}
                for obj in obj_list:
                    split_key_vals = {}
                    for key_fld in merge_info["key_flds"]:
                        try:
                            key_vals = split_key_vals[key_fld]
                        except KeyError:
                            key_vals = set()
                            split_key_vals[key_fld] = key_vals
                        key_vals.add(SplitterService._try_get_val(obj, key_fld))
                    for key_fld, key_vals in split_key_vals.items():
                        try:
                            all_split_key_vals[key_fld] |= key_vals
                        except KeyError:
                            all_split_key_vals[key_fld] = set(key_vals)
                    merge_key = self.merge_delim.join([str(x) for x in split_key_vals.items()])
                    try:
                        splt_amts[merge_key] = try_float(SplitterService._get_val(obj, amt_fld))
                    except:
                        splt_amts[merge_key] = 0.0
                SplitterService._set_val(ret_obj, amt_fld, sum(splt_amts.values()))
        SplitterService._set_vals(ret_obj, {k: self.merge_delim.join([str(x) for x in sorted(v)])
                                            for k, v in all_split_key_vals.items()})
        return ret_obj


class ViewGeneratorService(object):

    def __init__(self,
                 allowed_dims='*',
                 perspective=None,
                 drilldown_config=None,
                 filter_fn=lambda x: True,
                 uip_fld_map=None,
                 splitter_config=None,
                 base_ts=None):
        """ Service that knows how to generate views and put leaves on opportunities or opp results.
        perspective: is a node identifier governing which views you get;
            None means return list of all view
        drilldown_config: this is the config which comes from params.general and is passed
            to the hierarchy service. It will usually have 'drilldowns' defined
        filter_fn: component->bool determines which components can be used in a view
            to filter components
        uip_fld_map: a dict: display fld -> standard field. Allows dimensions that don't use explicit
            time prefixes. (Note that this will get updated with splitter source fields and the
            user is responsible to making sure those fields appear on the objects being passed)
        splitter_config: dictionary of the splitter config
        base_ts: usually will be the as_of of the model, but keep in mind that the heirarchy
            service in "D" mode will use the terminal date of the opportunity to simulate
            frozen accounting

        return_flds: only fields matching these keys will be returned by the view generator
        """
        self.allowed_dims = allowed_dims
        self.perspective = perspective

        if self.perspective is not None:
            self.seg, self.seg_val = self.perspective.split('~::~', 1)
        else:
            self.seg, self.seg_val = None, None

        #why does it do this then modify uip_fld_map
        self.hier_svc = HierarchyService(drilldown_config, uip_fld_map)

        self.filter_fn = filter_fn

        self.uip_fld_map = uip_fld_map or {}

        if splitter_config is None:
            self.splitter = None
        else:
            self.splitter = SplitterService(config=splitter_config)
            self.uip_fld_map.update(
                {fld: add_prefix(fld, "as_of_") for fld in self.splitter.source_fields})

        self.base_ts = base_ts

    @property
    def foreign_fields(self):
        return set(self.hier_svc.foreign_fields)

    @property
    def source_fields(self):
        try:
            return self._source_fields
        except:
            self._source_fields = list(self.hier_svc.source_fields)
            if self.splitter:
                self._source_fields.extend(list(set(self.splitter.source_fields) - set(self._source_fields)))
        return self._source_fields

    def get_comps_with_paths(self, obj):
        if self.splitter is not None:
            components = [self.hier_svc.adorn_with_paths(x, ts=self.base_ts) for x in self.splitter.get_splits(obj)
                          if self.filter_fn(x)]
        else:
            components = [self.hier_svc.adorn_with_paths(obj, ts=self.base_ts)] if self.filter_fn(obj) else []
        return components

    def get_views(self, obj):
        """ """
        if self.splitter is not None:
            components = [self.hier_svc.adorn_with_leaves(x) for x in self.splitter.get_splits(obj)
                          if self.filter_fn(x)]
        else:
            components = [self.hier_svc.adorn_with_leaves(obj)] if self.filter_fn(obj) else []

        # TODO: For now, we only support checking dimensions in groupby mode... Is that ok?
        if self.hier_svc.implementation == 'G':
            components = [
                comp for comp in components
                if any([check_dimension_values(self.allowed_dims, *leaf)]
                       for leaf in comp['leaves'])
            ]

        if not components:
            return []

        terminal_date = self.hier_svc.get_freeze_date(components[0])
        ts = terminal_date or self.base_ts

        component_combos = self.hier_svc.partition_ancestors([x['leaves'] for x in components], ts=ts)

        [self.hier_svc.adorn_with_foreign_flds(comp, ts) for comp in components]

        view_list = []
        single_view = len(component_combos) == 1
        for contrib_components_idxs, seg_dict in component_combos.items():
            if self.perspective is not None:
                try:
                    if seg_dict[self.seg] != self.seg_val:
                        continue
                except:
                    continue
            if self.splitter is not None:
                view_list.append(self.splitter.get_merge([components[i] for i in contrib_components_idxs],
                                                         single_view))
            else:
                view_list.append(components[0])
            view_list[-1]['__segs'] = seg_dict

        return view_list



class CoolerSplitterService(object):
    '''
    '''
    res_fields = {
        'eACV', 'forecast', 'bs_forecast',
        'active_amount', 'won_amount', 'lost_amount',
        'existing_pipe_active_amount', 'existing_pipe_won_amount', 'existing_pipe_lost_amount',
        'new_pipe_active_amount', 'new_pipe_won_amount', 'new_pipe_lost_amount',
    }

    dummy_info = {'num': res_fields, 'str': set()}

    def __init__(self, config):
        self.primary_config = config['primary']
        self.other_configs = {i: cfg for i, cfg in enumerate(config.get('other', []))}

    @cached_property
    def ratio_fields(self):
        return {
            'primary': self.primary_config['ratio_field'],
            'other': {idx: other_config['ratio_field'] for idx, other_config in self.other_configs.items()}
        }

    @cached_property
    def split_fields(self):
        output = {}
        output['primary'] = {
            'num': set(self.primary_config.get('num_fields', [])),
            'str': set(self.primary_config.get('str_fields', [])),
        }
        output['other'] = {}
        for idx, other_config in self.other_configs.items():
            output['other'][idx] = {
                'num': set(other_config.get('num_fields', [])),
                'str': set(other_config.get('str_fields', [])),
            }
        return output

    @property
    def unborn_amount_fields(self):
        # for the new v2 unborn
        output = set()
        for split in self.splitters.values():
            output |= {split.unborn_amount_fld}
        return output

    @cached_property
    def split_info(self):
        output = {'num': set(), 'str': set()}
        output['num'] |= self.split_fields['primary']['num'] | self.res_fields
        output['str'] |= self.split_fields['primary']['str']
        for idx, other_fields in self.split_fields['other'].items():
            output['num'] |= other_fields['num']
            output['str'] |= other_fields['str']
        return output

    @cached_property
    def all_fields(self):
        output = self.split_fields['primary']['num'] | self.split_fields['primary']['str'] | self.res_fields
        for idx, other in self.split_fields['other'].items():
            output |= other['num'] | other['str']
        return output

    @cached_property
    def unborn_fields(self):
        return {self.ratio_fields['primary']} | set(self.ratio_fields['other'].values())

    def create_splits(self, record):
        output = {fld: {} for fld in self.all_fields if fld in record}
        # first we handle the primary
        p_ratios = record[self.ratio_fields['primary']]
        # if you don't have ratios, you get unmapped, tough luck
        if not p_ratios or p_ratios == 'N/A':
            p_ratios = {'unmapped': 1.}
        split_ids = set(p_ratios.keys())
        prim_num_fields = set(output) & (self.split_fields['primary']['num'] | self.res_fields)
        prim_str_fields = set(output) & self.split_fields['primary']['str']
        for split_id, ratio in p_ratios.items():
            for field in prim_num_fields:
                output[field][split_id] = self.try_num_val(record[field], ratio)
            for field in prim_str_fields:
                output[field][split_id] = self.try_str_val(record[field], split_id)
            #for field in set(output) & self.ignore_fields:
            #    output[field][split_id] = record[field]

        # other configs are assumed to not have to deal with stuff
        for idx, other_fld in self.ratio_fields['other'].items():
            o_ratios = record[other_fld]
            if not o_ratios:
                o_ratios = {'unmapped': 1.}
            o_num_fields = set(output) & self.split_fields['other'][idx]['num']
            o_str_fields = set(output) & self.split_fields['other'][idx]['str']
            for split_id, ratio in o_ratios.items():
                for field in o_num_fields:
                    output[field][split_id] = self.try_num_val(record[field], ratio)
                for field in o_str_fields:
                    output[field][split_id] = self.try_num_ratio(record[field], split_id)
        return output, split_ids

    def try_num_val(self, val, ratio):
        try:
            return val * ratio
        except:
            return None

    def try_str_val(self, val, split_id):
        if isinstance(val, str):
            return val
        try:
            return val[split_id]
        except:
            return 'N/A'

    def create_unborn_splits(self, record, amt_field):
        raw_amount = record[amt_field]
        amount = 0 if raw_amount == 'N/A' else raw_amount
        raw_ratios = record[self.ratio_fields['primary']]
        ratios = raw_ratios if raw_ratios else {'dummy': 1}
        output = {}
        for split_id, ratio in ratios.items():
            try:
                output[split_id] = amount * ratio
            except TypeError:
                output[split_id] = 0
        return output, set(ratios.keys())

    @classmethod
    def dummy_splits(cls, record):
        return {fld: {'dummy': record[fld]} for fld in cls.res_fields if fld in record}, None

    @staticmethod
    def dummy_unborn_splits(record, fld):
        raw_amount = record[fld]
        amount = 0 if raw_amount == 'N/A' else raw_amount
        return {'dummy': amount}, None
