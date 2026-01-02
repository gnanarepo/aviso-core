import os
from collections import defaultdict
from collections import namedtuple
import json
import logging
import random

from django.utils.functional import cached_property
from aviso import settings
from aviso.utils.dateUtils import epoch
from aviso.settings import sec_context, gnana_db
from urllib.parse import urlparse
from utils.cache_utils import get_mongo_client
logger = logging.getLogger('gnana.%s' % __name__)
from dotenv import load_dotenv
load_dotenv()
HIER_COLL = 'hierarchy'

def try_float(maybe_float, default=None):
    try:
        return float(maybe_float)
    except:
        return default

class DataHandler(object):
    # keep track of instances on the object class
    DH_CACHES = {}
    # TODO: currently we keep metadata about the last prepare time to know
    # when a refresh is needed. It would be nice to enhance this to also keep track
    # of what fields were cached so we can incrementally add more fields without
    # wasting memory with a new cache
    DH_TIMESTAMPS = {}

    def __init__(
            self,
            ds_name,
            lookup_fld,
            other_flds,
            force_refresh=False,
            **args
    ):
        self.ds_name = ds_name
        self.lookup_fld = lookup_fld
        self.other_flds = set(other_flds) if other_flds else None
        self.force_refresh = force_refresh

    def __getitem__(self, k):
        raise NotImplementedError("Error __getitem__() not implemented")

    def get(self, k, default=None):
        raise NotImplementedError("Error get() not implemented")

    def __contains__(self, k):
        raise NotImplementedError("Error __contains__() not implemented")

    def __len__(self):
        raise NotImplementedError("Error __len__() not implemented")

    @classmethod
    def get_data_handler(
            cls,
            cache_name,
            ds_name,
            lookup_fld,
            other_flds,
            force_refresh=False,
            **args
    ):
        tenant_name = sec_context.name
        # NOTE: leaving out prepare as we will probably phase it out soon anyway
        cache_name = '.'.join([tenant_name, ds_name, lookup_fld, cache_name])
        tdetails = sec_context.details
        last_update_time = tdetails.get_flag('data_update', ds_name, '')
        if (
                cache_name not in cls.DH_CACHES or
                cls.DH_TIMESTAMPS[cache_name] != last_update_time or force_refresh
        ):
            logger.info("CACHES:%s | TIMESTAMPS:%s",
                        cls.DH_CACHES, cls.DH_TIMESTAMPS)
            logger.info("Creating DataHandler for cache_name=%s dataset=%s lookup_fld=%s last_update_time=%s" % (
                cache_name, ds_name, lookup_fld, last_update_time))
            data_handler = cls(
                ds_name=ds_name,
                lookup_fld=lookup_fld,
                other_flds=other_flds,
                force_refresh=force_refresh,
                **args
            )
            cls.DH_CACHES[cache_name] = data_handler
            cls.DH_TIMESTAMPS[cache_name] = last_update_time
            logger.info("Created DataHandler for cache_name=%s dataset=%s lookup_fld=%s (count=%s)" % (
                cache_name, ds_name, lookup_fld, len(data_handler)))

        return cls.DH_CACHES[cache_name]


class MongoDBMemoryCache(DataHandler):

    def __init__(
            self,
            ds_name,
            lookup_fld,
            other_flds,
            force_refresh=False,
            prepare_ds=False,
            cache_historic_keys=False,
    ):
        super(MongoDBMemoryCache, self).__init__(
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            other_flds=other_flds,
            force_refresh=force_refresh,
        )
        self.prepare_ds = prepare_ds
        self.cache_historic_keys = cache_historic_keys
        self.data = {}
        self.uip_dict = dict(dataset=self.ds_name,
                             record_filter={}, record_range=[0])
        self.load_data()

    def retrieve_data(self):
        self.data.clear()
        etl_shell = sec_context.etl
        return etl_shell.uip('UIPIterator', **self.uip_dict)

    def load_data(self):
        record_iterator = self.retrieve_data()
        self.set_data(record_iterator)

    def set_data(self, record_iterator):
        for rec in record_iterator:
            # NO PREPARE IN GBM
            #             if self.prepare_ds:
            #                 rec.prepare(ds_inst)
            # Removing the unwanted fields from cache to save memory
            if self.other_flds:
                rec.featMap = {k: v for k, v in rec.featMap.items()
                               if k in self.other_flds}
            if self.lookup_fld == 'ID':
                try:
                    self.data[rec.ID].append(rec)
                except KeyError:
                    self.data[rec.ID] = [rec]
            else:
                if self.lookup_fld not in rec.featMap:
                    logger.error("ERROR: ID=%s from dataset=%s does not have required lookup field '%s'..."
                                 "will be ignored", rec.ID, self.ds_name, self.lookup_fld)
                    continue
                lookup_hist = rec.featMap[self.lookup_fld]
                if len(lookup_hist) > 1:
                    if not self.cache_historic_keys:
                        logger.warn("WARNING: ID=%s from dataset=%s has multiple lookup values in '%s'..."
                                    " will use last value", rec.ID, self.ds_name, self.lookup_fld)
                        lookup_hist = lookup_hist[-1:]
                for _, val in lookup_hist:
                    try:
                        self.data[val].append(rec)
                    except KeyError:
                        self.data[val] = [rec]

    def __getitem__(self, k):
        return self.data[k]

    def get(self, k, default=None):
        return self.data.get(k, default)

    def __contains__(self, k):
        return k in self.data

    def __len__(self):
        return len(self.data)



def merge_dicts(*dictionaries):
    """
    combine dictionaries together
    """
    result = {}
    for dictionary in dictionaries:
        result.update(dictionary)
    return result



def get_subdomain(address):
    url = urlparse(address)
    subdomain = url.hostname.split('.')[0]
    return subdomain

def deduce_cname_and_tenant():
    tenant = sec_context.details
    config = sec_context.get_microservice_config('etl_data_service')
    cname = config.get('CNAME', get_subdomain(config.get('host')))
    return cname, config.get('source_tenant')


def get_key(uniq, dataset="CustomUserDS", sandbox=None):
    if sandbox is None:
        sandbox = 'None'
    cname, tenant = deduce_cname_and_tenant()
    return ":".join([
        uniq,
        cname,
        tenant,
        dataset,
        sandbox,
    ])


class AcrossServiceUIPCacheHandler(MongoDBMemoryCache):
    def get_from_service(self):
        # normally we shoudn't get into this flow, if we went in then cache
        self.etl_shell.api("/etl/data/set_in_global_cache/CustomUserDS", None)
        return self.etl_shell.uip('UIPIterator', **self.uip_dict)

    def retrieve_data(self):
        key = get_key('recs', self.ds_name, None)
        self.etl_shell = sec_context.etl
        try:
            result = settings.global_cache.get(key)
            recs = []

            uipinterface = self.etl_shell.uip("getUIPRecordsObject")
            for rec in json.loads(result):
                recs.append(uipinterface(rec, for_sdk=True))
        except:
            logger.exception(
                "getting customuserds data from service which is not intended")
            "handles both none and json decode error"
            recs = self.get_from_service()
        return recs



class HierarchyService(object):
    cool_version = False

    def __init__(self, dd_cfg, uip_fld_map=None):
        """dd_cfg lives in params.general.drilldown_config and has the following keys:
            hs_impl:
                * G - (groupby) Leaves are self-describing. Entire path comes from leaf name.
                * D - (dataset) Use in-memory copy of a dataset (eg CustomUserDS)
                * A - (awesome mode) V2 using purpose-built hierarchy collection
            drilldowns:
                list of hierarchy description strings (each hierarchy description
                string is a tilde-separated list of fields used in the hierarchy)
        """
        self.dd_cfg = dd_cfg or {}
        self.implementation = self.dd_cfg.get("hs_impl", "G")
        self.drilldowns = self.dd_cfg.get('drilldowns', []) or []
        if self.drilldowns == ['.']:
            self.drilldowns = []
        self.dd_flds = [x.split('~') for x in self.drilldowns]
        self.uip_fld_map = uip_fld_map or {}
        self.ancestor_cache = {}
        self.paths_cache = {}
        self.foreign_fld_cache = {}
        self.epoch_cache = {}

        self._root_leaf = ('.', 'summary')

        if self.implementation == 'D':
            self.hier_ds_name = self.dd_cfg.get('hier_ds_name', 'CustomUserDS')
            self.hier_lookup_fld = self.dd_cfg.get('hier_lookup_fld', 'ID')
            self.freeze_date_fld = self.dd_cfg.get('freeze_date_fld', 'as_of_CloseDate_adj')
            self.dd_fld_cfg = self.dd_cfg["fld_cfg"]
            self.join_fld = self.dd_cfg["join_fld"]
            self._source_fields = self.source_fields
            self.requested_foreign_flds = set([fld for fld in self.uip_fld_map.values()
                                               if fld in self.foreign_fields])

        self.Leaf = self.get_leaf_type()

    @cached_property
    def ds_cache(self):
        return AcrossServiceUIPCacheHandler(ds_name=self.hier_ds_name,
                                            lookup_fld=self.hier_lookup_fld,
                                            other_flds=None)

    @cached_property
    def source_fields(self):
        '''
        get a tuple of all the fields which are required from the Opportunity
        in order to be able to provide hierarchy service
        Note that this function mutates the self.dd_fld_cfg by adding any missing entries
        '''
        src_flds = tuple(sorted(set([fld for dd in self.dd_flds for fld in dd])))
        if self.implementation == "D":
            all_flds = src_flds
            missing_flds = set(all_flds) - set(self.dd_fld_cfg)
            for fld in missing_flds:
                logger.warning("HierarchyService assuming following fields have src=S: %s", missing_flds)
                self.dd_fld_cfg[fld] = {'src': 'S'}
            for fld, fld_cfg in self.dd_fld_cfg.items():
                if 'src' not in fld_cfg:
                    fld_cfg['src'] = 'S'
            src_flds = tuple(sorted(set(
                [self.join_fld] +
                [fld for fld in all_flds if self.dd_fld_cfg[fld]['src'] == 'S'] +
                [self.dd_fld_cfg[fld]['fallback_fld'] for fld in all_flds
                 if self.dd_fld_cfg[fld]['src'] == 'F' and 'fallback_fld' in self.dd_fld_cfg[fld]])))
        return src_flds

    @cached_property
    def component_fields(self):
        '''
        returns a sorted list of all the unique fields used in the drilldowns
        useful for unborn model, etc.
        '''
        if self.implementation == 'A':
            raise NotImplementedError("HierarchyService.component_fields not implemented!")
        return tuple(sorted(set([fld for dd in self.dd_flds for fld in dd])))

    @cached_property
    def foreign_fields(self):
        '''
        returns a sorted list of all the unique fields used in the drilldowns
        useful for unborn model, etc.
        '''

        if self.implementation == 'G':
            return tuple()
        elif self.implementation == 'D':
            return tuple(sorted(set([
                fld for fld in ([fld for dd in self.dd_flds for fld in dd] + list(self.uip_fld_map.values()))
                if self.dd_fld_cfg.get(fld, {}).get('src') == 'F'])))
        else:
            raise NotImplementedError("HierarchyService.foreign_fields not implemented!")

    @cached_property
    def level_groupby_flds(self):
        '''
        A map from segment name (level id) to fields you need to groupby to get the segment values
        '''
        lvl_grpby_flds = {'.': tuple()}
        for dd in self.dd_flds:
            lvl_grpby_flds.update(
                {'~'.join(dd[:idx + 1]): tuple(dd[:idx + 1]) for idx in range(len(dd))}
            )
        return lvl_grpby_flds

    def get_freeze_date(self, comp):
        if self.implementation == 'D':
            uip = comp.get('uip', comp)
            if 'won_amount' in comp.get('res', {}):
                return try_float(uip.get(self.uip_fld_map.get(self.freeze_date_fld,
                                                              self.freeze_date_fld)))
        return None

    def adorn_with_leaves(self, comp):
        """
        Given a opportunity component, find the leaves on it by referring to opp level fields.
        Leaf type depends on implementation. See get_leaf_type for more info.
        """
        if not self.drilldowns:
            # TODO: What do we do about leaf in 'D' mode?
            comp['leaves'] = [self._root_leaf]
        else:
            uip = comp.get('uip', comp)
            if self.implementation == 'G':
                comp['leaves'] = tuple([
                    self.Leaf(seg=self.drilldowns[i],
                              seg_val='~'.join([uip.get(self.uip_fld_map.get(dd_fld, dd_fld), 'N/A')
                                                for dd_fld in dd]))
                    for i, dd in enumerate(self.dd_flds)])
            elif self.implementation == 'D':
                comp['leaves'] = tuple([self.Leaf(
                    hier_idx=i,
                    **{fld: uip.get(self.uip_fld_map.get(fld, fld), 'N/A') for fld in self.source_fields})
                    for i in range(len(self.dd_flds))])
            else:
                raise NotImplementedError()
        return comp

    def adorn_with_paths(self, comp, ts=None):
        '''
        Given an opportunity components, give it back with "path_tuple" key which has
        the tuple of field values which define all the possible standardized drilldowns
        (useful for use in unborn model)
        '''
        if not self.drilldowns:
            comp["path_tuple"] = tuple([])
        else:
            uip = comp.get('uip', comp)
            if self.implementation == "G":
                comp['path_tuple'] = tuple([uip.get(self.uip_fld_map.get(fld, fld), 'N/A')
                                            for fld in self.component_fields])
            elif self.implementation == "D":
                if ts is None:
                    raise ValueError('get_ancestors in D mode requires valid ts, but got %s' % ts)
                comp = self.adorn_with_leaves(comp)
                try:
                    comp["path_tuple"] = self.paths_cache[comp["leaves"]]
                except KeyError:
                    fld_vals = {}
                    for leaf in comp["leaves"]:
                        matching_recs = self.ds_cache.get(getattr(leaf, self.join_fld))

                        flds = self.dd_flds[leaf.hier_idx]

                        if matching_recs is not None and len(matching_recs):
                            hier_rec = matching_recs[0]
                            fld_vals.update({
                                fld: hier_rec.getAsOfDate(
                                    self.dd_fld_cfg[fld]['in_fld'],
                                    ts,
                                    first_value_on_or_after=True,
                                    NA_before_created_date=False,
                                    epoch_cache=self.epoch_cache)
                                if self.dd_fld_cfg[fld]['src'] == 'F'
                                else getattr(leaf, fld, self.dd_fld_cfg[fld].get('fallback_val', 'N/A'))
                                for fld in flds})
                        else:
                            for fld in flds:
                                if fld not in fld_vals:
                                    fld_cfg = self.dd_fld_cfg[fld]
                                    if fld_cfg['src'] == 'F':
                                        fld_vals[fld] = getattr(
                                            leaf,
                                            fld_cfg.get('fallback_fld', 'N/A'),
                                            fld_cfg.get('fallback_val', 'N/A'))
                                    else:
                                        fld_vals[fld] = getattr(leaf, fld, fld_cfg.get('fallback_val', 'N/A'))
                    path_tuple = tuple([fld_vals[fld] for fld in self.component_fields])
                    self.paths_cache[comp["leaves"]] = path_tuple
                    comp["path_tuple"] = path_tuple
        return comp

    def adorn_with_foreign_flds(self, comp, ts):
        if self.implementation == "D":
            if self.requested_foreign_flds:
                try:
                    join_val = comp["uip"].get(self.join_fld, "")
                    foreign_uip = self.foreign_fld_cache[join_val]
                except:
                    foreign_uip = {}
                    matching_recs = self.ds_cache.get(join_val)

                    if matching_recs is not None and len(matching_recs):
                        hier_rec = matching_recs[0]
                        foreign_uip.update({
                            fld: hier_rec.getAsOfDate(
                                self.dd_fld_cfg[fld]['in_fld'],
                                ts,
                                first_value_on_or_after=True,
                                NA_before_created_date=False,
                                epoch_cache=self.epoch_cache)
                            if self.dd_fld_cfg[fld]['src'] == 'F'
                            else comp["uip"].get(fld, self.dd_fld_cfg[fld].get('fallback_val', 'N/A'))
                            for fld in self.requested_foreign_flds})
                    else:
                        for fld in self.requested_foreign_flds:
                            if fld not in foreign_uip:
                                fld_cfg = self.dd_fld_cfg[fld]
                                if fld_cfg['src'] == 'F':
                                    foreign_uip[fld] = comp["uip"].get(
                                        fld_cfg.get('fallback_fld', 'N/A'),
                                        fld_cfg.get('fallback_val', 'N/A'))
                                else:
                                    foreign_uip[fld] = comp["uip"].get(fld, fld_cfg.get('fallback_val', 'N/A'))
                    self.foreign_fld_cache[join_val] = foreign_uip
                comp["uip"].update(foreign_uip)
        return comp

    def get_leaf_type(self):
        '''
        Create a Leaf type. The key thing about leafs is that they should not require
        loading any extra data to be identified, however there is no guarantee for a leaf
        to be self-describing the hierarchy paths associated with it. In fact, the
        desired behavior is that the paths associated with a leaf are decoupled and
        stored elsewhere so they can be updated independently.
        '''
        if self.implementation == 'G':
            return namedtuple('Leaf', ['seg', 'seg_val'])
        elif self.implementation == 'D':
            return namedtuple('Leaf', ['hier_idx'] + list(self.source_fields))

    def get_ancestors(self, leaf, ts=None):
        """
        Given a leaf type, return a list of node ids which are ancestors of that leaf
        the result will include the node_id for the leaf node will not include the node id
        for the root ('.', 'summary').
        e.g. given:
        In "G" mode:
        Leaf("BusinessType~frozen_Level_0~frozen_Level_1~frozen_Owner", "Subscription~EMEA~UK~Gareth Walsh")
        or in "D" mode:
        Leaf(<Gareth Walsh's ID>, 0, BusinesType="Subscription")
        Would return the following node_ids:
        {("BusinessType~frozen_Level_0~frozen_Level_1~frozen_Owner", "Subscription~EMEA~UK~Gareth Walsh"),
        ("BusinessType~frozen_Level_0~frozen_Level_1", "Subscription~EMEA~UK"),
        ("BusinessType~frozen_Level_0", "Subscription~EMEA"),
        ("BusinessType", "Subscription")}
        """
        if leaf == self._root_leaf:
            return set()
        try:
            return self.ancestor_cache[(leaf, ts)]
        except KeyError:
            if self.implementation == 'G':
                # Leaf is a tuple (fld1~fld2, val1~val2)
                try:
                    flds, vals = [x.split('~') for x in leaf]
                except ValueError as e:
                    raise Exception('Invalid leaf: %s. Msg: %s' % (leaf, str(e)))
            elif self.implementation == 'D':
                if ts is None:
                    raise ValueError('get_ancestors in D mode requires valid ts, but got %s' % ts)
                # Here a leaf is a tuple of field name, fld value, hierarchy number
                # For dd_flds  = [['fld1','fld2'],['fld3','fld4']]
                # example leaves are (fld2, val2, 0, {}), (fld4, val4, 1, {})
                matching_recs = self.ds_cache.get(getattr(leaf, self.join_fld))

                flds = self.dd_flds[leaf.hier_idx]

                if matching_recs is not None and len(matching_recs):
                    hier_rec = matching_recs[0]
                    vals = [hier_rec.getAsOfDate(self.dd_fld_cfg[fld]['in_fld'],
                                                 ts,
                                                 first_value_on_or_after=True,
                                                 NA_before_created_date=False,
                                                 epoch_cache=self.epoch_cache)
                            if self.dd_fld_cfg[fld]['src'] == 'F'
                            else getattr(leaf, fld, self.dd_fld_cfg[fld].get('fallback_val', 'N/A'))
                            for fld in flds]
                else:
                    vals = [getattr(leaf,
                                    self.dd_fld_cfg[fld].get('fallback_fld', 'N/A'),
                                    self.dd_fld_cfg[fld].get('fallback_val', 'N/A'))
                            if self.dd_fld_cfg[fld]['src'] == 'F'
                            else getattr(leaf, fld, self.dd_fld_cfg[fld].get('fallback_val', 'N/A'))
                            for fld in flds]
            else:
                raise NotImplementedError()
            self.ancestor_cache[(leaf, ts)] = {('~'.join(flds[0:i + 1]), '~'.join(vals[0:i + 1]))
                                               for i, _fld in enumerate(flds)}
            return self.ancestor_cache[(leaf, ts)]

    def partition_ancestors(self, leaves_by_comp, ts=None):
        """Given a list of leaves, return a list of tuples paritioning the ancestors
        of those leaves into shared and not-shared ancestors.

        e.g. A is the parent of B1 and B2. B1 is the parent of C1, B2 is the parent of C2.
              A
            /  \
           B1  B2
           |    |
           C1  C2
        Then parition_ancestors([C1,C2]) returns
        {{0}:{C1, B1},
        {1}:{C2, B2},
        {0,1}:{A}}

        Here, 0 and 1 are the indexes in the input list and A...C2 are node identifiers.
        For 'groupby' implementation, node identifiers are tuples of the form ('fld1~fld2', 'val1~val2')
        """
        if self.implementation in ['G', 'D']:
            if len(leaves_by_comp) == 1:
                seg_set = {self._root_leaf}
                for leaf in leaves_by_comp[0]:
                    seg_set |= self.get_ancestors(leaf, ts)
                return {frozenset([0]): dict(seg_set)}
            else:
                node_to_contrib_comp = defaultdict(list)
                node_to_contrib_comp[self._root_leaf] = [i for i, _ in enumerate(leaves_by_comp)]
                for comp, leaves in enumerate(leaves_by_comp):
                    for leaf in leaves:
                        for node in self.get_ancestors(leaf, ts):
                            node_to_contrib_comp[node].append(comp)

                ret_dict = defaultdict(dict)
                for (seg, seg_val), comps_list in node_to_contrib_comp.items():
                    ret_dict[frozenset(comps_list)][seg] = seg_val
                #return ret_dict.items()
                return ret_dict
        else:
            raise NotImplementedError()



class CoolerHierarchyService(object):
    '''
    New version of the hierarchy service, compatible with the new app.

    Arguments:
        dd_config {} - ?
        drilldowns - ?
    '''
    # TODO remove
    cool_version = True

    def __init__(self, config, asof=None):
        # TODO maybe use the caches
        self.ancestor_cache = {}
        self.paths_cache = {}
        self.period_cache = {}

        # we only use the leaf field if the owner isn't defined by the splits
        self.leaf_field = config.get('leaf_field', 'as_of_OwnerID')
        self.closedate_field = config.get('closedate_field', 'as_of_CloseDate_adj')
        self.asof = epoch(asof).as_epoch() if asof is not None else epoch().as_epoch()
        self.hier_with_ancestors = self.load_ancestors_from_db(self.asof)
        random.seed(13579)

        # we set the func up so we can save a little time on lookups
        if config.get('versioned', False):
            logger.info(f'Hierarchy service - running in versioned mode, hierarchy asof {self.asof}')
            self.timestamp_func = self.versioned_ts
        else:
            logger.info(f'Hierarchy service - running in unversioned mode, hierarchy asof {self.asof}')
            self.timestamp_func = self.unversioned_ts

    @cached_property
    def unborn_fields(self):
        return {self.leaf_field}

    @cached_property
    def drilldown_fields(self):
        # a list of stringified fields to use in the drilldowns list
        return tuple([str(i) for i in range(self.max_depth)])

    @cached_property
    def groupby_fields(self):
        output = {}
        for i in range(self.max_depth):
            output[str(i)] = tuple([str(k) for k in range(i + 1)])
        return output

    @cached_property
    def max_depth(self):
        # since we got rid of [not_in_hier, node], max depth ok to be 1
        try:
            max_len = max(len(x) for x in self.hier_with_ancestors.values())
        except ValueError:
            max_len = 0
        # +1 is for yourself
        return max_len + 1

    def get_segs(self, record, split_ids):
        # grab ts outside the loop even if we don't use it
        ts = self.timestamp_func(record)
        # no split mode
        if split_ids is None:
            leaf = record[self.leaf_field]
            return {'dummy': self.get_ancestors(leaf, ts)}
        # assuming there were splits
        output = {}
        for split_id in split_ids:
            try:
                if isinstance(record[self.leaf_field], dict):
                    leaf = record[self.leaf_field][split_id]
                else:
                    leaf = record[self.leaf_field]
                raw_ancestors = self.get_ancestors(leaf, ts)
            except (TypeError, KeyError):
                raw_ancestors = ['unmapped']
            output[split_id] = raw_ancestors
        return output

    def get_paths(self, record, split_ids):
        # grab ts outside the loop even if we don't use it
        ts = self.timestamp_func(record)
        # no split mode
        if split_ids is None:
            leaf = record[self.leaf_field]
            raw_ancestors = self.get_ancestors(leaf, ts)
            ancestors = raw_ancestors + [None] * (self.max_depth - len(raw_ancestors))
            return {'dummy': tuple(ancestors)}
        # assuming there were splits
        output = {}
        for split_id in split_ids:
            try:
                if isinstance(record[self.leaf_field], dict):
                    leaf = record[self.leaf_field][split_id]
                else:
                    leaf = record[self.leaf_field]
                raw_ancestors = self.get_ancestors(leaf, ts)
            # for unborn, split ids isn't always
            except (TypeError, KeyError):
                raw_ancestors = ['unmapped']
            output[split_id] = tuple(raw_ancestors) + (None,) * (self.max_depth - len(raw_ancestors))
        return output

    def versioned_ts(self, record):
        # TODO this is the old logic, probably shouldn't be this way, especially if we fix
        # the dumb key bullshit
        return epoch(record[self.closedate_field]).as_epoch() if 'won_amount' in record else None

    def unversioned_ts(self, record):
        # passthrough for unversioned so we can be a tad more efficient
        return None

    def get_ancestors(self, node, ts=None):
        try:
            if ts is None:
                return self.hier_with_ancestors[node] + [node]
            else:
                stack=os.environ['stack']
                tenant_name=os.environ['tenant_name']
                from data_load.tenants import fa_connection_strings
                fa_connection_string = fa_connection_strings(stack, tenant_name)
                from pymongo import MongoClient
                client =get_mongo_client(fa_connection_string)
                db=client[(tenant_name.replace('.io', '-io') if tenant_name.endswith('.io') else tenant_name.split('.')[0]) + '_cache_' + stack]
                anc_dict = {rec['node']: rec['ancestors'] for rec in self.fetch_ancestors(as_of=ts, nodes=[node],db=db)}
                return anc_dict[node] + [node]
        except KeyError:
            # this would be a lot easier if we consolidated where we handled nulls
            return ['unmapped'] if node == 'N/A' else ['not_in_hier']

    def load_ancestors_from_db(self, asof):
        stack=os.environ['stack']
        tenant_name=os.environ['tenant_name']
        from data_load.tenants import fa_connection_strings
        fa_connection_string = fa_connection_strings(stack, tenant_name)
        client = get_mongo_client(fa_connection_string)
        db=client[(tenant_name.replace('.io', '-io') if tenant_name.endswith('.io') else tenant_name.split('.')[0]) + '_cache_' + stack]
        db=db
        # TODO: figure out actual timestamps
        return {node['node']: node['ancestors'] for node in self.fetch_ancestors(asof,db=db)}#, db=db)}

    def fetch_ancestors(self,
                        as_of,
                        nodes=None,
                        include_hidden=False,
                        include_children=False,
                        db=None,
                        ):
        """
        fetch ancestors of many hierarchy nodes
        Arguments:
            as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        Keyword Arguments:
            nodes {list} -- nodes to find ancestors for, if None grabs all          ['0050000FLN2C9I2', ]
                            (default: {None})
            include_hidden {bool} -- if True, include hidden nodes
                                     (default: {False})
            include_children {bool} -- if True, fetch children of nodes in nodes
                                (default: {False})
            db {object} -- instance of tenant_db (default: {None})
                           if None, will create one
        Returns:
            generator -- generator of ({node: node, parent: parent, ancestors: [ancestors]})
        """
        # hier_collection = sec_context.tenant_db[HIER_COLL]
        hier_collection = db[HIER_COLL]
        criteria = {'$and': [
            {'$or': [{'from': None},
                     {'from': {'$lte': as_of}}]},
            {'$or': [{'to': None},
                     {'to': {'$gt': as_of}}]}
        ]}
        if not include_hidden:
            criteria['hidden'] = {'$ne': True}
        if nodes:
            node_criteria = {'node': {'$in': list(nodes)}}
            if include_children:
                node_criteria = {'$or': [node_criteria, {'parent': {'$in': list(nodes)}}]}
            match = merge_dicts(criteria, node_criteria)
        else:
            match = criteria
        lookup = {'from': HIER_COLL,
                  'startWith': '$parent',
                  'connectFromField': 'parent',
                  'connectToField': 'node',
                  'depthField': 'level',
                  'restrictSearchWithMatch': criteria,
                  'as': 'ancestors'}
        project = {'node': 1,
                   'parent': 1,
                   'label': 1,
                   '_id': 0,
                   'ancestors': {'$map': {'input': '$ancestors',
                                          'as': 'ancestor',
                                          'in': {'node': '$$ancestor.node',
                                                 'level': '$$ancestor.level'}}}}
        pipeline = [{'$match': match},
                    {'$graphLookup': lookup},
                    {'$project': project}]
        return self._anc_yielder(hier_collection.aggregate(pipeline))

    def _anc_yielder(self, nodes):
        for node in nodes:
            node['ancestors'] = [x['node'] for x in sorted(node['ancestors'], key=lambda x: x.get('level'), reverse=True)]
            yield node


