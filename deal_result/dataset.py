from collections import namedtuple
import logging
import datetime
import pytz
from collections import defaultdict

from aviso.settings import sec_context, gnana_db
from aviso.utils import  is_true,dateUtils
from aviso.utils.dateUtils import   TimeHorizon, current_period,datetime2xl,get_a_date_time_as_float_some_how

from aviso.utils.dateUtils import get_quarter_from_cache_key,epoch

from deal_result.results import ModelRunDetails,IndividualResult,CompressedIndividualResultModel,RevenueIndividualResult,GroupResult
from domainmodel.model import Model
from utils.config_utils import config_pattern_expansion
from django.http.response import HttpResponseNotFound
from aviso.domainmodel.datameta import DSClass
import pymongo
import sys
import traceback
import re
import json
import os
import time
import memcache
# from analyticengine.forecast import Forecast
from analyticengine.forecast2 import Forecast2
# from analyticengine.forecast5 import Forecast5
from analyticengine.unborn_base import UnbornBaseModel
from analyticengine.unborn_base_zerodawn import UnbornBaseModelZeroDawn
from analyticengine.forecast2_no_ads import Forecast2_no_ds

#gnana settings may be removed later by waqas
from aviso.framework.postgresdb import GnanaPostgresDB

gnana_db2 = GnanaPostgresDB()
DEBUG='False'
memcache_server = 'localhost'
GLOBAL_CACHE_URL = os.environ.get('GLOBAL_CACHE_URL', None)
class RedisWithRetry:
    def __init__(self, client, retries=3, backoff=1):
        self.client = client
        self.retries = retries
        self.backoff = backoff

    def __getattr__(self, name):
        func = getattr(self.client, name)
        def wrapper(*args, **kwargs):
            for attempt in range(1, self.retries + 1):
                try:
                    return func(*args, **kwargs)
                except (redis.ConnectionError, redis.TimeoutError):
                    if attempt == self.retries:
                        raise
                    time.sleep(self.backoff * (2 ** (attempt - 1)))
        return wrapper

if DEBUG:
    global_cache = memcache.Client([memcache_server], server_max_value_length=30 * 1024 * 1024)
elif GLOBAL_CACHE_URL:
    import redis
    client = redis.from_url(GLOBAL_CACHE_URL, socket_keepalive=True, socket_timeout=300,
                            retry_on_timeout=True)
    global_cache = RedisWithRetry(client, retries=3)
else:
    global_cache = None






MAX_CREATED_DATE = 1000000
CNAME = os.environ.get('GNANA_CNAME', "localhost")
# DO NOT REMOVE THE BELOW UNUSED IMPORTS, THEY ARE REQUIRED FOR EVAL
old_value_delay = 10.0 / 60 / 60 / 24   # 5 seconds
default_stage = "__default__"
logger = logging.getLogger('gnana.%s' % __name__)
ALGORITHM_PARAMS = 'algorithm_params'
attrset_names = ('file_types', 'models', 'params', 'fields', 'maps')

basestring=str
long=int
FieldTypeTypes = {
    'string',
    'number',
    'date',
    'usage'
}
FieldType = namedtuple('FieldType', [
    'name',
    'type',
    'formats'
])


# --- Model Support ---
from analyticengine.forecast5 import Forecast5
model_types = {
    # 'forecast.Forecast': Forecast,
    'forecast2.Forecast2': Forecast2,
    'forecast5.Forecast5': Forecast5,
    'forecast.UnbornForecast': UnbornBaseModel,
    'forecast.UnbornForecastZD': UnbornBaseModelZeroDawn,
    'forecast2.Forecast2_no_ds': Forecast2_no_ds,
}
idx_list = {'extid': {},
            'when': {},
            'filename': {},
            'partition_uniqueness': {'unique': True,
                                     'sparse': True}}


def InboxEntryClass(dataset_name, inbox_name):
    class CustomerSpecificDatasetInboxClass(InboxEntry):
        collection_name = ".".join([dataset_name, "_uip", inbox_name])
        # We are just following patternhere.  Most likely there is no
        # customization needed for this

    return CustomerSpecificDatasetInboxClass


def InboxFileEntryClass(dataset_name, inbox_name):
    class CustomerSpecificDatasetInboxClass(InboxFileEntry):
        collection_name = ".".join([dataset_name, "_uip", inbox_name])
        # We are just following patternhere.  Most likely there is no
        # customization needed for this

    return CustomerSpecificDatasetInboxClass



class InboxEntry(Model):
    tenant_aware = True
    version = 1.0
    kind = "domainmodel.uip.InboxEntry"
    # All kinds sharing the same collection must declare all indexes for the
    # collection, since collections are checked only once
    index_list = idx_list

    def __init__(self, attrs=None):
        self.extid = None
        self.ts = None
        self.values = {}
        self.filetype = None
        super(InboxEntry, self).__init__(attrs)

    def encode(self, attrs):
        attrs['extid'] = self.extid
        attrs['when'] = self.ts
        attrs['values'] = self.values
        attrs['filetype'] = self.filetype
        return super(InboxEntry, self).encode(attrs)

    def decode(self, attrs):
        self.extid = attrs['extid']
        self.ts = attrs['when']
        if isinstance(self.ts, datetime) and not self.ts.tzinfo:
            self.ts = pytz.utc.localize(self.ts)
            # Mongo DB is returning naive date time objects.
            # This is a temporary fix.
            # LATER: Check the decode of all objects and make sure datatimes are localized
            # in the encode and not everywhere in the code.
            self.ts.tzinfo = None
        self.values = attrs['values']
        self.filetype = attrs.get('filetype')
        return super(InboxEntry, self).decode(attrs)

    update = decode



class InboxFileEntry(Model):
    tenant_aware = True
    version = 1.0
    kind = "domainmodel.uip.InboxFileEntry"
    index_list = idx_list.copy()

    def __init__(self, attrs=None):
        self.filename = None
        self.filetype = None
        self.when = None
        super(InboxFileEntry, self).__init__(attrs)

    def encode(self, attrs):
        attrs['filename'] = self.filename
        attrs['filetype'] = self.filetype
        attrs['when'] = self.when
        return super(InboxFileEntry, self).encode(attrs)

    def decode(self, attrs):
        self.filename = attrs['filename']
        self.filetype = attrs['filetype']
        self.when = attrs['when']
        return super(InboxFileEntry, self).decode(attrs)


class Dataset(DSClass):


    def InboxFileEntryClass(dataset_name, inbox_name):
        class CustomerSpecificDatasetInboxClass(InboxFileEntry):
            collection_name = ".".join([dataset_name, "_uip", inbox_name])
            # We are just following patternhere.  Most likely there is no
            # customization needed for this

        return CustomerSpecificDatasetInboxClass

    @classmethod
    def migrate_data_to_pg(cls):
        # Migrate data from Mongo to Postgres,
        # Rename the collection in Mongo to datameta_backup
        table_name = cls.getCollectionName()
        try:
            cls.create_postgres_table()
            count = 0
            for doc in gnana_db.findDocuments(table_name, {}):
                newdoc = doc.copy()
                new_ds_obj = Dataset(newdoc)
                new_ds_obj.id = None
                new_ds_obj.save()
                count += 1
            logger.info("Renaming collection in mongodb")
            if count:
                gnana_db.renameCollection(table_name, table_name + '_backup', True)
            logger.info("Datameta migration is completed for %s" % (sec_context.name))
        except Exception as e:
            logger.exception("Exception in migrating data from mongo to Postgres %s" % (e.message))
            logger.info("Dropping table in postgres")
            gnana_db2.dropCollection(table_name)
            raise


    @classmethod
    def getByName(cls, name, get_from_ms=True):
        ds = None
        if get_from_ms:
            ds = cls.get_from_micro(name) if (
                sec_context.is_etl_service_enabled) else None
        if not ds:
            try:
                return super(Dataset, cls).getByName(name)
            except Exception as e:
                collection_not_found = re.match(r"^relation .+ does not exist", e.args[0])
                if collection_not_found:
                    # Migration code
                    try:
                        logger.info("Started dataset migration for %s" % (sec_context.name))
                        cls.migrate_data_to_pg()
                        return super(Dataset, cls).getByName(name)
                    except:
                        raise
                else:
                    raise

    @classmethod
    def get_from_micro(cls, name, stage=None, target='_data', full_config=True, get_cached=True):
        etl_stage = sec_context.get_microservice_config('etl_data_service').get('sandbox')
        gbm_stage = sec_context.get_microservice_config('gbm_service').get('sandbox')
        def get_ds_config(etl_ds_attrs, gbm_ds_attrs):
            ds_attrs = {}
            ds_attrs.update(etl_ds_attrs)
            if gbm_ds_attrs:
                ds_attrs.update(gbm_ds_attrs)
                ds_attrs['models'] = gbm_ds_attrs['models']
            ds_attrs['maps'] = etl_ds_attrs.get('maps', {})
            ds_attrs['filetypes'] = etl_ds_attrs.get('filetypes', {})
            return ds_attrs

        def get_from_shell():
            etl_shell = sec_context.etl
            etl_ds_attrs = etl_shell.dataset('dataset',
                                             dataset=name,
                                             sandbox=etl_stage,
                                             raw=True)
            if etl_ds_attrs is None:
                etl_ds_attrs = {}
            if not sec_context.is_gbm_service_enabled:
                raise HttpResponseNotFound(
                    "microservices_info config found etl_data_service but not gbm_service")
            gbm_shell = sec_context.gbm
            gbm_ds_attrs = gbm_shell.dataset('dataset',
                                             dataset=name,
                                             sandbox=gbm_stage,
                                             raw=True)
            ds_attrs = get_ds_config(etl_ds_attrs, gbm_ds_attrs)
            try:
                global_cache.set(etl_key, json.dumps(etl_ds_attrs))
                global_cache.set(gbm_key, json.dumps(gbm_ds_attrs))
            except:
                pass
            return ds_attrs
        try:
            ds_attrs = {}
            etl_ds_attrs = {}
            gbm_ds_attrs = {}
            etl_key = ":".join([CNAME, sec_context.name, name,
                                etl_stage if etl_stage else 'None', target, str(full_config),"etl"])
            gbm_key = ":".join([CNAME, sec_context.name, name,
                                gbm_stage if gbm_stage else 'None', target, str(full_config),"gbm"])
            try:
                etl_ds_attrs = global_cache.get(etl_key)
                gbm_ds_attrs = global_cache.get(gbm_key)
            except Exception as e:
                logger.exception(e)
            logger.info("etl_key is %s and gbm_key is %s" % (etl_key, gbm_key))
            if get_cached and etl_ds_attrs and gbm_ds_attrs:
                try:
                    ds_attrs = get_ds_config(json.loads(etl_ds_attrs), json.loads(gbm_ds_attrs))
                    logger.info('got dataset config from global redis cache')
                except:
                    ds_attrs = get_from_shell()
            else:
                ds_attrs = get_from_shell()
            ds = Dataset(attrs=None)
            ds.name = name
            ds.decode(ds_attrs)
            return ds
        except Exception as e:
            logger.exception(e)



    @classmethod
    def getByNameAndStage(cls, name, stage=None, target='_data', full_config=True, get_cached=True,
                          load_configs=False):
        ds = cls.get_from_micro(name, stage, target, full_config, get_cached) if (
            sec_context.is_etl_service_enabled) else None
        if not ds:
            ds = cls.getByName(name, get_from_ms=False)
            if (not ds):
                return ds

            # Get a new dataset where setage information is merged if applicable
            if 'changes' in ds.stages.get(stage, {}):
                ds.merge_stage(stage, delete_stage=True)

            # If the stage is not present, fake a dummy stage
            if(stage and stage not in ds.stages):
                # Caller is expecting a stage we are not aware of. Could be
                # data stage
                ds.stage_used = stage
                ds.stages[ds.stage_used] = {}

            if stage:
                InboxFileEntryClassToUse = InboxFileEntryClass(name, stage)
                all_file_entries = gnana_db.findDocuments(
                    InboxFileEntryClassToUse.getCollectionName(),
                    {'_kind': InboxFileEntryClassToUse.kind})
                for x in all_file_entries:
                    file_entry = InboxFileEntryClassToUse(attrs=x)
                    ft_def = ds.filetype(file_entry.filetype)
                    if(ft_def.last_approved_time):
                        ft_def.last_approved_time = max(ft_def.last_approved_time,
                                                        file_entry.when or datetime.datetime(1970, 1, 1))

                    else:
                        ft_def.last_approved_time = file_entry.when
                ds.InboxEntryClass = InboxEntryClass(name, stage)
                ds.InboxFileEntryClass = InboxFileEntryClassToUse
            else:
                ds.InboxEntryClass = None
                ds.InboxFileEntryClass = None
            ds.DatasetClass = DatasetClass(ds, target=target)
            if full_config and ds:
                attrs = {}
                ds.encode(attrs)
                res = config_pattern_expansion(attrs)
                dataset_attrs = res.get('attrs', None)
                eval_errs = res.get('eval_errs', None)
                tdetails = sec_context.details
                if eval_errs:
                    for key in eval_errs.keys():
                        if (key in tdetails.get_flag('save_on_error', ds.name + '~' + stage, {}) or
                                key in tdetails.get_flag('save_on_error', ds.name, {})):
                            eval_errs.pop(key)
                    if eval_errs:
                        raise
                ds.decode(dataset_attrs)
        if(ds.ds_type is None or ds.ds_type != 'uip' or target != '_data' or load_configs):
            cls.load_configs(ds)
        return ds


    @classmethod
    def load_configs(self, ds):
        from .dataset_maps import build_id_source
        from domainmodel.uip_maps import build_uip_map
        for key, config in ds.maps.iteritems():
            if key.startswith('IDSource'):
                id_source = build_id_source(key, config, ds)
                ds.id_sources[id_source.reln_object_name] = id_source
            else:
                map_obj = build_uip_map(key, config, ds)
                if map_obj:
                    ds.map_objs[key] = map_obj

    def get_model_instance(self, model_name, time_horizon, drilldowns=None):
        if not time_horizon:
            logger.exception(
                'Getting model instance requires a time_horizon. Use get_model_class.')
        try:
            model_cls = self.get_model_class(model_name)
            return model_cls(time_horizon, dimensions=drilldowns)
        except:
            logger.exception(
                "Unable to create model instance for %s", model_name, exc_info=True)
            raise

    def get_model_class(self, model):
        '''
        Dynamically creates a subclass of a AnalysisModel that is tenant and config specific.

        model_name = name of the model in dataset config
        '''

        try:
            common_info = self.models['common']
            model_info = self.models[model]

            # Create a field map and configuration with combination of common
            # and specific values
            fld_map = common_info.field_map.copy()
            fld_map.update(model_info.field_map)

            # We will do some special magic for algorithm_params, so that
            # they are merged properly
            alg_config = common_info.config.get('algorithm_params', {}).copy()
            alg_config.update(model_info.config.get('algorithm_params', {}))

            cfg = common_info.config.copy()
            cfg.update(model_info.config)
            cfg['algorithm_params'] = alg_config
            cfg["ds_params"] = self.params

            base_class = model_types[self.models[model].type]

            # TODO: This is not not the expected use of dimensions parameter in
            # the dataset
            drilldowns = self.params['general'].get("dimensions")

            class CustomerSpecificModelClass(base_class):
                model_name = model
                gnana_super_class = base_class
                field_map = fld_map
                config = cfg

                def __init__(self, time_horizon, dimensions=None):
                    # Set up for parameters used in answers.py
                    self.buffer_size = int(
                        cfg.get('buffer_size', 1024))
                    self.master_buffer_size = int(
                        cfg.get('master_buffer_size', 50000))
                    self.hist_buffer_size = int(
                        cfg.get('hist_buffer_size', 5000))
                    self.hist_iterate_only = int(
                        cfg.get('hist_iterate_only', False))
                    self.skip_prepare = is_true(
                        cfg.get('skip_prepare'))
                    self.raise_prepare_errors = is_true(
                        model_info.config.get('raise_prepare_errors', True))

                    super(CustomerSpecificModelClass, self).__init__(field_map=fld_map,
                                                                     config=cfg,
                                                                     time_horizon=time_horizon,
                                                                     dimensions=drilldowns if dimensions is None
                                                                     else dimensions)

            return CustomerSpecificModelClass
        except:
            logger.error(
                "Unable to create model class for %s", model, exc_info=True)
            raise

    def get_model_filter(self, model_name):
        """ Return the filter to be used for running this model
        """
        filt = self.models[model_name].record_filter
        inherit_common = filt.get('inherit_common', True)
        filt = {k: v for k, v in filt.iteritems() if k != 'inherit_common'}
        if inherit_common:
            filt.update({k: v for k, v in self.models['common'].record_filter.iteritems()
                         if k not in filt})
        return filt

    def get_model_history_filter(self, model_name):
        """ Return the filter to be used for running this model for history filtering.
        If the filter value is literally 'record_filter', then simply return record_filter
        as history filter as well.
        """
        filt = self.models[model_name].history_filter
        if filt == 'record_filter':
            return self.get_model_filter(model_name)
        inherit_common = filt.get('inherit_common', True)
        filt = {k: v for k, v in filt.items() if k != 'inherit_common'}
        if inherit_common:
            filt.update({k: v for k, v in self.models['common'].history_filter.items()
                         if k not in filt})
        return filt


def get_meta_info(ds_inst, model_inst_or_cls, cache_key, col_type):

    # Proper name or stage name
    res_or_stage = ds_inst.stage_used or col_type

    # Set the collection name as per conventions
    coll_name = ".".join([ds_inst.name, "%ss" % col_type,
                          model_inst_or_cls.model_name,
                          res_or_stage])
    # get the number of days to retain the results past horizon date
    days_to_retain = model_inst_or_cls.config.get("retention_days", None)
    if(not days_to_retain):
        tenant_details = sec_context.details
        days_to_retain = tenant_details.get_config('results',
                                                   'retention_days', 7)
        days_to_retain = int(days_to_retain)
    seconds_to_retain = days_to_retain * 24 * 3600

    # Compute the cacke_key to use
    if cache_key:
        run_time_horizon = "custom_%s" % cache_key
    else:
        th = getattr(model_inst_or_cls, 'time_horizon', None)
        run_time_horizon = None if th is None else "custom_%s" % th.encode_time_horizon()

    return coll_name, seconds_to_retain, run_time_horizon



def get_result_class(
        ds_inst,
        model_inst_or_cls,
        cache_key=None,
        exec_time=None,
        never_expires=False, time_horizon=None, quarter=''):
    ''' Generate and return a class that can be used to store the results
    of the model '''
    run_info_class = get_run_info_class(
        ds_inst, model_inst_or_cls, cache_key, exec_time, never_expires)
    coll_name, seconds_to_retain, rth = get_meta_info(
        ds_inst, model_inst_or_cls, cache_key, '_result')
    max_allowed = None
    this_model_name = model_inst_or_cls.model_name
    json_fields = []
    if model_inst_or_cls.produce_individual_result:
        # IndividualResult is quarter specific
        base_class = IndividualResult
        # migrated = sec_context.details.get_config("compressed_model", "migrated", True)
        # if migrated and this_model_name in ['existingpipeforecast', 'pipelinesnapshot', 'winratios']:
        field_types = ds_inst.params.get('general').get('uipfield_types')
        if field_types:
            for key, value in field_types.items():
                if value == 'json_encoded':
                    json_fields.append(key)
        if this_model_name in ['existingpipeforecast']:
            max_allowed = sec_context.details.get_config(
                "compressed_model", this_model_name, {}).get('max_allowed', None)
            if not quarter:
                try:
                    if time_horizon:
                        quarter = current_period(time_horizon.as_of)[0]
                    elif (rth):
                        quarter = get_quarter_from_cache_key(rth)
                except Exception as e:
                    single_doc = gnana_db.findDocument(run_info_class.getCollectionName(), {'object.run_time_horizon':rth})
                    if single_doc:
                        quarter = single_doc['object']['as_of_mnemonic']
                finally:
                    if not quarter:
                        quarter = current_period(TimeHorizon().as_of)[0]
                        logger.warn("quarter name is not mentioned. so, considering current quarter %s",
                                    this_model_name)
            logger.info("Quarter is identified as %s", quarter)
            base_class = CompressedIndividualResultModel
            coll_name = '.'.join([coll_name, quarter])
        elif json_fields:
            base_class = RevenueIndividualResult
    else:
        # Group results are not quarter specific
        base_class = GroupResult

    # Add the retention index
    new_index_list = base_class.index_list.copy()
    new_index_list['expire_date'] = {
        'expireAfterSeconds': seconds_to_retain,
        'name': 'retention_index'
    }
    new_query_list = base_class.query_list.copy()
    for x in ds_inst.params['general'].get('dimensions', []):
        if x.find('~') != -1:
            logger.error(
                'Ignoring possible incorrect dimension %s during index creation', x)
            continue
        new_query_list['dimensions.%s' % x] = {'name': 'd_%s' % x}

    additional_indexes = model_inst_or_cls.config.get("additional_indexes", {})
    for x in additional_indexes:
        new_index_list[x.replace('~', '.')] = additional_indexes[x]

    th = getattr(model_inst_or_cls, 'time_horizon', None)

    class CustomerSpecificResultClass(base_class):
        collection_name = coll_name
        RunInfoClass = run_info_class
        batch_size = max_allowed
        collection_name = coll_name
        cls_model_name = this_model_name

        if th is not None:
            as_of = th.as_of
            begins = th.begins
            horizon = th.horizon

        run_time_horizon = rth
        expires = not never_expires
        if exec_time:
            execution_time = dateUtils.xl2datetime(exec_time)
        else:
            execution_time = None
        index_list = new_index_list
        query_list = new_query_list

        dataset_reference = ds_inst
        json_encoded_fields = json_fields

    return CustomerSpecificResultClass







def get_meta_info(ds_inst, model_inst_or_cls, cache_key, col_type):

    # Proper name or stage name
    res_or_stage = ds_inst.stage_used or col_type

    # Set the collection name as per conventions
    coll_name = ".".join([ds_inst.name, "%ss" % col_type,
                          model_inst_or_cls.model_name,
                          res_or_stage])
    # get the number of days to retain the results past horizon date
    days_to_retain = model_inst_or_cls.config.get("retention_days", None)
    if(not days_to_retain):
        tenant_details = sec_context.details
        days_to_retain = tenant_details.get_config('results',
                                                   'retention_days', 7)
        days_to_retain = int(days_to_retain)
    seconds_to_retain = days_to_retain * 24 * 3600

    # Compute the cacke_key to use
    if cache_key:
        run_time_horizon = "custom_%s" % cache_key
    else:
        th = getattr(model_inst_or_cls, 'time_horizon', None)
        run_time_horizon = None if th is None else "custom_%s" % th.encode_time_horizon()

    return coll_name, seconds_to_retain, run_time_horizon

def get_run_info_class(ds_inst, model_inst_or_cls, cache_key=None, exec_time=None, never_expires=False):
    """ Generate and return a class that can be used to store the rejections for
    of the model """
    col_name, seconds_to_retain, rth = get_meta_info(
        ds_inst, model_inst_or_cls, cache_key, '_run')

    # Add the retention index
    new_index_list = ModelRunDetails.index_list.copy()
    new_index_list['expire_date'] = {
        'expireAfterSeconds': seconds_to_retain,
        'name': 'retention_index'
    }

    th = getattr(model_inst_or_cls, 'time_horizon', None)

    class CustomerSpecificRunInfoClass(ModelRunDetails):
        collection_name = col_name

        if th is not None:
            as_of = th.as_of
            begins = th.begins
            horizon = th.horizon

        run_time_horizon = rth
        if exec_time:
            execution_time = dateUtils.xl2datetime(exec_time)
        else:
            execution_time = None
        index_list = new_index_list
        expires = not never_expires

    return CustomerSpecificRunInfoClass






def get_time_horizon(as_of=None, begins=None, horizon=None):
    """ This function makes a call to TimeHorizon. Later may support getting time horizon from a
    ds_name, model_name and cache_key."""

    def if_str_to_long(maybe_str):
        if isinstance(maybe_str, basestring):
            try:
                return long(maybe_str)
            except:
                logger.error("ERROR: Bad date as a string passed to get_time_horizon. "
                             "Args were as_of=%s, begins=%s, horizon=%s",
                             as_of, begins, horizon)
                return None
        return maybe_str

    return TimeHorizon(*[if_str_to_long(x) for x in (begins, as_of, horizon)])









def DatasetClass(ds, target='_data'):
    new_index_list = UIPRecord.index_list.copy()
    for k, v in ds.params['general'].get('indexed_fields', {}).items():
        new_index_list["values." + k] = v
        new_index_list["values_list." + k] = v

    class CustomerSpecificDatasetClass(UIPRecord):
        collection_name = ".".join([ds.name, "_uip", target])
        gnana_super_class = UIPRecord
        index_list = new_index_list
        created_date_field = ds.params['general'].get(
            'created_date_field', 'Created Date')
        mv_config = ds.params.get('multi_value_fields', {})
        fld_config = ds.fields
        prepare_warnings = None

        def prepare(self, ds):
            if(ds.ds_type is None or ds.ds_type != 'uip' or target != '_data'):
                self.prepare_warnings = defaultdict(set)
                self.translate_features(ds)
            return False


    return CustomerSpecificDatasetClass



class UIPRecord(Model):
    """ Represents the UIP Record and various methods to get the
    values.  There will be dynamic customer specific extensions made
    to this call in datameta, hence we don't define the collection
    name here. """
    tenant_aware = True
    version = 1.0
    kind = "domainmodel.uip.UIPRecord"
    created_date_field = 'Created Date'
    terminal_fate_field = 'terminal_fate'
    compress_excludes = {}
    fld_config = {}
    mv_config = {}
    index_list = {'extid': {'unique': True},
                  'last_update_date': {},
                  'created_date': {},
                  'term_extid_index': {
                      'index_spec': [('object.terminal_date', pymongo.ASCENDING),
                                     ('object.extid', pymongo.ASCENDING)],
                      'name': 'term_extid_index'}}

    def __init__(self, extid=None, attrs=None):
        # LATER: Remove this extid convention.  At least make it
        # second positional argument

        if(isinstance(extid, dict) and attrs is None):
            attrs = extid
            extid = attrs['object']['extid']

        self.ID = extid
        self.created_date = MAX_CREATED_DATE
        # { featureID1: [[date1,value1], [date2,value2],...],...}
        self.featMap = {}
        super(UIPRecord, self).__init__(attrs)
        return

    def all_features(self):
        return self.featMap

    @classmethod
    def getByFieldValue(cls, field, value, check_unique=False):
        # This is an abonimation.  We should really think about not
        # having it here.
        # Anyways, we have simplified the implementation compared to domainmodel __init__
        # since we know this is always tenant aware and always encrypted.  At least we
        # save some time
        attrs = gnana_db.findDocument(cls.getCollectionName(),
                                      {'object.' + field: sec_context.encrypt(value)}, check_unique)
        if(attrs):
            return cls(attrs=attrs)
        else:
            return None

    def encode(self, attrs):
        if(self.ID is None):
            raise UIPError("All UIP entries should have an external ID")
        attrs['extid'] = self.ID
        attrs['values'] = {}
        attrs['history'] = self.featMap
        attrs['values_list'] = {}
        for f in self.featMap:
            attrs['values'][f] = self.featMap[f][-1][1]  # Assign last value
        attrs['created_date'] = self.created_date

        if self.terminal_fate_field in self.featMap:
            attrs['terminal_date'] = self.featMap[self.terminal_fate_field][-1][0]
        else:
            attrs['terminal_date'] = MAX_CREATED_DATE

        for index in self.index_list.keys():
            if index.startswith('values.'):
                actual_index = index[7:]
                featMapValue = self.featMap.get(actual_index)
                if featMapValue:
                    attrs['values_list'][actual_index] = [x[1] for x in featMapValue]

        attrs['last_update_date'] = self.max_feature_date()
        return super(UIPRecord, self).encode(attrs)

    def translate_features(self, ds):
        """ Goes through the given translations (called maps) and runs them all.
        The map functions can optionally return a list or set of fields to be compressed.
        The compression is not done straight away, we keep track of them and once all the
        translations are complete, apply the compression. There are two main reaosns for this:
        First, a lot of the mapping functions assumed this historically and changing them is a pain.
        Second, it is more efficient as each field is only compressed exactly once at the end.
        UIP Developer beware that it is possible to have uncompressed history in the course of
        the translation functions!!!
        """
        sorted_maps = sorted(ds.maps, key=lambda x: ds.maps[x].get('exec_rank', 0))
        compress_set = set()
        for feature in sorted_maps:
            map_obj = ds.map_objs.get(feature)
            if map_obj:
                try:
                    compress_flds = map_obj.process(self)
                except Exception:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    err_msg = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                    message = "prepare error for ID=%s | map=%s | err_msg=%s" % (self.ID, feature, err_msg)
                    raise Exception(message)
                if compress_flds:
                    compress_set |= set(compress_flds)
        for fld in compress_set:
            if fld in self.featMap:
                pass
                # TODO: uncomment this
                # self.featMap[fld] = [tv for i, tv in enumerate(self.featMap[fld])
                #                    if i == 0 or tv[1] != self.featMap[fld][i-1][1]]

    def max_feature_date(self):
        ret_value = None
        for feature in self.featMap:
            timeseries_values = self.featMap[feature]
            if ret_value:
                ret_value = max(ret_value, timeseries_values[-1][0])
            else:
                ret_value = timeseries_values[-1][0]
        return ret_value

    def get_all_values(self, feature):
        """ Return all values from history"""
        if feature in self.featMap:
            return [x[1] for x in self.featMap[feature]]
        else:
            return ["N/A"]

    def passes_filterF(self, record_filter, as_of_dateF):
        # There is no filter at all.
        if not record_filter:
            return True, '', ''

        # Must pass each filter.
        for filter_expr, values in record_filter.items():
            if filter_expr[-1:] != ')':
                # Compatibility for the existing filters
                filter_type, feature = 'in', filter_expr
            else:
                filter_type, feature = filter_expr[0:-1].split('(')
            feature_list = feature.split(',')
            if not self.filter_list[filter_type](self, feature_list, values, as_of_dateF):
                return False, str(filter_expr), str(values)

        # Matched all dimensions
        return True, '', ''

    def filter_range(self, feature, values, as_of_dateF):
        """ Filter the records based on the given range.
        values is expected to have 2 or 3 values.  First two values provide
        the range and the optional 3rd one is the default value for invalid
        records.
        """
        # Make sure we are dealing with one feature
        if len(feature) != 1:
            raise Exception('Range needs one and only one feature')
        else:
            feature = feature[0]

        # If we can't convert a number, return default.
        # Default is set to True, meaning when a filter range
        # is specified all invalid values are filtered out
        # by default
        try:
            val = float(self.getAsOfDateF(feature, as_of_dateF))
        except ValueError:
            try:
                return values[2]
            except IndexError:
                return False

        # Check the boundary. If the value is outside
        # the boundary return True
        if values[0] and val < values[0]:
            return False
        if values[1] and val > values[1]:
            return False

        # Don't filter this record
        return True

    def filter_range_not(self, feature, values, as_of_dateF):
        """ Filter the records based on the given range, making sure they
        fall beyond the range specified.
        values is expected to have 2 or 3 values.  First two values provide
        the range and the optional 3rd one is the default value for invalid
        records.
        """
        if len(feature) != 1:
            raise Exception('Range needs one and only one feature')
        else:
            feature = feature[0]

        # Convert the value to float
        try:
            val = float(self.getAsOfDateF(feature, as_of_dateF))
        except ValueError:
            try:
                return values[2]
            except IndexError:
                return False

        # Make sure it is not in the range
        if values[0] and values[1]:
            # Both values are supplied.  Check and see if it in the specified range
            if values[0] <= val <= values[1]:
                return False
            else:
                return True
        else:
            raise Exception("for range_not_in a valid range is not supplied")

    def get_feature_value_combined(self, feature, as_of_dateF):
        l = None
        try:
            if feature[-1].startswith('use_value='):
                flag = feature[-1][10:]
                feature = feature[0:-1]
                if flag == 'as_of' or flag == 'as_of_fv':
                    l = lambda x: self.getAsOfDateF(x, as_of_dateF, first_value_on_or_after=True)
                elif flag == 'strict_as_of':
                    l = lambda x: self.getAsOfDateF(x, as_of_dateF, first_value_on_or_after=False)
                elif flag == 'any':
                    if len(feature) != 1:
                        # raise GnanaError('use_value=any can be used with one feature only')
                        raise
                    f = feature[0]
                    if f in self.featMap:
                        return [x[1] for x in self.featMap[f]]
                    else:
                        return ['N/A']

            # Default to latest
            l = l or (lambda x: self.getLatest(x))
            ret_val = "~".join([str(x) for x in map(l, feature)])
            return [ret_val]
        except:
            logger.exception(
                'ERROR in getting the combined feature values for record %s with error [%s]', self.ID, sys.exc_info()[0])
            raise

    def filter_regex_in(self, feature, values, as_of_dateF):
        """
        If any of the values returned matches regular expressions given return True

        :param feature:
        :param values:
        :param as_of_dateF:
        :return:
        """
        for val in self.get_feature_value_combined(feature, as_of_dateF):
            for x in values:
                if re.match(x, str(val)):
                    return True
        return False

    def filter_regex_not_in(self, feature, values, as_of_dateF):
        """
        If any of the values returned matches the regular expressions in values
        return False

        :param feature:
        :param values:
        :param as_of_dateF:
        :return:
        """
        for val in self.get_feature_value_combined(feature, as_of_dateF):
            for x in values:
                if re.match(x, str(val)):
                    return False
        return True

    def filter_in(self, feature, values, as_of_dateF):
        for val in self.get_feature_value_combined(feature, as_of_dateF):
            if val in values:
                return True
        return False

    def filter_not_in(self, feature, values, as_of_dateF):
        for val in self.get_feature_value_combined(feature, as_of_dateF):
            if val in values:
                return False
        return True

    def filter_exclude_id(self, feature, values, as_of_dateF):
        return self.ID not in values

    def filter_include_id(self, feature, values, as_of_dateF):
        return self.ID in values

    filter_list = {
        'in': filter_in,
        'range_in': filter_range,
        'range_not_in': filter_range_not,
        'not_in': filter_not_in,
        'matches': filter_regex_in,
        'not_matches': filter_regex_not_in,
        'exclude_ids': filter_exclude_id,
        'include_ids': filter_include_id,
    }

    def add_feature_value(self, when, name, value, create_list_if_exists=True, fld_config={}):

        if name is None:
            return
        # handling blank values in UIPRecord.compress if mark_nulls = True
        if value in (None, ''):
            null_marker_config = self.get_null_marker_config(name)
            if not null_marker_config["mark_nulls"]:
                return
        # Special handling for created_date
        if name == self.created_date_field:
            try:
                created_date = get_a_date_time_as_float_some_how(value)
                self.created_date = min(self.created_date, created_date)
            except:
                # Ignore failures
                pass

        if isinstance(when, datetime):
            when = datetime2xl(when)

        if name in self.featMap:
            # Find the right place and insert if necessary
            featValues = self.featMap[name]

            for i, pair in enumerate(featValues):
                if when == pair[0]:
                    if isinstance(pair[1], list) and create_list_if_exists:
                        if value not in pair[1]:
                            pair[1].append(value)
                    else:
                        if value != pair[1]:
                            if create_list_if_exists:
                                featValues[i] = [pair[0], [pair[1], value]]
                            else:
                                featValues[i] = [pair[0], value]
                    break
                elif when < pair[0]:
                    featValues.insert(i, [when, value])
                    break
            else:
                featValues.append([when, value])
        else:
            self.featMap[name] = [[when, value]]

        # If we find a feature before created date, we assume the record
        # must have existed before
        self.created_date = min(self.created_date, when)

    def remove_features_afterF(self, when):
        for feat in self.featMap.keys():

            # Pop the entire feature if the entire feature
            # values are after the given date
            if self.featMap[feat][0][0] > when:
                self.featMap.pop(feat)
                continue

            for this_date, _val in reversed(self.featMap[feat]):
                if this_date > when:
                    self.remove_feature_value(this_date, feat)
                else:
                    break

    def remove_feature_value(self, when, name):
        if name not in self.featMap:
            # Requested feature is not even loaded.
            return

        index = 0
        for date_index, dummy in self.featMap[name]:
            if date_index == when:
                self.featMap[name].pop(index)
                break
            else:
                index += 1

    def compress(self, field_list=[]):
        featMap = {
            'UIPCreatedDate': [[self.created_date, self.created_date]]
        }
        for fld, old_hist in self.featMap.iteritems():
            if fld is None:
                continue
            do_compress = not ((field_list and fld not in field_list) or
                               fld in self.compress_excludes)
            # if the hist is a mix of pairs and lists it will not sort properly
            old_hist = [list(x) for x in old_hist]
            old_hist.sort()

            nm_config = self.get_null_marker_config(fld)
            mark_nulls = nm_config["mark_nulls"]
            null_marker = nm_config['null_marker']

            hist = [[ts, self.handle_mv(val, fld, self.ID)] for ts, val in old_hist]

            if not mark_nulls:
                hist = [[ts, val] for ts, val in hist if val not in (None, '')]
            else:
                tmp_hist = []
                for ts, val in hist:
                    if val not in (None, ''):
                        tmp_hist += [[ts, val]]
                    elif tmp_hist:
                        tmp_hist += [[ts, null_marker]]
                hist = tmp_hist

            if do_compress:
                try:
                    hist = [tv for i, tv in enumerate(hist)
                            if i == 0 or float(tv[1]) != float(hist[i - 1][1])]
                except:
                    hist = [tv for i, tv in enumerate(hist)
                            if i == 0 or tv[1] != hist[i - 1][1]]
            try:
                hist = [[ts, val.strip()] for ts, val in hist]
            except:
                pass
            if hist:
                featMap[fld] = hist

        self.featMap = featMap

    @classmethod
    def get_null_marker_config(cls, fld):
        null_marker_config = {'mark_nulls': False, 'null_marker': ''}
        null_marker_config.update(cls.fld_config.get("__DEFAULT__", {}))
        null_marker_config.update(cls.fld_config.get(fld, {}))
        if null_marker_config["null_marker"] is None:
            raise Exception("ERROR: null_marker is not allowed to be None")
        return null_marker_config

    @classmethod
    def handle_mv(cls, val, f, ID=None):
        if not isinstance(val, list):
            return val
        mvc = cls.mv_config.get(f, ['last_value'])
        try:
            return MV_HANDLERS[mvc[0]](val, mvc[1:], ID)
        except Exception as e:
            e.message += 'Error with multi_value_fields. MVC: %s' % mvc
            logger.exception(e.message)
            raise e

    def decode(self, attrs):
        self.ID = attrs[u'extid']
        self.created_date = attrs.get('created_date')
        self.terminal_date = attrs.get(u'terminal_date', None)
        self.featMap = attrs.get('history')
        self.last_update_date = attrs.get('last_update_date')
        return super(UIPRecord, self).decode(attrs)

    def getCreatedDateF(self):
        return self.created_date

    def getLatest(self, aFeature, default_value='N/A'):
        if aFeature in self.featMap:
            return self.featMap[aFeature][-1][1]
        else:
            return default_value

    def getLatestTimeStamp(self, aFeature, default_value='N/A'):
        if aFeature in self.featMap:
            return self.featMap[aFeature][-1][0]
        else:
            return default_value

    def getEarliest(self, aFeature, default_value='N/A'):
        if aFeature in self.featMap:
            return self.featMap[aFeature][0][1]
        else:
            return default_value

    def getValueByPrefix(self, aFeature, th, ubf_accounting=False, run_date=None):
        """ Return a the value of feature at a specific time based
        the prefix used for the feature.  If the prefix is unknown
        just return None for feature and value and let the caller
        decide what is the default"""
        if aFeature.startswith('latest_'):
            if run_date:
                field_value = self.getAsOfDateF(aFeature[7:], run_date.as_ofF, first_value_on_or_after=False)
            else:
                field_value = self.getLatest(aFeature[7:])
        elif aFeature.startswith('as_of_fv_'):
            field_value = self.getAsOfDateF(aFeature[9:], th.as_ofF, first_value_on_or_after=True)
        elif aFeature.startswith('atbegin_fv_'):
            field_value = self.getAsOfDateF(aFeature[11:], th.beginsF, first_value_on_or_after=True)
        elif aFeature.startswith('athorizon_fv_'):
            field_value = self.getAsOfDateF(aFeature[13:], th.horizonF, first_value_on_or_after=True)
        elif aFeature.startswith('as_of_'):
            field_value = self.getAsOfDateF(aFeature[6:], th.as_ofF, first_value_on_or_after=False)
        elif aFeature.startswith('atbegin_'):
            field_value = self.getAsOfDateF(aFeature[8:], th.beginsF, first_value_on_or_after=False)
        elif aFeature.startswith('athorizon_'):
            field_value = self.getAsOfDateF(aFeature[10:], th.horizonF, first_value_on_or_after=False)
        elif aFeature.startswith('frozen_'):
            if ubf_accounting:
                field_value = self.getAsOfDateF(aFeature[7:], th.as_ofF, first_value_on_or_after=True)
            else:
                field_value = self.getAsOfDateF(aFeature, th.as_ofF, first_value_on_or_after=True)
        else:
            field_value = None
            aFeature = None
        return aFeature, field_value

    def getCacheValueF(self, aFeature, th, ubf_accounting=False, run_date=None):
        feat, field_value = self.getValueByPrefix(aFeature, th, ubf_accounting, run_date=run_date)
        if not feat:
            field_value = self.getAsOfDateF(aFeature, th.as_ofF, first_value_on_or_after=False)
        if aFeature.startswith('as_of_') and not aFeature.startswith('as_of_fv_'):
            aFeature = aFeature[6:]
        return aFeature, field_value

    def get_dimension_valueF(self, aFeature, th=None, ubf_accounting=False, run_date=None):
        feat, field_value = self.getValueByPrefix(aFeature, th, ubf_accounting, run_date=run_date)
        if run_date:
            return field_value if feat else self.getAsOfDateF(aFeature, run_date.as_ofF, first_value_on_or_after=False)
        else:
            return field_value if feat else self.getLatest(aFeature)

    def getAsOfDate(self,
                    aFeature,
                    asOfDate,
                    first_value_on_or_after=False,
                    NA_before_created_date=True,
                    default_value='N/A',
                    epoch_cache=None):
        fdate = self.get_fdate_from_epoch_cache(asOfDate, epoch_cache)
        return self.getAsOfDateF(aFeature,
                                 fdate,
                                 first_value_on_or_after,
                                 NA_before_created_date,
                                 default_value=default_value)

    def insert_epoch_cache_entry(self, asOfDate, epoch_cache):
        val = epoch(asOfDate).as_xldate()
        epoch_cache[asOfDate] = val
        return val

    def getStrictAsOfDate(self, aFeature, asOfDate, default_value='N/A'):
        fdate = epoch(asOfDate).as_xldate()
        return self.getAsOfDateF(aFeature, fdate, False, default_value=default_value)

    def getAsOfSpreadDateF(self, aFeature,
                           fdate,
                           first_value_on_or_after=False,
                           spread=0,
                           default_value='N/A'):
        return self.getAsOfDateF(aFeature,
                                 fdate,
                                 first_value_on_or_after=first_value_on_or_after,
                                 spread=spread,
                                 default_value=default_value)

    def getScheduleAsOfF(self,
                         aFeature,
                         fdate,
                         hist_fdate=None,
                         first_value_on_or_after=False,
                         NA_before_created_date=False,
                         default_value='N/A',
                         epoch_dates=False):
        '''  Returns array of tuples, each val is the as of value of the series found in the aFeature dictionary as of fdate
             The keys are sorted before insertion in the final output
        '''
        if hist_fdate is None:
            hist_fdate = fdate
        history = self.getAsOfDateF(aFeature,
                                    fdate,
                                    first_value_on_or_after=True,
                                    NA_before_created_date=NA_before_created_date,
                                    default_value=default_value)
        if isinstance(history, dict):
            rs = []
            for date in sorted(history.keys()):
                cf = None
                i = 0
                ll = len(history[date])
                # TODO replace with a real python loop
                while i < ll:
                    tpl = history[date][i]
                    date_ = float(tpl[0])
                    if date_ < hist_fdate:
                        cf = tpl[1]
                        i += 1
                    else:
                        break
                if cf is not None:
                    rs.append((float(date), cf))
                elif first_value_on_or_after and ll > 0:
                    rs.append((float(date), history[date][0][1]))
            if epoch_dates:
                rs = [[epoch(d).as_epoch(), cf] for d, cf in rs]
            return rs
        return default_value

    def getScheduleAsOf(self,
                        aFeature,
                        asOfDate,
                        hist_fdate=None,
                        first_value_on_or_after=False,
                        NA_before_created_date=False,
                        default_value='N/A',
                        epoch_dates=False,
                        epoch_cache=None):
        '''  Returns array of tuples, each val is the as of value of the series found in the aFeature dictionary as of fdate
             The keys are sorted before insertion in the final output
        '''
        fdate = self.get_fdate_from_epoch_cache(asOfDate, epoch_cache)
        return self.getScheduleAsOfF(aFeature,
                                     fdate,
                                     hist_fdate=hist_fdate,
                                     first_value_on_or_after=first_value_on_or_after,
                                     NA_before_created_date=NA_before_created_date,
                                     default_value=default_value,
                                     epoch_dates=epoch_dates)

    def getLatestSchedule(self,
                          aFeature,
                          default_value='N/A',
                          epoch_dates=False):
        '''  Returns array of tuples, each val is the as of value of the series found in the aFeature dictionary as of fdate
             The keys are sorted before insertion in the final output
        '''

        history = self.getLatest(aFeature,
                                 default_value=default_value)
        if isinstance(history, dict):
            rs = []
            for date, cf_hist in sorted(history.iteritems(), key=lambda x: float(x[0])):
                if cf_hist:
                    rs.append([float(date), cf_hist[-1][1]])
            if epoch_dates:
                rs = [[epoch(d).as_epoch(), cf] for d, cf in rs]
            return rs
        return default_value

    def getAsOfDateF(self, aFeature, fdate,
                     first_value_on_or_after=False,
                     NA_before_created_date=True,
                     spread=0,
                     default_value='N/A'):
        """ Get the value for the field at a give date.

         If the first_value_on_or_after fdate is true, first known value is given
        as long, the fdate is more than creation date and NA_before_created_date =True

        if NA_before_created_date = False  the created_date is ignored

        If the first_value_on_or_after is False, it will never returns the
        first value
        """

        try:
            fdate += spread
        except:
            pass
        if aFeature not in self.featMap:
            return default_value
        if NA_before_created_date and fdate < self.created_date:
            return default_value
        hist_values = self.featMap[aFeature]
        value = None
        try:
            first_ts = hist_values[0][0]
            if first_ts > fdate:
                raise ValueError

            for time_stamp, v in hist_values:
                if time_stamp > fdate:
                    break
                value = v

            if value is None:
                raise ValueError

            return value
        except:
            if not first_value_on_or_after:
                return default_value
            else:
                return hist_values[0][1]

    def getHistorySlice(
            self,
            feat,
            beginF,
            endF,
            first_value_on_or_after=False,
            honor_created_date=True,
    ):
        """
        Get a slice of the history in the usual format of possibly empty list of
        timestamp-value tuples
        feat: Feature to slice the history for
        beginF: float indicating beginning of slice. Set to None to default to history
        for the field. Always inclusive
        endF: float indicating end of slice. Set to None to not not end. Always exclusive.
        first_value_on_or_after will cause both begin and end to look forward for valid value
        honor_created_date will cause created_date of record to be honored for history
        """
        if feat not in self.featMap:
            return []
        self_hist = self.featMap[feat]
        if not self_hist:
            return []
        if beginF is None:
            beginF = self_hist[0][0]
        if endF is None:
            endF = self_hist[-1][0] + 0.0001  # make sure it will be included
            if endF < beginF:
                endF = beginF + 0.0001
        if honor_created_date and endF <= self.created_date:
            return []
        if honor_created_date and beginF < self.created_date:
            beginF = self.created_date
        if beginF >= endF:
            return []
        ret_hist = []
        beg_val_idx = None
        for i, ts_val in enumerate(self_hist):
            if ts_val[0] <= beginF:
                beg_val_idx = i
            else:
                break
        if beg_val_idx is None:
            start_idx = 0
        else:
            ts, val = self_hist[beg_val_idx]
            if ts > beginF:
                ret_hist.append([self_hist[i][0], self_hist[i][1]])
                start_idx = beg_val_idx + 1
            else:
                start_idx = beg_val_idx
        for ts_val in self_hist[start_idx:]:
            ts, val = ts_val
            if ts >= endF:
                break
            ret_hist.append([ts, val])
        if ret_hist and ret_hist[0][0] < beginF:
            ret_hist[0][0] = beginF
        if first_value_on_or_after:
            if not ret_hist:
                ret_hist.append([beginF, self_hist[0][1]])
            elif not honor_created_date:
                ret_hist[0][0] = beginF
        return ret_hist

    def getTupleAsOfDateF(self,
                          aFeature,
                          fdate,
                          first_value_on_or_after=False,
                          NA_before_created_date=True,
                          default_value='N/A'):
        """ Get the first timestamp and value for the field at a give date.

         If the first_value_on_or_after fdate is true, first known value is given
        as long, the fdate is more than creation date and NA_before_created_date =True

        if NA_before_created_date = False  the created_date is ignored

        If the first_value_on_or_after is False, it will never returns the
        first value
        """
        if aFeature not in self.featMap:
            return (default_value, default_value)
        if NA_before_created_date and fdate < self.created_date:
            return (default_value, default_value)
        hist_values = self.featMap[aFeature]
        value = None
        ts = None
        try:
            first_ts = hist_values[0][0]
            if first_ts > fdate:
                raise ValueError

            for time_stamp, v in hist_values:
                if time_stamp > fdate:
                    break
                value = v
                ts = time_stamp

            if value is None:
                raise ValueError

            return (ts, value)
        except:
            if not first_value_on_or_after:
                return (default_value, default_value)
            else:
                return (hist_values[0][0], hist_values[0][1])

    def firstKnownTimes(self, aFeature, value_to_match, asOfDate):
        fdate = datetime2xl(asOfDate)
        return self.firstKnownTimes(aFeature, value_to_match, fdate)

    def firstKnownTimesF(self, aFeature, value_to_match, fdate):
        values = self.featMap.get(aFeature, None)
        if values:
            max_len = len(values)
            prev_time = 0
            next_time = 999999
            for index, val_pair in enumerate(values):
                if val_pair[1] == value_to_match:
                    start = val_pair[0]
                    end = values[index + 1][0] if index + 1 < max_len else 999999
                    if end < fdate:
                        prev_time = max(prev_time, end)
                    elif start > fdate:
                        next_time = min(next_time, start)
                    else:
                        prev_time = fdate
                        next_time = fdate
                        return prev_time, next_time
            return prev_time if prev_time != 0 else None, next_time if next_time != 999999 else None
        else:
            return None, None

    def get_fdate_from_epoch_cache(self, asOfDate, epoch_cache):
        if epoch_cache is None:
            return epoch(asOfDate).as_xldate()
        return epoch_cache.get(asOfDate, False) or self.insert_epoch_cache_entry(asOfDate, epoch_cache)

    def lengthOfTimeAtCurrValue(self, aFeature, asOfDate, epoch_cache=None):
        fdate = self.get_fdate_from_epoch_cache(asOfDate, epoch_cache)
        return self.lengthOfTimeAtCurrValueF(aFeature, fdate)

    def lengthOfTimeAtCurrValueF(self, aFeature, fdate):
        """Returns the time gap between the given date, and the last known
        value change for the given feature
        """
        try:
            value_timestamp = None
            hist_values = self.featMap[aFeature]
            for time_stamp, dummy in hist_values:
                if(time_stamp > fdate):
                    break
                value_timestamp = time_stamp
            if(not value_timestamp):
                raise ValueError
            return fdate - value_timestamp
        except:
            if (aFeature in ['Type', 'ACV', 'Opportunity Owner']
                    and fdate >= self.created_date):
                return fdate - self.created_date
        return 0

    def lengthOfTimeAtEachValue(self, aFeature, asOfDate, epoch_cache=None):
        fdate = self.get_fdate_from_epoch_cache(asOfDate, epoch_cache)
        return self.lengthOfTimeAtEachValueF(aFeature, fdate)

    def lengthOfTimeAtEachValueF(self, aFeature, fdate):
        """Returns the time gap between the given date, and the last known
        value change for the given feature
        """
        ret_val = defaultdict(int)
        try:
            value_timestamp = None
            hist_values = self.featMap[aFeature]
            idx = 0
            for time_stamp, val in hist_values[:-1]:
                if(time_stamp > fdate):
                    break
                value_timestamp = time_stamp
                ret_val[val] += hist_values[idx + 1][0] - value_timestamp
                idx += 1
            if(not value_timestamp):
                raise ValueError
            if fdate >= hist_values[idx + 1][0]:
                ret_val[val] += fdate - hist_values[idx + 1][0]
        except:
            if (aFeature in ['Type', 'ACV', 'Opportunity Owner']
                    and fdate >= self.created_date):
                return fdate - self.created_date
        return dict(ret_val)



    def has(self, feature, value=''):
        """ Returns true if such a feature exists, and ever equals given
        value.  If no value is passed, simply tests if such a feature exist
        """
        if feature not in self.featMap:
            return False
        elif value == '':
            return True
        for date in self.featMap[feature]:
            if self.featMap[feature][date] == value:
                return True
            try:
                if (self.featMap[feature][date]).find(value) >= 0:
                    return True
            except:
                continue
        return False

    def matches(self, feature, value):
        if feature not in self.featMap:
            return False
        for _date, val in self.featMap[feature]:
            if val == value:
                return True
        return False

    def novalue(self, feature):
        if feature not in self.featMap:
            return True
        else:
            return False






class UIPError(Exception):

    def __init__(self, message):
        self.error_message = message


def last_value(vals, args, ID):
    return vals[-1]


def all_values(vals, args, ID):
    return vals


def terminal_value(vals, args, ID):
    for v in vals:
        if v in args:
            return v
    raise Exception('No terminal value found in given values %s for id "%s"' % (str(vals), ID))


def terminal_with_fallback_to_last(vals, args, ID):
    for v in vals:
        if v in args:
            return v
    logger.warning('No terminal value found in %s for id "%s". Defaulting to last value %s' % (str(vals), ID, vals[-1]))
    return vals[-1]


MV_HANDLERS = {
    'last_value': last_value,
    'terminal_value': terminal_value,
    'terminal_or_last': terminal_with_fallback_to_last,
    'all_values': all_values
}






