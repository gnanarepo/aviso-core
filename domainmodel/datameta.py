import datetime
import itertools
import json
import logging
import re
import urllib
from collections import defaultdict, namedtuple
from datetime import timezone

import pytz
from aviso.settings import (CNAME, DEBUG, global_cache, gnana_db, gnana_db2,
                            local_db, sec_context)
from django.http.response import HttpResponseNotFound

from ..domainmodel.model import Model, ModelError
from ..domainmodel.uip import (InboxEntry, InboxFileEntry, PartitionData,
                             UIPRecord)
from ..utils import GnanaError, forwardmap, update_dict
from ..utils.config_utils import config_pattern_expansion
from ..utils.date_utils import datetime2epoch, get_a_date_time_as_float_some_how
from ..utils.math_utils import excelToFloat

logger = logging.getLogger('gnana.%s' % __name__)


# DO NOT REMOVE THE BELOW UNUSED IMPORTS, THEY ARE REQUIRED FOR EVAL
old_value_delay = 10.0 / 60 / 60 / 24   # 5 seconds
default_stage = "__default__"
ALGORITHM_PARAMS = 'algorithm_params'
attrset_names = ('file_types', 'models', 'params', 'fields', 'maps')
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
            if ds.ds_type is None or ds.ds_type != 'uip' or target != '_data':
                self.prepare_warnings = defaultdict(set)
                self.translate_features(ds)
            return False

        exec(ds.params.get('customization', {}).get('dataset', ""))

    return CustomerSpecificDatasetClass


class BasicFileType:

    """
    ignore_not_tranlated - if True will ignore fields not in `field_name_translation` for UIP
    """

    def __init__(self, dataset, config):
        self.ds = dataset
        self.prefix = self.ds.params.get('general', {}).get('prefix')
        self.last_approved_time = None
        self.field_name_translation = config.pop('field_name_translation', {})
        if 'ignore_not_translated' in config:
            self.ignore_not_translated = config.pop(
                'ignore_not_translated', False)
        elif 'ignore_not_tranlated' in config:
            self.ignore_not_translated = config.pop(
                'ignore_not_tranlated', False)
        else:
            self.ignore_not_translated = False
        self.id_fields = config.pop('id_fields', [])

        # To satisfy static code checking
        self.ctr_config = {}
        self.field_name = None
        self.modified_date_field = None

        self.__dict__.update(config)
        # Fix for box.  If last_approved time is saved as epoch for whatever
        # reason convert it into datetime.
        if isinstance(self.last_approved_time, int):
            #self.last_approved_time = epoch2datetime(self.last_approved_time)
            from datetime import datetime
            self.last_approved_time = datetime.fromtimestamp(self.last_approved_time / 1000.0, tz=timezone.utc)
            pass
        return

    def validate_header(self, fieldnames):
        return True

    def validate_record(self, extid, record, ts):
        if self.prefix and not extid.startswith(self.prefix):
            return self.ds.INVALID
        return self.ds.VALID

    def get_inbox_record(self, extid, record, ts):
        raise NotImplementedError()

    def apply_inbox_record(self, uip_record, inbox_record, fld_config={}):
        raise NotImplementedError()

    def get_as_dict(self):
        ret = self.__dict__.copy()
        ret.pop('ds')
        return ret

    def apply_config(self, module_path, params):
        if(module_path == 'ctr_config' and
           getattr(self, 'ctr_config', None)):
            update_dict(self.ctr_config, params)
        else:
            if isinstance(params, dict):
                if hasattr(self, module_path):
                    update_dict(getattr(self, module_path), params)
                else:
                    setattr(self, module_path, params)
            else:
                setattr(self, module_path, params)


class SnapShotFileType(BasicFileType):

    def apply_inbox_record(self, uip_record, inbox_record, fld_config={}):
        for name, value in inbox_record.values.items():
            uip_record.add_feature_value(
                inbox_record.ts, name, value, fld_config=fld_config)


class HistoryFileType(BasicFileType):

    def get_inbox_record(self, extid, record, ts):
        modified_date = get_a_date_time_as_float_some_how(
            record[self.modified_date_field])
        if not modified_date:
            return None
        field_name_field = getattr(self, 'field_name_field', None)
        if field_name_field:
            value_field = getattr(self, 'value_field', 'New Value')
            old_value_field = getattr(self, 'old_value_field', None)

            field_name = record[field_name_field]
            try:
                field_name = self.field_name_translation[field_name]
            except KeyError:
                if self.ignore_not_translated:
                    raise KeyError('{} field is ignored'.format(field_name))
            values = [{
                'extid': extid,
                'when': modified_date,
                'values': {
                    field_name: record[value_field]
                }
            }]

            if old_value_field is not None:
                values.append({
                    'extid': extid,
                    'when': modified_date - old_value_delay,
                    'values': {
                        field_name: record[old_value_field]
                    }
                })

            return values
        else:
            new_record = record.copy()
            new_record.pop(self.modified_date_field)
            return {'extid': extid,
                    'when': modified_date,
                    'values': new_record}

    def apply_inbox_record(self, uip_record, inbox_record, fld_config={}):
        for name, value in inbox_record.values.items():
            uip_record.add_feature_value(
                inbox_record.ts, name, value, fld_config=fld_config)


class UsageFileType(BasicFileType):

    def get_inbox_record(self, extid, record, ts):
        field_name_field = getattr(self, 'field_name_field', None)
        if not field_name_field:
            field_name = getattr(self, 'field_name')
            if not field_name:
                raise GnanaError(
                    'Either field_name_field or field_name is required')
        else:
            field_name = record.pop(field_name_field, None)
            if not field_name:
                raise GnanaError('No field name found in the record')

        for k in record.keys():
            val = 0
            try:
                val = float(k)
            except ValueError:
                pass

            if not val:
                record.pop(k)
            else:
                record[k] = excelToFloat(record[k])

        return {'extid': extid, 'when': ts, 'values': [field_name, record.items()]}

    def apply_inbox_record(self, uip_record, inbox_record, fld_config={}):
        for name, value in inbox_record.values[1]:
            uip_record.add_feature_value(name,
                                         "usage_" + inbox_record.values[0],
                                         value,
                                         fld_config=fld_config)


FileTypeTypes = {
    'snapshot': SnapShotFileType,
    'history': HistoryFileType,
    'usage': UsageFileType,
}

class ModelDescription:

    """ Value object class to store the model configuration.
    """

    def __init__(self, name=None, type=None, config={}, field_map={}, record_filter={}, history_filter={}):
        self.name = name
        self.type = type
        self.config = config
        self.field_map = field_map
        self.record_filter = record_filter
        self.history_filter = history_filter

    def encode_to_save(self):
        return [
            self.name,
            self.type,
            self.config,
            self.field_map,
            self.record_filter,
            self.history_filter
        ]

    def get_as_dict(self):
        config = self.config.copy()
        algorithm_params = config.pop(ALGORITHM_PARAMS, {})

        return {
            'name': self.name,
            'type': self.type,
            'config': config,
            ALGORITHM_PARAMS: algorithm_params,
            'field_map': self.field_map,
            'record_filter': self.record_filter,
            'history_filter': self.history_filter
        }

    def apply_config(self, module_path, params):
        if module_path == 'config':
            update_dict(self.config, params)
        elif module_path == 'field_map':
            update_dict(self.field_map, params)
        elif module_path == 'record_filter':
            update_dict(self.record_filter, params)
        elif module_path == 'history_filter':
            if params == 'record_filter':
                self.history_filter = params
            elif isinstance(params, dict):
                update_dict(self.history_filter, params)
            else:
                raise GnanaError(
                    "Unable to understand the history_filter value")
        elif module_path == ALGORITHM_PARAMS:
            update_dict(self.config[ALGORITHM_PARAMS], params)
        else:
            raise GnanaError(
                "Unknwon module_path (%s) for model" % module_path)

blank_common = ModelDescription('common', 'common')

class Dataset(Model):
    tenant_aware = True
    collection_name = "datameta"
    index_list = {'name': {'unique': True}}
    version = 5.0
    kind = 'domianmodel.datameta.Dataset'
    encrypted = False
    INVALID = 1
    VALID = 2
    PARTIAL = 3
    postgres = True

    # --- ORM related methods ---

    def __init__(self, attrs=None):
        self.name = None
        self.fields = {}
        self.file_types = {}
        self.params = {'general': {}}
        self.models = {'common': blank_common}
        self.maps = {}
        self.stages = {}
        # If the attribute sets come from a requested stage,
        # the stage_ysed is set. This is transient value and is not
        # persisted into DB
        self.stage_used = None
        self.ds_type = None
        # Transient fields, no need to encode and decode
        self.id_sources = {}
        self.map_objs = {}
        self.map_version_checksum = None
        super(Dataset, self).__init__(attrs)
        return

    def encode(self, attrs):
        if not self.name:
            raise ModelError('Name is mandatory')

        attrs['name'] = self.name
        attrs['params'] = self.params

        attrs['fields'] = self.fields

        attrs['filetypes'] = []
        for f in self.file_types.values():
            file_type_values = f.__dict__.copy()
            lat = file_type_values.get('last_approved_time', None)
            if lat and isinstance(lat, datetime.datetime):
                utc = pytz.utc
                lat = lat.replace(tzinfo=utc)
                file_type_values['last_approved_time'] = datetime2epoch(lat)
            file_type_values.pop('ds')
            attrs['filetypes'].append(file_type_values)

        attrs['models'] = [v.get_as_dict() for v in self.models.values()]

        attrs['maps'] = {}
        for map_name, map_dict in self.maps.items():
            attrs['maps'][map_name] = map_dict.items()

        attrs['stages'] = {}
        for stage_name, stage_dict in self.stages.items():
            attrs['stages'][stage_name] = stage_dict

        attrs['ds_type'] = self.ds_type

        super(Dataset, self).encode(attrs)
        return

    def decode(self, attrs):
        self.name = attrs['name']
        if 'params' in attrs:
            self.params = attrs.get('params', {})

        if 'fields' in attrs:
            self.fields = attrs['fields']
        if 'filetypes' in attrs:
            for f in attrs['filetypes']:
                type_info = FileTypeTypes[f["type"]](self, f)
                self.file_types[type_info.name] = type_info

        if 'models' in attrs:
            for f in attrs.get('models', {}):
                # For backward compatibility
                f['config'][ALGORITHM_PARAMS] = f.pop(ALGORITHM_PARAMS, None)
                self.models[f['name']] = ModelDescription(**f)

        if 'maps' in attrs:
            self.maps = {}
            for map_name, map_items in attrs['maps'].items():
                self.maps[map_name] = dict(map_items)

        if 'stages' in attrs:
            self.stages = {}
            for stage_name, stage_dict in attrs['stages'].items():
                self.stages[stage_name] = stage_dict

        if 'ds_type' in attrs:
            self.ds_type = attrs['ds_type']

        super(Dataset, self).decode(attrs)
        return

    def upgrade_attrs(self, attrs):
        if attrs['_version'] == 1.0:
            attrs['object']['stages'] = {}
        if attrs['_version'] < 3.0:
            params = attrs['object']['params']
            params['general'] = {}
            for x in params.keys():
                if x not in ('customization', 'general'):
                    params['general'][x] = params.pop(x)
        if attrs['_version'] < 4.0:
            # Upgrade the maps to new format
            new_maps = {}
            for map_name, map_dict in attrs['object'].get('maps', {}).items():
                new_maps[map_name] = forwardmap(map_dict).items()
            attrs['object']['maps'] = new_maps
        if attrs['_version'] < 5.0:
            # Upgrade the models to new format
            new_models = []
            for mod_list in attrs['object'].get('models', {}):
                mdobj = ModelDescription(*mod_list)
                new_models.append(mdobj.get_as_dict())
            attrs['object']['models'] = new_models

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
                collection_not_found = re.match(r"^relation .+ does not exist", e.message)
                if collection_not_found:
                    # Migration code
                    try:
                        logger.info(f"Started dataset migration for {sec_context.name}")
                        cls.migrate_data_to_pg()
                        return super(Dataset, cls).getByName(name)
                    except:
                        raise
                else:
                    raise

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
            logger.info(f"Datameta migration is completed for {sec_context.name}")
        except Exception as e:
            logger.exception(f"Exception in migrating data from mongo to Postgres {e}")
            logger.info("Dropping table in postgres")
            gnana_db2.dropCollection(table_name)
            raise

    @classmethod
    def create_postgres_table(cls):
        datameta_schema = """CREATE TABLE   "{tablename}" (
                                    "_id"                 bigserial PRIMARY KEY,
                                    "_version"            int     ,
                                    "last_modified_time"  bigint  ,
                                    "name"                varchar ,
                                    "_kind"               varchar ,
                                    "ds_type"             varchar ,
                                    "params"              jsonb   ,
                                    "stages"              jsonb   ,
                                    "fields"              jsonb   ,
                                    "maps"                jsonb   ,
                                    "models"              jsonb ARRAY,
                                    "filetypes"           jsonb ARRAY
                                );"""
        try:
            table_name = cls.getCollectionName()
            tablename = table_name.replace('.', '$')
            gnana_db2.postgres_table_creator(datameta_schema.format(tablename=tablename),
                                             collection_name=table_name)
        except Exception as e:
            logger.exception(f"Exception in creating datameta table {e}")
            raise

    # --- Methods to manage stage ---

    @classmethod
    def deleteStageData(cls, name, stage=None):
        # We don't want to apply the stage data, so we get the dataset
        # by name only.
        ds = cls.getByName(name)
        if ds:
            attrs = {}
            ds.encode(attrs)
            if stage in attrs['stages']:
                del attrs['stages'][stage]
                newds = Dataset(attrs)
                newds.decode(attrs)
                newds.id = ds.id
                newds.save()

    def merge_stage(self, stage, delete_stage=False):
        """
        Return a new dataset where stage information is added into
        main area.

        if delete_stage is specified, original staging information
        is removed
        """
        if stage not in self.stages:
            return

        # Apply everything from stage into the dataset
        for k, v in self.stages[stage].items():
            if k == 'comment':
                continue
            if k == 'creator':
                continue
            if k == 'changes':
                for change in v:
                    action = change[0]
                    path = change[1]
                    new_value = change[2]
                    params = json.loads(new_value)
                    self.apply_config(action, path, params)
        # Delete the stage if requested
        if delete_stage:
            del self.stages[stage]
            self.stage_used = None
        else:
            self.stage_used = stage

    def remove_module(self, module_type, module_name):
        module_map = getattr(self, module_type)
        if module_name in module_map:
            del module_map[module_name]
        else:
            raise GnanaError('Conflicting Change made in dataset for %s[%s]'
                             % (module_type, module_name))

    def add_module(self, module_type, module_name, params):
        # Check the module is not added through another stage
        in_main = getattr(self, module_type).get(module_name)
        if in_main is not None:
            raise GnanaError('Conflicting Change made in dataset for %s[%s]'
                             % (module_type, module_name))

        # Add the module
        if module_type == 'file_types':
            if 'name' not in params:
                params['name'] = module_name
            else:
                if params['name'] != module_name:
                    raise GnanaError('Inconsistent Naming')
            if 'ctr_config' not in params:
                params['ctr_config'] = {}
            if 'type' not in params:
                raise GnanaError(
                    'Adding a file_type requires type to specified')
            self.file_types[module_name] = FileTypeTypes[
                params['type']](self, params)
        elif module_type == 'maps':
            self.maps[module_name] = params
        elif module_type == 'models':
            params1 = params.copy()
            # Add any missing maps
            for x in ('config', ALGORITHM_PARAMS, 'record_filter', 'field_map', 'history_filter'):
                if x not in params1:
                    params1[x] = {}
            params1['config'][ALGORITHM_PARAMS] = params1.pop(ALGORITHM_PARAMS)
            self.models[module_name] = ModelDescription(**params1)

        elif module_type == 'params':
            self.params[module_name] = params
        elif module_type == 'fields':
            self.fields[module_name] = params
        else:
            raise GnanaError('Unknown module type %s ' % module_type)

    # # from api.tenantmanager import backup_and_mail_changesfrom api.tenantmanager import backup_and_mail_changes
    def check_path(self, action, path_list, path):
        valid_action_list = ['set_value', 'add_values', 'prepend_values', 'remove_values',
                             'add_path', 'remove_path']
        if action not in valid_action_list:
            raise GnanaError("Unknown action %s.Valid actions are %s" % (action,
                                                                         '[' + ', '.join(valid_action_list) + ']'))
        existing_dataset_value = self.get_as_map()
        path_len = len(path_list)
        path_str = "existing_dataset_value"
        if action == "set_value" and path == "all":
            return path_str
        if action in ["add_path", "add_values", "remove_path", "prepend_values"]:
            if path == "all" or path_len < 2:
                raise GnanaError(
                    "Sorry. %s doesnot support this %s path" % (action, path))
        if action == "remove_values":
            if path == "all":
                raise GnanaError(
                    "Sorry.remove_values doesnot support this %s path" % path)
        if action == "add_path":
            path_last_item = path_list[-1]
            path_list = path_list[:-1]
        for i in path_list:
            if i in eval(path_str):
                path_str += "['" + i + "']"
            else:
                raise GnanaError(
                    "%s key is missing.Path %s doesnot exist" % (i, path))
        if action == "add_path":
            if path_last_item in eval(path_str):
                raise GnanaError("Path %s already exists" % path)
        return path_str

    def apply_config(self, action, path, new_value):
        path_list = path.split(".")
        path_len = len(path_list)
        existing_dataset_value = self.get_as_map()
        if 'stages' in existing_dataset_value:
            existing_dataset_value.pop("stages")
        if isinstance(new_value, dict) and 'stages' in new_value:
            new_value.pop("stages")
        path_str = "existing_dataset_value"
        if action == "set_value":
            if path_len == 1:
                if path_list[0] == "all":
                    module_name = None
                    module_type = None
                    for module_type, value1 in existing_dataset_value.items():
                        if isinstance(value1, dict):
                            for module_name in value1.keys():
                                self.remove_module(module_type, module_name)
                    # creating the modules with the given values
                    for module_type, value1 in new_value.items():
                        if isinstance(value1, dict):
                            for module_name, module_value in value1.items():
                                self.add_module(
                                    module_type, module_name, module_value)
                else:
                    module_type = path_list[0]
                    if module_type in existing_dataset_value.keys():
                        module_type_value = existing_dataset_value[module_type]
                        for module_name in module_type_value.keys():
                            self.remove_module(module_type, module_name)
                        for module_name in new_value.keys():
                            self.add_module(
                                module_type, module_name, new_value[module_name])
                    else:
                        raise GnanaError("Path %s doesnot exist" % path)
            else:
                if path_len == 2:
                    module_type = path_list[0]
                    module_name = path_list[1]
                    path_str = self.check_path(action, path_list, path)
                    self.remove_module(module_type, module_name)
                    self.add_module(module_type, module_name, new_value)
                elif path_len > 2:
                    module_type = path_list[0]
                    module_name = path_list[1]
                    path_str = self.check_path(action, path_list, path)
                    eval(path_str[:path_str.rfind("[")])[
                        path_list[-1]] = new_value
                    self.remove_module(module_type, module_name)
                    self.add_module(
                        module_type, module_name, existing_dataset_value[module_type][module_name])

        if action == "add_path":
            path_str = self.check_path(action, path_list, path)
            module_type = path_list[0]
            module_name = path_list[1]
            eval(path_str)[path_list[-1]] = new_value
            if path_len > 2:
                self.remove_module(module_type, module_name)
            self.add_module(
                module_type, module_name, existing_dataset_value[module_type][module_name])

        if action == "add_values":
            path_str = self.check_path(action, path_list, path)
            value = eval(path_str)
            if not type(new_value) == type(value):
                raise GnanaError("Type doesnot match.")
            if isinstance(new_value, dict):
                for i, v in new_value.items():
                    eval(path_str)[i] = v
            elif isinstance(new_value, list):
                for i in new_value:
                    eval(path_str).append(i)
            else:
                raise GnanaError(
                    "add_values support only dict and list types.")
            self.remove_module(path_list[0], path_list[1])
            self.add_module(
                path_list[0], path_list[1], existing_dataset_value[path_list[0]][path_list[1]])

        if action == "prepend_values":
            path_str = self.check_path(action, path_list, path)
            value = eval(path_str)
            if not type(new_value) == type(value):
                raise GnanaError("Type doesnot match.")
            if isinstance(new_value, dict):
                raise GnanaError(
                    "prepend_values supports only list.Please try add_values.")
            elif isinstance(new_value, list):
                for i in new_value:
                    eval(path_str).insert(0, i)
            else:
                raise GnanaError("prepend_values support list type.")
            self.remove_module(path_list[0], path_list[1])
            self.add_module(
                path_list[0], path_list[1], existing_dataset_value[path_list[0]][path_list[1]])

        if action == "remove_path":
            path_str = self.check_path(action, path_list, path)
            module_type = path_list[0]
            module_name = path_list[1]
            if isinstance(eval(path_str[:path_str.rfind("[")]), dict):
                eval(path_str[:path_str.rfind("[")]).pop(path_list[-1])
            self.remove_module(module_type, module_name)
            if module_name in existing_dataset_value[module_type]:
                existing_dataset_value[module_type]
                self.add_module(
                    module_type, module_name, existing_dataset_value[module_type][module_name])

        if action == "remove_values":
            path_str = self.check_path(action, path_list, path)
            value = eval(path_str)
            for remove_value in new_value:
                if remove_value not in value:
                    raise GnanaError("%s key is missing" % remove_value)
                if isinstance(value, dict):
                    eval(path_str).pop(remove_value)
                elif isinstance(value, list):
                    eval(path_str).remove(remove_value)
            if path_len == 1:
                for module_name in new_value:
                    self.remove_module(path_list[0], module_name)
                for module_name, module_value in existing_dataset_value[path_list[0]]:
                    self.add_module(path_list[0], module_name, module_value)
            elif path_len > 1:
                self.remove_module(path_list[0], path_list[1])
                if path_list[1] in existing_dataset_value[path_list[0]]:
                    self.add_module(
                        path_list[0], path_list[1], existing_dataset_value[path_list[0]][path_list[1]])
        return existing_dataset_value

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
            ds = Dataset()
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
            if not ds:
                return ds

            # Get a new dataset where setage information is merged if applicable
            if 'changes' in ds.stages.get(stage, {}):
                ds.merge_stage(stage, delete_stage=True)

            # If the stage is not present, fake a dummy stage
            if stage and stage not in ds.stages:
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
                    if ft_def.last_approved_time:
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
                        raise GnanaError(eval_errs)
                ds.decode(dataset_attrs)
        if ds.ds_type is None or ds.ds_type != 'uip' or target != '_data' or load_configs:
            cls.load_configs(ds)
        return ds

    @classmethod
    def load_configs(self, ds):
        from dataset_maps import build_id_source
        from uip_maps import build_uip_map
        for key, config in ds.maps.iteritems():
            if key.startswith('IDSource'):
                id_source = build_id_source(key, config, ds)
                ds.id_sources[id_source.reln_object_name] = id_source
            else:
                map_obj = build_uip_map(key, config, ds)
                if map_obj:
                    ds.map_objs[key] = map_obj

    def get_id_sources(self):
        return self.id_sources

    # --- Methods to get various types of information ---
    def filetype(self, name):
        if name in self.file_types:
            return self.file_types[name]
        else:
            return None

    def get_model_algorithm_parameters(self, model_name):
        common_info = self.models['common']
        model_info = self.models[model_name]
        alg_config = common_info.config.get(ALGORITHM_PARAMS, {}).copy()
        alg_config.update(model_info.config.get(ALGORITHM_PARAMS, {}))

        return alg_config

    def get_partition_data(self, create=False, stage=None):

        if not stage and self.stage_used:
            stage = self.stage_used
        if stage == default_stage:
            stage = None

        if stage:
            PartitionClass = PartitionDataClass(self.name, stage)
            pd = PartitionClass.getByFieldValue(
                'partition_uniqueness', 'partitiondata')
            if not pd and create:
                pd = PartitionClass()
            return pd
        else:
            return None


    # TODO: commenting this out because this is not required in hierarchy sync task.
    # def get_model_instance(self, model_name, time_horizon, drilldowns=None):
    #     if not time_horizon:
    #         logger.exception(
    #             'Getting model instance requires a time_horizon. Use get_model_class.')
    #     try:
    #         model_cls = self.get_model_class(model_name)
    #         return model_cls(time_horizon, dimensions=drilldowns)
    #     except:
    #         logger.exception(
    #             "Unable to create model instance for %s", model_name, exc_info=True)
    #         raise
    #
    # def get_model_class(self, model):
    #     '''
    #     Dynamically creates a subclass of a AnalysisModel that is tenant and config specific.
    #
    #     model_name = name of the model in dataset config
    #     '''
    #
    #     try:
    #         common_info = self.models['common']
    #         model_info = self.models[model]
    #
    #         # Create a field map and configuration with combination of common
    #         # and specific values
    #         fld_map = common_info.field_map.copy()
    #         fld_map.update(model_info.field_map)
    #
    #         # We will do some special magic for algorithm_params, so that
    #         # they are merged properly
    #         alg_config = common_info.config.get('algorithm_params', {}).copy()
    #         alg_config.update(model_info.config.get('algorithm_params', {}))
    #
    #         cfg = common_info.config.copy()
    #         cfg.update(model_info.config)
    #         cfg['algorithm_params'] = alg_config
    #         cfg["ds_params"] = self.params
    #
    #         base_class = model_types[self.models[model].type]
    #
    #         # TODO: This is not not the expected use of dimensions parameter in
    #         # the dataset
    #         drilldowns = self.params['general'].get("dimensions")
    #
    #         class CustomerSpecificModelClass(base_class):
    #             model_name = model
    #             gnana_super_class = base_class
    #             field_map = fld_map
    #             config = cfg
    #
    #             def __init__(self, time_horizon, dimensions=None):
    #                 # Set up for parameters used in answers.py
    #                 self.buffer_size = int(
    #                     model_info.config.get('buffer_size', 1024))
    #                 self.master_buffer_size = int(
    #                     model_info.config.get('master_buffer_size', 50000))
    #                 self.hist_buffer_size = int(
    #                     model_info.config.get('hist_buffer_size', 5000))
    #                 self.hist_iterate_only = int(
    #                     model_info.config.get('hist_iterate_only', False))
    #                 self.skip_prepare = is_true(
    #                     model_info.config.get('skip_prepare'))
    #                 self.raise_prepare_errors = is_true(
    #                     model_info.config.get('raise_prepare_errors', True))
    #
    #                 super(CustomerSpecificModelClass, self).__init__(field_map=fld_map,
    #                                                                  config=cfg,
    #                                                                  time_horizon=time_horizon,
    #                                                                  dimensions=drilldowns if dimensions is None
    #                                                                  else dimensions)
    #
    #         return CustomerSpecificModelClass
    #     except:
    #         logger.error(
    #             "Unable to create model class for %s", model, exc_info=True)
    #         raise

    def get_model_filter(self, model_name):
        """ Return the filter to be used for running this model
        """
        filt = self.models[model_name].record_filter
        inherit_common = filt.get('inherit_common', True)
        filt = {k: v for k, v in filt.items() if k != 'inherit_common'}
        if inherit_common:
            filt.update({k: v for k, v in self.models['common'].record_filter.items()
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

    def get_as_map(self):
        attrs = {'name': self.name, 'ds_type': self.ds_type}

        for attr_set in itertools.chain(attrset_names,
                                        ['stages'] if not self.stage_used else []):
            attrs[attr_set] = {}

            for attr_name, attr_value in (getattr(self, attr_set, {}) or {}).items():
                # String or list.  Nothing much we can do
                if(isinstance(attr_value, str) or
                   isinstance(attr_value, list) or
                   isinstance(attr_value, dict)):
                    attrs[attr_set][attr_name] = attr_value
                elif hasattr(attr_value, 'get_as_dict'):
                    attr_value = attr_value.get_as_dict()
                    attrs[attr_set][attr_name] = attr_value
                else:
                    logger.debug(
                        "Error in getting specific values for dataset %s[%s]" % (attr_set, attr_name))
                    raise GnanaError("String, list or get_as_dict() required")

                if isinstance(attr_value, dict):
                    for k, v in attr_value.items():
                        if isinstance(v, datetime.datetime):
                            if not v.tzinfo:
                                v = pytz.utc.localize(v)
                            attr_value[k] = datetime2epoch(v)
        return attrs

    # --- Dataset Modification Operations ---

    # IMPORTANT NOTE:
    #
    # These methods will only update the stage information. When the dataset
    # is read again with the stage, information is reflected there.  Hence
    # Don't use resuling dataset for operational purposes.
    def apply(self, stage, action, path, params):
        if 'changes' in self.stages[stage]:
            if [action, path, json.dumps(params)] in self.stages[stage]['changes']:
                raise GnanaError("This change is already added to stage")
            self.stages[stage]['changes'].append(
                (action, path, json.dumps(params)))

    def reset(self, stage, main_key):
        for k in self.stages[stage].keys():
            if k.startswith(main_key):
                del self.stages[stage][k]

    def rename(self, stage, to_stage):
        self.stages[to_stage] = self.stages.pop(stage)

    def validate_record(self):
        def validate_fieldmap(fields):
            from collections import defaultdict
            sf_dic = defaultdict(set)
            uip_dic = defaultdict(set)
            for key, val in fields:
                sf_dic[key].add(val)
                uip_dic[val].add(key)
            for each in uip_dic:
                if len(uip_dic[each]) > 1:
                    raise GnanaError(
                        'Duplicates maps occured: ' + ' '.join(uip_dic[each]) + ' for ' + each)

        def dic_travers(mydic):
            for each in mydic:
                if each == 'fieldmap':
                    validate_fieldmap(mydic[each])
                if isinstance(mydic[each], dict):
                    dic_travers(mydic[each])

        ds = self.get_as_map()
        dic_travers(ds)

    def validate_expression(self, attrs):
        valid_ds = config_pattern_expansion(attrs)
        return valid_ds

def InboxFileEntryClass(dataset_name, inbox_name):
    class CustomerSpecificDatasetInboxClass(InboxFileEntry):
        collection_name = ".".join([dataset_name, "_uip", inbox_name])
        # We are just following patternhere.  Most likely there is no
        # customization needed for this

    return CustomerSpecificDatasetInboxClass

def InboxEntryClass(dataset_name, inbox_name):
    class CustomerSpecificDatasetInboxClass(InboxEntry):
        collection_name = ".".join([dataset_name, "_uip", inbox_name])
        # We are just following patternhere.  Most likely there is no
        # customization needed for this

    return CustomerSpecificDatasetInboxClass


def PartitionDataClass(dataset_name, inbox_name):
    class CustomerSpecificPartitionDataClass(PartitionData):
        collection_name = ".".join([dataset_name, "_uip", inbox_name])
        # We are just following patternhere.  Most likely there is no
        # customization needed for this
    return CustomerSpecificPartitionDataClass

def getUIPCount(ds_inst, record_filter):
    stage = ds_inst.stage_used
    if stage:
        # BUG: record_filter is not applied in staged data.
        distinct_entries_in_stage = set(gnana_db.getDistinctValues(
            ds_inst.InboxEntryClass.getCollectionName(),
            "object.extid"))

        # If nothing is present inthe stage, just give the count of main
        if len(distinct_entries_in_stage) > 0:
            distinct_entries_in_main = set(gnana_db.getDistinctValues(
                ds_inst.DatasetClass.getCollectionName(),
                "object.extid"))
            distinct_entries = distinct_entries_in_main | distinct_entries_in_stage
            return len(distinct_entries)

    return gnana_db.find_count(ds_inst.DatasetClass.getCollectionName(), record_filter)

URL_TEMPLATE = 'http://localhost:7777/cache/make_local/%s/%s?%s'


def make_local_cache(dataset, stage):
    if DEBUG:
        return False
    x = None
    try:
        x = urllib.urlopen(URL_TEMPLATE % (sec_context.name,
                                           dataset,
                                           'stage=%s' % stage if stage else ""))
        response = json.loads(x.read())
        if 'success' in response:
            return response['success']
        else:
            return False
    except:
        logger.exception(
            "Exception during make_local_cache for dataset %s", dataset)
        return False
    finally:
        if x:
            x.close()


def UIPIterator(ds_inst, record_filter, record_range=None,
                fields_requested=None,
                partition_default=False,
                use_local_cache=False,
                data_as_of=None,
                skip_prepare=True,
                log_info=True,
                override_minimum=5000,
                iterate_only=False,
                sort=None,
                db_batch_size=1000):
    if sec_context.is_etl_service_enabled:
        etl_shell = sec_context.etl
        args = {'dataset': ds_inst.name,
                'record_filter': record_filter,
                'record_range': record_range,
                'fields_requested': fields_requested,
                'partition_default': partition_default,
                'use_local_cache': use_local_cache,
                'data_as_of': data_as_of,
                'skip_prepare': skip_prepare,
                'log_info': log_info,
                'override_minimum': override_minimum,
                'iterate_only': iterate_only,
                'sort': sort,
                'db_batch_size': db_batch_size
                }
        return etl_shell.uip('UIPIterator', **args)
    return _UIPIterator(ds_inst, record_filter, record_range=record_range,
                        fields_requested=fields_requested, partition_default=partition_default,
                        use_local_cache=use_local_cache, data_as_of=data_as_of, skip_prepare=skip_prepare,
                        log_info=log_info, override_minimum=override_minimum, iterate_only=iterate_only,
                        sort=sort, db_batch_size=db_batch_size)


def _UIPIterator(ds_inst, record_filter, record_range=None,
                 fields_requested=None,
                 partition_default=False,
                 use_local_cache=False,
                 data_as_of=None,
                 skip_prepare=True,
                 log_info=True,
                 override_minimum=5000,
                 iterate_only=False,
                 sort=None,
                 db_batch_size=1000):
    if ds_inst.ds_type == 'uip':
        uip_dataset = ds_inst.name
        for key in ds_inst.maps:
            if key.startswith('IDSource'):
                source_dataset = key.split('(')[1].split(')')[0]
                break
        source_data_update_timestamp = sec_context.details.get_flag(
            'data_update', source_dataset, {})
        uip_data_update_timestamp = sec_context.details.get_flag(
            'data_update', uip_dataset, {})
        if ((uip_data_update_timestamp and source_data_update_timestamp) and
                (uip_data_update_timestamp < source_data_update_timestamp) and
                (not data_as_of or data_as_of > uip_data_update_timestamp)):
            logger.warning(
                "ETL date is greater than the data prepare date. Iterating over stale data.")
    full_load = False
    start = record_range[0] if record_range else 0
    if record_range and len(record_range) > 1:
        remaining_count = record_range[1]
    else:
        remaining_count = getUIPCount(ds_inst, record_filter)
        if start == 0 and remaining_count < override_minimum:
            full_load = True
    if sort is None:
        sort = [('object.extid', 1)]
    remaining_count_at_begin = remaining_count
    while remaining_count:
        start_at_start = start
        for x in UIPIterator_batch(ds_inst, record_filter, [start, min(override_minimum, remaining_count)],
                                   fields_requested=fields_requested,
                                   partition_default=partition_default,
                                   use_local_cache=use_local_cache,
                                   data_as_of=data_as_of,
                                   skip_prepare=skip_prepare,
                                   log_info=log_info,
                                   iterate_only=iterate_only,
                                   db_batch_size=db_batch_size,
                                   sort=sort,
                                   full_load=full_load):
            yield x
            start += 1
            remaining_count -= 1
        if override_minimum == remaining_count_at_begin and remaining_count:
            processed_count = remaining_count_at_begin - remaining_count
            if processed_count > 5000:
                logger.info("Full load use case - processed   % records in one batch(log if >5000)", processed_count)
            break

        if start_at_start == start:
            if remaining_count:
                # logger.warning("No More records to.")
                # raise Exception('Unable to iterate through the records')
                pass
            break


def UIPIterator_batch(ds_inst, record_filter, record_range,
                      fields_requested=None,
                      partition_default=False,
                      use_local_cache=False,
                      data_as_of=None,
                      skip_prepare=True,
                      log_info=True,
                      iterate_only=False,
                      db_batch_size=1000,
                      sort=None,
                      full_load=False):
    """ Serve the requested count of records starting from the first position given in record_range.
    If no range is given, all records are yielded.  If a start is given with no end, all records starting
    from that location are yielded.
    """
    stage = ds_inst.stage_used
    used_local_cache = False
    if use_local_cache:
        if local_db and make_local_cache(ds_inst.name, stage):
            if log_info:
                logger.info('Using Local DB for iteration')
            db_to_use = local_db
            used_local_cache = True
        else:
            if log_info:
                logger.warning(
                    "Unable to use local_cache.  Reverting to main database")
            db_to_use = gnana_db
    else:
        if log_info:
            logger.info('Using main DB for iteration %s', record_range)
        db_to_use = gnana_db
    try:
        return _UIPIterator_batch(ds_inst, db_to_use, record_filter,
                                  record_range,
                                  used_local_cache=used_local_cache,
                                  fields_requested=fields_requested,
                                  partition_default=partition_default,
                                  data_as_of=data_as_of,
                                  skip_prepare=skip_prepare,
                                  log_info=log_info,
                                  iterate_only=iterate_only,
                                  db_batch_size=db_batch_size,
                                  sort=sort,
                                  full_load=full_load)
    except BaseException as e:
        if used_local_cache:
            logger.exception("switching to main db because of %s" % e.message)
            db_to_use = gnana_db
            return _UIPIterator_batch(ds_inst, db_to_use, record_filter,
                                      record_range,
                                      used_local_cache=used_local_cache,
                                      fields_requested=fields_requested,
                                      partition_default=partition_default,
                                      data_as_of=data_as_of,
                                      skip_prepare=skip_prepare,
                                      log_info=log_info,
                                      iterate_only=iterate_only,
                                      db_batch_size=db_batch_size,
                                      sort=sort,
                                      full_load=full_load)
        else:
            logger.info("used_local_cache is %s" % used_local_cache)
            raise e


def _UIPIterator_batch(ds_inst, db_to_use, record_filter, record_range,
                       used_local_cache=False,
                       fields_requested=None,
                       partition_default=False,
                       data_as_of=None,
                       skip_prepare=True,
                       log_info=True,
                       iterate_only=False,
                       db_batch_size=1000,
                       sort=None,
                       full_load=False):
    stage = ds_inst.stage_used
    start = record_range[0] if record_range else 0
    requested_record_count = record_range[
        1] if record_range and len(record_range) > 1 else -1

    if stage:
        # Find entries in the stage
        stage_entries = set(db_to_use.getDistinctValues(
            ds_inst.InboxEntryClass.getCollectionName(),
            "object.extid"))
        main_entries = set(db_to_use.getDistinctValues(
            ds_inst.DatasetClass.getCollectionName(),
            "object.extid"))
        stage_only_entries = stage_entries - main_entries

        # Look for partition data
        pd = ds_inst.get_partition_data()
    else:
        stage_entries = set()
        stage_only_entries = set()
        pd = None

    main_docs = db_to_use.findDocuments(name=ds_inst.DatasetClass.getCollectionName(),
                                        criteria=record_filter,
                                        fieldlist=fields_requested,
                                        sort=sort,
                                        batch_size=db_batch_size)
    # If we have more records than the start, iterate how many ever we have or until we reach the
    # limit specified.
    if main_docs.count() > start:
        if start:
            main_docs.skip(start)
            start = 0               # No need to skip from the inbox entries
        if requested_record_count > 0:
            main_docs.limit(requested_record_count)
        if iterate_only:
            logger.warning("Not loading the DB records into list, using as a pure Iterator. Should ONLY be used, \
                        when we have to iterate over a dataset for minimum processing, otherwise we will hit cursor closed issues")
            all_records = main_docs
        else:
            all_records = list(main_docs)
        for record in all_records:
            r = ds_inst.DatasetClass(attrs=record)
            if not skip_prepare:
                r.prepare(ds_inst)
            if stage and r.ID in stage_entries:
                add_inbox_entries(r, ds_inst, db_to_use)
                r.compress()
            if pd:
                partition_value = pd.partition_map.get(
                    r.ID, pd.default if partition_default else None)
                r.add_feature_value(
                    r.created_date, 'partition', partition_value, fld_config=ds_inst.fields)
            yield clear_data_after(r, data_as_of)
            requested_record_count -= 1
            if not requested_record_count:
                break
    else:
        # Remaining entries from the start count must be skipped
        start -= main_docs.count()

    if requested_record_count:
        # We have not satisfied the count yet. See if we have any in the inbox
        # entries

        for extid in sorted(stage_only_entries):
            # Skip entries without an ID
            if not extid:
                continue
            # Skip the entries until we reach the proper start
            if start:
                start -= 1
                continue

            r = ds_inst.DatasetClass()
            r.ID = extid
            if not skip_prepare:
                r.prepare(ds_inst)
            add_inbox_entries(r, ds_inst, db_to_use)
            r.compress()
            if pd:
                partition_value = pd.partition_map.get(
                    r.ID, pd.default if partition_default else None)
                r.add_feature_value(
                    r.created_date, 'partition', partition_value, fld_config=ds_inst.fields)
            yield clear_data_after(r, data_as_of)
            requested_record_count -= 1
            if not requested_record_count:
                break

def clear_data_after(r, data_as_of):
    if data_as_of:
        r.remove_features_afterF(data_as_of)
    return r

def add_inbox_entries(uip_record, ds_inst, db_to_use=None):

    # Default to gnana_db if no specific database is passed
    db_to_use = db_to_use or gnana_db

    docs = db_to_use.findDocuments(ds_inst.InboxEntryClass.getCollectionName(),
                                   {'object.extid': uip_record.ID})

    entries_added = False
    for doc in docs:
        if doc['_kind'] != ds_inst.InboxEntryClass.kind:
            continue
        entries_added = True
        inbox_entry = ds_inst.InboxEntryClass(doc)
        file_type = ds_inst.filetype(inbox_entry.filetype)
        file_type.apply_inbox_record(
            uip_record, inbox_entry, fld_config=ds_inst.fields)
    if entries_added:
        uip_record.compress()
        return True
    else:
        return False

class TargetSpec(Model):
    tenant_aware = True
    version = 1.0
    kind = "domainmodel.datameta.TargetSpec"
    collection_name = 'target_spec'
    # All kinds sharing the same collection must declare all indexes for the
    # collection, since collections are checked only once
    index_list = {
        'name': {'unique': True}
    }
    encrypted = False

    def __init__(self, attrs=None):
        self.name = None
        self.models = {}
        self.drilldowns = []
        self.report_spec = {}
        self.keys = {}
        self.module = ""
        self.task_v2 = False
        super(TargetSpec, self).__init__(attrs)

    def encode(self, attrs):
        attrs['name'] = self.name
        attrs['models'] = self.models
        attrs['drilldowns'] = self.drilldowns
        attrs['module'] = self.module
        attrs['keys'] = self.keys
        attrs['report_spec'] = self.report_spec
        attrs['task_v2'] = self.task_v2
        return super(TargetSpec, self).encode(attrs)

    def decode(self, attrs):
        self.name = attrs['name']
        self.models = attrs['models']
        self.drilldowns = attrs['drilldowns']
        self.module = attrs['module']
        self.keys = attrs['keys']
        self.task_v2 = attrs.get('task_v2', False)
        if 'report_spec' in attrs:
            self.report_spec = attrs['report_spec']
        return super(TargetSpec, self).decode(attrs)

    def get_combine_options(self, target_key, dataset, modelname):
        if target_key in self.keys:
            for d, m, combine_options in self.keys.itervalues():
                if dataset == d and modelname == m:
                    return combine_options
        for d, m, combine_options in self.models.itervalues():
            if dataset == d and modelname == m:
                return combine_options
        return {}

    @classmethod
    def getFullByName(cls, name):
        targetspec = cls.getByFieldValue('name', name)
        if targetspec is None:
            return targetspec
        attrs = {}
        targetspec.encode(attrs)
        res = config_pattern_expansion(attrs)
        target_spec_attrs = res.get('attrs', None)
        eval_errs = res.get('eval_errs', None)
        if eval_errs:
            tdetails = sec_context.details
            for key in eval_errs.keys():
                if key in tdetails.get_flag('save_on_error', targetspec.name, {}):
                    eval_errs.pop(key)
            if eval_errs:
                raise GnanaError(eval_errs)
        targetspec.decode(target_spec_attrs)
        return targetspec

    def validate(self, attrs):
        res = config_pattern_expansion(attrs)
        return res.get('eval_errs', None)
