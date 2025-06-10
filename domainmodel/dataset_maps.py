import hashlib
import logging
import sys
import traceback
from collections import OrderedDict, defaultdict

from aviso.settings import sec_context

from domainmodel.datameta import UIPIterator
from tasks.fields import parse_field
from utils import GnanaError

logger = logging.getLogger('gnana.%s' % __name__)
''' Possible copy_option values '''
copy_option_values = ['use_last', 'use_first', 'use_NA', 'use_Error']

def build_id_source(key, config, ds):
    # TODO - Introduce a new type field to specify if a ID Source is a Prepare or Reduce type.
    # if id_fields in the config not contains primary ID of the Source ds.
    # then Reduce the uip_obj_list to a single uip_obj
    base_config = SourceMap(key, config)
    if base_config.reln_join_field not in base_config.id_fields:
        return AggregateSourceMap(key, config, ds)
    else:
        return PrepareSourceMap(key, config, ds)


class FieldType:
    field_name = None
    in_fld = None
    required = False
    copy_option = None
    parser = None
    parser_fallback_lambda = None
    missing_lambda = None

    def __init__(self, key, config_rec):
        self.field_name = key
        self.in_fld = config_rec.get('in_fld', key)
        self.required = config_rec.get('required', False)
        self.copy_option = config_rec.get('copy_option', 'use_NA')
        self.parser = config_rec.get('parser', None)
        self.parser_fallback_lambda = config_rec.get(
            'parser_fallback_lambda', None)
        self.missing_lambda = config_rec.get('missing_lambda', None)

    def __str__(self):
        return ' FieldConfig %s -- input_field_name = %s,required = %s \
        parser = %s,parser_fallback_lambda = %s, missing_lambda %s' % (self.field_name, self.in_fld,
                                                                       self.required, self.parser,
                                                                       self.parser_fallback_lambda,
                                                                       self.missing_lambda)


class SourceMap:
    def get_map_details(self, output_fields_only=False):
        src_ds_name = self.reln_object_name
        fields_config = self.config_input.get('fields_config')
        reduce_maps = self.config_input.get("reduce_maps")
        id_fields = self.config_input.get("id_fields")
        if output_fields_only:
            out_fields = []
            if reduce_maps:
                for _, defs in reduce_maps.items():
                    out_fields.append({'name': defs.get("out_fld"), 'format': 'N/A',
                                    'dep_flds': {src_ds_name: defs.get("val_fld")}})
                for field in id_fields:
                    out_fields.append({'name': field, 'format': 'N/A', 'dep_flds': {src_ds_name: field}})
            else:
                for key, defs in fields_config.items():
                    out_fields.append({'name': key, 'format': 'N/A', 'source': 'Direct',
                                    'dep_flds': {src_ds_name: [defs.get('in_fld', key)]}})
            return out_fields
        else:
            map_details = {}
            field_info = {}
            other_attr = {}
            map_details["Source"] = src_ds_name
            map_details['reduce_map'] = False
            data = []
            if reduce_maps:
                map_details['reduce_map'] = True
                # field info
                schema = OrderedDict()
                schema["in_fld"] = {"type": "string", "label": "In Fields"}
                schema["out_fld"] = {"type": "string", "label": "Out Field"}

                for key, fld_defs in reduce_maps.items():
                    record = {}
                    for skey, _ in schema.items():
                        if skey not in fld_defs.keys():
                            if skey == 'in_fld':
                                if 'val_fld' in fld_defs.keys():
                                    record['in_fld'] = fld_defs['val_fld']
                            elif skey == 'out_fld':
                                record[skey] = key
                            elif skey == 'map_def':
                                record['map_def'] = key
                            else:
                                record[skey] = '-'
                        else:
                            record[skey] = fld_defs[skey]
                    data.append(
                        record
                    )
                field_info["data"] = data
                field_info['schema'] = schema
                field_info['order'] = ["in_fld","out_fld"]
                map_details['field_info'] = field_info
                # other info
                other_attr["all_fields"] = {"type": "boolean", "value": self.all_fields, 'label': 'All Fields'}
                if "id_fields" in self.config_input.keys():
                    other_attr["id_fields"] = {"type": "array", "value": self.config_input['id_fields'],
                                               'label': 'Id Fields'}
            else:
                # field info
                schema = OrderedDict()
                schema["in_fld"] = {"type": "string", "label": "In Fields"}
                schema["out_fld"] = {"type": "string", "label": "Out Field"}
                schema["parser_fallback_lambda"] = {"type": "string", "label": "fallback lambda"}
                schema["parser"] = {"type": "string", "label": "parser"}
                fld_defs = self.config_input['fields_config']
                for key, defs in fld_defs.items():
                    record = {}
                    for skey, _ in schema.items():
                        if skey not in defs.keys():
                            if skey == 'in_fld':
                                record[skey] = key
                            elif skey == 'out_fld':
                                record[skey] = key
                            else:
                                record[skey] = '-'
                        else:
                            record[skey] = defs[skey]
                    data.append(record)
                other_attr["all_fields"] = {"type": "boolean", "value": self.all_fields, 'label': 'All Fields'}
                field_info["data"] = data
                field_info['schema'] = schema
                field_info['order'] = ["in_fld", "out_fld","parser_fallback_lambda","parser"]
                map_details['field_info'] = field_info
            map_details['attributes'] = other_attr
            return map_details

    def save_map_details(self, map_def, map_details, dataset_name, comments=''):
        new_config = self.config_input
        other_attr = map_details["attributes"]
        new_config["all_fields"] = other_attr["all_fields"]['value']
        if 'reduce_maps' in self.config_input.keys():
            if "id_fields" in self.config_input.keys():
                new_config['id_fields'] = other_attr["id_fields"]['value']
        else:
            data = map_details["field_info"]["data"]
            fld_defs = self.config_input["fields_config"]
            new_fld_defs = {}
            for d in data:
                if d["out_fld"] in fld_defs.keys():
                    new_fld_defs[d["out_fld"]] = fld_defs[d["out_fld"]]
                else:
                    new_fld_defs[d["out_fld"]] = {}

                if d["out_fld"] != d["in_fld"][0]:
                    if d["in_fld"][0] != '' and d["in_fld"][0] != '-':
                        new_fld_defs[d["out_fld"]]["in_fld"] = d["in_fld"][0]
                    else:
                        if "in_fld" in new_fld_defs[d["out_fld"]].keys():
                            del new_fld_defs[d["out_fld"]]["in_fld"]

                if d["parser_fallback_lambda"] != '' and d["parser_fallback_lambda"] != '-':
                    new_fld_defs[d["out_fld"]]["parser_fallback_lambda"] = d["parser_fallback_lambda"]
                else:
                    if "parser_fallback_lambda" in new_fld_defs[d["out_fld"]].keys():
                        del new_fld_defs[d["out_fld"]]["parser_fallback_lambda"]
                if d["parser"] != '' and d["parser"] != '-':
                    new_fld_defs[d["out_fld"]]["parser"] = d['parser']
                else:
                    if "parser" in new_fld_defs[d["out_fld"]].keys():
                        del new_fld_defs[d["out_fld"]]["parser"]

                new_config["fields_config"] = new_fld_defs

        from feature import Feature
        Feature().commit_dataset_config_changes(dataset_name, 'ConfigUI',
                                                [('set_value', 'maps.' + map_def, new_config)],
                                                comments)

    def get_source_ds_fields(self):
        source_flds = {}
        req_flds = set()
        for fld, val in self.config_input['fields_config'].items():
            req_flds.add(val['in_fld'] if val.get('in_fld') else fld)
        source_flds[self.reln_object_name] = req_flds
        return source_flds

    def extract_params(self, key, min_list_size=1):
        split = key.split(':')
        idkey = split[1]
        k = split[0]
        args_list = k[(k.index('(') + 1): k.index(')')].split(',')
        config_type = k[0:k.index('(')]
        if len(args_list) < min_list_size:
            raise GnanaError(
                'Excepting at least %s arguments, found %s' % (min_list_size, len(args_list)))
        return idkey, args_list, config_type

    def __init__(self, fn_def_params, config, stage_ds=None):
        params = self.extract_params(
            fn_def_params, 1 if fn_def_params.startswith('IDSource') else 2)

        self.fields_config = {}
        self.reduce_maps = {}
        self.reln_join_field = params[0]
        self.reln_object_name = params[1][0]
        self.source_field = params[1][1] if len(params[1]) > 1 else None
        self.config_type = params[2]
        self.fn_def_params = fn_def_params
        self.config_input = config
        self.add_prefix = config.get('add_prefix', False)
        if 'fields_config' in config:
            for key, c in config['fields_config'].iteritems():
                self.fields_config[key] = FieldType(key, c)
        if 'reduce_maps' in config:
            for key, c in config["reduce_maps"].iteritems():
                self.reduce_maps[key] = c
        self.exclude_list = config.get('exclude_list', [])
        self.id_fields = config.get('id_fields', [params[0]])
        if 'fields_to_copy' in config:
            self.fields_to_copy = config['fields_to_copy']
        self.stage_ds = stage_ds
        self.all_fields = bool(config.get('all_fields', False))
        self.batch_size = config.get('batch_size', 5000)
        self.db_batch_size = config.get('db_batch_size', 1000)
        self.records = None
        self.add_extid_prefix = None

    def get_prefix(self):
        return '.'.join([self.reln_object_name, self.reln_join_field if self.reln_join_field else ''])

    def __str__(self):
        msg = ' self.fn_def_params = %s and self.config = %s' % (
            self.fn_def_params, self.config_input)
        msg += ' config_type = %s, reln_object_name = %s, id_fields = %s, reln_join_field = %s,\
            source_field = %s, fields_config = %s, reduce_maps = %s' % (
            self.config_type, self.reln_object_name, self.id_fields, self.reln_join_field,
            self.source_field, self.fields_config, self.reduce_maps)
        for key, fld in self.fields_config.iteritems():
            msg += ' key %s field type - %s ' % (key, fld)
        return msg

    def build_prepare_criteria(self):
        tenant = sec_context.details
        prepare_status_flag = tenant.get_flag('prepare_status', self.stage_ds.name, {})
        if prepare_status_flag.get('prepare_start_time'):
            criteria = {'last_modified_time': {'$gte': prepare_status_flag.get('prepare_start_time')}}
        else:
            criteria = {}
        return criteria

    def process(self, uip_obj=None):
        raise Exception('Implemented in sub classes')

    def pre_cache(self, cache_options):
        raise Exception('Implemented in sub classes')

    def get_records(self):
        raise Exception('Implemented in sub classes')

    def apply_fields_config(self, uip_obj, features):
        in_fld_map = defaultdict(list)
        for out_fld, field_config in self.fields_config.iteritems():
            if not field_config.in_fld:
                field_config.in_fld = out_fld
            in_fld_map[field_config.in_fld].append({
                'out_fld': out_fld, 'config': field_config})
            if field_config.in_fld not in features:
                if field_config.required:
                    msg = 'ERROR in IDSource : The following key %s is not found for ext id  %s.' % (
                        field_config.in_fld, uip_obj.ID)
                    uip_obj.prepare_errors[uip_obj.ID].append(msg)
                elif field_config.missing_lambda:
                    try:
                        features[field_config.in_fld] = [[
                            uip_obj.created_date, eval(field_config.missing_lambda)(field_config.in_fld)]]
                    except Exception as e:
                        msg = 'ERROR in IDSource : Unable to process missing lambda for field %s - exception %s' % (
                            field_config.in_fld, e.message)
                        uip_obj.prepare_errors[uip_obj.ID].append(msg)

        def add_to_featmap(in_fld, out_fld=None, field_config=None):
            try:
                uip_obj.featMap[out_fld if out_fld else in_fld] = [
                    [d, parse_field(val, field_config)] for d, val in features[in_fld]]
            except Exception as e:
                msg = 'ERROR in IDSource : Field conversion error for ID %s : field config %s - %s' % (
                    uip_obj.ID, field_config, e.message)
                uip_obj.prepare_errors[uip_obj.ID].append(msg)

        for k in features:
            if (self.all_fields and k not in self.exclude_list) or k in in_fld_map:
                try:
                    if k in in_fld_map:
                        for in_fld_rec in in_fld_map[k]:
                            field_config = in_fld_rec['config']
                            target_field_name = in_fld_rec['out_fld']
                            add_to_featmap(k, target_field_name, field_config)
                    else:
                        add_to_featmap(k)
                except Exception as e:
                    msg = 'ERROR in IDSource : prepare error for ext id %s' % (
                        uip_obj.ID)
                    uip_obj.prepare_errors[uip_obj.ID].append(msg)

        check_sum_value_of_record = hashlib.md5(str(sorted(features.items()))).hexdigest()
        uip_obj.check_sum = check_sum_value_of_record

    def build_uip_obj(self, record):
        uip_obj = self.stage_ds.DatasetClass()  # TODO add stage_uipds in init
        uip_obj.prepare_errors = defaultdict(list)
        uip_obj.ID = (
            self.reln_object_name + '_' + record.ID) if self.add_extid_prefix else record.ID
        uip_obj.created_date = record.created_date
        uip_obj.last_update_date = record.last_update_date
        return uip_obj


class PrepareSourceMap(SourceMap):
    def __init__(self, fn_def_params, config, ds=None):
        super(PrepareSourceMap, self).__init__(fn_def_params, config, ds)
        self.cache_handler = None  # Datahandler

    def process(self, uip_obj):
        ds_prep = uip_obj.prepare(self.stage_ds)
        field_list = (ds_prep or [])
        uip_obj.compress(field_list)
        return uip_obj

    def pre_cache(self, cache_options):
        self.cache_options = cache_options

    def get_records(self):
        record_iterator = UIPIterator(self.cache_options.src_uip_ds,
                                      {'object.extid': {'$in': self.cache_options.source_id_list}},
                                      None)

        for record in record_iterator:
            uip_obj = self.build_uip_obj(record)
            features = record.all_features()
            self.apply_fields_config(uip_obj, features)
            yield uip_obj


class AggregateSourceMap(SourceMap):
    def __init__(self, fn_def_params, config, ds=None):
        super(AggregateSourceMap, self).__init__(fn_def_params, config, ds)
        self.uip_reduce_dict = defaultdict(list)
        # Override the batchsize as all the processing should happen in a
        # single batch for the reduce to work
        self.batch_size = None

    def pre_cache(self, cache_options):
        record_iterator = UIPIterator(cache_options.src_uip_ds,
                                      {'object.extid': {'$in': cache_options.source_id_list}},
                                      None)
        for record in record_iterator:
            uip_obj = self.build_uip_obj(record)
            features = record.all_features()
            self.apply_fields_config(uip_obj, features)
            self.uip_reduce_dict['~'.join([str(uip_obj.getLatest(x))
                                           for x in self.id_fields])].append(uip_obj)

    def get_records(self):
        for extid, uip_obj_list in self.uip_reduce_dict.iteritems():
            yield extid, uip_obj_list

    def _aggregate(self, extid, uip_obj_list):
        if self.reln_join_field not in self.id_fields:
            uip_obj = self.stage_ds.DatasetClass()
            uip_obj.ID = extid
            uip_obj.prepare_errors = defaultdict(list)
            # created_date of the new object = least created date from all
            # uip_objects
            uip_obj.created_date = min(
                x.created_date for x in uip_obj_list)
            # last_update_date of the new object  = most recent
            # last_update_date from all uip_objects
            uip_obj.last_update_date = max(
                x.last_update_date for x in uip_obj_list)
            self.reduce_features(
                uip_obj, uip_obj_list, self.reduce_maps)
            for list_item in uip_obj_list:
                uip_obj.prepare_errors.update(list_item.prepare_errors)
            if not uip_obj.featMap:
                logger.warning(
                    "No records generated for id '%s' ", uip_obj.ID)
            else:
                # TODO - need to check if we need to skip yield in case featMap
                # is not populated
                uip_obj.featMap['UIPCreatedDate'] = [
                    [uip_obj.created_date, uip_obj.created_date]]
            return uip_obj

    def process(self, obj_tup):
        extid, uip_obj_list = obj_tup
        uip_obj = self._aggregate(extid, uip_obj_list)
        uip_obj.prepare(self.stage_ds)
        return uip_obj

    #                 uip_obj.prepare(self.stage_ds)
    #                 uip_dict[uip_obj.ID] = uip_obj
    # TODO - clean up while iteration
    #         del self.uip_reduce_dict

    def reduce_features(self, uip_obj, uip_obj_list, maps):
        """ Goes through the given reduce_maps and runs them all.
        The reduce_maps functions can optionally return a list or set of fields to be compressed.
        The compression is not done straight away, we keep track of them and once all the
        translations are complete, apply the compression. There are two main reaosns for this:
        First, a lot of the mapping functions assumed this historically and changing them is a pain.
        Second, it is more efficient as each field is only compressed exactly once at the end.
        UIP Developer beware that it is possible to have uncompressed history in the course of
        the translation functions!!!
        """
        sorted_maps = sorted(maps, key=lambda x: maps[x].get('exec_rank', 0))
        compress_set = set()
        for feature in sorted_maps:
            config = dict(maps[feature])
            split_point = feature.find('(')
            reduce_function = feature[:split_point]
            try:
                # self is the uip_obj in this case.
                compress_flds = (self.reduce_functions(reduce_function))(
                    uip_obj, uip_obj_list, config)
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = "\n".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback))
                message = "prepare error for ID=%s | map=%s | err_msg=%s" % (
                    uip_obj.ID, feature, err_msg)
                raise Exception(message)
            if compress_flds:
                compress_set |= set(compress_flds)
        for fld in compress_set:
            if fld in uip_obj.featMap:
                uip_obj.featMap[fld] = [tv for i, tv in enumerate(uip_obj.featMap[fld])
                                        if i == 0 or tv[1] != uip_obj.featMap[fld][i - 1][1]]

    def reduce_functions(self, fn):
        available_fns = {
            'time_series_reduce': self.time_series_reduce,
        }
        if fn not in available_fns:
            raise KeyError(
                'The function ' + fn + ' is not defined in the reduce_maps')
        return available_fns[fn]

    def time_series_reduce(self, out_rec, uip_obj_list, config):
        """ Reduce list of uip_objects into one single uip_object, by creating a timeseries on the value_field.
         Fields that need to be in config:
         val_fld : the latest value of this field will be used to create values in the timeseries
         ts_fld : the latest value of this field will be used to make timestamps in the timeseries. If there
             are multiple records with same latest value for this field, the record with the more recent creation
             date will be used. It is expected to be in excel float format (use yyyymmdd_to_xl to convert if needed).
         out_fld : this contain the name of the timeseries field which will exist on the uip record which results
             from the reduce"""

        val_fld = config["val_fld"]
        ts_fld = config["ts_fld"]
        out_fld = config["out_fld"]
        # this should be in float format
        ts_offset = config.get("ts_offset", 0)
        hist = {}
        '''Sort the uip_objects by created_date,
        so the duplicate entries in hist map will always be overwritten by the latest one'''
        for uip_obj in sorted(uip_obj_list, key=lambda x: x.created_date):
            try:
                ts = uip_obj.featMap[ts_fld][-1][1]
            except KeyError:
                logger.warning("WARNING: field '%s' missing for id ='%s', during time series reduce" % (
                    ts_fld, uip_obj.ID))
                continue
            try:
                val = uip_obj.featMap[val_fld][-1][1]
            except KeyError:
                logger.warning("WARNING: field '%s' missing for id ='%s', during time series reduce" % (
                    val_fld, uip_obj.ID))
                continue
            ts = uip_obj.getLatest(ts_fld)
            ts = ts + ts_offset
            hist[ts] = val
        if hist:
            out_rec.featMap[out_fld] = sorted(hist.iteritems())
        if config.get('__COMPRESS__', True):
            return [out_fld]
