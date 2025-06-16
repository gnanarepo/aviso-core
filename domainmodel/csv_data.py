import hashlib
from utils import GnanaError
from aviso.settings import sec_context, gnana_db2, gnana_db, sec_context

import logging
from bson import BSON
from utils import crypto_utils, date_utils
import re
from random import choice
from string import ascii_uppercase
from datetime import datetime
import json


logger = logging.getLogger('gnana.%s' % __name__)


def csv_version_decorator(f):
    """ A decorator function that wraps the given function with the ability to execute
        some common behaviour of different methods of csv version class """
    fn = f

    def new_func(*args, **options):
        csv_type = options.get('csv_type', None)
        tenant = sec_context.details
        csv_config = tenant.get_config('csv_data', csv_type)
        auto_promote = False
        auto_promote_res = {}
        if csv_config and csv_config.get('versioned', False):
            if not sec_context.csv_version_info or sec_context.csv_version_info.get('promoted', False):
                c_time = datetime.now()
                snapshot_name = 'auto_version '
                snapshot_name += c_time.strftime("%B-%d-%Y %H:%M:%S.%f")
                ver_data = {'snapshot_name': snapshot_name,
                            'snapshot_type': 'hourly',
                            'partial': True}
                ver_details = CSVVersions.create_version(ver_data)
                peek_context = sec_context.peek_context()
                sec_context.set_context(peek_context[0],
                                        peek_context[1],
                                        peek_context[2],
                                        peek_context[3],
                                        peek_context[4],
                                        csv_version_info=ver_details)
                auto_promote = True

        ret_val = fn(*args, **options)
        if auto_promote:
            auto_promote_res = CSVVersions.promote_version()
            snapshot = sec_context.csv_version_info.get('snapshot')
            CSVPromotionData.load_promote_table(snapshot=snapshot)
            peek_context = sec_context.peek_context()
            sec_context.set_context(peek_context[0],
                                    peek_context[1],
                                    peek_context[2],
                                    peek_context[3],
                                    peek_context[4],
                                    csv_version_info={},
                                    override_version_info=True)

            val = {}
            if ret_val and isinstance(ret_val, dict):
                if 'value' in ret_val:
                    val = json.loads(ret_val.pop('value'))
                auto_promote_res['auto_promote'] = auto_promote_res.pop('success', None)
                if val:
                    val.update(auto_promote_res)
                    ret_val.update({'value': json.dumps(val)})
                else:
                    ret_val.update(auto_promote_res)
        return ret_val

    return new_func

from domainmodel.model import Model
class CSVData(Model):
    kind = 'domainmodel.csv_data.CSVData'
    version = 1.0
    index_list = {
        'extid': {'unique': True}
    }

    def __init__(self, attrs=None):
        self.dynamic_fields = {}
        super(CSVData, self).__init__(attrs)

    def encode(self, attrs):
        for x in self.all_known_fields:
            attrs[x] = getattr(self, x, None)
        attrs['dynamic_fields'] = self.dynamic_fields
        attrs['extid'] = self.extid

    def decode(self, attrs):
        self.extid = attrs.get('extid')
        self.dynamic_fields = attrs.get('dynamic_fields',{})
        for x in self.all_known_fields:
            setattr(self, x, attrs.get(x))

    @classmethod
    def getUniqueValue(cls, record):
        try:
            return "~::~".join(
                str(hash(str([str(y) for y in record[x]]))) if isinstance(record[x], list)
                else str(record[x])
                for x in cls.id_fields
            )
        except KeyError as ke:
            raise GnanaError('Missing identity field: %s' % ke.message)

    def apply_fields(self, record, partial_dd=False, partial_static=False):
        record = record.copy()
        self.extid = self.getUniqueValue(record)

        if 'id' in record:
            raise ValueError("Can't have 'id' field in csv data")

        for x in self.all_known_fields:
            if not partial_static or x in record:
                setattr(self, x, record.pop(x, None))

        # Update the dynamic fields
        if not partial_dd:
            self.dynamic_fields = {}
        for k, v in record.items():
            self.dynamic_fields[k] = v


    def save(self, is_partial=False, bulk_list=None, id_field=None):
        id_field_list = ['object.extid']
        if id_field:
            id_field_list.append(id_field)
        try:
            self.id = Model.save(self, is_partial=is_partial, bulk_list=bulk_list, id_field=id_field_list)
        except BaseException as e:
            no_relation_found = re.match(r"^relation .+ does not exist", e.message)
            if no_relation_found:
                self.create_postgres_table()
                self.id = Model.save(self, is_partial=is_partial, bulk_list=bulk_list, id_field=id_field_list)
            else:
                raise
        return

    def update_object_doc(self, object_doc):
        localattrs = object_doc['object']
        query_fields = []
        dynamic_fields = {}
        if self.postgres:
            query_fields = self.all_known_fields
            if self.typed_fields:
                localattrs = self.check_postgres_typed_fields(localattrs)
            dynamic_fields['dynamic_fields'] = localattrs.get('dynamic_fields', {})
        if self.tenant_aware and self.encrypted and sec_context.details.is_encrypted:
            encdata = sec_context.encrypt(BSON.encode(dynamic_fields if self.postgres else localattrs), self)
            if encdata:
                self.encdata = encdata
                object_doc.update({"_encdata": encdata,
                                   "object": crypto_utils.extract_index(self.index_list, localattrs, query_fields)})
        try:
            json.dumps(object_doc)
        except Exception as e:
            logger.exception("Exception in inserting data.. %s" % e.message)
            raise e
        return object_doc

    @classmethod
    def getAll(cls, criteria=None, fieldList=[], ignore_df=False, return_dict=False):
        if ignore_df:
            fieldList = list(cls.all_known_fields)
        data = super(CSVData, cls).getAll(criteria=criteria, fieldList=fieldList, return_dict=return_dict)
        for doc in data:
            yield doc

    def create_postgres_table(self, config=None, is_encrypted=None):

        def default_field_adder(field_list):
            st = ''
            for field in field_list:
                if field not in fields.keys():
                    # No type is specified default to varchar
                    # if snapshot make it as bigint
                    if field == 'snapshot':
                        st += '"' + field + '"' + " bigint,"
                    else:
                        st += '"' + field + '"' + " varchar,"
            return st
        if not self.postgres:
            return
        if not config:
            # try to get from the tenant config
            c_tenant = sec_context.details
            config = c_tenant.get_config('csv_data', self.csv_type)
        if not is_encrypted:
            is_encrypted = sec_context.details.is_encrypted
        table_name = str(self.getCollectionName()).replace(".", "$")
        st = 'CREATE TABLE "' + table_name + '" (_id bigserial primary key, '
        fields = config.get('fields', {})
        dftype = 'varchar' if is_encrypted and self.encrypted else 'json'
        column = '_encdata' if is_encrypted and self.encrypted else 'dynamic_fields'
        if fields:
            for field in fields:
                st += '"' + field + '"' + " " + fields[field]['type'] + ","
        all_known_fields = self.all_known_fields
        if all_known_fields:
            st += default_field_adder(all_known_fields)
        st += column + " " + dftype + ","
        st += "extid  varchar , last_modified_time bigint, _kind varchar, _version real, _check real )"
        try:
            res = gnana_db2.postgres_table_creator(st)
        except Exception as e:
            raise e
        return res

    @classmethod
    def postgres_copy_task(cls, csv_name, csv_to_name, criteria):
        return gnana_db2.postgres_copy_task(csv_name, csv_to_name, criteria=criteria)

    @classmethod
    def truncate_or_drop(cls, criteria=None, drop=False):
        return super(CSVData, cls).truncate_or_drop(criteria=criteria)

    @classmethod
    def getAllByFieldValue(cls, field, value):
        return super(CSVData, cls).getAllByFieldValue(field, value)

    @classmethod
    def get_count(cls, criteria=None):
        return super(CSVData, cls).get_count(criteria=criteria)

    @classmethod
    def getMultipleDistinctValues(cls, key, key_separator, criteria={}, encrypted=True):
        keys = key.split(key_separator)
        ret = gnana_db2.getMultipleDistinctValues(cls.getCollectionName(), keys, criteria, encrypted)
        if ret:
            ret_vals = []
            for val in ret:
                ret_vals.append(key_separator.join(sec_context.decrypt(val[key]) if encrypted else val[key] for key in keys))
            return ret_vals

    @classmethod
    def get_postgres_query(cls, criteria):
        return None

class CSVVersionData(CSVData):
    index_list = {
        'snapshot~extid': {'unique': True},
        'extid': {}
    }

    def save(self, is_partial=False, bulk_list=None, id_field=None):
        id_field = 'object.snapshot'
        return CSVData.save(self, is_partial=is_partial, bulk_list=bulk_list, id_field=id_field)

    def apply_fields(self, record, partial_dd=False, partial_static=False):
        # get snapshot from context if no snapshot raise error
        snapshot_from_context = sec_context.csv_version_info.get('snapshot')
        if not snapshot_from_context:
            raise GnanaError("Please create or switch to a version to upload data")
        record.update({'snapshot': snapshot_from_context})
        CSVData.apply_fields(self, record, partial_dd=partial_dd, partial_static=partial_static)

    @classmethod
    def get_mnemonic_from_criteria(cls, criteria):
        """Takes criteria as input and returns the mnemonics inside the criteria
           input: {'object.Q': '2016Q4'} output: '2016Q4',
           input: {'$or': [{'object.Q': x} for x in ['2016Q1', '2016Q2', '2016Q3', '2016Q4']]}
           output: ['2016Q1', '2016Q2', '2016Q3', '2016Q4']
           input: {'object.Q': {'$in': ['2016Q1', '2016Q2', '2016Q3', '2016Q4']}}
           output: ['2016Q1', '2016Q2', '2016Q3', '2016Q4']"""
        value = None
        if len(criteria) > 1:
            criteria = {"$and": [{k: v} for k, v in criteria.items()]}
        for k, v in criteria.items():
            if k == 'object.'+cls.quarter_field:
                if isinstance(v, str):
                    return sec_context.decrypt(v, cls)
                if isinstance(v, dict):
                    """in , not in, gte, lte, gt, lt, ne and eq case"""
                    for key, val in v.items():
                        if key in ['$in', '$nin', '$eq', '$gt', '$gte', '$lt', '$lte', '$ne']:
                            value = val
                    return value
            if isinstance(v, dict):
                value = cls.get_mnemonic_from_criteria(v)
            if isinstance(v, list):
                vals = []
                for i in v:
                    if isinstance(i, dict):
                        va = cls.get_mnemonic_from_criteria(i)
                        if va:
                            vals.append(va)
                value = vals
                if len(vals) == 1:
                    value = vals[0]

        return value

    @classmethod
    def get_queryable_snapshots(cls, projected_snapshot):
        collection = str(CSVPromotionData.getCollectionName())
        tablename = collection.replace(".", "$")
        statement = """select * from "{tablename}"
                       where csv_type='{csv_type}' and csv_name='{csv_name}'
                       and snapshot<='{snapshot}'""".format(
                       tablename=tablename,
                       csv_type=cls.csv_type,
                       csv_name=cls.csv_name,
                       snapshot=projected_snapshot)
        logger.info("Snapshot Query : %s" % statement)
        result = gnana_db2.executeDQL(collection, statement)
        quarter_cache = {}
        qc = {}
        for i in result:
            q = i['quarter']
            partial = i['partial']
            snapshot = i['snapshot']
            d = {'partial': partial,
                 'snapshot': snapshot}
            if q in quarter_cache.keys():
                quarter_cache[q].append(d)
            else:
                quarter_cache[q] = [d]
        for oquarter in quarter_cache:
            qs = []
            snap_dict = {}
            for i in quarter_cache[oquarter]:
                qs.append(i['snapshot'])
                snap_dict[i['snapshot']] = i['partial']
            qs.sort(reverse=True)
            for s in qs:
                if not snap_dict[s]:
                    if oquarter in qc.keys():
                        qc[oquarter].append(s)
                    else:
                        qc[oquarter] = [s]
                    break
                else:
                    if oquarter in qc.keys():
                        qc[oquarter].append(s)
                    else:
                        qc[oquarter] = [s]

        return qc

    @classmethod
    def encrypt(self, val):
        if sec_context.details.is_encrypted and self.encrypted:
            return sec_context.encrypt(val)
        else:
            return gnana_db2.mogrify_values(val)

    @classmethod
    # Note:- count records and distinct are mutually exclusive in this use case
    def get_postgres_query(cls, criteria=None, count_recs=False,
                           distinct_keys=None, distinct_keys_separator='~', fieldList=[]):
        if criteria is None:
            criteria = {}
        col_name = cls.getCollectionName().replace('.', '$')
        selection, fc = cls.get_postgres_query_condition(criteria, fieldList=fieldList)
        if count_recs:
            selection = 'count(b.*)'
        elif distinct_keys:
            distinct_keys_list = distinct_keys.split(distinct_keys_separator)
            if len(distinct_keys_list)>1:
                distinct_projection = ','.join("b.\"%s\"" % key for key in distinct_keys_list)
                selection = 'DISTINCT ' + distinct_projection
            elif len(distinct_keys_list) == 1:
                selection = 'DISTINCT ' + "b.\"%s\"" % distinct_keys
        else:
            if selection and selection != '*':
                selection = selection.split(',')
                selection = ["b.%s" % selected_field for selected_field in selection]
            else:
                selection = 'b.*'
        query = """SELECT {selection}
                         FROM
                           (SELECT _id as id,
                                   *,
                                   row_number() over(PARTITION BY extid
                                                     ORDER BY snapshot DESC) AS rn
                            FROM "{table_name}"
                            WHERE {final_condition}) AS b
                         WHERE b.rn = 1""".format(
                                                   selection=selection if not isinstance(selection, list) else ",".join(selection),
                                                   table_name=col_name,
                                                   final_condition=fc)
        return query

    @classmethod
    def get_projected_snapshot(cls):
        projected_snapshot = sec_context.csv_version_info.get('snapshot')
        if not projected_snapshot:
            tenant = sec_context.details
            projected_snapshot = tenant.get_flag('csv_version_details', 'ui_snapshot', -1)
            if projected_snapshot == -1:
                tab_name = CSVVersions.getCollectionName().replace('.', '$')
                get_max_snapshot = """select max(snapshot) as max_snapshot from "{table_name}"
                                                  where promoted=True""".format(
                    table_name=tab_name)
                result = gnana_db2.executeDQL(tab_name.replace('$', '.'), get_max_snapshot)
                for res in result:
                    max_snapshot = res['max_snapshot']
                projected_snapshot = max_snapshot
                if not projected_snapshot:
                    raise GnanaError("Unable to find the snapshot to use..")
        return projected_snapshot

    @classmethod
    def get_postgres_query_condition(cls, criteria=None, fieldList=[]):
        try:
            mnemonic = None
            if criteria:
                mnemonic = cls.get_mnemonic_from_criteria(criteria)
            projected_snapshot = cls.get_projected_snapshot()
            quarter_snapshots_info = cls.get_queryable_snapshots(projected_snapshot)
            snapshot = projected_snapshot
            con = ''

            selection, condition = gnana_db2.__criteria_processing__(criteria=criteria,
                                                                     typed_fields=cls.typed_fields,
                                                                     field_list=fieldList)
            con = condition[6:] if condition else ''
            fc = ''
            if not mnemonic and not quarter_snapshots_info:
                if not quarter_snapshots_info:
                    if con:
                        fc = """{condition} AND """.format(condition=con)
                    fc += """snapshot IN ({snapshot})""".format(snapshot=str(snapshot))
            elif not mnemonic:
                con_list = []
                tenant = sec_context.details
                encrypted = tenant.is_encrypted and cls.encrypted
                for mnemonic in quarter_snapshots_info:
                    x = cls.encrypt(mnemonic)
                    c = """ "{q_field}"={quarter} {condition}
                            AND snapshot IN ({snapshots})""".format(
                            q_field=cls.quarter_field,
                            condition='AND ' + con if con else '',
                            quarter="'"+x+"'" if encrypted else x,
                            snapshots=','.join(str(snap) for snap in quarter_snapshots_info[mnemonic]))
                    con_list.append(c)
                fc = 'or'.join("( " + con + ")" for con in con_list)
            elif isinstance(mnemonic, list):
                # $in condition
                qm = mnemonic
                snapshots = []
                for m in qm:
                    mn = sec_context.decrypt(m, cls)
                    snapshots.extend(quarter_snapshots_info.get(mn) if quarter_snapshots_info.get(mn) else [])
                snapshots.append(str(snapshot))
                if con:
                    fc = """ {condition} AND """.format(condition=con)
                fc += """snapshot IN ({snapshots})""".format(
                        snapshots=','.join(str(snap) for snap in list(set(snapshots))))
            else:
                snapshots = quarter_snapshots_info.get(mnemonic)
                if not snapshots:
                    snapshots = []
                snapshots.append(str(snapshot))
                if con:
                    fc = """{condition} AND """.format(condition=con)
                fc += """snapshot IN ({snapshots})""".format(
                        snapshots=','.join(str(snap) for snap in snapshots))
            return selection, fc
        except Exception as e:
            logger.exception(e.message)
            raise

    def create_postgres_table(self, config, is_encrypted):
        return CSVData.create_postgres_table(self, config, is_encrypted)

    @classmethod
    def getAll(cls, criteria=None, fieldList=[], ignore_df=False, return_dict=False):
        # WARNING: ignore_df is NOT safe if you want to ignore records deleted with 'marked_delete'
        if criteria is None:
            criteria = {}
        if ignore_df:
            fieldList = list(cls.all_known_fields)
        query = cls.get_postgres_query(criteria, fieldList=fieldList)
        obj_iter = cls.queryExecutor(query, return_dict)
        if return_dict:
            for obj in obj_iter:
                if fieldList:
                    obj['read_only'] = True
                try:
                    if obj['dynamic_fields'].get('marked_delete'):
                        continue
                except KeyError:
                    pass
                yield obj
        else:
            for obj in obj_iter:
                if fieldList:
                    setattr(obj, 'read_only', True)
                if obj.dynamic_fields.get('marked_delete'):
                    continue
                yield obj

    @classmethod
    def getByFieldValue(cls, field, value, check_unique=False):
        criteria = {'object.' + field:
                    sec_context.encrypt(value, cls) if cls.tenant_aware else value}
        ret_val = list(cls.getAll(criteria))
        if check_unique:
            if len(ret_val) > 1:
                raise GnanaError("TooManyMatchesFound")
        return ret_val

    @classmethod
    def renameCollection(cls, new_col_name, overwrite=False):
        # update the promotion table with the new csv name
        table_name = (CSVPromotionData.getCollectionName()).replace('.', '$')
        csv_to_name = new_col_name.split('.')[4]
        statement = """update "{table_name}" set "csv_name"='{csv_to_name}'
                       where "csv_type"='{csv_type}' and "csv_name"='{csv_name}'""".format(
                        table_name=table_name,
                        csv_to_name=csv_to_name,
                        csv_type=cls.csv_type,
                        csv_name=cls.csv_name
                        )
        super(CSVVersionData, cls).renameCollection(new_col_name, overwrite=overwrite)
        try:
            gnana_db2.executeDML(CSVPromotionData.getCollectionName(), statement)
        except Exception as e:
            raise e

    @classmethod
    def copyCollection(cls, new_col_name, criteria):
        super(CSVVersionData, cls).copyCollection(new_col_name, criteria)

    @classmethod
    def postgres_copy_task(cls, csv_name, csv_to_name, criteria):
        selection, fc = cls.get_postgres_query_condition(criteria)
        condition = """(SELECT *,
                                row_number() over(PARTITION BY extid
                                                     ORDER BY snapshot DESC) AS rn
                            FROM "{table_name}"
                            WHERE {final_condition}) AS b
                         WHERE b.rn = 1 """.format(
                                                   table_name=csv_name.replace('.', '$'),
                                                   final_condition=fc)
        logger.info("Condition is : %s" % condition)
        gnana_db2.postgres_copy_task(csv_name, csv_to_name, condition=condition)
        # Change the snapshot in the collection so that it can be accessible further.
        # With out the current version it will not be accessible
        if sec_context.csv_version_info.get('snapshot'):
            statement = """update "{table_name}" set snapshot={snapshot}""". format(
                                table_name=csv_to_name.replace('.', '$'),
                                snapshot=sec_context.csv_version_info.get('snapshot'))
            gnana_db2.executeDML(csv_to_name, statement)

    @classmethod
    def getAllByFieldValue(cls, field, value):
        criteria = {'object.' + field:
                    sec_context.encrypt(value, cls) if cls.tenant_aware else value}
        my_iter = cls.getAll(criteria)
        for obj in my_iter:
            yield obj

    @classmethod
    def get_count(cls, criteria=None):
        query = cls.get_postgres_query({} if criteria is None else criteria, count_recs=True)
        ret = gnana_db2.executeDQL(cls.getCollectionName(), query)
        count = None
        for r in ret:
            count = r['count']
        return count

    @classmethod
    def getDistinctValues(cls, key, criteria={}):
        if key.upper() == "Q":
            projected_snapshot = cls.get_projected_snapshot()
            quarter_snapshots_info = cls.get_queryable_snapshots(projected_snapshot)
            return quarter_snapshots_info.keys()
        query = cls.get_postgres_query({} if criteria is None else criteria, False, key)
        ret = gnana_db2.executeDQL(cls.getCollectionName(), query)
        if ret:
            ret_vals = []
            for val in ret:
                ret_vals.append(val.get(key))
            return ret_vals

    @classmethod
    def getMultipleDistinctValues(cls, keys, key_separator, criteria={}, encrypted=True):
        query = cls.get_postgres_query({} if criteria is None else criteria, False, keys, key_separator)
        ret = gnana_db2.executeDQL(cls.getCollectionName(), query)
        if ret:
            ret_vals = []
            for val in ret:
                val = key_separator.join(sec_context.decrypt(val[f]) if encrypted else val[f] for f in keys.split(key_separator))
                ret_vals.append(val)
            return ret_vals


def CSVDataClass(csv_type, csv_name, used_for_write=False):
    """
    csv_type : The type of csv. Ex : epf_cache, dtfo_cache
    csv_name : Equivalent to csv suffix. Appended at the
               end of the collection. Ex : forecast
    used_for_write : TO indicate whether CSVDataClass
                     is being used for reads or writes.
    """
    # Get the configuration for this category
    tenant = sec_context.details
    csv_type_info = tenant.get_config('csv_data', csv_type)
    if not csv_type_info:
        raise GnanaError("No configuration found for %s" % csv_type)

    q_field = csv_type_info.get('quarter_field')
    is_versioned = csv_type_info.get('versioned', False)
    postgres_enabled = csv_type_info.get('postgres_enabled', False)
    dimension_field_list = set()
    unique_field_list = csv_type_info.get('unique_fields', None)
    static_field_list = csv_type_info.get('static_fields', set())
    all_typed_fields = csv_type_info.get('fields', {})
    if not unique_field_list:
        raise GnanaError('Unique field list is not defined')
    # Create indexes
    CSVClass = CSVVersionData if is_versioned else CSVData
    csv_name_suffix = csv_name
    if not is_versioned and csv_name_suffix and not used_for_write:
        # sometimes csv_name_suffix can be None
        # Append snapshot only if this class is instantiated for reads
        snapshot_details = tenant.get_flag('snapshot_details', csv_type, {})
        if snapshot_details:
            csv_name_suffix += "_" + str(snapshot_details["current_snapshot"])
    col_name = '_csvdata.%s.%s' % (csv_type, csv_name_suffix)
    idx_list = CSVClass.index_list.copy()
    for x in csv_type_info.get('indexes', []):
        if isinstance(x, str):
            fields = x.split('~')
            if len(fields) == 1:
                idx_list[fields[0]] = {}
            else:
                # We are expectig mongo db wrapper to take care of indexing
                # multiple fields
                idx_list[x] = {}
        elif isinstance(x, list):
            fields = list(i[0] for i in x)
            name = hashlib.md5(str(list)).hexdigest()
            idx_list[name] = {'index_spec': x}
        dimension_field_list |= set(fields)

    # Populate dimension and static fields for the given
    # category of the file

    csvtype = csv_type
    csvname = csv_name
    if is_versioned:
        static_field_list |= {'snapshot'}
        if q_field is None:
            q_field = 'Q'

    # encrypted csv?
    encrypted_csv = tenant.get_flag('csv_data', 'encrypted', True)

    class CSVDataClassForCategory(CSVClass):
        postgres = postgres_enabled
        typed_fields = all_typed_fields
        collection_name = col_name
        index_list = idx_list
        dimension_fields = dimension_field_list
        static_fields = static_field_list
        id_fields = unique_field_list
        versioned = is_versioned
        quarter_field = q_field
        csv_type = csvtype
        csv_name = csvname
        encrypted = encrypted_csv
        all_known_fields = dimension_field_list | set(static_field_list) | set(unique_field_list) | set(all_typed_fields.keys())

    return CSVDataClassForCategory


class CSVVersions(Model):
    kind = 'domainmodel.csv_data.CSVVersion'
    version = 1.0
    index_list = {
    }
    postgres = True
    collection_name = '_csv_versions'
    encrypted = False

    def __init__(self, attrs=None):
        self.snapshot = None
        self.snapshot_name = None
        self.promoted = None
        self.snapshot_type = None
        self.partial = None
        super(CSVVersions, self).__init__(attrs)

    def encode(self, attrs):
        attrs['snapshot'] = self.snapshot
        attrs['snapshot_name'] = self.snapshot_name
        attrs['snapshot_type'] = self.snapshot_type
        attrs['promoted'] = self.promoted
        attrs['partial'] = self.partial
        return super(CSVVersions, self).encode(attrs)

    def decode(self, attrs):
        self.snapshot = attrs['snapshot']
        self.snapshot_name = attrs['snapshot_name']
        self.snapshot_type = attrs['snapshot_type']
        self.promoted = attrs['promoted']
        self.partial = attrs['partial']
        return super(CSVVersions, self).decode(attrs)

    @classmethod
    def create_postgres_table(self):
        table_name = str(self.getCollectionName()).replace(".", "$")
        schema = 'create table "{table_name}"(\
                    "_id" bigserial primary key,\
                    "snapshot_type" varchar,\
                    "snapshot_name" varchar,\
                    "snapshot" bigint,\
                    "promoted" boolean,\
                    "partial" boolean,\
                    "_kind" varchar,\
                    "_version" int,\
                    "last_modified_time" bigint\
                    )'

        return gnana_db2.postgres_table_creator(schema.format(table_name=table_name))

    @classmethod
    def delete_version(cls, version_name):
        def delete_from_db(col_name, snapshot):
            tablename = col_name.replace('.', '$')
            statement = """delete from "{tablename}" where snapshot='{snapshot}'""".format(
                            tablename=tablename,
                            snapshot=snapshot)
            try:
                gnana_db2.executeDML(col_name, statement)
            except Exception as e:
                logger.exception("Exception in deleting records from %s and exception is %s" % (csv_col, e.message))
        # check whether the version is available in the version table
        version_info = CSVVersions.getByFieldValue('snapshot_name', version_name)
        if not version_info:
            return {'success': False,
                    'error_msg': 'Version not available to delete'}
        # Delete data from Promotion table
        # Delete version from version table
        # Delete version data from all csv tables
        prefix = f"{sec_context.name}._csvdata."
        csv_col_list = list(x for x in gnana_db2.collection_names(prefix))
        t = sec_context.details
        for col in csv_col_list:
            csv_type = col.split('.')[3]
            tc = t.get_config('csv_data', csv_type)
            if not tc.get('versioned', False):
                csv_col_list.remove(col)
        promotion_table = str(CSVPromotionData.getCollectionName())
        version_table = cls.getCollectionName()
        csv_col_list.append(promotion_table)
        csv_col_list.append(version_table)
        for csv_col in csv_col_list:
            delete_from_db(csv_col, version_info.snapshot)
        return {'success': True}

    @classmethod
    def create_version(cls, details, snapshot=None):
        details['snapshot'] = snapshot if snapshot else date_utils.epoch().as_epoch()
        details['promoted'] = False
        version_obj = cls()
        version_obj.decode(details)
        try:
            version_obj.save()
        except Exception as e:
            no_relation = re.match(r"^relation .+ does not exist", e.message)
            if no_relation:
                logger.info("Creating Version and Promotion Tables....")
                cls.create_postgres_table()
                # Create Promote table at the same time
                try:
                    CSVPromotionData.create_promote_table()
                except Exception as e:
                    logger.exception(e.message)
                # Try to save the obj again
                version_obj = cls()
                version_obj.decode(details)
                version_obj.save()
            else:
                raise
        return details

    @classmethod
    def promote_version(cls):
        version_info = sec_context.csv_version_info
        snapshot = version_info.get('snapshot')
        snapshot_name = version_info.get('snapshot_name')
        if version_info.get('promoted', False):
            logger.error("Version promotion can be done only on unpromoted version,\
                               use rollback instead")
            ret_msg = {'success': False,
                       'msg': 'Version promotion can be done only on unpromoted version,\
                               use rollback instead'}
        else:
            tenant = sec_context.details
            available_snapshot = tenant.get_flag('csv_version_details', 'ui_snapshot', -1)
            update_ui_snapshot = True
            if not available_snapshot == -1 and available_snapshot > snapshot:
                ui_snapshot = CSVVersions.getByFieldValue('snapshot', available_snapshot, check_unique=True)
                if "auto_version" not in ui_snapshot.snapshot_name:
                    logger.error("Promotion is not possible ...... Version timestamp is out of order")
                    ret_msg = {'success': False,
                               'msg': 'Promotion is not possible ...... Version timestamp is out of order'}
                    return ret_msg
                else:
                    update_ui_snapshot = False

            v_info = CSVVersions.getByFieldValue('snapshot_name', snapshot_name, check_unique=True)
            v_info.promoted = True
            v_info.save()
            if update_ui_snapshot:
                tenant.set_flag('csv_version_details', 'ui_snapshot', snapshot)
            sec_context.csv_version_info['promoted'] = True
            # Update data in promotion table
            CSVPromotionData.load_promote_table(snapshot=snapshot)
            ret_msg = {'success': True,
                       'promoted_version': snapshot_name}
        return ret_msg


class CSVPromotionData(Model):
    kind = 'domainmodel.csv_data.CSVPromotionData'
    version = 1.0
    index_list = {
    }
    postgres = True
    collection_name = 'promotion'
    encrypted = False

    def __init__(self, attrs=None):
        self.snapshot = None
        self.quarter = None
        self.csv_type = None
        self.csv_name = None
        self.partial = None
        super(CSVPromotionData, self).__init__(attrs)

    def encode(self, attrs):
        attrs['snapshot'] = self.snapshot
        attrs['quarter'] = self.quarter
        attrs['csv_type'] = self.csv_type
        attrs['csv_name'] = self.csv_name
        attrs['partial'] = self.partial
        return super(CSVPromotionData, self).encode(attrs)

    def decode(self, attrs):
        self.snapshot = attrs['snapshot']
        self.quarter = attrs['quarter']
        self.csv_type = attrs['csv_type']
        self.csv_name = attrs['csv_name']
        self.partial = attrs['partial']
        return super(CSVPromotionData, self).decode(attrs)

    @classmethod
    def create_promote_table(cls):
        table_name = str(cls.getCollectionName()).replace(".", "$")
        rstr = ''.join(choice(ascii_uppercase) for _ in range(8))
        schema = 'create table "{table_name}"(\
                    "_id"      bigserial primary key,\
                    "snapshot" bigint,\
                    "quarter"  varchar,\
                    "csv_type" varchar,\
                    "csv_name" varchar, \
                    "partial"  boolean,\
                    constraint utpq{rstr} unique(snapshot,quarter,csv_type,csv_name)\
                    )'
        return gnana_db2.postgres_table_creator(schema.format(table_name=table_name, rstr=rstr))

    @classmethod
    def __load_promoted_version_data(cls, csv_type, csv_name, snapshot=None):
        # Clear data belongs to snapshots which are higher than the current snapshot
        partial = sec_context.csv_version_info.get('partial', False)
        table_name = str(cls.getCollectionName()).replace(".", "$")
        statement = f'delete from "{table_name}" '
        statement += "where csv_type='%s' and csv_name='%s' and snapshot>='%s'" % (csv_type,
                                                                                   csv_name,
                                                                                   snapshot)
        try:
            if snapshot:
                gnana_db2.executeDML(cls.getCollectionName(), statement)
        except Exception as e:
            logger.info(f"Got exception in clearing data {e}")

        tenant = sec_context.details
        csv_type_info = tenant.get_config('csv_data', csv_type)
        q_field = csv_type_info.get('quarter_field', 'Q')

        timestamp = snapshot
        CSVClass = CSVDataClass(csv_type, csv_name)
        st = '''select distinct("%s") from %s where snapshot='%s' ''' % (
                    q_field,
                    gnana_db2.__mongo_to_postgres_name_converter__(CSVClass.getCollectionName()),
                    timestamp)
        result = gnana_db2.executeDQL(CSVClass.getCollectionName(), st)
        quarters = []
        for rel in result:
            quarters.append(sec_context.decrypt(rel[q_field], CSVClass))
        rec = {'snapshot': snapshot,
               'partial': partial}
        rec_list = []
        for quarter in quarters:
            mr = {'quarter': quarter,
                  'csv_type': csv_type,
                  'csv_name': csv_name}
            mrec = rec.copy()
            mrec.update(mr)
            rec_list.append(mrec)
        if rec_list:
            gnana_db2.postgres_bulk_execute(rec_list, table_name.replace('$', '.'))

    @classmethod
    def load_promote_table(cls, snapshot=None):

        def get_info(ct=None):
            csv_info = {}
            if ct is None:
                prefix = f"{sec_context.name}._csvdata."
            else:
                prefix = f"{sec_context.name}._csvdata.{ct}."
            csv_col_list = list(x for x in gnana_db2.collection_names(prefix))
            for col in csv_col_list:
                csv_type = col.split('.')[3]
                csv_name = col.split('.')[4]
                if csv_info.get(csv_type):
                    csv_info[csv_type].add(csv_name)
                else:
                    csv_info[csv_type] = {csv_name}
            return csv_info
        # get all the available csv collections to generate the promote table
        csv_info = get_info()
        tenant = sec_context.details
        csv_types = csv_info.keys()
        for csv_t in csv_types:
            csv_type_info = tenant.get_config('csv_data', csv_t, {})
            if not csv_type_info.get('versioned', False):
                csv_info.pop(csv_t)
        for csv_type in csv_info:
            for csv_name in csv_info[csv_type]:
                cls.__load_promoted_version_data(csv_type, csv_name, snapshot)
        return {'success': True}
