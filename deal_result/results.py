import collections
import logging
import os
from subprocess import Popen, PIPE
import traceback

from aviso.domainmodel import Model, ModelError
from aviso.settings import ISPROD, sec_context
from aviso.utils import dateUtils, crypto_utils
from aviso.utils.configUtils import get_max_allowed_batch_size
from aviso.utils.dateUtils import current_period
from bson import BSON
import json
import pymongo
import pytz
from aviso.utils import GnanaError
import types

logger = logging.getLogger('gnana.%s' % __name__)

long = int
basestring = str

def extract_run_time_horizon(criteria):
    if isinstance(criteria, dict):
        for key, value in criteria.items():
            if key == 'object.run_time_horizon':
                return value
            elif key == 'run_time_horizon':
                return value
            elif key == 'object':
                return extract_run_time_horizon(value)
            elif key == '$and':
                for i in value:
                    output = extract_run_time_horizon(i)
                    if isinstance(output, basestring):
                        return output
            elif key == "$or":
                raise GnanaError("or in criteria is not supported")

def cache_key_criteria(criteria):
    encrypted_run_time_horizon = extract_run_time_horizon(criteria)
    if encrypted_run_time_horizon:
        return {"object.run_time_horizon": encrypted_run_time_horizon}
    else:
        raise GnanaError("not able to find run_time_horizon please pass using run_time_horizon")

def filter_out(records, criteria):
    try:
        from mongomock.filtering import filter_applies
    except ImportError:
        raise GnanaError("mongomock is not installed please do $pip install -r requirements.txt")
    # decrypt_criteria(criteria)
    for record in records:
        if filter_applies(criteria, record):
            yield record


def criteria_check(function):
    def wrapper(model, criteria={}, sort=None):
        compressed_criteria = {}
        if criteria:
            compressed_criteria = cache_key_criteria(criteria)
        if sort:
            output = function(model, compressed_criteria, sort)
        else:
            output = function(model, compressed_criteria)
        need_filter = True
        if compressed_criteria == criteria:
            need_filter = False
        if isinstance(output, types.GeneratorType):
            return filter_out(output, criteria) if need_filter else output
        else:
            return output
    return wrapper



class BasicResult(Model):
    """Base class for all results to be stored"""
    begins = None
    as_of = None
    horizon = None
    run_time_horizon = None  # Time horizon at which the model was run
    as_of_mnemonic = None
    index_list = {
        'run_time_horizon': {
            'name': 'model_runs_index'
        },
        'as_of_mnemonic': {
            'name': 'as_of_mnemonic_index'
        }
    }

    def __init__(self, attrs=None):
        # If we are constructing a brand new result, initialize with
        # class level attributes
        if not attrs:
            self.begins = self.__class__.begins
            self.as_of = self.__class__.as_of
            self.horizon = self.__class__.horizon
            self.run_time_horizon = self.__class__.run_time_horizon
            self.as_of_mnemonic = self.__class__.as_of_mnemonic
        super(BasicResult, self).__init__(attrs)

    def encode(self, attrs):
        if self.execution_time:
            if self.expires:
                attrs['expire_date'] = max(self.horizon, self.execution_time)
            else:
                # Results are not expected to expire.  No need to set expire_date
                pass
        else:
            raise ModelError('ResultClass is not constructed for saving results')

        attrs["as_of"] = self.as_of
        attrs["begins"] = self.begins
        attrs["horizon"] = self.horizon
        attrs["run_time_horizon"] = self.run_time_horizon
        attrs["as_of_mnemonic"] = self.as_of_mnemonic

        super(BasicResult, self).encode(attrs)

    def decode(self, attrs):

        self.as_of = dateUtils.from_db(attrs["as_of"])
        self.begins = dateUtils.from_db(attrs["begins"])
        self.horizon = dateUtils.from_db(attrs["horizon"])
        self.run_time_horizon = attrs["run_time_horizon"]
        self.as_of_mnemonic = attrs.get("as_of_mnemonic", None)
        super(BasicResult, self).encode(attrs)

    def __str__(self):
        super_str = super(BasicResult, self).__str__()
        return "%s\nBasicResult <model=%s, results=%s>" % (
            super_str, self.model_name, self.results)


class ModelResult(BasicResult):

    def __init__(self, attrs=None):
        self.model_name = None
        self.results = None
        super(ModelResult, self).__init__(attrs)

    def encode(self, attrs):
        attrs["model"] = self.model_name
        # We expect all models to return basic data structures only
        attrs["results"] = self.results
        super(ModelResult, self).encode(attrs)

    def decode(self, attrs):
        self.model_name = attrs["model"]
        self.results = attrs["results"]
        super(ModelResult, self).decode(attrs)


class ModelResultHelper(object):

    @classmethod
    def findDocuments(cls, criteria={}, sort=None):
        if ("extid" not in cls.index_list and sort == [("object.extid", 1)]):
            sort = None
            frame = traceback.extract_stack()[-2]
            logger.info('sort is not needed in this case %s filename:{filename} lineno:{lineno} function:{function}'.format(
                filename=frame[0], lineno=frame[1], function=frame[2]
            ))
        results = cls.get_db().findDocuments(cls.getCollectionName(), criteria, sort=sort)
        try:
            if issubclass(cls, RevenueIndividualResult):
                return (cls.raw_decode(result) for result in results)
            else:
                return results
        except:
            logger.exception("findDocuments failed")
            return cls.get_db().findDocuments(cls.getCollectionName(), criteria, sort=sort)

    @classmethod
    def find_count(cls, criteria={}):
        return cls.get_db().find_count(cls.getCollectionName(), criteria)

    @classmethod
    def findDocument(cls, criteria={}):
        return cls.get_db().findDocument(cls.getCollectionName(), criteria)


class IndividualResult(ModelResult, ModelResultHelper):
    """ Purpose of this class is to store the results of the individual
    records, for the models that support comparison.

    """
    kind = "domainmodel.results.IndividualResults"
    version = 1.0
    quarter = ''
    index_list = {
        'run_time_horizon~extid': {'name': 'rth_eid'},
        'extid': {},
        'run_time_horizon': {
            'name': 'runs'
        },
        'as_of_mnemonic': {
            'name': 'as_of_mnemonic_index'
        },
    }

    # Collection name will be defined by the specific instances
    # tenant_aware is true, no need add it again

    def __init__(self, attrs=None):
        self.extid = None
        self.dimensions = {}
        self.uipfield = {}
        super(IndividualResult, self).__init__(attrs)

    def encode(self, attrs):
        attrs["extid"] = self.extid
        attrs['dimensions'] = self.dimensions
        attrs['uipfield'] = self.uipfield
        super(IndividualResult, self).encode(attrs)

    def decode(self, attrs):
        self.extid = attrs["extid"]
        self.dimensions = attrs.get('dimensions', {})
        self.uipfield = attrs.get('uipfield', {})
        super(IndividualResult, self).decode(attrs)

    def __str__(self):
        super_str = super(IndividualResult, self).__str__()
        return "%s\nIndividualResult <extid=%s>" % (super_str, self.extid)



class RevenueIndividualResult(IndividualResult):
    """
        individual result which have revenue fields('.' in keys) have a problem inserting in to mongo
        encoding those fields
    """

    def encode(self, attrs):
        super(RevenueIndividualResult, self).encode(attrs)
        for field in self.json_encoded_fields:
            if field in attrs['uipfield']:
                try:
                    attrs['uipfield'][field] = json.dumps(attrs['uipfield'][field])
                except:
                    pass
    @classmethod
    def raw_decode(cls, attrs):
        for field in cls.json_encoded_fields:
            if field in attrs['object']['uipfield']:
                try:
                    attrs['object']['uipfield'][field] = json.loads(attrs['object']['uipfield'][field])
                except:
                    pass
        return attrs

    def decode(self, attrs):
        super(RevenueIndividualResult, self).decode(attrs)
        for field in self.json_encoded_fields:
            if field in self.uipfield:
                try:
                    self.uipfield[field] = json.loads(self.uipfield[field])
                except:
                    pass


class CompressedIndividualResultModel(IndividualResult, ModelResultHelper):
    kind = "domainmodel.results.CompressedIndividualResultModel"
    version = 2.0
    encrypted = True
    tenant_aware = True
    index_list = {
        'run_time_horizon': dict(name='cache_key'),
        'as_of_mnemonic': dict(name='quarter'),
        'no_of_records': dict(name='count'),
        'batch_no': dict(
            index_spec=[('object.run_time_horizon', 1), ('object.batch_no', 1)],
            name='batch_index',
        )
    }
    primary_key = 'run_time_horizon'
    compressed = True

    def __init__(self, attrs=dict()):
        self.run_time_horizon = attrs.get('run_time_horizon', None)
        self.as_of_mnemonic = attrs.get('as_of_mnemonic', None)
        self.no_of_records = attrs.get('no_of_records', 0)
        self.records = attrs.get('records', [])
        self.batch_no = attrs.get('batch_no', 0)
        super(ModelResult, self).__init__(attrs)

    def encode(self, attrs):
        if getattr(self, "extid", False):
            super(CompressedIndividualResultModel, self).encode(attrs)
        else:
            attrs['run_time_horizon'] = self.run_time_horizon
            attrs['as_of_mnemonic'] = self.as_of_mnemonic
            attrs['no_of_records'] = self.no_of_records
            attrs['records'] = self.records
            attrs['batch_no'] = self.batch_no

    def decode(self, attrs):
        self.run_time_horizon = attrs.get('run_time_horizon', None)
        self.as_of_mnemonic = attrs.get('as_of_mnemonic', None)
        self.no_of_records = attrs.get('no_of_records', 0)
        self.records = attrs.get('records', [])
        self.batch_no = attrs.get('batch_no', 0)
        super(CompressedIndividualResultModel, self).decode(attrs)

    @classmethod
    def update_object_doc(cls, object_doc):
        localattrs = object_doc['object']
        if 'batch_no' in localattrs:
            encdata = sec_context.compress(BSON.encode(localattrs), cls)
            if encdata:
                cls.encdata = encdata
                object_doc.update({
                    "_encdata": encdata,
                    "object": crypto_utils.extract_index(cls.index_list, localattrs)
                })
                object_doc["object"].update(
                    crypto_utils.extract_index(cls.query_list, localattrs)
                )
        return object_doc

    @classmethod
    @criteria_check
    def findDocument(cls, criteria={}):
        record = cls.get_db().findDocument(cls.getCollectionName(), criteria)
        if record and 'object' in record and 'records' in record['object'] and len(record['object']['records']) >= 1:
            identity = record['_id']
            sample_record = record['object']['records'][0]
            sample_record['_id'] = identity
            return sample_record
        else:
            return None

    @classmethod
    def getAll(cls, criteria={}):
        kind = CompressedIndividualResultModel.kind
        version = CompressedIndividualResultModel.version
        for record in cls.findDocuments(criteria):
            record['_kind'] = kind
            record['_version'] = version
            obj = cls(record)
            yield obj

    @classmethod
    @criteria_check
    def findDocuments(cls, criteria={}, sort=None):
        l = []
        for doc in cls.get_db().findDocuments(cls.getCollectionName(), criteria, sort=[('object.run_time_horizon', 1),
                                                                                       ('object.batch_no', 1)]):
            identity = doc['_id']
            for record in doc['object']['records']:
                record['_id'] = identity
                if sort:
                    l.append(record)
                else:
                    yield record
        if sort:
            l = sorted(l, key=lambda a: a['object']['extid'])
            for record in l:
                yield record

    @classmethod
    def compressedSave(cls, list_of_records, model_name=None):
        model_name = model_name or cls.cls_model_name
        records_count = len(list_of_records)
        if records_count > 0:
            batch = getattr(cls, "batch_size", False)
            if not batch:
                batch = get_max_allowed_batch_size(list_of_records, model_name)
            run_time_horizon = list_of_records[0]['object']['run_time_horizon']
            as_of_mnemonic = list_of_records[0]['object']['as_of_mnemonic']
            for i in range(0, int(records_count / batch + 1)):
                current_batch = list_of_records[i * batch: (i + 1) * batch]
                if current_batch:
                    attrs = dict(run_time_horizon=str(run_time_horizon),
                                 as_of_mnemonic=str(as_of_mnemonic),
                                 no_of_records=len(current_batch),
                                 records=current_batch,
                                 batch_no=i + 1
                                 )
                    obj = cls(attrs)
                    obj.save()
                    logger.info("Saving mnemonic %s, rth %s batch %s with number %s",
                                as_of_mnemonic,
                                run_time_horizon,
                                len(current_batch),
                                attrs.get('batch_no', -1))
        else:
            logger.warning("No records to save for the compressed Model name %s" % model_name)

    @classmethod
    def getAllByFieldValue(cls, field, value):
        criteria = {"object.%s" % field: value}
        return cls.getAll(criteria)

    @classmethod
    def getAllByCachekey(cls, cache_key):
        return cls.getAll({'object.run_time_horizon': cache_key})

    @classmethod
    @criteria_check
    def find_count(cls, criteria={}):
        count = 0
        for i in cls.get_db().findDocuments(cls.getCollectionName(), criteria, {'object.no_of_records': 1}):
            count += i['object']['no_of_records']
        return count

    @classmethod
    @criteria_check
    def truncate_or_drop(cls, criteria={}):
        count = cls.find_count(criteria)
        super(CompressedIndividualResultModel, cls).truncate_or_drop(criteria=criteria)
        return count

    @classmethod
    def UpdateAndSave(cls, update_dict):
        ids = [doc['_id'] for doc in update_dict.values()]
        docs = cls.get_db().findDocuments(cls.getCollectionName(),
                                          {'_id': {'$in': ids}}
                                          )
        for doc in docs:
            for index, record in enumerate(doc['object']['records']):
                if record['object']['extid'] in update_dict:
                    extid = record['object']['extid']
                    doc['object']['records'][index] = update_dict[extid]
            cls.update_object_doc(doc)
            doc['last_modified_time'] = dateUtils.epoch().as_epoch()
            cls.get_db().saveDocument(cls.getCollectionName(), doc)

    def __str__(self, *args, **kwargs):
        return "<CompressedModel cache_key=%s count=%s>" % (self.run_time_horizon, self.no_of_records)


class GroupResult(ModelResult, ModelResultHelper):
    """ Purpose of this class is to store the results for a combination group.
    """
    kind = "domainmodel.results.GroupResult"
    version = 1.0
    index_list = {
        'run_time_horizon': {
            'name': 'model_runs_index'
        }, 'name': {},
        'as_of_mnemonic': {
            'name': 'as_of_mnemonic_index'
        }
    }

    def __init__(self, attrs=None):
        self.name = None
        super(GroupResult, self).__init__(attrs)

    def __str__(self):
        super_str = super(GroupResult, self).__str__()
        return "%s\nGroupResult < %s >" % (super_str, str(self.filter))

    def encode(self, attrs):
        attrs['name'] = self.name
        super(GroupResult, self).encode(attrs)

    def decode(self, attrs):
        self.name = attrs['name']
        super(GroupResult, self).decode(attrs)

    @classmethod
    def get_for_filter(cls, ordered_filter):
        raise NotImplemented


class DimensionResult(BasicResult):
    """ Stores the dimension results such as quantiles, won amounts etc """
    kind = "domainmodel.results.DimensionResult"
    version = 1.0
    begin_month_mnemonic = None
    as_of_month_mnemonic = None
    horizon_month_mnemonic = None
    index_list = {
        'run_time_horizon~extid': {'name': 'rth_eid'},
        'extid': {'unique': True},
        'run_time_horizon': {},
        'as_of': {},
        'begins': {},
        'as_of_mnemonic': {
            'name': 'as_of_mnemonic_index'
        },
        'begin_month_mnemonic': {
            'name': 'begin_m_index'
        },
        'as_of_month_mnemonic': {
            'name': 'as_of_m_index'
        },
        'horizon_month_mnemonic': {
            'name': 'horizon_m_index'
        },
        'dim_attrs.dimensions': {},
        'dim_attrs.full_dim_type': {},
        'sort_index': {
            'index_spec': [
                ('object.dim_attrs.full_dim_val', pymongo.DESCENDING),
                ('object.begins', pymongo.ASCENDING),
                ('object.as_of', pymongo.ASCENDING),
                ('object.horizon', pymongo.ASCENDING)
            ],
            'name': 'sort_index'
        },
        'dim_mnemonic_dim_val_th': {
            'index_spec': [
                ('object.dim_attrs.full_dim_type', pymongo.ASCENDING),
                ('object.as_of_mnemonic', pymongo.ASCENDING),
                ('object.dim_attrs.full_dim_val', pymongo.DESCENDING),
                ('object.begins', pymongo.ASCENDING),
                ('object.as_of', pymongo.ASCENDING),
                ('object.horizon', pymongo.ASCENDING)
            ],
            'name': "dim_mnemonic_dim_val_th"
        },
    }

    def __init__(self, attrs=None):
        self.extid = None
        self.extended_attrs = {}
        self.expires = None
        self.dim_attrs = {}
        super(DimensionResult, self).__init__(attrs)

    def encode(self, attrs):
        attrs['extid'] = self.extid
        attrs['extended_attrs'] = self.extended_attrs
        attrs['dim_attrs'] = self.dim_attrs
        attrs['begin_month_mnemonic'] = self.begin_month_mnemonic
        attrs['as_of_month_mnemonic'] = self.as_of_month_mnemonic
        attrs['horizon_month_mnemonic'] = self.horizon_month_mnemonic
        super(DimensionResult, self).encode(attrs)

    def decode(self, attrs):
        self.extid = attrs['extid']
        self.extended_attrs = attrs.get('extended_attrs', {})
        self.dim_attrs = attrs.get('dim_attrs', {})
        self.begin_month_mnemonic = attrs.get('begin_month_mnemonic', None)
        self.as_of_month_mnemonic = attrs.get('as_of_month_mnemonic', None)
        self.horizon_month_mnemonic = attrs.get('horizon_month_mnemonic', None)
        super(DimensionResult, self).decode(attrs)

    def set_attr(self, name, value):
        if value is None:
            self.extended_attrs.pop(name, None)
        else:
            self.extended_attrs[name] = value

    def set_dim_attr(self, name, value):
        if value is None:
            self.dim_attrs.pop(name, None)
        else:
            self.dim_attrs[name] = value

    @classmethod
    def set_time_attrs(cls, run_time_horizon, begins, as_of, horizon, begin_month_mnemonic=None,
                       as_of_month_mnemonic=None,
                       horizon_month_mnemonic=None):
        cls.run_time_horizon = run_time_horizon
        cls.begins = begins
        cls.horizon = horizon
        cls.as_of = as_of
        if begin_month_mnemonic:
            cls.begin_month_mnemonic = begin_month_mnemonic
        if as_of_month_mnemonic:
            cls.as_of_month_mnemonic = as_of_month_mnemonic
        if horizon_month_mnemonic:
            cls.horizon_month_mnemonic = horizon_month_mnemonic
        if as_of.tzinfo:
            cls.as_of_mnemonic = current_period(as_of)[0]
        else:
            cls.as_of_mnemonic = current_period(pytz.utc.localize(as_of))[0]

    def set_dim_attrs(self, group_type, group_value):
        group_value_segs = group_value.split('~')
        group_type_segs = group_type.split('~')
        dimensions = []
        for i in range(len(group_type_segs)):
            dimensions.append('~'.join(group_type_segs[0:i + 1]) + '~::~' + '~'.join(group_value_segs[0:i + 1]))
        self.set_dim_attr('dimensions', dimensions)
        self.set_dim_attr('final_dim_type', group_type_segs[-1])
        self.set_dim_attr('final_dim_val', group_value_segs[-1])
        self.set_dim_attr('full_dim_type', group_type)
        self.set_dim_attr('full_dim_val', group_value)

    def remove_prefix(self, prefix):
        full_prefix = prefix + "__"
        for x in list(self.extended_attrs.keys()):
            if x.startswith(full_prefix):
                self.extended_attrs.pop(x)


class ModelRunDetails(ModelResult):
    """ Domain class to save the configuration details used to run the model. """
    kind = "domainmodel.results.ModelRunDetails"
    version = 1.0
    index_list = {
        'run_time_horizon': {
            'name': 'model_runs_index',
            'unique': True
        },
        'as_of_mnemonic': {
            'name': 'as_of_mnemonic_index'
        },
        'begin_month_mnemonic': {
            'name': 'begin_m_index'
        },
        'as_of_month_mnemonic': {
            'name': 'as_of_m_index'
        },
        'horizon_month_mnemonic': {
            'name': 'horizon_m_index'
        }
    }

    def __init__(self, attrs=None):
        self.effective_config = None
        self.code_version = None
        self.config_version = None
        self.maps = None
        self.effective_filter = None
        self.effective_history_filter = None
        self.run_count = None
        self.user = None
        self.run_date = None
        self.begin_month_mnemonic = None
        self.as_of_month_mnemonic = None
        self.horizon_month_mnemonic = None
        super(ModelRunDetails, self).__init__(attrs)

    def encode(self, attrs):
        if not self.code_version:
            process = Popen(["git", "log", "-1", "--oneline", "--no-merges"], stdout=PIPE)
            exit_code = os.waitpid(process.pid, 0)
            output = process.communicate()[0]
            self.code_version = output
        attrs['code_version'] = self.code_version
        if not self.config_version:
            if ISPROD:
                repopath = os.environ.get('CONFIG_REPO', '/opt/gnana/config')
                process = Popen(["git", "log", "-1", "--oneline", "--no-merges"], cwd=repopath, stdout=PIPE)
                exit_code = os.waitpid(process.pid, 0)
                output = process.communicate()[0]
                self.config_version = output
            else:
                self.config_version = None
        attrs['config_version'] = self.config_version
        attrs['code_version'] = self.code_version
        attrs['config'] = self.effective_config
        attrs['maps_used'] = self.maps
        attrs['filter_used'] = self.effective_filter
        attrs['history_filter_used'] = self.effective_history_filter
        attrs['run_count'] = self.run_count
        attrs['user'] = self.user
        attrs['run_date'] = self.execution_time
        attrs['begin_month_mnemonic'] = self.begin_month_mnemonic
        attrs['as_of_month_mnemonic'] = self.as_of_month_mnemonic
        attrs['horizon_month_mnemonic'] = self.horizon_month_mnemonic
        super(ModelRunDetails, self).encode(attrs)

    def decode(self, attrs):
        self.effective_config = attrs['config']
        self.code_version = attrs['code_version']
        self.config_version = attrs.get('config_version', 'unknown')
        self.effective_filter = attrs.get('filter_used', None)
        self.maps = attrs.get('maps_used', {})
        self.effective_history_filter = attrs.get('history_filter_used', None)
        self.run_count = attrs.get('run_count', 0)
        self.user = attrs.get('user', None)
        self.run_date = attrs.get('run_date', None)
        if self.run_date.tzinfo is None:
            self.run_date = pytz.utc.localize(self.run_date)
        self.begin_month_mnemonic = attrs.get('begin_month_mnemonic', None)
        self.as_of_month_mnemonic = attrs.get('as_of_month_mnemonic', None)
        self.horizon_month_mnemonic = attrs.get('horizon_month_mnemonic', None)
        super(ModelRunDetails, self).decode(attrs)

    def set_compressed_time_attrs(self, begins, as_of, horizon):
        current_period_tuple = None
        if as_of.tzinfo:
            current_period_tuple = current_period(as_of)
        else:
            as_of = pytz.utc.localize(as_of)
            current_period_tuple = current_period(pytz.utc.localize(as_of))
        self.as_of_mnemonic = current_period_tuple[0]
        if not begins.tzinfo:
            begins = pytz.utc.localize(begins)
        if not horizon.tzinfo:
            horizon = pytz.utc.localize(horizon)
        months_list = []
        start_month = current_period_tuple[1].month
        end_month = current_period_tuple[2].month
        if end_month < start_month:
            end_month = end_month + 12
        for m in range(start_month, end_month + 1):
            if m > 12:
                m = m - 12
            months_list.append(m)

        def month_menmonic(run_time_date):
            if (run_time_date >= current_period_tuple[1] and run_time_date <= current_period_tuple[2]):
                try:
                    return months_list.index(run_time_date.month) + 1
                except:
                    return None

        self.begin_month_mnemonic = month_menmonic(begins)
        self.as_of_month_mnemonic = month_menmonic(as_of)
        self.horizon_month_mnemonic = month_menmonic(horizon)