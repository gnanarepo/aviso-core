import logging
import re
import sys
import traceback
from collections import defaultdict
from datetime import datetime

import pymongo
import pytz
from aviso import settings
from aviso.settings import sec_context, gnana_db

from domainmodel import Model
from tasks.fields import yyyymmdd_to_xl
from utils import GnanaError
from utils.date_utils import get_a_date_time_as_float_some_how, datetime2xl, epoch, xl2datetime

MAX_CREATED_DATE = 1000000

ALL_PERIODS_CACHE = {}
EVENT_AGGR_CACHE = {}

logger = logging.getLogger('gnana.%s' % __name__)
idx_list = {'extid': {},
            'when': {},
            'filename': {},
            'partition_uniqueness': {'unique': True,
                                     'sparse': True}}

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

class UIPError(Exception):

    def __init__(self, message):
        self.error_message = message


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


class PartitionData(Model):
    tenant_aware = True
    version = 1.0
    kind = "domainmodel.uip.PartitionData"
    index_list = idx_list.copy()

    def __init__(self, attrs=None):
        self.partition_map = {}
        self.default = None
        self.partition_uniqueness = 'partitiondata'
        super(PartitionData, self).__init__(attrs)

    def encode(self, attrs):
        attrs['default'] = self.default
        attrs['partition_map'] = self.partition_map
        attrs['partition_uniqueness'] = 'partitiondata'

    def decode(self, attrs):
        self.default = attrs['default']
        self.partition_map = attrs['partition_map']
        return super(PartitionData, self).decode(attrs)


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
                        raise GnanaError('use_value=any can be used with one feature only')
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
        for fld, old_hist in self.featMap.items():
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
            for date, cf_hist in sorted(history.items(), key=lambda x: float(x[0])):
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

    def printData(self):
        print('opporty\tfeature\tdate\tvalue')
        oppData = self.featMap
        print(self.ID)
        for feature in sorted(oppData):
            print('\t', feature)
            for date, value in sorted(oppData[feature]):
                print('\t\t', date, value)
        return

    def printFeature(self, feature):
        print(self.ID)
        print('opporty\tfeature\tdate\tvalue')
        if feature in self.featMap:
            for date, value in self.featMap[feature]:
                print("%s\t%s\t%s\t%s" % (self.ID, feature, xl2datetime(float(date)), value))
        else:
            print('N/A')
        return

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

class RevenueSchedule:

    """ A wrapper class for the Revenue Schedule format used in Forecast3.
    Contains methods to manipulate the revenue schedule."""

    def __init__(self, created_date, cf_offset=0, cf_date_type='yyyymmdd', allow_null_cd_date=False):
        """Create a blank revenue schedule, given the opp's created_date."""
        self.schedule = {}
        self.created_date = created_date
        # date typing not implemented.
        self.cf_date_type = cf_date_type
        self.cf_offset = cf_offset
        self.compressed = True
        # Cache flows for these dates will be ignored.
        self.null_vals = set() if allow_null_cd_date else {'N/A'}

    def get_uip(self):
        """Returns the value of the revenue schedule in
        the right uip format for the feat map."""
        if not self.compressed:
            self.compress()
        return [[self.created_date, self.schedule]]

    def add_cashflow(self, cf_date,):  # cf_date_type):
        """Add a new (empty) cashflow on the specified date."""
        if cf_date not in self.schedule:
            self.schedule[cf_date] = []
        self.compressed = False

    def update_cf_amt(self, cf_date, change_date, amt_diff):
        """Change the amount of a cashflow on a given date.
        cf_date - date of the cashflow.
        change_date - date the change will be made.
        amt_diff - amount to change the cashflow by.

        Returns True if schedule was changed, False if not.
        """
        if cf_date in self.null_vals:
            return False
        if cf_date not in self.schedule:
            self.add_cashflow(cf_date)
        self.schedule[cf_date].append([change_date, amt_diff])
        self.compressed = False
        return True

    def compress(self, th=0.01):
        """
        Transform a noncompressed revenue schedule
        where ts vals are (outoforder) diffs to
        compressed ts where values are amts.
        th - min time between time stamps in compressed time series.
        """
        if self.compressed:
            return

        # We use .items() since we will be deleting entries
        for cf_date, cashflow in self.schedule.items():
            if not cashflow:
                del self.schedule[cf_date]
            else:
                compressed_cf = []
                current_val = 0
                events = sorted(cashflow)
                current_ts = events[0][0]
                for (event_ts, event_val) in events:
                    if event_ts - current_ts > th:
                        compressed_cf.append([current_ts, current_val])
                        current_ts = event_ts
                    # TODO Don't assume this needs to be converted
                    current_val += float(event_val)

                compressed_cf.append([current_ts, current_val])
                try:
                    cf_key = str(int(yyyymmdd_to_xl(cf_date)) - self.cf_offset)
                except:
                    # TODO: This is not safe and can result in a prepare error. BEWARE.
                    cf_key = str(cf_date - self.cf_offset)
                if cf_key != cf_date:
                    del self.schedule[cf_date]
                self.schedule[cf_key] = compressed_cf
        # Add some initial 0's to ensure that cutting at fv won't give you nonsense.
        first_date = min([cf[0][0] for (cf_date, cf) in self.schedule.items()])
        for (cf_date, cf) in self.schedule.items():
            old_cf = self.schedule[cf_date]
            if old_cf[0][0] > first_date:
                self.schedule[cf_date] = [[first_date, 0.0]] + old_cf


class DataHandler:
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
        self.load_data()

    def load_data(self):
        self.data.clear()
        ds_inst = sec_context.get_dataset_by_name_and_stage(self.ds_name, None)
        from domainmodel.datameta import UIPIterator
        for rec in UIPIterator(ds_inst=ds_inst,
                               record_filter={},
                               use_local_cache=False if settings.DEBUG else True,
                               record_range=[0]):
            if self.prepare_ds:
                rec.prepare(ds_inst)
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
                        logger.warning("WARNING: ID=%s from dataset=%s has multiple lookup values in '%s'..."
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

class MongoDBDirect(DataHandler):

    def __init__(
        self,
        ds_name,
        lookup_fld,
        other_flds,
        force_refresh=False,
        prepare_ds=False,
    ):
        super(MongoDBDirect, self).__init__(
            ds_name,
            lookup_fld,
            other_flds,
            force_refresh=force_refresh
        )
        self.prepare_ds = prepare_ds
        from domainmodel.datameta import Dataset, DatasetClass
        self.ds_inst = Dataset.getByNameAndStage(self.ds_name)
        self.ds_class = DatasetClass(self.ds_inst)
        self.collection_name = self.ds_class.getCollectionName()

        if self.lookup_fld == "ID":
            self.criteria_key = "object.extid"
        else:
            self.criteria_key = "object.values." + self.lookup_fld

    def __getitem__(self, k):
        from domainmodel.datameta import UIPIterator
        recs = list(
            UIPIterator(
                ds_inst=self.ds_inst,
                record_filter={self.criteria_key: sec_context.encrypt(k)},
                use_local_cache=False,  # local cache is too slow for indididual calls
                override_minimum=1000000, # assuming that one-to-may will not use more than a million recs
                record_range=[0,1000000], log_info=False))
        if not recs:
            raise KeyError("ERROR: %s==%s not found in mongodb" % self.criteria_key, k)
        if self.prepare_ds:
            [rec.prepare(self.ds_inst) for rec in recs]
        if self.other_flds:
            for rec in recs:
                rec.featMap = {k: v for k, v in rec.featMap.items()
                               if k in self.other_flds}
        return recs

    def get(self, k, default=None):
        try:
            return self[k]
        except:
            return default

    def __contains__(self, k):
        return gnana_db.find_count(self.collection_name,
                                   {self.criteria_key: sec_context.encrypt(k)}) > 0

    def __len__(self):
        return gnana_db.find_count(self.collection_name)

def get_data_handler(
    dh_type,
    **args
):
    if dh_type == "M":
        return MongoDBMemoryCache.get_data_handler(**args)
    elif dh_type == "D":
        try:
            cache_historic_keys = args.pop("cache_historic_keys")
        except:
            cache_historic_keys = False
        if cache_historic_keys:
            raise Exception("ERROR: cache_historic_keys=True not supported for dh_type=D")
        return MongoDBDirect.get_data_handler(**args)
    raise Exception("ERROR: dh_type=%s not supported" % dh_type)
