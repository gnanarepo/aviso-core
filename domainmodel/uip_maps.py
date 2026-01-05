import re
import traceback
import numpy as np
from itertools import chain
from aviso.settings import sec_context
import logging
from domainmodel.uip import get_data_handler, RevenueSchedule
from utils import date_utils, is_true
from uip import MAX_CREATED_DATE
from utils.time_series_utils import SplitsEventAggregator, LineItemEventAggregator
import math
from utils.misc_utils import try_float, trimsort, format_val
import operator
from utils.math_utils import excelToFloat
from utils.date_utils import datetime2xl, epoch2xl, datetime2epoch, xl2datetime, now, epoch, get_future_bom
from domainmodel.app import AccountEmails
from utils import relativedelta
from domainmodel.csv_data import CSVDataClass
from collections import OrderedDict

logger = logging.getLogger('gnana.%s' % __name__)

EVENT_AGGR_CACHE = {}
ALL_PERIODS_CACHE = {}
ATTACHMENT_MAP = {'xls': 'xls',
                  'xlsx': 'xls',
                  'xltx': 'xls',
                  'ics': 'meetings',
                  'pdf': 'pdf',
                  'key': 'slideshow',
                  'ppt': 'slideshow',
                  'pptx': 'slideshow',
                  'docx': 'doc',
                  'doc': 'doc',
                  'txt': 'doc'}


def build_uip_map(feature, config, ds):
    map_obj = None
    if feature.find('):') > 5:
        map_function_and_args, out_fld = feature.split('):')
        split_point = map_function_and_args.find('(')
        map_function, args = map_function_and_args[:split_point], map_function_and_args[split_point + 1:]
        map_class = translation_functions(map_function)
        if map_class:
            map_obj = map_class(feature, config, ds)
            map_obj.in_flds = args.split(',')
            map_obj.out_fld = out_fld
            map_obj.map_fn_name = map_function
    else:
        map_class = translation_functions('translate')
        map_obj = map_class(feature, config, ds)
        map_obj.out_fld = feature + 'Trans'
        map_obj.in_flds = [feature]
        map_obj.map_fn_name = 'translate'
    return map_obj


def translation_functions(fn):
    available_fns = {
        'translate': TranslateValues,
        'adorn_with_product_categories': AdornWithProductCategories,
        'join': Join,
        'combine_fields2': CombineFields2,
        'calc_adjusted_stages': CalculateAdjustedStages,
        'backdate_after_close': BackdateAfterClose,
        'freeze_fields': FreezeFields,
        'adorn_with_hierarchy': AdornWithHierarchy,
        'adorn_with_splits': AdornWithSplits,
        'adorn_with_revenue_schedule': AdornWithRevenueSchedule,
        'bookings_to_revenue_schedule': BookingsToRevenueSchedule,
        'split_ratios_from_amount_flds': SplitRatiosFromAmountFields,
        'adorn_with_lineitem_table': AdornWithLineItemTable,
        'adorn_with_lineitem_schedule': AdornWithLineitemSchedule,
        'adorn_with_raw_lineitem_schedule': AdornWithRawLineitemSchedule,
        'adorn_with_quote_info': AdornWithQuoteInfo,
        'adorn_with_fields': AdornWithFields,
        'event_aggregation': EventAggregation,
        'cleanup_enabled_history': CleanupEnabledHistory,
        'bands': TranslateBands,
        'ccy_conv': CCYConv,
        'ccy_conv2': CCYConv2,
        'datedccy_conv': DatedCCYConv,
        'merge_fields': MergeFields,
        'transform_field': TransformFields,
        'transform_timestamps': TransformTimestamps,
        'transform_pairs': TransformPairs,
        'transpose_schedule': TransposeSchedule,
        'backdate': Backdate,
        'adorn_with_previous_value': AdornWithPreviousValue,
        'split_field': SplitField,
        'combine_fields': CombineFields,
        'delete_fields': DeleteFields,
        'filter_field': FilterField,
        'create_acv_timeline': CreateACVTimeline,
        'cumsum': Cumsum,
        'count_distinct_values': CountDistinctValues,
        'delta': Delta,
        'sum_daily_moves': SumDailyMoves,
        'move_date_values': MoveDateValues,
        'move_all_timestamps': MoveAllTimestamps,
        'adhoc_replacements': AdhocReplacements,
        'IDSource': IDSource,
        'email_activity_score': EmailActivityScore,
        'activity_counts': ActivityCounts,
        'range_filter': RangeFilter,
        'range_filter2': RangeFilter2,
        'adorn_with_product_amts': AdornWithProductAmounts,
        'adorn_with_relative_month': AdornWithRelativeMonth,
        'adorn_with_months': AdornWithMonths,
        'reset_obj_created_date': ResetObjCreatedDate,
        'calculate_closedate_pushes': CalulateCloseDatePushes,
        'change_count': ChangeCount,
        'copy_filter_field': CopyFilterField,
    }
    if fn not in available_fns:
        raise KeyError(
            'The function ' + fn + ' is not defined in the maps')
    if available_fns.get(fn):
        return available_fns[fn]
    else:
        return None


class UIPMap:

    def __init__(self, feature, config, stage_ds):
        # Changed to 1.1 so that after this code is pushed to various environments
        # full prepare shall be done for the first time and checksum shall be populated
        self.version = 1.1
        self.feature = feature
        self.config = config
        self.stage_ds = stage_ds

    def prepare_criteria(self):
        return {}

    def get_map_details(self, output_fields_only=False):
        return None


class DBMap(UIPMap):

    def __init__(self, feature, config, stage_ds=None):
        self.feature = feature
        self.config = config
        self.in_flds = None
        self.out_fld = None
        self.map_fn_name = None
        self.prepare_criteria = {}
        self.cache_criteria = None
        self.stage_ds = stage_ds
        super(DBMap, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        raise Exception('abstract class')


class OneToManyDBMap(DBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        # Like OpporutnityLineItem dataset which has One to Many relationship with Opportunity
        self.relationship_dataset = config.get('ds_name')
        super(OneToManyDBMap, self).__init__(feature, config, stage_ds)

    def get_prepare_criteria(self):
        tenant = sec_context.details
        prepare_status_flag = tenant.get_flag('prepare_status', self.stage_ds.name, {})
        if prepare_status_flag.get('prepare_start_time'):
            criteria = {'last_modified_time': {'$gte': prepare_status_flag.get('prepare_start_time')}}
        else:
            criteria = {}
        logger.info('One to Many criteria : %s' % criteria)
        return criteria


class ManyToOneDBMap(DBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        # Like Account or User dataset which has Many to One relationship with Opportunity
        self.relationship_dataset = config.get('ds_name')
        # Like AccountID or OwnerID. This field will be on the source dataset (Opportunity)
        self.join_fld = config.get('source_ref_key')
        # This field will be on the relationship dataset (Like ID which represents UserID)
        self.lookup_fld = config['lookup_fld']
        super(ManyToOneDBMap, self).__init__(feature, config, stage_ds)

    def post_init(self):
        if len(self.in_flds) > 1:
            raise Exception('ManyToOneDBMap supports only one input field')
        # If source_ref_key is not defined in the config, use the first input field of the map as ref key
        if self.join_fld is None:
            self.join_fld = self.in_flds[0]

    def get_prepare_criteria(self):
        tenant = sec_context.details
        prepare_status_flag = tenant.get_flag('prepare_status', self.stage_ds.name, {})
        if prepare_status_flag.get('prepare_start_time'):
            criteria = {'last_modified_time': {'$gte': prepare_status_flag.get('prepare_start_time')}}
        else:
            criteria = {}
        logger.info('Many to one criteria : %s' % criteria)
        return criteria


class Join(ManyToOneDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(Join, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            fld_defs = self.config['fld_defs']
            src_ds_name = self.config['ds_name']
            for key, defs in fld_defs.items():
                fld = {"name": key, "dep_flds": {src_ds_name: defs['req_fields']}, "format": "N/A", "source": "Direct"}
                out_fields.append(fld)
            return out_fields

        else:
            map_details = {}
            field_info = {}
            other_attr = {}
            map_details["Source"] = self.config['ds_name']
            fld_defs = self.config['fld_defs']
            schema = OrderedDict()
            schema["req_fields"] = {"type": "array", "label": "In Fields"}
            schema["out_fld"] = {"type": "string", "label": "Out Field"}
            schema["fallback_val"] = {"type": "string", "label": "fallback value"}
            field_info['schema'] = schema
            field_info['order'] = ["req_fields", "out_fld", "fallback_val"]
            data = []
            for key, defs in fld_defs.items():
                record = {}
                for skey, _ in schema.items():
                    if skey not in defs.keys():
                        if skey == 'out_fld':
                            record[skey] = key
                        else:
                            record[skey] = '-'
                    else:
                        record[skey] = defs[skey]
                data.append(
                    record
                )
            field_info["data"] = data
            if "lookup_fld" in self.config.keys():
                other_attr["lookup_fld"] = {"type": "singleValueSelect", "label": "lookup field",
                                            "value": self.config["lookup_fld"],
                                            "choices": [self.config["lookup_fld"]]}
            if "exec_rank" in self.config.keys():
                other_attr["exec_rank"] = {"type": "integer", "label": "exec rank",
                                           "value": self.config["exec_rank"]}
            if "log_warnings" in self.config.keys():
                other_attr["log_warnings"] = {"type": "boolean", "label": "log warning",
                                              "value": self.config["log_warnings"]}
            map_details["field_info"] = field_info
            map_details["attributes"] = other_attr
            return map_details

    def process(self, uip_obj):
        """
        Join using the history of <join_fld> by looking it up in <ds_name>.<lookup_fld>
        join_fld: (is the in_flds[0]) is the field on the current dataset. It can be a feature
            which will have history, or it can be "ID" in which case the implied history will
            be the ID of the record from it's creation date.
        lookup_fld: field name from other dataset to look up using the join_fld.
        fld_defs: keys are output field names, values are dicts with following elements:
            'req_fields': list of fields required for the output field (max one element allowed per list)
            'fallback_fld': name of field from current dataset to fall back to if no matches found
            'fallback_val': constant value to fall back to if no matches found
        backdate_join_fld: having this parameter set to True means you will get the entire history of the
            first lookup history (useful when for example OwnerID does not have history but you want to
            make sure that when you join the CustomUserDS, you get all the historic as well)
        """

        log_warnings = self.config.get('log_warnings', False)
        join_fld = self.in_flds[0]
        if join_fld not in uip_obj.featMap and join_fld != 'ID':
            if log_warnings:
                logger.warning('join fld %s is not found (ID=%s)', join_fld, uip_obj.ID)
            return
        ds_name = self.config['ds_name']
        lookup_fld = self.config['lookup_fld']
        lookup_latest_only = self.config.get('lookup_latest_only', False)
        fld_defs = self.config['fld_defs']
        has_splits = self.config.get('has_splits', False)
        split_join_fld_use_keys = self.config.get('split_join_fld_use_keys', False)
        prepare_ds = self.config.get('prepare_ds', False)
        cache_name = self.config.get('cache_name', self.out_fld)
        cache_force_refresh = self.config.get('cache_force_refresh', False)
        use_fv = self.config.get('use_fv', True)
        honor_created = self.config.get('honor_created', False)
        backdate_join_fld = self.config.get('backdate_join_fld', False)
        fail_multiple_rows = self.config.get('fail_multiple_rows', False)
        fail_lookup_ref_not_found = self.config.get('fail_lookup_ref_not_found', False)

        req_flds = set()
        for out_fld, dtls in fld_defs.items():
            in_flds = dtls['req_fields']
            if len(in_flds) > 1:
                raise Exception("join currently only supports req_fields with only one element")
            req_flds.add(in_flds[0])
        req_flds = list(req_flds)

        data_handler = get_data_handler(
            dh_type=self.config.get("dh_type", "M"),
            cache_name=cache_name,
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            other_flds=req_flds,
            force_refresh=cache_force_refresh,
            prepare_ds=prepare_ds,
            cache_historic_keys=not lookup_latest_only,
        )

        if join_fld == 'ID':
            lookup_hist = [(uip_obj.created_date, uip_obj.ID)]
        else:
            lookup_hist = uip_obj.featMap.get(join_fld, [])
        if lookup_latest_only:
            lookup_hist = lookup_hist[-1:]

        trans_map = {x: [] for x in fld_defs.iterkeys()}
        if not has_splits:
            lookup_hist = [[ts, {val: val}] for ts, val in lookup_hist]

        def get_fallback(fld_def, fbts, rec=None):
            if use_fv and rec and fld_def["req_fields"][0] in rec.featMap:
                fallback_val = rec.featMap[fld_def["req_fields"][0]][0][1]
            else:
                fallback_val = fld_def.get('fallback_val', None)
                fallback_fld = fld_def.get('fallback_fld', None)
                if fallback_fld is not None:
                    fallback_fld_val = uip_obj.getAsOfDateF(fallback_fld,
                                                            fbts,
                                                            first_value_on_or_after=use_fv,
                                                            NA_before_created_date=not use_fv)
                    if fallback_fld_val != "N/A" or fallback_val is None:
                        fallback_val = fallback_fld_val
            return fallback_val

        for i, (beg_ts, lookup_vals) in enumerate(lookup_hist):
            if not i and backdate_join_fld:
                beg_ts = None
            end_ts = lookup_hist[i + 1][0] if (i + 1) < len(lookup_hist) else None
            if has_splits:
                split_trans_map = {x: {} for x in fld_defs.iterkeys()}
                if split_join_fld_use_keys:
                    lookup_vals = {x: x for x in lookup_vals}
            for splt_key, luv in lookup_vals.items():
                rec_list = data_handler.get(luv, [])
                if not rec_list:
                    if fail_lookup_ref_not_found:
                        raise Exception("join was not able to find %s=%s (ID=%s)",
                                        join_fld, luv, uip_obj.ID)
                    if log_warnings:
                        logger.warning("WARNING: join was not able to find %s=%s (ID=%s)",
                                       join_fld, luv, uip_obj.ID)

                    for out_fld, dtls in fld_defs.items():
                        fbts = beg_ts if beg_ts else uip_obj.created_date
                        fallback_val = get_fallback(dtls, fbts)
                        if fallback_val is not None:
                            if has_splits:
                                if fbts not in split_trans_map[out_fld]:
                                    split_trans_map[out_fld][fbts] = {}
                                split_trans_map[out_fld][fbts][splt_key] = fallback_val
                            else:
                                trans_map[out_fld].append([fbts, fallback_val])
                else:
                    if len(rec_list) > 1:
                        if fail_multiple_rows:
                            raise Exception('join expects at most one record')
                        if log_warnings:
                            logger.warning("WARNING: join found multiple values for %s=%s (ID=%s)"
                                           "- defaulting to first", join_fld, luv, uip_obj.ID)
                    rec = rec_list[0]
                    for out_fld, dtls in fld_defs.items():
                        hist_slice = rec.getHistorySlice(
                            dtls['req_fields'][0], beg_ts, end_ts, use_fv, honor_created)
                        if not hist_slice or (beg_ts is not None and hist_slice[0][0] != beg_ts):
                            fbts = beg_ts if beg_ts else uip_obj.created_date
                            fallback_val = get_fallback(dtls, fbts, rec)
                            if fallback_val is not None:
                                hist_slice = [[fbts, fallback_val]] + hist_slice
                        if has_splits:
                            for slice_ts, slice_val in hist_slice:
                                if slice_ts not in split_trans_map[out_fld]:
                                    split_trans_map[out_fld][slice_ts] = {}
                                split_trans_map[out_fld][slice_ts][splt_key] = slice_val
                        else:
                            trans_map[out_fld].extend(hist_slice)
            if has_splits:
                for out_fld, hist_dict in split_trans_map.items():
                    split_trans_map[out_fld] = [[ts, val] for ts, val in sorted(hist_dict.items())]
                    for i, (ts, split_dict) in enumerate(split_trans_map[out_fld][1:]):
                        for split_name, split_val in split_trans_map[out_fld][i][1].items():
                            if split_name not in split_dict:
                                split_dict[split_name] = split_val
                    fallback_val = fld_defs[out_fld].get('fallback_val', None)
                    if beg_ts is None and (use_fv or fallback_val is not None):  # make sure to fill back as well
                        for i, (ts, split_dict) in enumerate(split_trans_map[out_fld][::-1][1:]):
                            for split_name, split_val in split_trans_map[out_fld][::-1][i][1].items():
                                if split_name not in split_dict:
                                    split_dict[split_name] = split_val if use_fv else fallback_val
                    trans_map[out_fld].extend(split_trans_map[out_fld])
        errors = []
        for out_fld, dtls in fld_defs.items():
            if not trans_map[out_fld]:
                if 'fallback_fld' in dtls:
                    fallback_hist = uip_obj.featMap.get(dtls['fallback_fld'], None)
                    if fallback_hist:
                        if has_splits:
                            trans_map[out_fld] = []
                            for ts, lu_dict in lookup_hist:
                                trans_map[out_fld].append(
                                    [ts, {lu_val: uip_obj.getAsOfDateF(out_fld, ts, use_fv, not use_fv)
                                          for lu_val in lu_dict.itervalues()}])
                        else:
                            trans_map[out_fld] = fallback_hist

                if dtls.get('fail_no_val', False) and not trans_map[out_fld]:
                    errors.append('values not found for field %s (ID=%s)' % (out_fld, uip_obj.ID))
        if errors:
            raise Exception('\n'.join(errors))
        # fields without history shall not be added
        trans_map = {k: v for k, v in trans_map.items() if v}
        uip_obj.featMap.update(trans_map)
        if self.config.get('__COMPRESS__', True):
            return trans_map.keys()


class AdornWithProductCategories(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        # Like OpportunityLineItem which has One to Many relationship with Opportunity
        # capturing the ext_id_fld which is the primary key of the source dataset
        self.ref_key = config.get('ext_id_fld')
        super(AdornWithProductCategories, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        from domainmodel.datameta import Dataset, DatasetClass
        ds_name = self.config['ds_name']
        ext_id_fld = self.config['ext_id_fld']  # usually OpportunityID
        splt_label_fld = self.config['splt_label_fld']
        splt_amount_fld = self.config.get('splt_amount_fld', None)
        filters = self.config.get('filters', None)

        ds_inst = Dataset.getByNameAndStage(ds_name)
        ds_class = DatasetClass(ds_inst)
        splt_dtls = ds_class.getAllByFieldValue('values.' + ext_id_fld, uip_obj.ID)
        splts = {}
        for splt_dtl in splt_dtls:
            splt_dtl.prepare(ds_inst)
            splt_dtl = splt_dtl.featMap
            if filters:
                filter_me = False
                for filter_fld, filter_vals in filters.items():
                    val = splt_dtl.get(filter_fld, [[None, 'N/A']])[-1][1]
                    if val in filter_vals:
                        filter_me = True
                        break
                if filter_me:
                    continue
            splt_val = splt_dtl.get(splt_label_fld, [[None, 'N/A']])[-1][1]  # get label
            if splt_val not in splts:
                splts[splt_val] = 0.0
            amt = splt_dtl.get(splt_amount_fld, [[None, 0.0]])[-1][1]
            if amt == "N/A":
                amt = 0.0
            else:
                amt = float(amt)  # add amount for date
            splts[splt_val] += amt
        for label in splts.keys():
            uip_obj.featMap['ProductLabel_' + label] = [[uip_obj.created_date, splts[label]]]
        if self.config.get('__COMPRESS__', True):
            return splts.keys()


class TranslateValues(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(TranslateValues, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{"name": self.out_fld, "format": "N/A", "dep_flds": {"self": [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        out_fld = self.out_fld
        in_flds = self.in_flds
        config = self.config

        if len(in_flds) > 1:
            raise Exception('translate supports only one field')
        feature = in_flds[0]
        trans_vals = []
        orig_vals = uip_obj.featMap.get(feature, None)
        ignore_pattern = re.compile(
            config['__IGNORES__']) if '__IGNORES__' in config else None
        default_value = config.get('__DEFAULT__', None)

        if orig_vals is None:
            if 'N/A' in config:
                # Look for N/A
                uip_obj.featMap[out_fld] = [[uip_obj.created_date, config['N/A']]]
            else:
                # No N/A Mapping foind, and no values found for the UIP field
                pass
        else:
            for ts, ov in orig_vals:
                if isinstance(ov, dict):
                    if ignore_pattern:
                        raise Exception(
                            str.format("ignores incompatible with with dictionary {0} in translation map {1}.",
                                       ov, out_fld))
                    tv = {}
                    # we will always store the original values as keys and their
                    # translations as the values of the output dict
                    for k, v in ov.items():
                        try:
                            tv[k] = config[v]
                        except KeyError:
                            if default_value is None:
                                raise Exception(
                                    str.format("No translation is available for {0} for translation map {1}.",
                                               v, out_fld))
                            elif default_value == "__PASS_THROUGH__":
                                tv[k] = v
                            elif default_value == "__WARN_PASS_THROUGH__" or default_value == "__WARN_WITH_PREFIX__":
                                if default_value == "__WARN_WITH_PREFIX__":
                                    v = '__UNASSIGNED__' + v
                                uip_obj.prepare_warnings[
                                    in_flds[0] + '~' + out_fld].add(v)
                                tv[k] = v
                            else:
                                tv[k] = default_value
                        except:
                            traceback.print_exc()
                            raise Exception(
                                'Unable to run the regular expression match for ignores')
                    trans_vals.append([ts, tv])
                # it is a value that can be mapped directly (string or number)
                else:
                    try:
                        if ignore_pattern is None or not ignore_pattern.match(ov):
                            trans_vals.append([ts, config[ov]])
                    except KeyError:
                        if default_value is None:
                            raise Exception(
                                str.format("No translation is available for {0} for translation map {1}.", ov, out_fld))
                        elif default_value == "__PASS_THROUGH__":
                            trans_vals.append([ts, ov])
                        elif default_value == "__WARN_PASS_THROUGH__" or default_value == "__WARN_WITH_PREFIX__":
                            if default_value == "__WARN_WITH_PREFIX__":
                                ov = '__UNASSIGNED__' + ov
                            uip_obj.prepare_warnings[
                                in_flds[0] + '~' + out_fld].add(ov)
                            trans_vals.append([ts, ov])
                        else:
                            trans_vals.append([ts, default_value])
                    except:
                        traceback.print_exc()
                        raise Exception(
                            'Unable to run the regular expression match for ignores')
            if trans_vals:
                uip_obj.featMap[out_fld] = trans_vals
        if config.get('__COMPRESS__', True):
            return [out_fld]


class CombineFields2(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(CombineFields2, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{"name": self.out_fld, "format": "N/A", "dep_flds": {"self": self.in_flds}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        combine_fn = eval(self.config['combine_fn'])
        default_vals = self.config['default_values']
        first_ts_from_fld = self.config.get('first_ts_from_fld', None)
        first_value_on_or_afters = self.config.get('first_value_on_or_afters',
                                                   [True] * len(self.in_flds))
        NA_before_created_dates = self.config.get('NA_before_created_dates',
                                                  [True] * len(self.in_flds))
        blanc_enforcing_flds = self.config.get('blanc_enforcing_flds', [])
        ignore_ts_from_flds = self.config.get('ignore_ts_from_flds', [])
        lambda_failure_val = self.config.get('lambda_failure_val', None)
        ignore_none_outputs = self.config.get('ignore_none_outputs', False)
        all_tss = set()

        for feat in self.in_flds:
            feat_hist = uip_obj.featMap.get(feat, None)
            if not feat_hist:
                if feat in blanc_enforcing_flds:
                    return
            else:
                if feat not in ignore_ts_from_flds:
                    all_tss |= set([ts for ts, val in feat_hist])
        all_tss = sorted(all_tss)
        out_hist = []
        for ts in all_tss:
            in_vals = [uip_obj.getAsOfDateF(x, ts, first_value_on_or_afters[i], NA_before_created_dates[i])
                       for i, x in enumerate(self.in_flds)]
            in_vals = [x if (x != 'N/A' and x is not None) else default_vals[i]
                       for i, x in enumerate(in_vals)]
            try:
                out_val = combine_fn(*in_vals)
            except:
                out_val = lambda_failure_val
            if ignore_none_outputs and out_val is None:
                continue
            out_hist.append([ts, out_val])
        if first_ts_from_fld:
            feat_hist = uip_obj.featMap.get(first_ts_from_fld, None)
            if feat_hist:
                fts = feat_hist[0][0]  # getting the frist timestamp of field provided
                out_hist = [[ts, val] for ts, val in out_hist if ts >= fts]
        if out_hist:
            uip_obj.featMap[self.out_fld] = out_hist
            if self.config.get('__COMPRESS__', True):
                return [self.out_fld]


class CalculateAdjustedStages(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(CalculateAdjustedStages, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            adjusted_stage_fld = self.out_fld
            adjusted_orig_stage_fld = self.config['adjusted_orig_stage_field']
            adjusted_win_date_fld = self.config['adjusted_win_date_field']
            adjusted_lose_date_fld = self.config['adjusted_lose_date_field']
            adjusted_created_date_fld = self.config['adjusted_created_date_field']
            adjusted_w2l_date_field = self.config.get('adjusted_win_to_lose_date', 'win_to_lose_date_adj')
            adjusted_l2w_date_field = self.config.get('adjusted_lose_to_win_date', 'lose_to_win_date_adj')
            adjusted_w2a_date_field = self.config.get('adjusted_win_to_active_date', 'win_to_active_date_adj')
            adjusted_l2a_date_field = self.config.get('adjusted_lose_to_active_date', 'lose_to_active_date_adj')
            adjusted_close_date_field = self.config.get('adjusted_close_date', 'CloseDate_adj')
            terminal_fate_field = self.config.get('terminal_fate_field', 'terminal_fate')
            out_fields = [{"name": adjusted_stage_fld, "format": "string", "dep_flds": None},
                          {"name": adjusted_orig_stage_fld, "format": "string", "dep_flds": None},
                          {"name": adjusted_win_date_fld, "format": "xl_date", "dep_flds": None},
                          {"name": adjusted_lose_date_fld, "format": "xl_date", "dep_flds": None},
                          {"name": adjusted_created_date_fld, "format": "xl_date", "dep_flds": None},
                          {"name": adjusted_w2l_date_field, "format": "xl_date", "dep_flds": None},
                          {"name": adjusted_l2w_date_field, "format": "xl_date", "dep_flds": None},
                          {"name": adjusted_w2a_date_field, "format": "xl_date", "dep_flds": None},
                          {"name": adjusted_l2a_date_field, "format": "xl_date", "dep_flds": None},
                          {"name": adjusted_close_date_field, "format": "xl_date", "dep_flds": None},
                          {"name": terminal_fate_field, "format": "char", "dep_flds": None}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 2:
            raise Exception('calc_adjusted_stages only supports two fields at a time, namely stage and stageTrans')
        if self.config.get('win_date_basis', None) not in ['close_date', 'stage']:
            raise ValueError("ERROR: win_date_basis must be one of 'stage' or 'close_date' (it was '%s')" %
                             (self.config.get('win_date_basis', None)))

        orig_stage_fld = self.in_flds[0]
        stage_fld = self.in_flds[1]
        stage_hist = uip_obj.featMap.get(stage_fld, None)
        orig_stage_hist = uip_obj.featMap.get(orig_stage_fld, None)
        if not stage_hist or not orig_stage_hist:
            return
        if not len(stage_hist) == len(orig_stage_hist):
            print("ERROR: length of %s and %s must be equal for calc_adjusted_stages" % (stage_fld, orig_stage_fld))
            return
        uip_obj.featMap[orig_stage_fld + '__back_up__'] = orig_stage_hist
        uip_obj.featMap[stage_fld + '__back_up__'] = stage_hist
        orig_stage_fld = orig_stage_fld + '__back_up__'
        stage_fld = stage_fld + '__back_up__'
        adjusted_stage_fld = self.out_fld
        adjusted_orig_stage_fld = self.config['adjusted_orig_stage_field']
        adjusted_win_date_fld = self.config['adjusted_win_date_field']
        adjusted_lose_date_fld = self.config['adjusted_lose_date_field']
        adjusted_created_date_fld = self.config['adjusted_created_date_field']
        adjusted_w2l_date_field = self.config.get('adjusted_win_to_lose_date', 'win_to_lose_date_adj')
        adjusted_l2w_date_field = self.config.get('adjusted_lose_to_win_date', 'lose_to_win_date_adj')
        adjusted_w2a_date_field = self.config.get('adjusted_win_to_active_date', 'win_to_active_date_adj')
        adjusted_l2a_date_field = self.config.get('adjusted_lose_to_active_date', 'lose_to_active_date_adj')
        adjusted_close_date_field = self.config.get('adjusted_close_date', 'CloseDate_adj')
        #default adjustment is 8 hours, which works well for california timezone
        close_date_tz_adjustment_1 = self.config.get('close_date_tz_adjustment_1', 0.333333333333333333)
        close_date_tz_adjustment_2 = self.config.get('close_date_tz_adjustment_2', 0.666666)
        terminal_fate_field = self.config.get('terminal_fate_field', 'terminal_fate')
        remove_non_final_wins = self.config.get('remove_non_final_wins', False)
        stage_data = uip_obj.featMap[orig_stage_fld]
        trans_stage_data = uip_obj.featMap[stage_fld]
        win_date = None
        uip_obj.featMap[adjusted_orig_stage_fld] = list(stage_data)
        uip_obj.featMap[adjusted_stage_fld] = list(trans_stage_data)
        if trans_stage_data is None:
            raise ValueError('stage has not been translated for id=' + uip_obj.ID)
        win_stages = self.config['win_stages']
        close_date_fld = self.config['close_date_field_name']
        last_stage_is_not_win = uip_obj.featMap[stage_fld][-1][-1] not in win_stages
        last_stage_timestamp = uip_obj.featMap[stage_fld][-1][0]
        if self.config.get('win_date_basis', None) == 'close_date':
            if len(uip_obj.featMap[stage_fld]) != len(uip_obj.featMap[orig_stage_fld]):
                logger.error('the length of ' + stage_fld + ' and ' + orig_stage_fld +
                             ' are not the same. Should not use zip for id =' + uip_obj.ID)
            for tup1, tup2 in zip(uip_obj.featMap[stage_fld], uip_obj.featMap[orig_stage_fld]):
                date, stage_trans_val = tup1
                stage_val = tup2[1]
                if stage_trans_val in win_stages:
                    # it is a win so get the latest close date'
                    closedDate = uip_obj.getLatest(close_date_fld)
                    if closedDate == 'N/A':
                        logger.warning('win_date_basis is close_date but there is no close date for id ' + uip_obj.ID)
                        break
                    try:
                        closedDateF = float(closedDate)
                    except:
                        closedDateF = date_utils.datestr2xldate(closedDate)
                    closedDateF = int(closedDateF) + close_date_tz_adjustment_1
                    # if the actual transition time is within the same close date, take that as the win time
                    if closedDateF < date < closedDateF + close_date_tz_adjustment_2:
                        win_date = date
                    else:
                        win_date = closedDateF
                    if last_stage_is_not_win and win_date > last_stage_timestamp:
                        # in this case the win_date will be ignored
                        win_date = None
                        uip_obj.add_feature_value(date, adjusted_orig_stage_fld, stage_val, create_list_if_exists=False)
                        uip_obj.add_feature_value(date, adjusted_stage_fld, stage_trans_val,
                                                  create_list_if_exists=False)
                    else:
                        uip_obj.add_feature_value(
                            win_date, adjusted_orig_stage_fld, stage_val, create_list_if_exists=False)
                        uip_obj.add_feature_value(
                            win_date, adjusted_stage_fld, stage_trans_val, create_list_if_exists=False)
                        break
            # Ali thinks this can be done better without using getAsOf
            datelist = [x[0] for x in uip_obj.featMap[adjusted_stage_fld]]
            for date in datelist:
                if date < win_date and uip_obj.getAsOfDateF(stage_fld, date) in win_stages:
                    uip_obj.remove_feature_value(date, adjusted_stage_fld)
                    uip_obj.remove_feature_value(date, adjusted_orig_stage_fld)
            # if last stage is win, but there is non-win stages between win date and end, remove them
            if uip_obj.getLatest(stage_fld) in win_stages:
                for date in datelist:
                    if date > win_date and uip_obj.getAsOfDateF(stage_fld, date) not in win_stages:
                        uip_obj.remove_feature_value(date, adjusted_stage_fld)
                        uip_obj.remove_feature_value(date, adjusted_orig_stage_fld)
            if remove_non_final_wins:
                # if last stage is not win, but there is win stages before, remove them
                if uip_obj.getLatest(stage_fld) not in win_stages:
                    for date in datelist:
                        if uip_obj.getAsOfDateF(stage_fld, date) in win_stages:
                            uip_obj.remove_feature_value(date, adjusted_stage_fld)
                            uip_obj.remove_feature_value(date, adjusted_orig_stage_fld)
        # if somehow close date was not available to determine win date, fall back here
        if (self.config.get('win_date_basis', None) == 'stage') or (win_date is None):
            if stage_fld in uip_obj.featMap:
                # set win_date to be the first time stage goes to win and never comes out of win afterwards
                for pair in uip_obj.featMap[stage_fld][::-1]:
                    if pair[1] in win_stages:
                        win_date = pair[0]
                    else:
                        break

        if win_date:
            uip_obj.featMap[adjusted_win_date_fld] = [[win_date, win_date]]

        #lose_date treatment
        lose_stages = self.config['lose_stages']
        lose_date = None
        lose_date_basis = self.config.get('lose_date_basis', 'stage')
        remove_non_final_losses = self.config.get('remove_non_final_losses', False)
        last_stage_is_not_lose = uip_obj.featMap[stage_fld][-1][-1] not in lose_stages

        if lose_date_basis == 'close_date':
            if len(uip_obj.featMap[stage_fld]) != len(uip_obj.featMap[orig_stage_fld]):
                logger.error('the length of ' + stage_fld + ' and ' + orig_stage_fld +
                             ' are not the same. Should not use zip for id =' + uip_obj.ID)
            for tup1, tup2 in zip(uip_obj.featMap[stage_fld], uip_obj.featMap[orig_stage_fld]):
                date, stage_trans_val = tup1
                stage_val = tup2[1]
                if stage_trans_val in lose_stages:
                    # it is a lose so get the latest close date'
                    closedDate = uip_obj.getLatest(close_date_fld)
                    if closedDate == 'N/A':
                        logger.warning('lose_date_basis is close_date but there is no close date for id ' + uip_obj.ID)
                        break
                    try:
                        closedDateF = float(closedDate)
                    except:
                        closedDateF = date_utils.datestr2xldate(closedDate)
                    closedDateF = int(closedDateF) + close_date_tz_adjustment_1
                    # if the actual transition time is within the same close date, take that as the lose time
                    if closedDateF < date < closedDateF + close_date_tz_adjustment_2:
                        lose_date = date
                    else:
                        lose_date = closedDateF
                    if last_stage_is_not_lose and lose_date > last_stage_timestamp:
                        # in this case the lose_date will be ignored
                        lose_date = None
                        uip_obj.add_feature_value(date, adjusted_orig_stage_fld, stage_val, create_list_if_exists=False)
                        uip_obj.add_feature_value(date, adjusted_stage_fld, stage_trans_val,
                                                  create_list_if_exists=False)
                    else:
                        uip_obj.add_feature_value(
                            lose_date, adjusted_orig_stage_fld, stage_val, create_list_if_exists=False)
                        uip_obj.add_feature_value(
                            lose_date, adjusted_stage_fld, stage_trans_val, create_list_if_exists=False)
                        break
            # Ali thinks this can be done better without using getAsOf
            datelist = [x[0] for x in uip_obj.featMap[adjusted_stage_fld]]
            for date in datelist:
                if date < lose_date and uip_obj.getAsOfDateF(stage_fld, date) in lose_stages:
                    uip_obj.remove_feature_value(date, adjusted_stage_fld)
                    uip_obj.remove_feature_value(date, adjusted_orig_stage_fld)
            # if last stage is lose, but there is non-lose stages between lose date and end, remove them
            if uip_obj.getLatest(stage_fld) in lose_stages:
                for date in datelist:
                    if date > lose_date and uip_obj.getAsOfDateF(stage_fld, date) not in lose_stages:
                        uip_obj.remove_feature_value(date, adjusted_stage_fld)
                        uip_obj.remove_feature_value(date, adjusted_orig_stage_fld)
            if remove_non_final_losses:
                # if last stage is not losses, but there is lose stages before, remove them
                if uip_obj.getLatest(stage_fld) not in lose_stages:
                    for date in datelist:
                        if uip_obj.getAsOfDateF(stage_fld, date) in lose_stages:
                            uip_obj.remove_feature_value(date, adjusted_stage_fld)
                            uip_obj.remove_feature_value(date, adjusted_orig_stage_fld)
        # if somehow close date was not available to determine lose date, fall back here
        if (lose_date_basis == 'stage') or (lose_date is None):
            if stage_fld in uip_obj.featMap:
                # set lose_date to be the first time stage goes to lose and never comes out of win afterwards
                for pair in uip_obj.featMap[stage_fld][::-1]:
                    if pair[1] in lose_stages:
                        lose_date = pair[0]
                    else:
                        break

        if lose_date:
            uip_obj.featMap[adjusted_lose_date_fld] = [[lose_date, lose_date]]

        #end lose_date  treatment

        # check if it goes from win to lose:
        win_to_lose_date = None
        for i, pair in enumerate(uip_obj.featMap[adjusted_stage_fld][:-1]):
            if pair[1] in win_stages and uip_obj.featMap[adjusted_stage_fld][i + 1][1] in lose_stages:
                win_to_lose_date = uip_obj.featMap[adjusted_stage_fld][i + 1][0]
                break
        if win_to_lose_date:
            uip_obj.featMap[adjusted_w2l_date_field] = [[win_to_lose_date, win_to_lose_date]]
        # check if it goes from lose_to_win:
        lose_to_win_date = None
        for i, pair in enumerate(uip_obj.featMap[adjusted_stage_fld][:-1]):
            if pair[1] in lose_stages and uip_obj.featMap[adjusted_stage_fld][i + 1][1] in win_stages:
                lose_to_win_date = uip_obj.featMap[adjusted_stage_fld][i + 1][0]
                break
        if lose_to_win_date:
            uip_obj.featMap[adjusted_l2w_date_field] = [[lose_to_win_date, lose_to_win_date]]
        win_to_active_date = None
        for i, pair in enumerate(uip_obj.featMap[adjusted_stage_fld][:-1]):
            if pair[1] in win_stages and (uip_obj.featMap[adjusted_stage_fld][i + 1][1] not in lose_stages) and \
                    (uip_obj.featMap[adjusted_stage_fld][i + 1][1] not in win_stages):
                win_to_active_date = uip_obj.featMap[adjusted_stage_fld][i + 1][0]
                break
        if win_to_active_date:
            uip_obj.featMap[adjusted_w2a_date_field] = [[win_to_active_date, win_to_active_date]]
        lose_to_active_date = None
        for i, pair in enumerate(uip_obj.featMap[adjusted_stage_fld][:-1]):
            if pair[1] in lose_stages and (uip_obj.featMap[adjusted_stage_fld][i + 1][1] not in lose_stages) and \
                    (uip_obj.featMap[adjusted_stage_fld][i + 1][1] not in win_stages):
                lose_to_active_date = uip_obj.featMap[adjusted_stage_fld][i + 1][0]
                break
        if lose_to_active_date:
            uip_obj.featMap[adjusted_l2a_date_field] = [[lose_to_active_date, lose_to_active_date]]

        # finally do the terminal fate
        # notice you need to check teminal_fate to make sure the opportunity win is final.
        # terminal_fat will not be populated unless the latest stage is win or loss.
        # and it will have eventual win_date or loss date as timestamp with W or L as values.
        if adjusted_stage_fld in uip_obj.featMap:
            adj_stages = uip_obj.featMap[adjusted_stage_fld]
            if adj_stages[-1][1] in win_stages:
                if win_date:
                    uip_obj.featMap[terminal_fate_field] = [[win_date, 'W']]
            if adj_stages[-1][1] in lose_stages:
                if lose_date:
                    uip_obj.featMap[terminal_fate_field] = [[lose_date, 'L']]
        if close_date_fld in uip_obj.featMap:
            close_date_adj = []
            if win_date:
                for pair in uip_obj.featMap[close_date_fld]:
                    if pair[0] < win_date:
                        close_date_adj.append(pair)
                close_date_adj.append([win_date, win_date])
            elif lose_date and lose_date_basis == 'close_date':
                for pair in uip_obj.featMap[close_date_fld]:
                    if pair[0] < lose_date:
                        close_date_adj.append(pair)
                close_date_adj.append([lose_date, lose_date])
            else:
                close_date_adj = list(uip_obj.featMap[close_date_fld])
            if close_date_adj:
                uip_obj.featMap[adjusted_close_date_field] = close_date_adj

        # finally adjust created date so that the record is not dropped in case the win date was back-dated
        new_created_date = min(uip_obj.featMap[adjusted_stage_fld][0][0], uip_obj.featMap[stage_fld][0][0])
        uip_obj.featMap[adjusted_created_date_fld] = [[new_created_date, new_created_date]]
        # remove the backups
        uip_obj.featMap.pop(orig_stage_fld)
        uip_obj.featMap.pop(stage_fld)
        if self.config.get('__COMPRESS__', True):
            return [
                adjusted_stage_fld,
                adjusted_orig_stage_fld,
                adjusted_win_date_fld,
                adjusted_lose_date_fld,
                adjusted_created_date_fld,
                adjusted_w2l_date_field,
                adjusted_l2w_date_field,
                adjusted_w2a_date_field,
                adjusted_l2a_date_field,
                adjusted_close_date_field,
                terminal_fate_field,
            ]


class BackdateAfterClose(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(BackdateAfterClose, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            backdate_config = self.config.get('backdate_config', {})
            for in_fld, fld_cfg in backdate_config.items():
                out_fld = fld_cfg.get('output_fld_name', in_fld)
                out_fields.append({'name': out_fld, 'format': 'N/A', 'dep_flds': {"self": [in_fld]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        '''
        backdate_after_close creates fields that are concatenations of the timeseries of
        the as_of value of a field until the deal is closed and then takes the latest value of the
        field after close dates. This mapping function is useful for allowing backdating for
        accounting purposes without allowing cheating during forecasting.
        [Note that this will create discontinuities when a deal is won in the case of backdating.]

        sample config
        'backdate_after_win(field_to_backdate1,field_to_backdate2,field_to_backdate3):dummy':
            {
            'terminal_fate_field': 'terminal_fate',
            'backdate_config': { 'field_to_freeze1': {'use_fv': False
                                         'output_fld_name': 'field1_output_name'},
                                    'field_to_freeze2':{'use_fv': True
                                         'output_fld_name': 'field2_output_name'}
                            }
            }
        '''
        terminal_fate_fld = self.config.get('terminal_fate_field', 'terminal_fate')

        backdate_config = self.config.get('backdate_config', {})
        # Calculate close date:
        close_date = None
        if terminal_fate_fld in uip_obj.featMap:
            close_date = uip_obj.featMap[terminal_fate_fld][-1][0]
        out_flds = set()
        for in_fld, fld_cfg in backdate_config.items():
            if in_fld not in uip_obj.featMap:
                continue
            # This is the meat of the treatment. If there's a close date, we take all values that existed
            # before the close date. Then we compute the latest value and, if needed, add a final entry
            # in the time series corresponding to the latest value at the win date.
            backdate_ts = [[ts, val] for ts, val in uip_obj.featMap[in_fld]
                           if ts < close_date] if close_date else uip_obj.featMap[in_fld]
            latest_val = uip_obj.featMap[in_fld][-1][1]
            # Note: backdate_ts might be empty at this point if there was a close_date so we need to check.
            if (not backdate_ts) or (latest_val != backdate_ts[-1][1]):
                backdate_ts += [[close_date, latest_val]]
            out_fld = fld_cfg.get('output_fld_name', in_fld)
            out_flds.add(out_fld)
            uip_obj.featMap[out_fld] = backdate_ts

        if self.config.get('__COMPRESS__', True):
            return out_flds


class FreezeFields(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(FreezeFields, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            freeze_config = self.config.get('freeze_config', {})
            for in_fld, fld_cfg in freeze_config.items():
                out_fld = fld_cfg.get('output_fld_name', in_fld)
                out_fields.append({'name': out_fld, 'format': 'N/A', 'dep_flds': {"self": [in_fld]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        terminal_fate_fld = self.config.get('terminal_fate_field', 'terminal_fate')
        freeze_config = self.config.get('freeze_config', {})
        freeze_date = None
        if terminal_fate_fld in uip_obj.featMap:
            freeze_date = uip_obj.featMap[terminal_fate_fld][-1][0]
        out_flds = set()
        for in_fld, fld_cfg in freeze_config.items():
            if in_fld not in uip_obj.featMap:
                continue
            frozen_ts = [(ts, val) for (ts, val) in uip_obj.featMap[in_fld]
                         if ts <= freeze_date] if freeze_date else uip_obj.featMap[in_fld]
            out_fld = fld_cfg.get('output_fld_name', in_fld)
            out_flds.add(out_fld)
            if frozen_ts == [] and freeze_config.get(in_fld, {}).get('use_fv', True):
                uip_obj.featMap[out_fld] = uip_obj.featMap[in_fld][:1]
            elif frozen_ts == []:
                del uip_obj.featMap[out_fld]
            else:
                uip_obj.featMap[out_fld] = frozen_ts
        if self.config.get('__COMPRESS__', False):
            return out_flds


class AdornWithHierarchy(ManyToOneDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(AdornWithHierarchy, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        return None

    def process(self, uip_obj):
        if len(self.in_flds) > 1:
            raise Exception('adorn_with_hierarchy supports only one input field')
        input_fld = self.in_flds[0]
        if input_fld not in uip_obj.featMap:
            return
        ds_name = self.config['ds_name']
        lookup_fld = self.config['lookup_fld']
        display_fld = self.config['display_fld']
        parent_lookup_fld = self.config['parent_lookup_fld']
        parent_display_fld = self.config['parent_display_fld']
        other_hier_ds_flds = self.config.get('other_hier_ds_flds', [])
        drop_completely_na = self.config.get('drop_completely_na', True)
        look_fwd_if_na = self.config.get('look_fwd_if_na', False)
        na_b4_created = self.config.get('na_b4_created', True)
        action_if_does_not_honor_ceo = self.config.get('action_if_does_not_honor_ceo', '')
        ceo_lookup_id = self.config.get('ceo_lookup_id', '')
        ceo_display_name = self.config.get('ceo_display_name', '')
        if action_if_does_not_honor_ceo and not ceo_lookup_id:
            raise Exception('adorn_with_hierarchy missing ceo_lookup_id but action_if_does_not_honor_ceo is turned on')
        fill_down_scheme = self.config.get('fill_down_scheme', 'self.' + display_fld)
        depth = self.config['depth']
        warn_if_missing = self.config.get('warn_if_missing', True)
        prepare_ds = self.config.get('prepare_ds', True)
        cache_name = self.config.get('cache_name', self.out_fld)
        cache_historic_keys = self.config.get('cache_historic_keys', True)
        cache_force_refresh = self.config.get('cache_force_refresh', False)

        data_handler = get_data_handler(
            dh_type=self.config.get("dh_type", "M"),
            cache_name=cache_name,
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            other_flds=[parent_lookup_fld, display_fld, parent_display_fld] + other_hier_ds_flds,
            force_refresh=cache_force_refresh,
            prepare_ds=prepare_ds,
            cache_historic_keys=cache_historic_keys,
        )

        change_times = self.get_hier_change_times(
            base_lookup_hist=uip_obj.featMap[input_fld],
            parent_lookup_fld=parent_lookup_fld,
            data_handler=data_handler,
            warn_if_missing=warn_if_missing,
        )

        for ts in change_times:
            base_lookup_val = uip_obj.getAsOfDateF(input_fld, ts, look_fwd_if_na)
            if isinstance(base_lookup_val, str):
                hierarchy = self.get_hierarchy_as_of(
                    as_of=ts,
                    base_lookup_val=base_lookup_val,
                    parent_lookup_fld=parent_lookup_fld,
                    display_fld=display_fld,
                    parent_display_fld=parent_display_fld,
                    drop_completely_na=drop_completely_na,
                    look_fwd_if_na=look_fwd_if_na,
                    na_b4_created=na_b4_created,
                    action_if_does_not_honor_ceo=action_if_does_not_honor_ceo,
                    ceo_lookup_id=ceo_lookup_id,
                    ceo_display_name=ceo_display_name,
                    fill_down_scheme=fill_down_scheme,
                    depth=depth,
                    data_handler=data_handler,
                )
                for i, parent in enumerate(hierarchy):
                    display_val = parent[1]
                    feat_name = "%s_%s" % (self.out_fld, i)
                    if feat_name not in uip_obj.featMap:
                        uip_obj.featMap[feat_name] = []
                    uip_obj.featMap[feat_name].append([ts, display_val])
            elif isinstance(base_lookup_val, dict):
                # below could be dangerous if we have hierarchy for one but not the other user
                # should probably find a better way of doing this...
                for bluv in base_lookup_val:
                    hierarchy = self.get_hierarchy_as_of(
                        as_of=ts,
                        base_lookup_val=bluv,
                        parent_lookup_fld=parent_lookup_fld,
                        display_fld=display_fld,
                        parent_display_fld=parent_display_fld,
                        drop_completely_na=drop_completely_na,
                        look_fwd_if_na=look_fwd_if_na,
                        na_b4_created=na_b4_created,
                        action_if_does_not_honor_ceo=action_if_does_not_honor_ceo,
                        ceo_lookup_id=ceo_lookup_id,
                        ceo_display_name=ceo_display_name,
                        fill_down_scheme=fill_down_scheme,
                        depth=depth,
                        data_handler=data_handler,
                    )
                    for i, parent in enumerate(hierarchy):
                        display_val = parent[1]
                        feat_name = "%s_%s" % (self.out_fld, i)
                        if feat_name not in uip_obj.featMap:
                            uip_obj.featMap[feat_name] = [[ts, {}]]
                        if uip_obj.featMap[feat_name][-1][0] != ts:
                            uip_obj.featMap[feat_name].append([ts, {}])
                        uip_obj.featMap[feat_name][-1][1][bluv] = display_val
        if self.config.get('__COMPRESS__', True):
            return ["%s_%s" % (self.out_fld, i) for i in range(depth)]

    def get_hier_change_times(
            self,
            base_lookup_hist,
            parent_lookup_fld,
            data_handler,
            warn_if_missing,
    ):
        '''
        provide a list of timestamps which denote the times when a hierarchy change takes place
        '''
        from queue import Queue
        change_times = set()
        review_queue = Queue()
        n_points = len(base_lookup_hist)
        # first add  review point for the history of the lookup values
        for i, x in enumerate(base_lookup_hist):
            change_times.add(x[0])
            if isinstance(x[1], str):
                review_queue.put((
                    x[1],
                    x[0],
                    MAX_CREATED_DATE if i == n_points - 1 else base_lookup_hist[i + 1][0]
                ))
            elif isinstance(x[1], dict):
                for lookup_val in x[1]:
                    review_queue.put((
                        lookup_val,
                        x[0],
                        MAX_CREATED_DATE if i == n_points - 1 else base_lookup_hist[i + 1][0]
                    ))
        # not repeat same thing for the parents of each lookup value in the given range
        while not review_queue.empty():
            base_lookup_val, begin_ts, end_ts = review_queue.get()
            if base_lookup_val not in data_handler:
                if warn_if_missing:
                    logger.warning('WARNING: %s does not exist in data_handler... will ignore it', base_lookup_val)
                continue
            rec = data_handler[base_lookup_val][0]
            if parent_lookup_fld not in rec.featMap:
                continue
            parent_lookup_hist = rec.featMap[parent_lookup_fld]
            last_idx = len(parent_lookup_hist) - 1
            for i, ts_val in enumerate(parent_lookup_hist):
                ts, parent_val = ts_val
                if begin_ts <= ts <= end_ts and ts not in change_times:
                    change_times.add(ts)
                    review_queue.put((
                        parent_val,
                        max(begin_ts, parent_lookup_hist[i - 1][0] if i > 0 else begin_ts),
                        min(end_ts, parent_lookup_hist[i + 1][0] if i < last_idx else end_ts),
                    ))
        return sorted(change_times)

    def get_hierarchy_as_of(
            self,
            as_of,
            base_lookup_val,
            parent_lookup_fld,
            display_fld,
            parent_display_fld,
            drop_completely_na,
            look_fwd_if_na,
            na_b4_created,
            action_if_does_not_honor_ceo,
            ceo_lookup_id,
            ceo_display_name,
            fill_down_scheme,
            depth,
            data_handler,
    ):
        hierarchy = []
        base_display_val = 'N/A'
        base_rec = None
        base_parent_rec = None
        # add details for this node
        if base_lookup_val in data_handler:
            base_rec = data_handler[base_lookup_val][0]
            base_display_val = base_rec.getAsOfDateF(display_fld, as_of, look_fwd_if_na, na_b4_created)
            hierarchy.append([base_lookup_val, base_display_val])
            parent_lookup_val = base_rec.getAsOfDateF(parent_lookup_fld, as_of, look_fwd_if_na, na_b4_created)
            base_parent_rec = data_handler[parent_lookup_val][0] if parent_lookup_val in data_handler else None
        else:
            hierarchy.append(['N/A', 'N/A'])
        if not base_parent_rec and drop_completely_na:
            return []
        parent_rec = base_parent_rec
        while parent_rec:
            hierarchy.append(
                [parent_lookup_val, parent_rec.getAsOfDateF(parent_display_fld, as_of, look_fwd_if_na, na_b4_created)])
            parent_lookup_val = parent_rec.getAsOfDateF(parent_lookup_fld, as_of, look_fwd_if_na, na_b4_created)
            parent_rec = data_handler[parent_lookup_val][0] if parent_lookup_val in data_handler else None
        hierarchy = hierarchy[::-1]  # will have at least one entry for the base_lookup
        if action_if_does_not_honor_ceo:
            if ceo_lookup_id and (hierarchy[0][0] != ceo_lookup_id):
                if action_if_does_not_honor_ceo == 'drop':
                    return []
                elif action_if_does_not_honor_ceo == 'force':
                    try:
                        ceo_idx = [x[0] for x in hierarchy].index(ceo_lookup_id)
                    except:
                        ceo_idx = -1
                    if ceo_idx == -1:  # add the CEO to the top
                        hierarchy = [(ceo_lookup_id, ceo_display_name)] + hierarchy
                    elif ceo_idx > 0:  # remove up to CEO
                        hierarchy = hierarchy[ceo_idx:]
                elif action_if_does_not_honor_ceo == 'force_na':
                    hierarchy = [['N/A', 'N/A'] for _ in range(depth)]
                elif action_if_does_not_honor_ceo == 'force_na_except_base':
                    hierarchy = [['N/A', 'N/A'] for _ in range(depth - 1)] + [[base_lookup_val, base_display_val]]
        hierarchy = hierarchy[:depth]
        if len(hierarchy) < depth:
            fill_val = base_display_val
            if fill_down_scheme.startswith('self.'):
                fld_name = fill_down_scheme[5:]
                if fld_name == display_fld:
                    fill_val = base_display_val
                elif base_rec:
                    fill_val = base_rec.getAsOfDateF(fld_name, as_of, look_fwd_if_na, na_b4_created)
            elif fill_down_scheme.startswith('parent.'):
                if base_parent_rec:
                    fld_name = fill_down_scheme[7:]
                    fill_val = base_parent_rec.getAsOfDateF(fld_name, as_of, look_fwd_if_na, na_b4_created)
            hierarchy = hierarchy[:-1] + [[base_lookup_val, fill_val]
                                          for _ in range(depth - len(hierarchy))] + hierarchy[-1:]
        return hierarchy


class AdornWithProductAmounts(OneToManyDBMap):
    """ This map is the base case for constructing totals of amounts from opportunity line items
    grouped by product family. Especially useful for customers who want pillars without
    splits.

    NB: This map creates a time series of aggregate totals of a given field on some matching records,
    with a groupby by a specified field. This can be used for multiple things but almost always will be
    used for something like computing totals of line item amounts by product family.

    Mandatory Configuration

    * pllr_fld: Name of the field to do groupby on.

    Optional Configuration

    * opli_ds: [Default: OpportunityLineItem] Name of dataset product amounts should be joined from.
    * lookup_fld: [Default: OpportunityId] The field on the opli_ds records that tell you the ID of which
        source dataset records to use.
    * bad_pllrs: [Default: []] List of values in pillar field that cause a line item to get ignored. Uses latest.

    * splt_amt_fld: [Default: 'TotalPrice'] Field on opportunity line item (or other) object that needs to be
        aggregated.
    * grpby_amt_fld: [Default: Amount] Name of amount field prefix to use in groupby field outputs.
    * tot_amt_fld: [Default: TotalAmountFromSplits] Total Amount from splits amount
        field to use in groupby outputs.
    * dummy_val: [Default: Forecast] This map only does a one dimensional groupby, but the UI requires a 2
        dimensional groupby for pillars. The first value in the groupby will always be the static value used here.

    * prune_zeros: [Default: False] Remove line items from maps if their values are zero.
    * log_warnings: [Default: False] Turn on to log warnings for debug purposes. Typically we will only log
        errors so we do not spam logs.
    * cache_name: [Default: name of output field] Name of cache to create when storing line items. Use this
        for optimization if multiple maps need to load the same dataset into memory.

    Sample Configuration

    adorn_with_product_amts(dummy):TotalAmountFromSplits: {
        prune_zeros: True,
        pillar_field: "Division__c",
        grpby_amt_fld: "AttainmentValue",
        opli_ds: "OpLiItem",
        pillars_to_exclude: [],
        splt_amt_fld: "Attainment_Value__c"
    },

    """

    def get_map_details(self, output_fields_only=False):
        return None

    def __init__(self, feature, config=None, stage_ds=None):
        # OpportunityID
        # Will need support for more than just this one.
        self.ref_key = config.get('lookup_fld', 'OpportunityId')
        super(AdornWithProductAmounts, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):

        log_warnings = self.config.get('log_warnings', False)
        cache_prefix = self.config.get('cache_name', self.out_fld)

        opli_ds_name = self.config.get('ds_name', u'OpportunityLineItem')

        splt_amt_fld = self.config.get("splt_amt_fld", 'TotalPrice')

        try:
            pllr_fld = self.config["pillar_field"]
        except KeyError:
            raise Exception('No pllr_fld configured.')

        bad_pllrs = self.config.get("pillars_to_exclude", [])
        prune_zeros = self.config.get("prune_zeros", False)
        splits_style = self.config.get("splits_style", False)

        grpby_amt_fld = self.config.get("grpby_amt_fld", "Amount")
        tot_amt_fld = self.config.get("tot_amt_fld", "TotalAmountFromSplits")

        # Retrieve Splits for opportunity
        opli_data_handler = get_data_handler(
            dh_type="D",
            cache_name=cache_prefix + opli_ds_name,
            ds_name=opli_ds_name,
            lookup_fld=self.ref_key,  # OpportunityId
            other_flds=([splt_amt_fld, pllr_fld, 'Stage'])  # For Deletion
        )

        line_items = opli_data_handler.get(uip_obj.ID, [])
        line_items = [li for li in line_items
                      if li.getLatest(pllr_fld) not in bad_pllrs]

        DUMMY_FLD = '_dummy_'
        dummy_val = self.config.get('dummy_val', 'Forecast')

        for li in line_items:
            li.featMap[DUMMY_FLD] = [[li.created_date, dummy_val]]

        grpby_flds = [DUMMY_FLD, pllr_fld]

        ea = LineItemEventAggregator(ID=uip_obj.ID,
                                     first_until=uip_obj.created_date,
                                     li_amt_fld=splt_amt_fld,
                                     tot_amt_fld=tot_amt_fld,
                                     grpby_amt_fld=grpby_amt_fld,
                                     grpby_fld=grpby_flds,
                                     prune_zeros=prune_zeros,
                                     log_warnings=log_warnings)

        ea.process_records(line_items)
        ea.process_timeline()

        if not splits_style:
            uip_obj.featMap.update(ea.output_featmap)
            for fld in ea.output_featmap:
                uip_obj.featMap[fld] = [tv for i, tv in enumerate(uip_obj.featMap[fld])
                                        if i == 0 or tv[1] != uip_obj.featMap[fld][i - 1][1]]

        else:
            all_tss = set()
            for feat in ea.output_featmap:
                all_tss |= set([ts for ts, val in ea.output_featmap[feat]])
            all_tss = sorted(all_tss)
            newDict = {}
            total = {ts: 0 for ts in all_tss}
            for product in ea.output_featmap:
                if product == '_'.join((grpby_amt_fld, dummy_val)) or product == tot_amt_fld:
                    continue
                new_product = product[len(grpby_amt_fld) + len(dummy_val) + 2:]
                newDict[new_product] = []
                counter = 0
                for ts in all_tss:
                    if ea.output_featmap[product][0][0] > ts:
                        newDict[new_product].append([ts, 0])
                    else:
                        while counter < len(ea.output_featmap[product]) - 1:
                            if ea.output_featmap[product][counter + 1][0] > ts:
                                break
                            counter += 1
                        newDict[new_product].append([ts, ea.output_featmap[product][counter][1]])
                        total[ts] += ea.output_featmap[product][counter][1]
            outfield = []
            i = 0
            for ts in all_tss:
                val = {}
                for product in newDict:
                    if (not prune_zeros or newDict[product][i][1] != 0) and total[ts] != 0:
                        val[product] = newDict[product][i][1] / total[ts]
                outfield.append([ts, val])
                i += 1
            uip_obj.featMap['_'.join((grpby_amt_fld, dummy_val, 'Splits'))] = outfield
            temp = '_'.join((grpby_amt_fld, dummy_val))
            uip_obj.featMap[temp] = ea.output_featmap[temp] if ea.output_featmap[temp] else [
                [uip_obj.created_date, 0.0]]
            uip_obj.featMap[tot_amt_fld] = ea.output_featmap[tot_amt_fld]

        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class AdornWithSplits(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        self.ref_key = config.get('lookup_fld')
        super(AdornWithSplits, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        return None

    def process(self, uip_obj):
        ds_name = self.config['ds_name']
        log_warnings = self.config.get('log_warnings', True)
        prune_zeros = self.config.get('prune_zeros', False)
        cache_name = self.config.get('cache_name', self.out_fld)
        lookup_fld = self.config['lookup_fld']  # eg. OpportunityID
        splt_defs = self.config['splt_defs']  # eg. OwnerID
        if isinstance(splt_defs, dict):
            splt_defs = [splt_defs]

        splt_type_ds_name = self.config.get('splt_type_ds', None)

        all_splt_flds = set() if not splt_type_ds_name else {'SplitTypeId'}
        all_splt_flds.add('Stage')
        for splt_def in splt_defs:
            all_splt_flds |= {x['in_fld'] for x in splt_def['dep_dds'].values()}
            all_splt_flds |= set(splt_def.get('filters'))
            all_splt_flds |= {splt_def.get('splt_amt_fld'), splt_def.get('splt_pct_fld')}

        # Retrieve Splits for opportunity
        data_handler = get_data_handler(
            dh_type=self.config.get("dh_type", "D"),
            cache_name=cache_name,
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            # TODO: Confirm this works with Nones.
            other_flds=all_splt_flds,
        )

        splt_recs = data_handler.get(uip_obj.ID, [])

        if splt_type_ds_name:
            splt_type_flds = ['IsActive', 'MasterLabel', 'SplitField']
            # Retrieve all split types.
            splt_types_handler = get_data_handler(
                dh_type='M',
                cache_name=cache_name + 'type',
                ds_name=splt_type_ds_name,
                lookup_fld="ID",
                other_flds=splt_type_flds)

            for splt in splt_recs:
                splt_type_id = splt.featMap.get('SplitTypeId', [[splt.created_date, 'N/A']])[-1][1]
                try:
                    splt_type = splt_types_handler.get(splt_type_id)[0]
                    for fld in splt_type_flds:
                        splt.featMap[fld] = [[splt.created_date,
                                              splt_type.featMap.get(fld, [[splt.created_date, 'N/A']])[-1][1]]]
                except:
                    logger.warning('Could not find split type definition with id=%s', splt_type_id)

        for i, splt_def in enumerate(splt_defs):
            splt_ratios_fld = splt_def['splt_ratios_fld']
            splt_amt_fld = splt_def['splt_amt_fld']
            splt_pct_fld = splt_def['splt_pct_fld']
            splt_tot_amt_fld = splt_def.get('splt_tot_amt_fld', None)
            # tot_amt_fallback_fld = splt_def['tot_amt_fallback_fld']
            dep_dds = splt_def['dep_dds']

            for dep_fld in dep_dds:
                uip_obj.featMap[dep_fld] = [[uip_obj.created_date, {}]]

            filters = splt_def.get('filters', {})

            def passes_filter(splt):
                if filters:
                    for filter_fld, filter_vals in filters.items():
                        if filter_fld.startswith('not_in('):
                            val = splt.get(filter_fld[7:-1], [[None, 'N/A']])[-1][1]
                            if val in filter_vals:
                                return False
                        else:
                            val = splt.get(filter_fld, [[None, 'N/A']])[-1][1]
                            if val not in filter_vals:
                                return False
                    return True
                else:  # nofilter
                    return True

            new_splt_recs = [splt for splt in splt_recs if passes_filter(splt.featMap)]

            ea = SplitsEventAggregator(ID=uip_obj.ID,
                                       first_until=uip_obj.created_date,
                                       dep_dds=dep_dds,
                                       splt_amt_fld=splt_amt_fld,
                                       splt_pct_fld=splt_pct_fld,
                                       splt_tot_amt_fld=splt_tot_amt_fld,
                                       splt_ratios_fld=splt_ratios_fld,
                                       log_warnings=log_warnings,
                                       prune_zeros=prune_zeros)

            ea.process_records(new_splt_recs)
            ea.process_timeline()
            uip_obj.featMap.update(ea.output_featmap)

            # TODO: Be less dumb.
            for fld in ea.output_featmap:
                uip_obj.featMap[fld] = [tv for i, tv in enumerate(uip_obj.featMap[fld])
                                        if i == 0 or tv[1] != uip_obj.featMap[fld][i - 1][1]]
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class AdornWithRevenueSchedule(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        self.ref_key = config.get('ext_id_fld')
        super(AdornWithRevenueSchedule, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            splt_amount_fld = self.config.get('splt_amount_fld', None)
            ds_name = self.config['ds_name']
            out_fields = [{'name': self.out_fld, 'format': 'revenue_schedule',
                           'dep_flds': {ds_name: [splt_amount_fld]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        from domainmodel.datameta import Dataset, DatasetClass
        ds_name = self.config['ds_name']
        ext_id_fld = self.config['ext_id_fld']  # usually OpportunityID
        splt_date_fld = self.config['splt_date_fld']
        splt_amount_fld = self.config.get('splt_amount_fld', None)
        splt_deleted_fld = self.config.get('splt_deleted_fld', None)
        splt_created_fld = self.config.get('splt_created_fld', None)
        stage_fld = self.config.get('stage_fld', None)
        splt_probability_fld = self.config.get('probability_fld', None)
        rs_amount_hi = self.config.get('rs_limit_hi', 30000000)
        rs_amount_low = self.config.get('rs_limit_low', -30000000)
        # if you know all the splits should add up to something like 100 then add it here
        # splt_ratio_base = self.config.get('splt_ratio_base', None)
        filters = self.config.get('filters', None)
        ds_inst = Dataset.getByNameAndStage(ds_name)
        ds_class = DatasetClass(ds_inst)
        splt_dtls = ds_class.getAllByFieldValue('values.' + ext_id_fld, uip_obj.ID)
        splts = {}
        for splt_dtl in splt_dtls:
            try:
                splt_dtl = splt_dtl.featMap
                if filters:
                    filter_me = False
                    for filter_fld, filter_vals in filters.items():
                        val = splt_dtl.get(filter_fld, [[None, 'N/A']])[-1][1]
                        if val in filter_vals:
                            filter_me = True
                            break
                    if filter_me:
                        continue
                splt_date = float(splt_dtl.get(splt_date_fld, [[None, 0.0]])[-1][1])  # get date
                splt_date = str(int(math.ceil(splt_date)))
                if splt_date not in splts.keys():
                    splts[splt_date] = []
                amount = float(splt_dtl.get(splt_amount_fld, [[None, 0.0]])[0][1])
                probability = splt_dtl.get(splt_probability_fld, [[None, None]])[0][1]
                if probability is not None:
                    if probability:
                        amount = amount / (float(probability) / 100.0)
                    else:
                        amount = 0.0
                splts[splt_date].append((float(splt_dtl[splt_created_fld][0][1]), amount))
                if splt_dtl.get(stage_fld, [[None, None]])[0][1] == splt_deleted_fld:
                    splts[splt_date].append((float(splt_dtl[stage_fld][0][0]), -amount))
            except:
                raise
        splts_ = {}
        try:
            for splt_date in splts.keys():
                events = sorted(splts[splt_date])
                th = 0.9
                rs = []
                current_val = 0
                current_ts = events[0][0]
                previous_val = 0
                for event in events:
                    event_ts = event[0]
                    event_val = event[1]
                    if event_val > rs_amount_hi or event_val < rs_amount_low:
                        if event_val * previous_val > 0.0:
                            event_val = previous_val
                        else:
                            event_val = -previous_val
                    previous_val = event_val
                    if event_ts - current_ts > th:
                        current_ts = event_ts
                        rs.append([current_ts, current_val])
                    current_val += event_val
                rs.append([current_ts, current_val])
                splts_[splt_date] = rs
        except:
            pass
        if splts_:
            uip_obj.featMap[self.out_fld] = [[uip_obj.created_date, splts_]]
        # if splts:
        #     uip_obj.featMap["temp"] = [[uip_obj.created_date, splts]]
        if self.config.get('__COMPRESS__', True):
            return [
                self.out_fld,
                # "temp"
            ]


class BookingsToRevenueSchedule(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(BookingsToRevenueSchedule, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            amt_flds = self.config["amount_flds"]
            for amt_fld in amt_flds:
                out_fields.append({'name': self.out_fld + '_' + amt_fld, 'format': 'revenue_schedule',
                                   "dep_flds": {"self": [amt_fld]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        """
        Given amount and close date fields, we create a revenue schedule to be used
        in the unified model
        ``amount_fallback``: if None, there will be no fallback and close dates with timestamps
        before first amount timestamp will be ignored. If a float or an int value, then if the
        first amount timestamp is after the first close_date timestamp, then the amount_fallback
        will be injected into the history at the timestamp of the first close_date. If 'use_fv'
        is passed for amount_fallback, then if the first amount timestamp is after the first
        close_date timestamp, then the first amount's timestamp is backdated to the first
        close_date's timestamp
        """
        try:
            log_warnings = self.config.get('log_warnings', False)
            close_date_fld = self.config["close_date_fld"]
            amt_flds = self.config["amount_flds"]
            amt_fallback = self.config.get("amount_fallback", None)

            cd_hist = uip_obj.featMap.get(close_date_fld, [])
            cd_hist = [x for x in cd_hist if x[1] != "N/A"]

            try:
                cd_ts_0 = cd_hist[0][0]
            except:
                if log_warnings:
                    logger.warning("ERROR: bookings_to_revenue_schedule for oppid=%s"
                                   " did not have a close date", uip_obj.ID)
                return
            for amt_fld in amt_flds:
                try:
                    amt_hist = uip_obj.featMap[amt_fld]
                except:
                    if amt_fallback is None or amt_fallback == 'use_fv':
                        return
                    else:
                        amt_hist = [[cd_ts_0, amt_fallback]]
                if amt_fallback is not None:
                    if amt_hist[0][0] > cd_ts_0:
                        if isinstance(amt_fallback, (float, int)):
                            amt_hist = [[cd_ts_0, amt_fallback]] + amt_hist
                        elif amt_fallback == 'use_fv':
                            amt_hist = [[cd_ts_0, amt_hist[0][1]]] + amt_hist[1:]
                        else:
                            raise Exception("ERROR: bookings_to_revenue_schedule amount_fallback" +
                                            " must be numeric or 'use_fv'")

                tss, cds = zip(*cd_hist)
                slices = self.get_slices_(amt_hist, tss, 0.0)
                schedule = {}
                for idx, slc in enumerate(slices[1:]):
                    if slc:
                        cd = str(round(float(cds[idx]), 6) + 0.00001)  # making sure of float approximation in period
                        try:
                            # if schedule already exists and timestmaps overlap,
                            # ensure we take value from new slice
                            cd_sch = schedule[cd]
                            if schedule[cd][-1][0] == slc[0][0]:
                                del cd_sch[-1]
                            # enter the rest of values ensuring no repeats
                            for t, v in slc:
                                if cd_sch[-1][1] != v:
                                    cd_sch.append([t, v])
                        except KeyError:
                            schedule[cd] = slc
                uip_obj.featMap[self.out_fld + '_' + amt_fld] = [[uip_obj.created_date, schedule]]
        except Exception as e:
            logger.error("Could not prepare bookings to revenue: " + str(e))
            raise

    def get_slices_(self, hist, tss, end_val=None):
        '''
        given a feature history ``hist`` and an array of timestamps ``tss``
        output a list of slices for each implied timestamp range
        ``end_val`` can be used to mark each interval at specific interval
        with a known value. If None, no end value will be enforced.
        '''
        if not tss:
            return []
        slices = [[] for _ in range(len(tss) + 1)]
        max_idx = len(hist) - 1
        idx = 0
        ts0 = tss[0]
        # first interval is from negative infinity to first timestamp
        for t, v in hist:
            if t >= ts0:
                break
            slices[0].append([t, v])
            idx += 1
        # add end val for first internval
        if end_val is not None and slices[0] and slices[0][-1][1] != end_val:
            slices[0].append([ts0, end_val])
        # loop through each inner interval (where begin and end timestamps exist)
        for slice_idx, end_ts in enumerate(tss[1:]):
            beg_ts = tss[slice_idx]
            # inner slices start at index 1
            slice_idx += 1
            # if no more history, or the history is after this interval's beginning
            # and we have some history before, we need to take the last value
            if idx > max_idx or (idx and hist[idx][0] > beg_ts):
                slices[slice_idx].append([beg_ts, hist[idx - 1][1]])
            # add rest of history for interval
            for t, v in hist[idx:]:
                if t >= end_ts:
                    break
                slices[slice_idx].append([t, v])
                idx += 1
            # add end_val for inner interval
            if end_val is not None and slices[slice_idx] and slices[slice_idx][-1][1] != end_val:
                slices[slice_idx].append([end_ts, end_val])
        # last interval is from last timestamp to infinity
        # handle the first point as per the inner intervals
        if idx > max_idx or (idx and hist[idx][0] > tss[-1]):
            slices[-1].append([tss[-1], hist[idx - 1][1]])
        slices[-1].extend(hist[idx:])
        return slices


class SplitRatiosFromAmountFields(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(SplitRatiosFromAmountFields, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'N/A', "dep_flds": None}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        '''
        ``amt_fld_map``: A dictionary where keys are amount fields and the values
        are the name to be used for that amount field in the output ratio
        ``use_fv``: whether to look forward for the amount_flds
        '''
        use_fv = self.config.get("use_fv", False)
        amt_fld_maps = self.config["amt_fld_map"]
        amts_by_fld = {}
        tss = set()
        for amt_fld in amt_fld_maps:
            hist = uip_obj.featMap.get(amt_fld, [])
            tss |= set([x[0] for x in hist])
        tss = sorted(tss)
        tots = [0] * len(tss)

        for amt_in_fld, amt_out_fld in amt_fld_maps.items():
            hist = uip_obj.featMap.get(amt_in_fld, [])
            std_hist = [0] * len(tss)
            amts_by_fld[amt_out_fld] = std_hist
            if not hist:
                continue
            hist_idx = 0
            ts, val = hist[0]
            val = try_float(val)
            for std_idx, std_ts in enumerate(tss):
                if ts == std_ts:
                    std_hist[std_idx] = val
                    tots[std_idx] += val
                    hist_idx += 1
                    try:
                        ts, val = hist[hist_idx]
                        val = try_float(val)
                    except:
                        pass
                elif use_fv or ts < std_ts:
                    std_hist[std_idx] = val
                    tots[std_idx] += val

        ratio_hist = [[ts, {}] for ts in tss]
        for amt_fld, amts in amts_by_fld.items():
            for idx, amt in enumerate(amts):
                tot = float(tots[idx])
                ratio_hist[idx][1][amt_fld] = amt / tot if tot else 1. / len(amts_by_fld)
        if ratio_hist:
            uip_obj.featMap[self.out_fld] = ratio_hist
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class AdornWithLineItemTable(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        self.ref_key = config.get('lookup_fld')
        super(AdornWithLineItemTable, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        """
        Construct a table of line items to be used in the UI with a format='table' field.

        Returned value is a time series of values shaped like this:

        [{'Name': 'Foo', 'Qty': 1, 'Discount': 50%},
        {'Name': 'Bar', 'Qty': 3, 'Discount': 4%}]

        NOTE: For now this only constructs a single entry timeseries with the latest values.

        Sample config:

            {'other_fields': {'Quantity': {'label': 'Qty',
                                        'format': 'none'} ,
                           'TotalPrice' : {'label': 'Price',
                                       'format': 'amount'},
                            }
            }

        Basic Logic:

        Look through each opplineitem:
            If it's not deleted, make a dictionary of the latest values.
        """

        record_filter = self.config.get('record_filter', {})
        other_flds = self.config.get('other_fields', {})

        ds_name = self.config['ds_name']
        lookup_fld = self.config['lookup_fld']
        lookup_latest_only = self.config.get('lookup_latest_only', True)
        prepare_ds = self.config.get('prepare_ds', True)
        cache_name = self.config.get('cache_name', self.out_fld)
        cache_force_refresh = self.config.get('cache_force_refresh', False)

        # Get a cache of all Opportunity Line Items
        data_handler = get_data_handler(
            dh_type=self.config.get("dh_type", "M"),
            cache_name=cache_name,
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            other_flds=other_flds.keys() + ['Stage'],
            cache_historic_keys=not lookup_latest_only,
            force_refresh=cache_force_refresh,
            prepare_ds=prepare_ds
        )

        li_list = data_handler.get(uip_obj.ID, [])

        ret_val = []
        # TODO: Sort the list.
        for rec in li_list:
            if 'Stage' in rec.featMap and rec.featMap['Stage'][-1][1] == 'SFDCDELETED':
                continue

            filter_results = rec.passes_filterF(record_filter, uip_obj.created_date)
            if not filter_results[0]:
                logger.warning('Record %s filtered out from map because %s',
                               rec.ID, filter_results)
                continue

            rendered_li = {dtls.get('label', fld): format_val(rec.getLatest(fld), dtls.get('format', 'none'))
                           for fld, dtls in other_flds.items()}
            ret_val.append(rendered_li)

        uip_obj.featMap[self.out_fld] = [[uip_obj.created_date, ret_val]]
        return self.out_fld


class AdornWithLineitemSchedule(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        self.ref_key = config.get('lookup_fld')
        super(AdornWithLineitemSchedule, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'N/A', "dep_flds": None}]

            return out_fields
        else:
            return None

    def process(self, uip_obj):
        """
        Adorn with revenue from line items.

        Sample config:

            ....

        Basic Logic:

        Look through each opplineitem and note the following events:
            When an opplineitem is created,
                create a key equal to its first Start_c (plus some logic), with blank ts
            When an opplineitem is updated,
                AMT: change cashflow for current period
                Start Date : deduct last amount from cashflow on old date
                    and last amount to cashflow on current date
            When an opplineitem is deleted,
                deduct its last amount from the cashflow

        """

        amt_fld = self.config.get('amount_field', 'TotalPrice')
        date_fld = self.config.get('date_field', 'Start__c')
        opp_cd_fld = self.config.get('opp_cd_field', 'CloseDate_adj')

        # Line items matching this filter will use the close date
        # of the opp instead of the date field on the line item.
        use_cd_filter = self.config.get('use_cd_filter', None)

        # This is a list of pairs:
        # [(filter1, mult1), (filter2,mult2)]
        amt_mult_filters = self.config.get('amt_mult_filters', [])
        # use if necessary
        record_filter = self.config.get('record_filter', {})

        other_flds = [amt_fld, date_fld] + self.config.get('other_fields', [])

        cf_offset = self.config.get('cashflow_offset', 0)

        ds_name = self.config['ds_name']
        lookup_fld = self.config['lookup_fld']
        lookup_latest_only = self.config.get('lookup_latest_only', False)
        prepare_ds = self.config.get('prepare_ds', True)
        cache_name = self.config.get('cache_name', self.out_fld)
        cache_force_refresh = self.config.get('cache_force_refresh', False)

        # Get a cache of all Opportunity Line Items
        data_handler = get_data_handler(
            dh_type=self.config.get("dh_type", "M"),
            cache_name=cache_name,
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            other_flds=other_flds,
            cache_historic_keys=not lookup_latest_only,
            force_refresh=cache_force_refresh,
            prepare_ds=prepare_ds
        )

        li_list = data_handler.get(uip_obj.ID, [])
        if li_list:
            # Get created date and create a new revenue schedule
            rs = RevenueSchedule(uip_obj.created_date, cf_offset=cf_offset)
        else:
            return

        for rec in li_list:
            # Get its histories of amount and start date.
            amt_hist = [[ts, float(amt)] for (ts, amt) in rec.featMap.get(amt_fld, [])]

            for (amt_filter, amt_mult) in amt_mult_filters:
                if rec.passes_filterF(amt_filter, uip_obj.created_date)[0]:
                    amt_hist = [[ts, amt_mult * amt] for (ts, amt) in amt_hist]
                    break

            if use_cd_filter and rec.passes_filterF(use_cd_filter, uip_obj.created_date)[0]:
                date_hist = uip_obj.featMap.get(opp_cd_fld, [])
            else:
                date_hist = rec.featMap.get(date_fld, [])

            # If either start field or amount field are missing, skip the LI.
            if not (amt_hist and date_hist):
                continue
            filter_results = rec.passes_filterF(record_filter, uip_obj.created_date)
            if not filter_results[0]:
                logger.warning('Record %s filtered out from map because %s',
                               rec.ID, filter_results)
                continue

            # Initialize bookkeeping vars
            amt_idx, date_idx = 0, 0

            # Do first update
            first_update_date = min(date_hist[date_idx][0], amt_hist[amt_idx][0])
            rs.update_cf_amt(date_hist[date_idx][1],  # cf_date
                             first_update_date,  # update_date
                             amt_hist[amt_idx][1]  # amt_diff
                             )

            # Step through the amount and date histories, one leg at a time.
            while amt_idx < len(amt_hist) - 1 or date_idx < len(date_hist) - 1:
                # Determine which leg to step with next.
                if amt_idx == len(amt_hist) - 1:
                    next_fld = date_fld
                elif date_idx == len(date_hist) - 1:
                    next_fld = amt_fld
                elif amt_hist[amt_idx + 1][0] <= date_hist[date_idx + 1][0]:
                    next_fld = amt_fld
                else:
                    next_fld = date_fld

                # Step with amt leg
                if next_fld == amt_fld:
                    rs.update_cf_amt(date_hist[date_idx][1],  # cf_date
                                     amt_hist[amt_idx + 1][0],  # update_date
                                     amt_hist[amt_idx + 1][1] - amt_hist[amt_idx][1]  # amt_diff
                                     )
                    amt_idx += 1

                # Step with date leg
                elif next_fld == date_fld:
                    rs.update_cf_amt(date_hist[date_idx][1],  # cf_date
                                     date_hist[date_idx + 1][0],  # update_date
                                     - 1 * float(amt_hist[amt_idx][1])  # amt_diff
                                     )

                    rs.update_cf_amt(date_hist[date_idx + 1][1],  # cf_date
                                     date_hist[date_idx + 1][0],  # update_date
                                     amt_hist[amt_idx][1]  # amt_diff
                                     )

                    date_idx += 1

            # If deleted, process deletion. Deletions only happen last.
            delete_date = None
            if 'Stage' in rec.featMap and rec.featMap['Stage'][-1][1] == 'SFDCDELETED':
                delete_date = rec.featMap['Stage'][-1][0]
            is_deleted_hist = rec.featMap.get('IsDeleted', [[0.0, 'False']])
            if is_deleted_hist[-1][1] not in ['False', 'false']:
                is_deleted_date = is_deleted_hist[-1][0]
                delete_date = min(delete_date, is_deleted_date) if delete_date else is_deleted_date
            if delete_date:
                rs.update_cf_amt(date_hist[date_idx][1],  # cf_date
                                 delete_date,  # update_date
                                 - 1 * amt_hist[amt_idx][1]  # amt_diff
                                 )
        rs_hist = rs.get_uip()
        if rs_hist:
            uip_obj.featMap[self.out_fld] = rs_hist
        return self.out_fld


class AdornWithRawLineitemSchedule(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        self.ref_key = config.get('lookup_fld')
        super(AdornWithRawLineitemSchedule, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        """
        Adorn with revenue from OpportunityLineItemSchedule object.

        Sample config:

            ....

        Basic Logic:

        For each line item, grab the corresponding OpportunityLineItemSchedule objects:
            * whenever an amount is updated, date is updated, or schedule is deleted,
             modify cashflows. We pretend first amount / first date are set on created date.

            Nothing about the line items are used aside from filtering.

        """

        record_filter = self.config.get('record_filter', {})
        sched_record_filter = self.config.get('sched_record_filter', {})

        other_flds = self.config.get('other_fields', [])
        sched_other_flds = self.config.get('sched_other_fields', [])

        cf_offset = self.config.get('cashflow_offset', 0)

        ds_name = self.config['ds_name']
        lookup_fld = self.config['lookup_fld']

        sched_ds_name = self.config.get('sched_ds_name', 'OpportunityLineItemSchedule')
        sched_lookup_fld = self.config.get('sched_lookup_fld', 'OpportunityLineItemId')

        prepare_ds = self.config.get('prepare_ds', True)
        cache_name = self.config.get('cache_name', self.out_fld)
        cache_force_refresh = self.config.get('cache_force_refresh', False)

        # Get a cache of all Opportunity Line Items
        data_handler = get_data_handler(
            dh_type=self.config.get("dh_type", "D"),
            cache_name=cache_name,
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            other_flds=other_flds + ['Stage'],
            cache_historic_keys=False,
            force_refresh=cache_force_refresh,
            prepare_ds=prepare_ds
        )

        sched_dt_fld, cd_fld, amt_fld = 'ScheduleDate', 'CreatedDate', 'Revenue'
        sched_data_handler = get_data_handler(
            dh_type=self.config.get("sched_dh_type", "D"),
            cache_name=cache_name,
            ds_name=sched_ds_name,
            lookup_fld=sched_lookup_fld,
            other_flds=sched_other_flds + [sched_dt_fld, cd_fld, amt_fld, 'Stage'],
            cache_historic_keys=False,
            force_refresh=cache_force_refresh,
            prepare_ds=prepare_ds
        )

        li_list = data_handler.get(uip_obj.ID, [])

        if not li_list:
            return

        # Get created date and create a new revenue schedule
        rs = RevenueSchedule(uip_obj.created_date, cf_offset=cf_offset)

        # TODO : Do we care about 'dangling schedule objects' ie schedule objects which are
        #  associated to line items that have 'IsDeleted'? We handle all other cases.
        for rec in li_list:
            filter_results = rec.passes_filterF(record_filter, uip_obj.created_date)
            if not filter_results[0]:
                logger.warning('Record %s filtered out from map because %s',
                               rec.ID, filter_results)
                continue

            # Get its histories of amount and start date.
            si_list = sched_data_handler.get(rec.ID, [])

            for sched_rec in si_list:

                if not sched_rec.passes_filterF(sched_record_filter, sched_rec.created_date)[0]:
                    continue

                cd = sched_rec.created_date

                amt_hist = [[ts if i else cd, float(amt)]
                            for (i, (ts, amt))
                            in enumerate(sched_rec.featMap.get(amt_fld, []))]
                sched_dt_hist = [[ts if i else cd, dt]
                                 for (i, (ts, dt))
                                 in enumerate(sched_rec.featMap.get(sched_dt_fld, []))]

                # If either start field or amount field are missing, skip the LI.
                if not (amt_hist and sched_dt_hist):
                    continue

                # Initialize bookkeeping vars
                amt_idx, date_idx = 0, 0

                # Do first update
                first_update_date = min(sched_dt_hist[date_idx][0], amt_hist[amt_idx][0])
                rs.update_cf_amt(sched_dt_hist[date_idx][1],  # cf_date
                                 first_update_date,  # update_date
                                 amt_hist[amt_idx][1]  # amt_diff
                                 )

                # Step through the amount and date histories, one leg at a time.
                while amt_idx < len(amt_hist) - 1 or date_idx < len(sched_dt_hist) - 1:
                    # Determine which leg to step with next.
                    if amt_idx == len(amt_hist) - 1:
                        next_fld = sched_dt_fld
                    elif date_idx == len(sched_dt_hist) - 1:
                        next_fld = amt_fld
                    elif amt_hist[amt_idx + 1][0] <= sched_dt_hist[date_idx + 1][0]:
                        next_fld = amt_fld
                    else:
                        next_fld = sched_dt_hist

                    # Step with amt leg
                    if next_fld == amt_fld:
                        rs.update_cf_amt(sched_dt_hist[date_idx][1],  # cf_date
                                         amt_hist[amt_idx + 1][0],  # update_date
                                         amt_hist[amt_idx + 1][1] - amt_hist[amt_idx][1]  # amt_diff
                                         )
                        amt_idx += 1

                    # Step with date leg
                    elif next_fld == sched_dt_fld:
                        rs.update_cf_amt(sched_dt_hist[date_idx][1],  # cf_date
                                         sched_dt_hist[date_idx + 1][0],  # update_date
                                         - 1 * float(amt_hist[amt_idx][1])  # amt_diff
                                         )

                        rs.update_cf_amt(sched_dt_hist[date_idx + 1][1],  # cf_date
                                         sched_dt_hist[date_idx + 1][0],  # update_date
                                         amt_hist[amt_idx][1]  # amt_diff
                                         )

                        date_idx += 1

                # If deleted, process deletion. Deletions only happen last.
                delete_date = None
                if 'Stage' in rec.featMap and rec.featMap['Stage'][-1][1] == 'SFDCDELETED':
                    delete_date = rec.featMap['Stage'][-1][0]
                sched_delete_date = None
                if 'Stage' in sched_rec.featMap and sched_rec.featMap['Stage'][-1][1] == 'SFDCDELETED':
                    sched_delete_date = sched_rec.featMap['Stage'][-1][0]
                    delete_date = min(delete_date, sched_delete_date) if delete_date is not None else sched_delete_date

                is_deleted_hist = rec.featMap.get('IsDeleted', [[0.0, 'False']])
                if is_deleted_hist[-1][1] not in ['False', 'false']:
                    is_deleted_date = is_deleted_hist[-1][0]
                    delete_date = min(delete_date, is_deleted_date) if delete_date else is_deleted_date
                if delete_date:
                    rs.update_cf_amt(sched_dt_hist[date_idx][1],  # cf_date
                                     delete_date,  # update_date
                                     - 1 * amt_hist[amt_idx][1]  # amt_diff
                                     )
        rs_hist = rs.get_uip()
        if rs_hist:
            uip_obj.featMap[self.out_fld] = rs_hist
        return self.out_fld


class TransposeSchedule(UIPMap):
    """ Transposes a revenue schedule field from RevenueSchedule format to standard UIP format:
    Input:
        Single element history. Keys of dictionary are credit dates with separate histories
        of change dates and values.

        [[1, {'1000': [[1, 200], [2, 300]],
              '2000': [[1, 400], [2, 300], [3, 500]]}]]
    Output:
        UIP-style History of the complete revenue schedule at each time.
        Each value is a dictionary with keys are credit dates, values are credit amounts
        on those days.

        [[1, {'1000': 200, '2000': 400}],
         [2, {'1000': 300, '2000': 300}],
         [3, {'1000': 300, '2000': 500}]]
    """

    def process(self, uip_obj):
        if len(self.in_flds) > 1:
            raise Exception('Transpose schedule supports only one field')
        feature = self.in_flds[0]
        if feature not in uip_obj.featMap:
            return
        schedule = uip_obj.featMap[feature][-1][1]
        if not schedule:
            uip_obj.featMap[self.out_fld] = uip_obj.featMap[feature]
        all_vals = [[creddate, changedate, val]
                    for creddate, hist in schedule.items()
                    for (changedate, val) in hist]
        all_vals.sort(key=lambda x: x[1])
        last_date, curr, hist, changedate = 0, {}, [], None
        for i, (creddate, changedate, val) in enumerate(all_vals):
            if curr.get(creddate) == val:
                continue
            if i and changedate > last_date + .0001:
                hist.append([last_date, curr.copy()])
            last_date = changedate
            if val:
                curr[creddate] = val
            else:
                curr.pop(creddate, None)
        if changedate is not None:
            hist.append([changedate, curr.copy()])
        if hist:
            uip_obj.featMap[self.out_fld] = hist
            if self.config.get('__COMPRESS__', False):
                return [self.out_fld]


class AdornWithQuoteInfo(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        self.ref_key = config.get('lookup_fld')
        super(AdornWithQuoteInfo, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        """
        Adorn with info from standard SFDC quote object.

        We'll compute the following things:
        1) SyncedQuote = was any quote syncing at the time. [[t, v], [t2, v2]]
        2) QuoteAmount = what was the last amount update on any quote (even if since deleted)
        3) TotQuotes = How many total quotes ever

        Sample config:

            {'ds_name': 'Quote',
             'lookup_fld': 'OpportunityId',
            }

        """

        tot_amt_fld = self.config.get('tot_amt_field', 'TotalPrice')
        li_amt_fld = self.config.get('li_amt_field', 'SubTotal')
        sync_fld = self.config.get('sync_field', 'IsSyncing')
        cr_dt_fld = self.config.get('date_field', 'CreatedDate')

        # Disabled:
        # record_filter = self.config.get('record_filter', {})

        other_flds = ['Stage', tot_amt_fld, li_amt_fld, sync_fld, cr_dt_fld] + self.config.get('other_fields', [])

        ds_name = self.config['ds_name']
        lookup_fld = self.config['lookup_fld']
        prepare_ds = self.config.get('prepare_ds', True)
        cache_name = self.config.get('cache_name', self.out_fld)
        cache_force_refresh = self.config.get('cache_force_refresh', False)

        # Get a cache of all Opportunity Line Items
        data_handler = get_data_handler(
            dh_type=self.config.get("dh_type", "M"),
            cache_name=cache_name,
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            other_flds=other_flds,
            cache_historic_keys=False,
            force_refresh=cache_force_refresh,
            prepare_ds=prepare_ds
        )

        qu_list = data_handler.get(uip_obj.ID, [])

        is_syncing = False

        # TODO Verbose mode:
        # 4) ActQuotes = how many quotes alive at this time
        # 5) RankedQuoteAmount = like quote amount except synced > alive > deleted
        # 6) QuoteDiscount. = ratio between total price and subtotal on last quote (regardless of syncing)
        # 7) RankedQuoteDiscount = like quote amount except synced > alive > deleted

        if not qu_list:
            uip_obj.featMap['SyncedQuote'] = [[uip_obj.created_date, False]]
            return ['SyncedQuote']

        # A list of events is a list of tuples (t, x) where t is a timestamp and x is some data
        sq_events = []
        qa_events = []
        tq_events = []

        for rec in qu_list:
            # Think about using cr_dt_fld
            tq_events.append((rec.created_date, None))
            # check sync
            sq_events.extend([(t, (rec.ID, is_true(x)))
                              for (t, x) in rec.featMap.get(sync_fld, [])])
            # check deletions
            sq_events.extend([(t, (rec.ID, False))
                              for (t, x) in rec.featMap.get('Stage', [])])
            qa_events.extend(rec.featMap.get(tot_amt_fld, []))

        sq_events.sort()
        sq_hist, synced_quotes = [], set()
        for (t, (ID, event)) in sq_events:
            if not event:
                synced_quotes.discard(ID)
            else:
                synced_quotes.add(ID)
            sq_hist.append([t, bool(synced_quotes)])
        if sq_hist:
            uip_obj.featMap['SyncedQuote'] = sq_hist

        if qa_events:
            qa_events.sort()
            uip_obj.featMap['QuoteAmount'] = qa_events

        if tq_events:
            tq_events.sort()
            uip_obj.featMap['TotQuotes'] = [[t, i] for (i, (t, _)) in enumerate(tq_events, 1)]

        return ['TotQuotes', 'SyncedQuote', 'QuoteAmount']


class AdornWithFields(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        self.ref_key = config.get('ext_id_fld')
        super(AdornWithFields, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            fields = self.config.get('out_flds', [])
            for fld in fields.keys():
                out_fields.append({'name': fld, 'format': 'N/A', "dep_flds": None})
                return out_fields
        else:
            return None

    def process(self, uip_obj):
        from domainmodel.datameta import Dataset, DatasetClass
        ds_name = self.config['ds_name']
        ext_id_fld = self.config['ext_id_fld']  # usually OpportunityID
        filters = self.config.get('filters', None)
        out_flds = self.config.get('out_flds', [])
        ds_inst = Dataset.getByNameAndStage(ds_name)
        ds_class = DatasetClass(ds_inst)
        splts = ds_class.getAllByFieldValue('values.' + ext_id_fld, uip_obj.ID)
        splts = list(splts)
        for out_fld in out_flds.keys():
            out_fld_val = None
            for splt in splts:
                splt = splt.featMap
                try:
                    if filters:
                        filter_me = False
                        for filter_fld, filter_vals in filters.items():
                            val = splt.get(filter_fld, [[None, 'N/A']])[-1][1]
                            if val in filter_vals:
                                filter_me = True
                                break
                        if filter_me:
                            continue
                    out_fld_val = splt.get(out_fld, [[None, None]])[-1][1]
                    if out_fld_val is not None:
                        break
                except:
                    pass
            if out_fld_val:
                if out_fld_val not in out_flds[out_fld].get("fallback_list", []):
                    out_fld_val = out_flds[out_fld].get("fallback_val", 'Uncategorized')
            #  uip_obj.featMap[out_fld] = [[uip_obj.created_date, out_fld_val]]
            uip_obj.featMap[out_fld] = [[uip_obj.created_date, out_fld_val or 'Uncategorized']]
        if self.config.get('__COMPRESS__', True):
            return out_flds.keys()


class EventAggregation(OneToManyDBMap):

    def __init__(self, feature, config=None, stage_ds=None):
        self.ref_key = config.get('lookup_fld')
        super(EventAggregation, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            ds_name = self.config['ds_name']
            fld_defs = self.config['fld_defs']
            for out_fld, defs in fld_defs.items():
                out_fields.append({'name': out_fld, 'format': 'N/A', "dep_flds": {ds_name: defs["req_fields"]}})
                return out_fields
        else:
            return None

    def process(self, uip_obj):
        """
        Event aggregation performs a lookup in a target dataset to find all records whose ID
        refers to the active record. From each of these matching target records a list of 'events' is created
        where an event is a pair (timestamp, data). Each of these events are processed in timestamp order
        to create the timeseries of the out_fld.

        This can be used, for example, to create a field that has the time series of
        the count of Tasks for a given Opportunity.
        """
        if len(self.in_flds) > 1:
            raise Exception('event_aggregation supports only one input field')
        join_fld = self.in_flds[0]

        log_warnings = self.config.get('log_warnings', True)

        if join_fld not in uip_obj.featMap and join_fld != 'ID':
            if log_warnings:
                logger.warning('Join fld %s is not found', join_fld)
            return

        ds_name = self.config['ds_name']
        lookup_fld = self.config['lookup_fld']

        lookup_latest_only = self.config.get('lookup_latest_only', False)
        fld_defs = self.config['fld_defs']
        prepare_ds = self.config.get('prepare_ds', False)
        cache_name = self.config.get('cache_name', self.out_fld)
        cache_force_refresh = self.config.get('cache_force_refresh', False)

        backdate_join_fld = self.config.get('backdate_join_fld', False)

        req_fields_lol = [dtls['req_fields'] for dtls in fld_defs.values()]
        other_flds = list(set([fld for fld_list in req_fields_lol for fld in fld_list]))

        data_handler = get_data_handler(
            dh_type=self.config.get("dh_type", "M"),
            cache_name=cache_name,
            ds_name=ds_name,
            lookup_fld=lookup_fld,
            other_flds=other_flds,  # not like join
            force_refresh=cache_force_refresh,
            prepare_ds=prepare_ds,
            cache_historic_keys=not lookup_latest_only,
        )

        trans_map = {x: [] for x in fld_defs}

        if join_fld == 'ID':
            lookup_hist = [(uip_obj.created_date, uip_obj.ID)]
        else:
            lookup_hist = uip_obj.featMap.get(join_fld, [])

        if lookup_latest_only:
            lookup_hist = lookup_hist[-1:]

        # Construct a big record list of all matching records and the dates which you matched
        # according to the object in the native DS.
        master_rec_list = []
        for i, ts_val in enumerate(lookup_hist):
            beg_ts, lookup_val = ts_val
            if not i and backdate_join_fld:
                beg_ts = None
            end_ts = lookup_hist[i + 1][0] if (i + 1) < len(lookup_hist) else None
            master_rec_list += [(beg_ts, end_ts, rec) for rec in data_handler.get(lookup_val, [])]

        # So now we have a list of records that matched the join and need to be aggregated.
        # We pass these to an aggregation function to get a list of events from each record.
        # Then we go through the events and process them.

        # fld defs work opposite way join fld defs work. The keys are the output fields. The required flds
        # are under config['fld_dtls']['']

        def_event_creator = lambda begin_ts, end_ts, record: [(record.created_date, 1)] \
            if ((end_ts is None) or (record.created_date <= end_ts)) and \
               ((begin_ts is None) or (record.created_date >= begin_ts)) \
            else []
        EVENT_AGGR_CACHE["def_event_creator"] = lambda begin_ts, end_ts, record: [(record.created_date, 1)] \
            if ((end_ts is None) or (record.created_date <= end_ts)) and \
               ((begin_ts is None) or (record.created_date >= begin_ts)) \
            else []
        EVENT_AGGR_CACHE["add"] = operator.add
        for out_fld, dtls in fld_defs.items():
            evnt_lmbda_expr = dtls.get('event_lambda', "def_event_creator")
            reduce_lmbda_expr = dtls.get('reduce_lambda', "add")
            if not EVENT_AGGR_CACHE.get(evnt_lmbda_expr, None):
                EVENT_AGGR_CACHE[evnt_lmbda_expr] = eval(evnt_lmbda_expr)
            if not EVENT_AGGR_CACHE.get(reduce_lmbda_expr, None):
                EVENT_AGGR_CACHE[reduce_lmbda_expr] = eval(reduce_lmbda_expr)
            event_lambda = EVENT_AGGR_CACHE[evnt_lmbda_expr]
            reduce_lambda = EVENT_AGGR_CACHE[reduce_lmbda_expr]
            initial_value = dtls.get('init_val', 0)
            scratch = dtls.get('uses_scratch', False)

            # Event is (ts, data)
            event_lol = [event_lambda(*rec_tuple) for rec_tuple in master_rec_list]

            events = [event for event_list in event_lol for event in event_list]

            if events == [] and dtls.get('vals_if_no_events', True):
                trans_map[out_fld] = [[uip_obj.created_date, initial_value]]
            elif events != []:
                # Sort events
                events = sorted(events, key=lambda x: x[0])
                # Iterate through events and process them.
                if not scratch:
                    running_result = initial_value
                    for (i, (ts, data)) in enumerate(events):
                        incremental_result = reduce_lambda(running_result, data)
                        running_result = incremental_result
                        if trans_map.get(out_fld) and trans_map[out_fld][-1][0] == ts:
                            trans_map[out_fld][-1][1] = incremental_result
                        else:
                            trans_map[out_fld].append([ts, incremental_result])
                else:
                    if log_warnings:
                        logger.warning("Scratch not yet implemented")

        trans_map = {k: v for k, v in trans_map.items() if v}
        uip_obj.featMap.update(trans_map)
        if self.config.get('__COMPRESS__', True):
            return trans_map.keys()


class CleanupEnabledHistory(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(CleanupEnabledHistory, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'N/A', "dep_flds": {"self": [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) > 1:
            raise Exception('join supports only one input field')
        enabled_fld = self.in_flds[0]
        fallback_policy = self.config.get('fallback_policy', 'force_true')
        if enabled_fld not in uip_obj.featMap:
            if fallback_policy == 'force_true':
                uip_obj.featMap[self.out_fld] = [[uip_obj.created_date, True]]
            elif fallback_policy == 'force_false':
                uip_obj.featMap[self.out_fld] = [[uip_obj.created_date, False]]
            elif not fallback_policy == 'empty':
                raise Exception("fallback_policy=%s not supported, but be one of force_true, force_false, or empty")
            return
        enabled_vals = set(self.config['enabled_vals'])
        log_warnings = self.config.get('log_warnings', True)
        enabled_hist = [[ts, True if val in enabled_vals else False] for ts, val in uip_obj.featMap[enabled_fld]]
        # compress it to be sure
        enabled_hist = [x for i, x in enumerate(enabled_hist)
                        if i < 1 or enabled_hist[i - 1][1] != x[1]]
        if not enabled_hist[-1][1]:  # is currently disabled
            if enabled_hist[0][0] > uip_obj.created_date:  # seems to be missing history
                if len(enabled_hist) == 1:  # yes definitely missing history
                    last_login_fld = self.config['last_login_fld']
                    last_login_hist = uip_obj.featMap.get(last_login_fld, [])
                    if not last_login_hist:
                        if log_warnings:
                            logger.warning("WARNING: cleanup_enabled_history: %s missing for %s",
                                           last_login_fld, uip_obj.ID)
                    else:
                        last_login = None
                        try:
                            last_login = float(last_login_hist[-1][1])
                        except:
                            logger.error(
                                "ERROR: cleanup_enabled_history: could not get last_login for %s: %s",
                                uip_obj.ID, last_login_hist)
                        if last_login is not None:
                            enabled_hist = [[uip_obj.created_date, True], [last_login + 1, False]]
        enabled_hist[0][0] = uip_obj.created_date  # make sure history is from beginning whatever it may be
        uip_obj.featMap[self.out_fld] = enabled_hist
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class TranslateBands(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(TranslateBands, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'N/A', "dep_flds": {"self": [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) > 1:
            raise Exception('bands supports only one field')
        feature = self.in_flds[0]
        trans_val_map = []
        untrans_val_map = uip_obj.featMap.get(feature, None)
        if untrans_val_map is None:
            if 'N/A' in self.config:
                # Look for N/A
                uip_obj.featMap[self.out_fld] = [[uip_obj.created_date, self.config['N/A']]]
            else:
                # No N/A Mapping found, and no values found for the UIP field
                pass
        else:
            for date, untrans_value in untrans_val_map:
                untrans_value_f = excelToFloat(untrans_value)
                for band_name, interval in self.config.items():
                    if band_name == 'N/A' or band_name == '__COMPRESS__':
                        continue
                    if interval[0] is not None and untrans_value_f < interval[0]:
                        continue
                    if interval[1] is not None and untrans_value_f >= interval[1]:
                        continue
                    trans_val_map.append([date, band_name])
                    break
                else:
                    raise Exception(str.format("No band is available for {0} in map for {1}.", untrans_value,
                                               self.out_fld))
            if trans_val_map:
                uip_obj.featMap[self.out_fld] = trans_val_map

        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class CCYConv(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(CCYConv, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'float', "dep_flds": {"self": [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) > 1:
            raise Exception('translate conversion only supports one field at a time')
        feature = self.in_flds[0]
        orig_hist = uip_obj.featMap.get(feature, [])
        if not isinstance(orig_hist, list) or len(orig_hist) == 0:
            return
        else:  # the history exists and has at least one element
            def get_conv_rate(ccy):  # make looking up ccy a function so it is more clear if it fails
                try:
                    return self.config['conv_rates'][ccy]
                except:  # make sure it is noticed that a conversion rate is missing
                    raise Exception("ERROR: For %s we are missing currency conversion rate for '%s'" % (uip_obj.ID,
                                                                                                        ccy))

            ccy_fld = self.config['ccy_field']
            base_ccy = self.config['base_ccy']
            if ccy_fld not in uip_obj.featMap:
                if self.config.get('warn_no_ccy', False):
                    logger.warning("id='%s' has no currency code: assuming to base currency '%s'",
                                   uip_obj.ID,
                                   base_ccy)
                ccy = base_ccy
                conv_rate = get_conv_rate(ccy)
                uip_obj.featMap[self.out_fld] = [(ts, excelToFloat(amt) * conv_rate)
                                                 for ts, amt in uip_obj.featMap[feature]]
            elif self.config.get('use_latest_ccy', False) or len(uip_obj.featMap[ccy_fld]) == 1:
                ccy = uip_obj.getLatest(ccy_fld)
                conv_rate = get_conv_rate(ccy)
                uip_obj.featMap[self.out_fld] = [(ts, excelToFloat(amt) * conv_rate)
                                                 for ts, amt in uip_obj.featMap[feature]]
            else:  # more than one currency, must have changed over time
                # add additional entries for when ccy changes but amount does not
                orig_hist_map = {ts: excelToFloat(amt) for ts, amt in uip_obj.featMap[feature]}
                min_ts = excelToFloat(uip_obj.featMap[feature][0][0])
                # get sorted timestamps needed (note: since we do look-forward, do not add ts for first ccy entry)
                add_tss = [x[0] for x in uip_obj.featMap[ccy_fld][1:] if x[0] > min_ts and x[0] not in orig_hist_map]
                tss = sorted(orig_hist_map.keys() + add_tss)
                uip_obj.featMap[self.out_fld] = [(ts,
                                                  orig_hist_map.get(ts, excelToFloat(uip_obj.getAsOfDateF(feature, ts)))
                                                  * get_conv_rate(uip_obj.getAsOfDateF(ccy_fld, ts,
                                                                                       first_value_on_or_after=True))
                                                  ) for ts in tss]
            if self.config.get('drop_extreme_acv_updates', False) is True:
                acv_limit_lo = self.config.get('acv_limit_lo', -100000000000)
                acv_limit_hi = self.config.get('acv_limit_hi', 100000000000)
                conv_hist = uip_obj.featMap[self.out_fld]
                if conv_hist:
                    keeps = [(amt > acv_limit_lo) and (amt < acv_limit_hi)
                             for ts, amt in conv_hist]
                    uip_obj.featMap[self.out_fld] = [y[1] for y in filter(lambda x: keeps[x[0]],
                                                                          enumerate(conv_hist))]
                    # delete the feature from the map if it has no history (otherwise all other functions will fail)
                    if not uip_obj.featMap[self.out_fld]:
                        del uip_obj.featMap[self.out_fld]
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class DatedCCYConv(UIPMap):

    def __init__(self, feature, config=None, sandbox_ds=None):
        super(DatedCCYConv, self).__init__(feature, config, sandbox_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            fld_map = self.config['fld_map']
            out_fields = []
            for fld, defs in fld_map.items():
                in_fld = defs.get("in_fld", fld)
                out_fields.append({'name': fld, 'format': 'float', "dep_flds": {"self": [in_fld]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        '''
        Sample:
            ds['maps']['datedccy_conv(DatedConversionRates,CloseDate):dummy'] = {         # Field which contain conversion rates
                CloseDate Check is needed for validation whether deal falls between the Start & NextStartDate.
                u'convert_function': u'lambda amt,fxrate : float(amt) / float(fxrate)', #Optional, amt/fxrate is default
                u'default_rate': 1,                                                     # Optional, Default = 1
                u'lambda_failure_val': 0,                                               # Optional, Default = 0
                u'first_value_on_or_after': True,                                       # Optional, Default = True
                u'fld_map':{u'Amount_USD' : {u'in_fld' : u'Amount', u'default_value':0}, # default_value is optional
                u'MergedAmount_USD' : {u'in_fld' : u'MergedAmount', u'default_value':0},
                u'ServicesAmount_USD' : {u'in_fld' : u'ServicesAmount', u'default_value':0},
                u'UpsideValue_USD' : {u'in_fld' : u'UpsideValue', u'default_value':0}},
                u'exec_rank': 3}
        '''
        if len(self.in_flds) != 2:
            raise Exception('DatedCCYConv only supports 2 field at a time')
        ccy_fld = self.in_flds[0]
        closedate_fld = self.in_flds[1]
        convert_function = eval(self.config.get('convert_function', u'lambda amt, fxrate : float(amt) / float(fxrate)'))
        default_rate = self.config.get('default_rate', 1)
        lambda_failure_val = self.config.get('lambda_failure_val', 0)
        first_value_on_or_after = self.config.get('first_value_on_or_after', True)
        if 'fld_map' not in self.config:
            raise Exception('no fld_map specified')
        fld_map = self.config['fld_map']
        for fld in fld_map:
            if fld in uip_obj.featMap:
                raise Exception('the field, ' + fld + ', you are creating already exists. choose a different name')
            if 'in_fld' not in fld_map[fld]:
                raise Exception('No in_fld specified for target field %s', str(fld))
            if fld_map[fld]['in_fld'] not in uip_obj.featMap:
                continue
            in_fld = fld_map[fld]['in_fld']
            all_tss = set(ts for ts, _ in uip_obj.featMap[in_fld])
            closedate_value = uip_obj.getLatest(closedate_fld)
            all_tss = sorted(all_tss)
            out_feat = []
            for ts in all_tss:
                amt = uip_obj.getAsOfDateF(in_fld, ts, first_value_on_or_after, False)
                fxrate = uip_obj.getAsOfDateConversion(ccy_fld, closedate_value, first_value_on_or_after, False)
                amt = amt if (amt != 'N/A' and amt is not None) else fld_map[fld].get('default_value', 0)
                fxrate = fxrate if (fxrate != 'N/A' and fxrate is not None) else default_rate
                try:
                    out_val = convert_function(amt, fxrate)
                except:
                    out_val = float(lambda_failure_val)
                if out_val is not None:
                    out_feat.append([ts, out_val])
            uip_obj.featMap[fld] = out_feat
        if self.config.get('__COMPRESS__', True):
            return fld_map.keys()


class CCYConv2(UIPMap):
    def __init__(self, feature, config=None, stage_ds=None):
        super(CCYConv2, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            fld_map = self.config['fld_map']
            out_fields = []
            for fld, defs in fld_map.items():
                in_fld = defs.get("in_fld", fld)
                out_fields.append({'name': fld, 'format': 'float', "dep_flds": {"self": [in_fld]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        '''
        Sample:
            ds['maps']['curr_conv2(ConversionRates):dummy'] = {         # Field which contain conversion rates
                u'convert_function': u'lambda amt,fxrate : float(amt) / float(fxrate)', #Optional, amt/fxrate is default
                u'default_rate': 1,                                                     # Optional, Default = 1
                u'lambda_failure_val': 0,                                               # Optional, Default = 0
                u'first_value_on_or_after': True,                                       # Optional, Default = True
                u'fld_map':{u'Amount_USD' : {u'in_fld' : u'Amount', u'default_value':0}, # default_value is optional
                u'MergedAmount_USD' : {u'in_fld' : u'MergedAmount', u'default_value':0},
                u'ServicesAmount_USD' : {u'in_fld' : u'ServicesAmount', u'default_value':0},
                u'UpsideValue_USD' : {u'in_fld' : u'UpsideValue', u'default_value':0}},
                u'exec_rank': 3}
        '''
        if len(self.in_flds) != 1:
            raise Exception('ccy_conv2 only supports 1 field at a time')
        ccy_fld = self.in_flds[0]
        convert_function = eval(self.config.get('convert_function', u'lambda amt, fxrate : float(amt) / float(fxrate)'))
        default_rate = self.config.get('default_rate', 1)
        lambda_failure_val = self.config.get('lambda_failure_val', 0)
        first_value_on_or_after = self.config.get('first_value_on_or_after', True)
        if 'fld_map' not in self.config:
            raise Exception('no fld_map specified')
        fld_map = self.config['fld_map']
        for fld in fld_map:
            if fld in uip_obj.featMap:
                raise Exception('the field, ' + fld + ', you are creating already exists. choose a different name')
            if 'in_fld' not in fld_map[fld]:
                raise Exception('No in_fld specified for target field %s', str(fld))
            if fld_map[fld]['in_fld'] not in uip_obj.featMap:
                continue
            in_fld = fld_map[fld]['in_fld']
            first_time_stamp = uip_obj.featMap[in_fld][0][0]
            all_tss = set(ts for ts, _ in uip_obj.featMap[in_fld])
            if ccy_fld in uip_obj.featMap:
                all_tss |= set(ts for ts, _ in uip_obj.featMap[ccy_fld] if ts > first_time_stamp)
            all_tss = sorted(all_tss)
            out_feat = []
            for ts in all_tss:
                amt = uip_obj.getAsOfDateF(in_fld, ts, first_value_on_or_after, False)
                fxrate = uip_obj.getAsOfDateF(ccy_fld, ts, first_value_on_or_after, False)
                amt = amt if (amt != 'N/A' and amt is not None) else fld_map[fld].get('default_value', 0)
                fxrate = fxrate if (fxrate != 'N/A' and fxrate is not None) else default_rate
                try:
                    out_val = convert_function(amt, fxrate)
                except:
                    out_val = float(lambda_failure_val)
                if out_val is not None:
                    out_feat.append([ts, out_val])
            uip_obj.featMap[fld] = out_feat
        if self.config.get('__COMPRESS__', True):
            return fld_map.keys()


class MergeFields(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(MergeFields, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'N/A', 'dep_flds': {'self': self.in_flds[0:2]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 2:
            raise Exception('merge_fields only supports two fields at a time')
        field1 = self.in_flds[0]
        field2 = self.in_flds[1]
        old_field = self.config['old_field']
        if old_field != field1 and old_field != field2:
            raise Exception('old_field ' + old_field + ' has to be one of the fields ' + field1 + ' or ' + field2)
        new_field = self.config['new_field']
        if new_field != field1 and new_field != field2:
            raise Exception('new_field ' + new_field + ' has to be one of the fields ' + field1 + ' or ' + field2)
        if self.config.get('switch_over_fld') is None:
            switch_over_date = self.config.get('switch_over_date')
        else:
            switch_over_fld = self.config.get('switch_over_fld')
            # switch_over_fld is usually terminal_fate
            # if it's not in the record, then the deal isn't closed, so return the old values
            if switch_over_fld not in uip_obj.featMap:
                if old_field in uip_obj.featMap:
                    uip_obj.featMap[self.out_fld] = uip_obj.featMap[old_field]
                elif new_field in uip_obj.featMap:
                    uip_obj.featMap[self.out_fld] = uip_obj.featMap[new_field]
                return
            else:
                # switch_over_trans is the transformation function, defaulted to the end of the quarter of the timestamp given
                # switch_over_ts is the last timestamp of the switch_over_field
                switch_over_trans = eval(
                    self.config.get('switch_over_trans', 'lambda x:datetime2xl(next_period(xl2datetime(x)).begin)'))
                switch_over_ts = uip_obj.featMap.get(switch_over_fld)[-1][0]
                switch_over_date = switch_over_trans(switch_over_ts)
        switch_over_default_value = self.config.get('switch_over_default_value', None)
        old_field_transform = eval(self.config.get('old_field_transform', 'lambda x:x'))
        new_field_transform = eval(self.config.get('new_field_transform', 'lambda x:x'))
        old_field_transf = None
        new_field_transf = None
        if old_field in uip_obj.featMap:
            old_field_transf = [[x[0], old_field_transform(x[1])] for x in uip_obj.featMap[old_field]]
            if switch_over_date:
                for pair in uip_obj.featMap[old_field][::-1]:
                    if pair[0] >= switch_over_date:
                        old_field_transf.pop()

        if new_field in uip_obj.featMap:
            new_field_transf = [[x[0], new_field_transform(x[1])] for x in uip_obj.featMap[new_field]]

            if switch_over_date:
                last_val = None
                for pair in uip_obj.featMap[new_field]:
                    if pair[0] <= switch_over_date:
                        last_val = new_field_transf.pop(0)
                if last_val is not None:
                    new_field_transf = [[switch_over_date, last_val[1]]] + new_field_transf

        if switch_over_date and (switch_over_default_value is not None):
            if not new_field_transf:
                new_field_transf = [[switch_over_date, switch_over_default_value]]
            elif new_field_transf[0][0] > switch_over_date:
                new_field_transf = [[switch_over_date, switch_over_default_value]] + new_field_transf

        if (not new_field_transf) and (not old_field_transf):
            return
        if not new_field_transf:
            uip_obj.featMap[self.out_fld] = list(old_field_transf)
            return
        if not old_field_transf:
            uip_obj.featMap[self.out_fld] = list(new_field_transf)
            return
        # if made it here both fields are present
        new_field_first_date = new_field_transf[0][0]
        # remove items in the old filed that are entered while the new field exists
        old_field_copy = list(old_field_transf)
        for pair in old_field_copy[::-1]:
            if pair[0] >= new_field_first_date:
                old_field_transf.pop()
            else:
                break
        uip_obj.featMap[self.out_fld] = list(old_field_transf) + list(new_field_transf)
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class TransformFields(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(TransformFields, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'N/A', 'dep_flds': {'self': [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('transform_field only supports 1 field at a time')
        in_fld = self.in_flds[0]
        if in_fld not in uip_obj.featMap:
            return
        field_transform = eval(self.config['trans_fn'])
        uip_obj.featMap[self.out_fld] = [[x[0], field_transform(x[1])] for x in uip_obj.featMap[in_fld]]
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class TransformTimestamps(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(TransformTimestamps, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        field_transform = eval(self.config.get('trans_fn', 'lambda x: x'))
        offset = self.config.get('offset')
        fld_sfx = self.config.get('sfx', '')
        for field in self.in_flds:
            if field not in uip_obj.featMap:
                continue
            old_uip = uip_obj.featMap[field]
            new_uip = [[ts + offset, val] for ts, val in old_uip] if offset is not None else \
                [[field_transform(ts), val] for ts, val in old_uip]
            uip_obj.featMap[field + fld_sfx] = new_uip


class AdornWithPreviousValue(UIPMap):
    """
    Description:
    This map adds information on the previous values of the fields passed to it.
    The map is useful for the "Deal Insights" feature.
    For each field it is given the map ignores any items in the history of the field that had
    a time duration less than the sensitivity, and then creates two time series:
        * previouses: [Default name: Field_prev] The previous value for each time in the field history.
        * last updated dates: [Default name: Field_lud] The time when the value was last changed in the field history.

    Optional Configuration:
        * sensitivity: [Default: .00035] The minimum amount of time (in Excel format) a value
        must have existed for. This is used to discard value changes that are due to user error.
        * prev_suffix: [Default: '_prev'] Suffix of the new field for the previous values.
        * lud_suffix: [Default: '_lud'] Suffix of the new field for the last updated date values.

    Sample:
    adorn_with_previous_value(Stage,ForecastCategory,CloseDate,Amount):Dummy: {}
    """

    def __init__(self, feature, config=None, stage_ds=None):
        super(AdornWithPreviousValue, self).__init__(feature, config, stage_ds)
        self.sensitivity = self.config.get('sensitivity', .00035)  # ~30 seconds
        self.prev_suffix = self.config.get('prev_suffix', '_prev')
        self.lud_suffix = self.config.get('lud_suffix', '_lud')

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            for field in self.in_flds:
                prev_field = field + self.prev_suffix
                lud_field = field + self.lud_suffix
                out_fields.append({'name': prev_field, 'format': 'N/A', 'dep_flds': {'self': [field]}})
                out_fields.append({'name': lud_field, 'format': 'xl_date', 'dep_flds': {'self': [field]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        out_fields = set()

        for field in self.in_flds:
            if field not in uip_obj.featMap:
                continue
            field_history = uip_obj.featMap[field]
            # keep first and last items in field history and remove any with a time duration less than sensitivity
            smoothed_history = [[time, value] for index, (time, value) in enumerate(field_history)
                                if index in [0, len(field_history) - 1]
                                or time - field_history[index - 1][0] > self.sensitivity]
            # time series with [time, value before time] for each item in smoothed history
            previouses = [[time, smoothed_history[index - 1][1]] for index, (time, _)
                          in enumerate(smoothed_history) if index]
            last_updated_dates = [[time, time] for time, _ in smoothed_history]

            if previouses:
                prev_field = field + self.prev_suffix
                uip_obj.featMap[prev_field] = previouses
                out_fields.add(prev_field)
            if last_updated_dates:
                lud_field = field + self.lud_suffix
                uip_obj.featMap[lud_field] = last_updated_dates
                out_fields.add(lud_field)

        return out_fields


class CopyFilterField(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(CopyFilterField, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': "N/A", 'dep_flds': {'self': [self.inflds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('copy_filter_field only supports 1 field at a time')
        in_fld = self.in_flds[0]
        if in_fld not in uip_obj.featMap:
            return
        filter_fn = eval(self.config['filter_fn'])
        filter_fld = self.config.get('filter_fld', in_fld)
        fill_val = self.config.get('fill_val', "N/A")
        default_val = self.config.get('default_val', 'N/A')
        uip_obj.featMap[self.out_fld] = []
        for ts, val in uip_obj.featMap[in_fld]:
            if filter_fld not in uip_obj.featMap:
                uip_obj.featMap[self.out_fld].append([ts, default_val])
                break
            if filter_fn(
                    uip_obj.getAsOfDateF(filter_fld, ts, first_value_on_or_after=True, NA_before_created_date=False)):
                uip_obj.featMap[self.out_fld].append([ts, val])
            else:
                uip_obj.featMap[self.out_fld].append([ts, fill_val])
        if filter_fld in uip_obj.featMap:
            try:
                for ts, val in uip_obj.featMap[filter_fld]:
                    if filter_fn(val):
                        in_val = uip_obj.getAsOfDateF(in_fld, ts, first_value_on_or_after=True,
                                                      NA_before_created_date=False)
                        uip_obj.featMap[self.out_fld].append([ts, in_val])
                    else:
                        uip_obj.featMap[self.out_fld].append([ts, fill_val])
            except Exception as e:
                logger.warning('something bad: %s', e)
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class TransformPairs(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(TransformPairs, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'N/A', 'dep_flds': {'self': [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('transform_pairs only supports 1 field at a time')
        in_fld = self.in_flds[0]
        out_fld = self.out_fld

        field_transform = eval(self.config['trans_fn'])

        if in_fld not in uip_obj.featMap:
            return

        uip_obj.featMap[out_fld] = [field_transform(x[0], x[1]) for x in uip_obj.featMap[in_fld]]
        if self.config.get('__COMPRESS__', True):
            return [out_fld]


class Backdate(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(Backdate, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'N/A', 'dep_flds': {'self': [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        '''
        force a field to
        '''
        if len(self.in_flds) > 1:
            raise Exception('backdate supports only one field')
        feature = self.in_flds[0]
        if feature not in uip_obj.featMap:
            return
        featHist = uip_obj.featMap[feature]
        if not featHist or len(featHist) < 1:
            return
        uip_obj.featMap[self.out_fld] = [
                                            (uip_obj.created_date, featHist[0][1])] + [(ts, val) for ts, val in
                                                                                       featHist[1:]]
        if self.config.get('__COMPRESS__', False):  # backdating should not require compression
            return [self.out_fld]


class SplitField(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(SplitField, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            split_config = self.config['split_config']
            out_fields = []
            for fld in split_config.keys():
                out_fields.append([{'name': fld, 'format': "N/A", 'dep_flds': {'self': [self.in_flds[0]]}}])
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        '''
        sample config
        'split_field(in_fld):':
        {
        'split_config': {'part1': 'lambda in_fld: in_fld[:2]',
        'part2': 'lambda in_fld: in_fld[2:4]',
        'part3': 'lambda in_fld: in_fld[4:]'}
        }
        '''
        if len(self.in_flds) != 1:
            raise Exception('split_field only supports 1 field at a time')
        in_fld = self.in_flds[0]
        if in_fld not in uip_obj.featMap:
            return
        if self.out_fld != '':
            raise Exception('correct usage is split_field(input_field):')
        if 'split_config' not in self.config:
            raise Exception('no split_config specified')
        split_config = self.config['split_config']
        for field_name, fn in split_config.items():
            fn = eval(fn)
            if field_name in uip_obj.featMap:
                raise Exception(
                    'the field you are splitting to ' + field_name + ' already exists. choose a different name')
            uip_obj.featMap[field_name] = [[x[0], fn(x[1])] for x in uip_obj.featMap[in_fld]]
        if self.config.get('__COMPRESS__', True):
            return split_config.keys()


class CombineFields(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(CombineFields, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'float', 'dep_flds': {'self': self.in_flds}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        first_field = self.in_flds[0]
        combine_fn = eval(self.config['combine_fn'])
        default_values = self.config['default_values']
        if first_field not in uip_obj.featMap:
            return
        comb_hist = []
        for date, val in uip_obj.featMap[first_field]:
            values = [uip_obj.getAsOfDateF(x, date, True) for x in self.in_flds]
            values[0] = val
            values2 = []
            for x, y in zip(values, default_values):
                if x == 'N/A' and y is not None:
                    values2.append(y)
                else:
                    values2.append(x)
            comb_hist.append([date, combine_fn(*values2)])

        if comb_hist:
            uip_obj.featMap[self.out_fld] = comb_hist
            if self.config.get('__COMPRESS__', True):
                return [self.out_fld]


class DeleteFields(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(DeleteFields, self).__init__(feature, config, stage_ds)
        self.field_list = self.config['field_list']

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            for field in self.field_list:
                out_fields = [{'name': self.out_fld, 'format': 'float',
                               'dep_flds': {'self': [field]}, 'action': 'removed'}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        for field in self.field_list:
            if field in uip_obj.featMap:
                del uip_obj.featMap[field]


class FilterField(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(FilterField, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': "N/A", 'dep_flds': {'self': [self.inflds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('filter_field only supports 1 field at a time')
        in_fld = self.in_flds[0]
        if in_fld not in uip_obj.featMap:
            return
        filter_fn = eval(self.config['filter_fn'])
        filt_out = filter(lambda x: filter_fn(x[1]), uip_obj.featMap[in_fld])
        if filt_out:
            uip_obj.featMap[self.out_fld] = filt_out
        elif self.out_fld in uip_obj.featMap:
            del uip_obj.featMap[self.out_fld]
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class CreateACVTimeline(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(CreateACVTimeline, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            out_fields.append({'name': "ACV_timeline", 'format': "N/A", 'dep_flds': None})
            out_fields.append({'name': "First_win_date", 'format': "N/A", 'dep_flds': None})
            out_fields.append({'name': "Downsell_timeline", 'format': "N/A", 'dep_flds': None})
            out_fields.append({'name': "Live_churn_date", 'format': "N/A", 'dep_flds': None})
            out_fields.append({'name': "won_then_lost_opps", 'format': "N/A", 'dep_flds': None})
            out_fields.append({'name': "num_won_opps", 'format': "N/A", 'dep_flds': None})
            out_fields.append({'name': "num_lost_opps", 'format': "N/A", 'dep_flds': None})
            out_fields.append({'name': "closed_opps", 'format': "N/A", "dep_flds": None})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1 and self.in_flds[0] != '':
            raise Exception('create_acv_timeline takes no field input. you entered' + str(self.in_flds))
        if self.out_fld != '':
            raise Exception('correct usage is create_acv_timeline():')
        config = {
            'opp_dataset': 'OppDS',
            'opp_dataset_stage': None,
            'opp_filter_accept': {u'SubType': [u'SaaS Contract']},
            'opp_filter_reject': {u'Stage': [u'SFDCDELETED', 'Closed - Deleted']},
            'amount_field_name': 'Amount_USD',
            'oppds_account_id_field': 'AccountID',
            'close_date_field_name': 'Close Date',
            'opp_win_date_field_name': 'win_date_adj',
            'opp_lose_date_field_name': 'lose_date_adj',
            'opp_type_field_name': 'Type',
            'closed_opps_field': 'closed_opps',
            'debug': False,
            'suppress_warnings': True,
            'churn_indicator_field_name': 'Churned',
            'opp_type_map': {
                'New Business': 'new',
                'Additional Licenses': 'upsell',
                'ASP Renewal': 'reset',
                'Switch To SaaS': 'ignore',
                'Add-on Module': 'ignore',
                'Professional Services': 'ignore',
                'Maintenance Renewal': 'ignore',
            },
        }

        config.update(config)
        from domainmodel import datameta

        oppds = datameta.Dataset.getByNameAndStage(config['opp_dataset'], config['opp_dataset_stage'])
        OppDatasetClass = datameta.DatasetClass(oppds)
        opps_set = OppDatasetClass.getAllByFieldValue('values.' + config['oppds_account_id_field'], uip_obj.ID)
        closed_opps = []
        won_then_lost_opps = []

        for opp in opps_set:
            opp.prepare(oppds)

            opp_passes_filter = True
            for feature, values in config['opp_filter_accept'].items():
                if opp.getLatest(feature) not in values:
                    opp_passes_filter = False
                    break
            if opp_passes_filter is False:
                continue
            for feature, values in config['opp_filter_reject'].items():
                if opp.getLatest(feature) in values:
                    opp_passes_filter = False
                    break
            if opp_passes_filter is False:
                continue
            opp_id = opp.ID
            win_date = opp.getLatest(config['opp_win_date_field_name'])
            lose_date = opp.getLatest(config['opp_lose_date_field_name'])

            opp_is_won = None
            if win_date != 'N/A' and lose_date == 'N/A':
                opp_is_won = True
            if win_date == 'N/A' and lose_date != 'N/A':
                opp_is_won = False
            if win_date != 'N/A' and lose_date != 'N/A':
                if win_date <= lose_date:
                    won_then_lost_opps.append([lose_date, (win_date, opp_id)])
                    opp_is_won = False
                else:
                    opp_is_won = True

            try:
                ACV = float(opp.getLatest(config['amount_field_name']))
            except:
                ACV = 0.0

            if opp_is_won is True:
                if ACV != 0.0:
                    closed_opps.append({'ID': opp_id,
                                        'ACV': ACV,
                                        'close_date': win_date,
                                        'win': True,
                                        'type': opp.getLatest(config['opp_type_field_name']),
                                        }
                                       )
            if opp_is_won is False:
                if ACV != 0.0:
                    closed_opps.append({'ID': opp_id,
                                        'ACV': ACV,
                                        'close_date': lose_date,
                                        'win': False,
                                        'type': opp.getLatest(config['opp_type_field_name'])
                                        }
                                       )

        if won_then_lost_opps:
            uip_obj.featMap['won_then_lost_opps'] = sorted(won_then_lost_opps, key=lambda x: x[0])

        closed_opps = [[x['close_date'], x] for x in sorted(closed_opps, key=lambda x: x['close_date'])]
        if not closed_opps:
            return
        if closed_opps:
            if config['debug']:
                uip_obj.featMap[config['closed_opps_field']] = closed_opps
        if closed_opps:
            uip_obj.featMap['num_won_opps'] = [[
                closed_opps[-1][0],
                len(filter(lambda x: x[1]['win'], closed_opps))
            ]]
            uip_obj.featMap['num_lost_opps'] = [[
                closed_opps[-1][0],
                len(filter(lambda x: not x[1]['win'], closed_opps))
            ]]
        #        if uip_obj.featMap['num_won_opps'][-1][1] == 0:
        #            return
        opp_type_map = config['opp_type_map']
        ACV_timeline = []
        downsell_timeline = []
        for x in closed_opps:
            t = x[0]
            opp_info = x[1]
            opp_type = opp_info['type']
            if opp_type not in opp_type_map:
                if not config['suppress_warnings']:
                    logger.warning(
                        'for opp ' + opp_info['ID'] + ' the type ' + opp_type + ' is not in the opp_type_map')
                continue
            if opp_type_map[opp_type] == 'ignore':
                continue
            mapped_type = opp_type_map[opp_type]

            #            if opp_info['ACV']<0.0:
            # no matter if it is won or lost, take that ACV away
            #                if not ACV_timeline:
            #                    if not config['suppress_warnings']:
            #                        logger.warning('The opp '+opp_info['ID']+'  which is  first won or lost opp, ACV cannot be negative')
            #                else:
            # this if shouldn't be there but replicon's data is so messy - so I am hacking to move forward
            #                    if mapped_type == 'reset':
            #                        downsell_timeline.append([t, -opp_info['ACV']])
            # if this causes a total churn do not add to acv timeline so that the last acv point is preserved
            #                    if ACV_timeline[-1][1]+ opp_info['ACV']>0.0:
            #                        ACV_timeline.append([t, ACV_timeline[-1][1]+ opp_info['ACV']])
            #                continue

            if mapped_type == 'new':
                if opp_info['win'] is True:
                    if ACV_timeline:
                        if not config['suppress_warnings']:
                            logger.warning('The opp ' + opp_info['ID'] +
                                           '  which is not the first opp, the type ' +
                                           opp_type + ' should not be new business')
                        if ACV_timeline[-1][1] + opp_info['ACV'] > 0:
                            ACV_timeline.append([t, ACV_timeline[-1][1] + opp_info['ACV']])
                    else:
                        if opp_info['ACV'] > 0:
                            ACV_timeline.append([t, opp_info['ACV']])

            if mapped_type == 'upsell' and opp_info['win'] is True:
                if ACV_timeline:
                    if ACV_timeline[-1][1] + opp_info['ACV'] > 0:
                        ACV_timeline.append([t, ACV_timeline[-1][1] + opp_info['ACV']])

                else:
                    if opp_info['ACV'] > 0:
                        ACV_timeline.append([t, opp_info['ACV']])
            if mapped_type == 'reset' and opp_info['win'] is True:
                if ACV_timeline and opp_info['ACV'] == ACV_timeline[-1][1]:
                    pass
                else:
                    if opp_info['ACV'] > 0:
                        ACV_timeline.append([t, opp_info['ACV']])
            if mapped_type == 'reset' and opp_info['win'] is False:
                if opp_info['ACV'] > 0:
                    downsell_timeline.append([t, opp_info['ACV']])

        if downsell_timeline:
            if self.getLatest(config['churn_indicator_field_name']) is True:
                uip_obj.featMap['Live_churn_date'] = [[downsell_timeline[-1][0], downsell_timeline[-1][0]]]
                if ACV_timeline:
                    if ACV_timeline[-1][0] < downsell_timeline[-1][0]:
                        ACV_timeline.append(downsell_timeline.pop())
                else:
                    ACV_timeline.append(downsell_timeline.pop())

        if ACV_timeline:
            uip_obj.featMap['ACV_timeline'] = list(ACV_timeline)
            uip_obj.featMap['First_win_date'] = [[ACV_timeline[0][0], ACV_timeline[0][0]]]
            uip_obj.created_date = min(ACV_timeline[0][0], uip_obj.created_date)

        if downsell_timeline:
            uip_obj.featMap['Downsell_timeline'] = list(downsell_timeline)
        return


class Cumsum(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(Cumsum, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'float', 'dep_flds': {'self': [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('cumsum only supports 1 field at a time')
        in_fld = self.in_flds[0]

        if in_fld not in uip_obj.featMap:
            return
        output = []
        cumsum = 0.0
        for pair in uip_obj.featMap[in_fld]:
            cumsum += float(pair[1])
            output.append([pair[0], cumsum])
        uip_obj.featMap[self.out_fld] = output
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class CountDistinctValues(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(CountDistinctValues, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'number', 'dep_flds': {'self': [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('CountDistinctValues only supports 1 field at a time')
        in_fld = self.in_flds[0]
        if in_fld not in uip_obj.featMap:
            return

        ignore_vals = self.config.get('vals_to_ignore', [None])

        output = []
        distinct = set()
        for ts, v in uip_obj.featMap[in_fld]:
            if v not in ignore_vals:
                distinct.add(v)
            output.append([ts, len(distinct)])
        uip_obj.featMap[self.out_fld] = output
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class Delta(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(Delta, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'float', 'dep_flds': {'self': [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('delta only supports 1 field at a time')
        in_fld = self.in_flds[0]

        if in_fld not in uip_obj.featMap:
            return
        if len(uip_obj.featMap[in_fld]) < 2:
            return
        output = []
        prior = float(uip_obj.featMap[in_fld][0][1])
        for pair in uip_obj.featMap[in_fld][1:]:
            new_val = float(pair[1])
            output.append([pair[0], new_val - prior])
            prior = new_val
        uip_obj.featMap[self.out_fld] = output
        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class SumDailyMoves(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(SumDailyMoves, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'float', 'dep_flds': {'self': [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('sum_daily_moves only supports 1 field at a time')
        in_fld = self.in_flds[0]
        if in_fld not in uip_obj.featMap:
            return
        input_arr = uip_obj.featMap[in_fld]
        if not input_arr:
            return
        output = [input_arr[0]]

        for pair in input_arr[1:]:
            if int(pair[0]) == int(output[-1][0]):
                output[-1][1] += pair[1]
            else:
                output.append(pair)

        if output:
            uip_obj.featMap[self.out_fld] = output
            if self.config.get('__COMPRESS__', True):
                return [self.out_fld]


class AdornWithRelativeMonth(UIPMap):
    """
    Description:
    Acts like relative period in app.py
    From a (ts, date) series it maps if the date is in the current month ('c'), a future month ('f'),
    or a historic month ('h') based on the ts. Also finds boundaries where date will become current and historic.
        Example:
        in: [Jan-15, Feb-15), (Feb-28, Mar-15)]
        out: [(Jan-15, 'f'), (Feb-1, 'c'), (Feb-28 'f'), (Mar-1, 'c'), (Apr-1, 'h')]

    Configuration:
    date_fld [OPTIONAL, default: CloseDate]: the uip field for date, must be date values.

    Sample:
    adorn_with_relative_month():rel_month
    """

    def __init__(self, feature, config=None, stage_ds=None):
        super(AdornWithRelativeMonth, self).__init__(feature, config, stage_ds)
        self.date_fld = self.config.get('date_fld', 'CloseDate')

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            self.date_fld = self.config.get('date_fld', 'CloseDate')
            out_fields = [{'name': self.out_fld, 'format': 'float', 'dep_flds': {"self": [self.date_fld]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        dates = uip_obj.featMap.get(self.date_fld, [])
        bounds = chain.from_iterable([self.make_boundaries(ts, d) for ts, d in dates])
        sorted_bounds = list(trimsort(bounds, 0, key=lambda x: (x[0], -x[2])))
        rel_months = [[date, rel_val] for i, (date, rel_val, ts) in enumerate(sorted_bounds)
                      if i in [0, len(sorted_bounds) - 1]
                      or ts >= sorted_bounds[i - 1][2]]
        uip_obj.featMap[self.out_fld] = rel_months
        return self.out_fld

    def make_boundaries(self, ts, d):
        ts, d = try_float(ts), try_float(d)
        ts_ep, d_ep = epoch(ts), epoch(d)
        delta = relativedelta.relativedelta(d_ep.as_datetime(), ts_ep.as_datetime())
        tot_diff = delta.months + (12 * delta.years)
        if delta < 0:
            return [(ts, 'h', ts)]
        if tot_diff == 0:
            return [(ts, 'c', ts), (get_future_bom(ts_ep, delta, 1).as_xldate(), 'h', ts)]
        return [(ts, 'f', ts), (get_future_bom(ts_ep, delta).as_xldate(), 'c', ts),
                (get_future_bom(ts_ep, delta, 1).as_xldate(), 'h', ts)]


class AdornWithMonths(UIPMap):
    """ Adorn with the relative month in quarter of the input field.
        For example, for a tenant with Q1: Jan-March.
        Note that this might not play nicely with tenants using more advanced features like
        custom fiscal calendars, dropped or shortened quarters etc.

        Optional Arguments:
            * time_buffer: [Default: .001] adding tiny bit of time (~1 minute) to deal with messiness of float
                    date values
    """

    def __init__(self, feature, config=None, stage_ds=None):
        super(AdornWithMonths, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'float', 'dep_flds': {"self": [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('Adorn with months only supports 1 field at a time')
        in_fld = self.in_flds[0]
        time_buffer = self.config.get('time_buffer', .001)
        if in_fld not in uip_obj.featMap:
            return
        in_series = uip_obj.featMap[in_fld]
        from numpy import searchsorted
        tenant_name = sec_context.name

        try:
            month_starts = ALL_PERIODS_CACHE[tenant_name]['month_starts']
        except:
            from utils.date_utils import get_all_periods
            from numpy import array
            month_offset = sec_context.details.get_config('forecast', 'quarter_begins')[0]
            month_starts = array([datetime2xl(x[1])
                                  for x in get_all_periods('M', [])])[12 + month_offset:]
            ALL_PERIODS_CACHE[tenant_name] = {'month_starts': month_starts}

        uip_obj.featMap[self.out_fld] = [
            (ts, (searchsorted(month_starts, float(date_val) + time_buffer, side='right') % 3) + 1)
            for (ts, date_val) in in_series]


class MoveDateValues(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(MoveDateValues, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        num_m = self.config.get('num_m', 0)
        if not num_m:
            num_m = 3 * self.config.get('num_q', 0)
        from numpy import searchsorted
        tenant_name = sec_context.name

        try:
            all_begins = ALL_PERIODS_CACHE[tenant_name]['all_begins']
            all_ends = ALL_PERIODS_CACHE[tenant_name]['all_ends']
        except:
            from utils.date_utils import get_all_periods
            from numpy import array
            all_periods = array(get_all_periods('M', []))
            all_begins = array([datetime2xl(x[1]) for x in all_periods])
            all_ends = array([datetime2xl(x[2]) for x in all_periods])
            ALL_PERIODS_CACHE[tenant_name] = {}
            ALL_PERIODS_CACHE[tenant_name]['all_begins'] = all_begins
            ALL_PERIODS_CACHE[tenant_name]['all_ends'] = all_ends

        for field in self.in_flds:
            if field not in uip_obj.featMap:
                continue
            num_pairs = len(uip_obj.featMap[field])
            for i in range(num_pairs):
                pair = uip_obj.featMap[field][i]
                a_val = pair[1]
                try:
                    a_valF = float(a_val)
                except:
                    a_valF = date_utils.datestr2xldate(a_val)

                period_index = searchsorted(all_ends, a_valF, side='left')
                try:
                    shifted_valF = all_ends[period_index + num_m] - (all_ends[period_index] - a_valF)
                    if shifted_valF < all_begins[period_index + num_m]:
                        shifted_valF = all_begins[period_index + num_m]
                    pair[1] = str(shifted_valF)
                except:
                    pass


class MoveAllTimestamps(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(MoveAllTimestamps, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        num_m = self.config.get('num_m', 0)
        if not num_m:
            num_m = 3 * self.config.get('num_q', 0)

        from numpy import searchsorted
        tenant_name = sec_context.name

        try:
            all_begins = ALL_PERIODS_CACHE[tenant_name]['all_begins']
            all_ends = ALL_PERIODS_CACHE[tenant_name]['all_ends']
        except:
            from utils.date_utils import get_all_periods
            from numpy import array
            all_periods = array(get_all_periods('M', []))
            all_begins = array([datetime2xl(x[1]) for x in all_periods])
            all_ends = array([datetime2xl(x[2]) for x in all_periods])
            ALL_PERIODS_CACHE[tenant_name] = {}
            ALL_PERIODS_CACHE[tenant_name]['all_begins'] = all_begins
            ALL_PERIODS_CACHE[tenant_name]['all_ends'] = all_ends

        for field in uip_obj.featMap:
            num_pairs = len(uip_obj.featMap[field])
            for i in range(num_pairs):
                pair = uip_obj.featMap[field][i]
                a_date = pair[0]
                try:
                    period_index = searchsorted(all_ends, a_date, side='left')
                    shifted_date = all_ends[period_index + num_m] - (all_ends[period_index] - a_date)
                    if shifted_date < all_begins[period_index + num_m]:
                        shifted_date = all_begins[period_index + num_m]
                    pair[0] = shifted_date
                except:
                    pass
        return


class ResetObjCreatedDate(UIPMap):
    ''' Resets the uip object's created date to the  adjusted creted date
    which is the smallest timestamp of adjusted stages. Run this after
    calculating the adjustedstages, especially if you have shifted the
    timestamps for demo purposes
    '''

    def __init__(self, feature, config=None, stage_ds=None):
        super(ResetObjCreatedDate, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        adjusted_created_date_fld = self.config.get('adjusted_created_date_fld', 'created_date_adj')
        uip_obj.created_date = uip_obj.featMap[adjusted_created_date_fld][-1][-1]

        return


class AdhocReplacements(UIPMap):
    ''' Used to adjust a value in a map or add a time,value at the end of the timeseries
    used for demo purposes
    example config:
    { u'list': [ [ u'0066000001SnPaw',u'Amount','last+30',6000000.0],
    [ [ u'0066000001SnPaw',u'Amount','last-30',6000000.0],
    [ u'0066000001SnPaw',u'Amount',41229,6000000.0]],
                                          'exec_rank':-3}
    '''

    def __init__(self, feature, config=None, stage_ds=None):
        super(AdhocReplacements, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            the_list = self.config['list']
            for x in the_list:
                my_field = x[1]
            out_fields = [{'name': my_field, 'format': 'N/A', 'dep_flds': {"self": my_field}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        the_list = self.config['list']
        for x in the_list:
            my_id = x[0]
            if uip_obj.ID != my_id:
                continue
            my_field = x[1]
            if str(x[2]).startswith('last'):
                x[2] = x[2][4:]
                try:
                    uip_obj.featMap[my_field].append([uip_obj.featMap[my_field][-1][0] + float(x[2]), x[3]])
                    uip_obj.featMap[my_field] = sorted(uip_obj.featMap[my_field])
                except:
                    pass
            else:
                try:
                    for y in uip_obj.featMap[my_field]:
                        if y[0] == float(x[2]):
                            y[1] = x[3]
                except:
                    pass

        if self.config.get('__COMPRESS__', True):
            return [self.out_fld]


class IDSource(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(IDSource, self).__init__(feature, config, stage_ds)

    def process(self, uip_obj):
        pass


class ActivityCounts(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(ActivityCounts, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'float', 'dep_flds': {"self": [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        low = self.config.get('Low')
        medium = self.config.get('Medium')
        days_history = self.config.get('days_history')

        if not low:
            raise Exception('\'Low\' activity measure must be set.')

        if not medium:
            raise Exception('\'Medium\' activity measure must be set.')

        if not days_history:
            days_history = 7

        if len(self.in_flds) > 1:
            raise Exception('get_activity_counts supports only one input field')
        input_fld = self.in_flds[0]
        if input_fld not in uip_obj.featMap:
            return

        lookup_history = uip_obj.featMap[input_fld]
        lookup_history = lookup_history[-1:]
        lookup_val = None
        for ts_val in lookup_history:
            _, lookup_val = ts_val

        now_date_time = now()
        end_epoch = datetime2epoch(now_date_time)
        begin_epoch = datetime2epoch(now_date_time +
                                     relativedelta.relativedelta(days=-days_history))

        account = AccountEmails.getBySpecifiedCriteria({'object.account_id':
                                                            sec_context.encrypt(lookup_val, AccountEmails)})

        activity_count = 0
        if account and account.emails:
            sorted_account_emails = sorted(account.emails,
                                           key=lambda x: x[0],
                                           reverse=True)
            for email in sorted_account_emails:
                email_ts = email[0]
                if begin_epoch <= int(email_ts) <= end_epoch:
                    activity_count += 1
                else:
                    break

        ts = epoch2xl(end_epoch)
        if activity_count >= 0 and activity_count <= int(low):
            uip_obj.featMap[self.out_fld] = [[ts, 'Low']]
        elif activity_count > int(low) and activity_count <= int(medium):
            uip_obj.featMap[self.out_fld] = [[ts, 'Medium']]
        else:
            uip_obj.featMap[self.out_fld] = [[ts, 'High']]

        if self.config.get('__COMPRESS__', True):
            return self.out_fld


class RangeFilter(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(RangeFilter, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = [{'name': self.out_fld, 'format': 'float', 'dep_flds': {'self': [self.in_flds[0]]}}]
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) > 1:
            raise Exception('range filter only supports one field at a time')
        feature = self.in_flds[0]
        orig_hist = uip_obj.featMap.get(feature, [])
        if not orig_hist:
            return

        min_val = self.config.get('min', -100000000000)
        max_val = self.config.get('max', 100000000000)
        min_replace = self.config.get('min_replace', None)
        max_replace = self.config.get('max_replace', None)
        if min_replace is not None:
            if max_replace is not None:
                conv_hist = [[ts, min_replace if val < min_val
                else val if val <= max_val else max_replace] for ts, val in orig_hist]
            else:
                conv_hist = [[ts, min_replace if val < min_val else val]
                             for ts, val in orig_hist if val <= max_val]
        else:
            if max_replace is not None:
                conv_hist = [[ts, val if val <= max_val else max_replace]
                             for ts, val in orig_hist if val >= min_val]
            else:
                conv_hist = [[ts, val] for ts, val in orig_hist if min_val <= val <= max_val]

        if not conv_hist:
            try:
                del uip_obj.featMap[self.out_fld]
            except Exception:
                pass
        else:
            uip_obj.featMap[self.out_fld] = conv_hist
            if self.config.get('__COMPRESS__', True):
                return [self.out_fld]


class RangeFilter2(UIPMap):

    def __init__(self, feature, config=None, stage_ds=None):
        super(RangeFilter2, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            fld_list = self.config['fld_list']
            for fld, dtls in fld_list.items():
                in_fld = dtls.get("in_fld", fld)
                out_fields.append({'name': fld, 'format': 'float', 'dep_flds': {'self': [in_fld]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        '''
        Sample:
            ds['maps']['range_filter2():'] = {
                u'fld_list':{
                    u'fld1' : {u'in_fld' : u'fld1', u'min':-25000000, u'max':25000000, u'min_replace':-25000000},
                    u'fld2' : {u'in_fld' : u'fld2', u'min':-25000000, u'max':25000000, u'max_replace':25000000},
                    u'fld4' : {u'in_fld' : u'fld3', u'min':-25000000, u'max':25000000, u'min_replace':-25000000,
                                u'max_replace':25000000}},
                    u'exec_rank': 4}
            fld4 is the output field which will be generated after filtering its in_fld which is fld3. The output range will be
            in between min and max. The values less than min and greater than max will be replaced by min_replace and max_replace
            respectively if they are provided else discarded.
        '''
        #         if len(self.in_flds) < 1:
        #             raise Exception('range filter requires at least one field')
        if 'fld_list' not in self.config:
            raise Exception('no fld_list specified')

        fld_list = self.config['fld_list']
        for fld in fld_list:
            if 'in_fld' not in fld_list[fld]:
                raise Exception('No field is specified to filter')
            if fld_list[fld]['in_fld'] not in uip_obj.featMap:
                continue
            feature = fld_list[fld]['in_fld']
            orig_hist = uip_obj.featMap.get(feature, [])
            min_val = fld_list[fld].get('min', -100000000000)
            max_val = fld_list[fld].get('max', 100000000000)
            min_replace = fld_list[fld].get('min_replace', None)
            max_replace = fld_list[fld].get('max_replace', None)
            if min_replace is not None:
                conv_hist = []
                if max_replace is not None:
                    conv_hist = [[ts, min_replace if val < min_val
                    else val if val <= max_val else max_replace] for ts, val in orig_hist]
                else:
                    conv_hist = [[ts, min_replace if val < min_val else val]
                                 for ts, val in orig_hist if val <= max_val]
            else:
                if max_replace is not None:
                    conv_hist = [[ts, val if val <= max_val else max_replace]
                                 for ts, val in orig_hist if val >= min_val]
                else:
                    conv_hist = [[ts, val] for ts, val in orig_hist if min_val <= val <= max_val]

            if not conv_hist:
                try:
                    del uip_obj.featMap[fld]
                except Exception:
                    pass
            else:
                uip_obj.featMap[fld] = conv_hist

        if self.config.get('__COMPRESS__', True):
            return fld_list.keys()


class CalulateCloseDatePushes(UIPMap):
    def __init__(self, feature, config=None, stage_ds=None):
        super(CalulateCloseDatePushes, self).__init__(feature, config, stage_ds)

    def get_map_details(self, output_fields_only=False):
        if output_fields_only:
            out_fields = []
            base_out_fld = 'CloseDate'
            pushes_name = []
            if self.config.get(u'calculate_push_count', True):
                pushes_name.extend([u'total_pushes', u'total_pulls', u'net_pushes'])
            if self.config.get(u'calculate_total_delay', True):
                pushes_name.append(u'total_delay')
            if self.config.get(u'calculate_quarters_pushed', True):
                pushes_name.append(u'months_pushed')
            if self.config.get(u'calculate_months_pushed', True):
                pushes_name.append(u'quarters_pushed')
            for push_name in pushes_name:
                out_fld = '{}_{}'.format(base_out_fld, push_name)
                out_fields.append({'name': out_fld, 'format': 'float', 'dep_flds': {"self": [self.in_flds[0]]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        '''
        Sample:
            ds['maps']['calculate_closedate_pushes(CloseDate):dummy'] = {         # Date field for which pushes
                                                                                                 needs to be calculated
                u'calculate_push_count': True,                  # Optional, Default = True
                u'calculate_total_delay': True,                   # Optional, Default = True
                u'calculate_quarters_pushed': True,               # Optional, Default = True
                u'calculate_months_pushed': True,                 # Optional, Default = True
                u'close_date_threshold_year_high': None,          # Optional, Default = None
                u'close_date_threshold_year_low': None,           # Optional, Default = None
                u'exec_rank': 3}
        '''
        if len(self.in_flds) != 1:
            raise Exception('calculate_closedate_pushes only supports 1 date field at a time')

        closedate_fld = self.in_flds[0]
        #         stage_field_name = self.config.get(u'stage_field_name', u'Stage')
        base_out_fld = 'CloseDate'

        MAX_YEAR, MIN_YEAR = 2050, 0
        cd_threshold_year_high = self.config.get(u'close_date_threshold_year_high', MAX_YEAR) or MAX_YEAR
        cd_threshold_year_low = self.config.get(u'close_date_threshold_year_low', MIN_YEAR) or MIN_YEAR

        modified_fields = []
        if closedate_fld not in uip_obj.featMap:
            return

        close_date_list = [[ts, try_float(cd)]
                           for [ts, cd] in uip_obj.featMap[closedate_fld]
                           if cd_threshold_year_low < xl2datetime(try_float(cd)).year < cd_threshold_year_high
                           and xl2datetime(try_float(ts)).year < cd_threshold_year_high]

        if not close_date_list:
            return

        if self.config.get(u'calculate_push_count', True):
            total_pushes_name = '{}_{}'.format(base_out_fld, 'total_pushes')
            total_pulls_name = '{}_{}'.format(base_out_fld, 'total_pulls')
            net_pushes_name = '{}_{}'.format(base_out_fld, 'net_pushes')

            push_count, pull_count, net_count = 0, 0, 0
            total_pushes_val = [[close_date_list[0][0], 0]]
            total_pulls_val = [[close_date_list[0][0], 0]]
            net_pushes_val = [[close_date_list[0][0], 0]]

            for i, [ts, cd] in enumerate(close_date_list[:-1]):
                if cd < close_date_list[i + 1][1]:
                    push_count += 1
                    net_count += 1
                    total_pushes_val.append([close_date_list[i + 1][0], push_count])
                    net_pushes_val.append([close_date_list[i + 1][0], net_count])
                else:
                    pull_count += 1
                    net_count -= 1
                    total_pulls_val.append([close_date_list[i + 1][0], pull_count])
                    net_pushes_val.append([close_date_list[i + 1][0], net_count])

            uip_obj.featMap.update({
                total_pushes_name: total_pushes_val,
                total_pulls_name: total_pulls_val,
                net_pushes_name: net_pushes_val
            })
            modified_fields += [total_pushes_name, total_pulls_name, net_pushes_name]

        if self.config.get(u'calculate_total_delay', True):
            new_fld_name = '{}_{}'.format(base_out_fld, 'total_delay')
            new_fld_val = [[close_date_list[0][0], 0]]
            first_cd = xl2datetime(close_date_list[0][1])
            for (ts, cd) in close_date_list[1:]:
                num_days_delay = (xl2datetime(cd) - first_cd).days

                new_fld_val.append([ts, num_days_delay])
            uip_obj.featMap[new_fld_name] = new_fld_val
            modified_fields.append(new_fld_name)

        if self.config.get(u'calculate_months_pushed', True):
            dt1 = xl2datetime(close_date_list[0][1])

            new_fld_name = '{}_{}'.format(base_out_fld, 'months_pushed')
            new_fld_val = [[close_date_list[0][0], 0]]

            for (ts, cd) in close_date_list[1:]:
                dt2 = xl2datetime(cd)
                months_pushed = (dt2.year - dt1.year) * 12 + dt2.month - dt1.month
                new_fld_val.append([ts, months_pushed])

            uip_obj.featMap[new_fld_name] = new_fld_val
            modified_fields.append(new_fld_name)

        if self.config.get(u'calculate_quarters_pushed', True):
            new_fld_name = '{}_{}'.format(base_out_fld, 'quarters_pushed')

            new_fld_val = [[close_date_list[0][0], 0]]

            dt1 = xl2datetime(close_date_list[0][1])
            mnemonic1 = date_utils.period_details(dt1).mnemonic
            [year1, quarter1] = map(int, mnemonic1.split('Q'))

            for (ts, cd) in close_date_list[1:]:
                try:
                    dt2 = xl2datetime(cd)
                    mnemonic2 = date_utils.period_details(dt2).mnemonic
                    [year2, quarter2] = map(int, mnemonic2.split('Q'))
                    quarters_pushed = (year2 - year1) * 4 + quarter2 - quarter1
                    new_fld_val.append([ts, quarters_pushed])
                except Exception as e:
                    logger.warning(e)
                    logger.warning('close date map bad data: %s', uip_obj.featMap)

            uip_obj.featMap[new_fld_name] = new_fld_val
            modified_fields.append(new_fld_name)

        if self.config.get('__COMPRESS__', True):
            return modified_fields


class ChangeCount(UIPMap):
    """
    Counts the number of changes in a deal's field over its lifetime.

    Sample Configuration:
    ds['maps']['change_count(Amount):dummy'] = {
        'changes': ['increases', 'decreases', 'changes'],
        'ignore_threshold': 1 (in hours),
        'exec_rank': 3
    }

    This configuration would create the following fields:
    - Amount_increases: number of times the Amount field increased
    - Amount_decreases: number of times the Amount field decreased
    - Amount_changes: number of times the Amount field changed
    """

    def get_map_details(self, output_fields_only=False):
        change_types = ['increases', 'decreases', 'changes']
        if output_fields_only:
            out_fields = []
            for change in change_types:
                in_fld = self.in_flds[0]
                out_fields.append({'name': in_fld + '_' + change, 'format': "number", 'dep_flds': {"self": [in_fld]}})
            return out_fields
        else:
            return None

    def process(self, uip_obj):
        if len(self.in_flds) != 1:
            raise Exception('ChangeCount only supports 1 field at a time')

        fld = self.in_flds[0]

        if fld not in uip_obj.featMap:
            return

        fld_recs = uip_obj.featMap.get(fld)
        change_types = ['increases', 'decreases', 'changes']

        # initialize dict to store recs
        increase_ct, decrease_ct, change_ct = 0, 0, 0
        start_list = [[fld_recs[0][0], 0]]

        # contains info about the amount changes
        change_dict = {change_type: list(start_list) for change_type in change_types}

        # store this as we iterate through the recs
        prev_val = fld_recs[0][1]
        prev_ts = fld_recs[0][0]

        for idx, [ts, val] in enumerate(fld_recs[1:]):
            if ts < prev_ts + self.config.get('ignore_threshold', 1) / 24.0:
                continue
            if val > prev_val:
                increase_ct += 1
                change_dict['increases'].append([fld_recs[idx + 1][0], increase_ct])
            elif val < prev_val:
                decrease_ct += 1
                change_dict['decreases'].append([fld_recs[idx + 1][0], decrease_ct])
            change_ct += 1
            change_dict['changes'].append([fld_recs[idx + 1][0], change_ct])

            prev_ts = ts
            prev_val = val

        new_uip_recs = {
            fld + '_' + change: dtls for change, dtls in change_dict.items() if
            change in self.config.get('changes', change_types)
        }

        uip_obj.featMap.update(new_uip_recs)
        modified_fields = new_uip_recs.keys()

        if self.config.get('__COMPRESS__', True):
            return modified_fields
