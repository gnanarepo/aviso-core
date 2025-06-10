import copy
import csv
import logging
import threading
from collections import OrderedDict
from datetime import datetime, timedelta
from io import BytesIO
from itertools import product
from multiprocessing.pool import ThreadPool

import boto3
import pytz
from aviso.settings import sec_context
from date_utils import prev_periods_allowed_in_deals

from config.fm_config import DEFAULT_ROLE, FMConfig
from config.periods_config import PeriodsConfig
from infra import (ADMIN_MAPPING, CRM_SCHEDULE, EDW_DATA, EDW_PROCESS_UPDATE,
                   EXPORT_ALL, FM_COLL, FM_LATEST_COLL, FM_LATEST_DATA_COLL,
                   FORECAST_SCHEDULE_COLL, FORECAST_UNLOCK_REQUESTS,
                   GBM_CRR_COLL, MOBILE_SNAPSHOT_COLL,
                   MOBILE_SNAPSHOT_ROLE_COLL, NEXTQ_COLL, SNAPSHOT_COLL,
                   SNAPSHOT_HIST_COLL, SNAPSHOT_HIST_ROLE_COLL,
                   SNAPSHOT_ROLE_COLL, USER_LEVEL_SCHEDULE, WATERFALL_COLL,
                   WATERFALL_HISTORY_COLL, WEEKLY_EDW_DATA,
                   WEEKLY_EDW_PROCESS_START_TIME, WEEKLY_EDW_PROCESS_STATUS,
                   WEEKLY_FORECAST_EXPORT_ALL, WEEKLY_FORECAST_EXPORT_COLL,
                   WEEKLY_FORECAST_FM_COLL)
from infra.read import (fetch_boundry, fetch_children, fetch_crr_deal_totals,
                        fetch_deal_totals, fetch_descendants,
                        fetch_eligible_nodes_for_segment, fetch_labels,
                        fetch_many_dr_from_deals, fetch_prnt_DR_deal_totals,
                        find_all_subtree_height,
                        find_map_in_nodes_and_lth_grand_children,
                        get_available_quarters_and_months, get_now,
                        get_period_and_close_periods,
                        get_period_and_component_periods, get_period_as_of,
                        get_period_begin_end, get_periods_editable,
                        get_quarter_period, get_time_context,
                        get_waterfall_weekly_totals, render_period)
from infra.read import time_context as time_context_tuple
from infra.rules import node_aligns_to_segment
from utils.date_utils import (EpochClass, datetime2epoch, epoch,
                              epoch2datetime, get_all_periods__m, get_eod,
                              get_eom, get_eoq_for_period, is_same_day,
                              monthly_periods, weekly_periods, xl2datetime_ttz)
from utils.misc_utils import get_nested, try_float

logger = logging.getLogger('gnana.%s' % __name__)


s3 = boto3.client('s3')
ONE_DAY = 24 * 60 * 60 * 1000
ONE_HOUR = 60 * 60 * 1000
MAX_THREADS = 200
SIX_HOURS = 6 * 60 * 60 * 1000


def read_from_collection(year, timestamp=None, field_type=None, call_from=None, config=None,
                         year_accepted_from=None):
    if timestamp is not None:
        return FM_COLL
    fm_config = config if config else FMConfig()
    read_from_latest_collection = fm_config.read_from_latest_collection
    if year_accepted_from is None:
        try:
            t = sec_context.details
            year_accepted_from = int(t.get_flag('fm_latest_migration', 'year', 0))
        except:
            year_accepted_from = 0
    coll = FM_COLL
    if year_accepted_from != 0 and read_from_latest_collection and int(year) >= year_accepted_from:
        if field_type is not None and field_type in ['DR', 'PrntDR']:
            coll = FM_LATEST_COLL
        else:
            coll = FM_LATEST_DATA_COLL
    return coll


# not in use please verify
def bulk_fetch_fm_recs(time_context,
                       descendants_list,
                       fields,
                       segments,
                       config,
                       eligible_nodes_for_segs,
                       timestamp=None,
                       recency_window=None,
                       db=None,
                       ):
    """
    fetch fm records from db for multiple nodes/fields
    optimized for fetching for many nodes and fields at once
    it may turn out this is faster in all cases than regular fetch_fm_recs
    in which case we should just delete that function and always use this one
    you are 100% guaranteed to get records for all the params you request

    Arguments:
        time_context {time_context} -- fm period, components of fm period
                                       deal period, close periods of deal period
                                       deal expiration timestamp,
                                       relative periods of component periods
                                       ('2020Q2', ['2020Q2'],
                                        '2020Q2', ['201908', '201909', '201910'],
                                        1556074024910,
                                        ['h', 'c', 'f'])
        descendants_list {list} -- list of tuples of
                                   (node, [children], [grandchildren])
                                    fetches data for each node
                                    using children/grandkids to compute sums
                                    [('A', ['B, 'C'], ['D', 'E'])]
        fields {list} -- list of field names
                         ['commit', 'best_case']
        segments {list} -- list of segment names
                          ['all_deals', 'new', 'upsell']
        config {FMConfig} -- instance of FMConfig

    Keyword Arguments:
        timestamp {int} -- epoch timestamp to get data as of (default: {None})
                           if None, gets most recent record
                           1556074024910
        recency_window {int} -- window to reject data from before timestamp - recency_window  (default: {None})
                                if None, will accept any record, regardless of how stale
                                ONE_DAY
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        dict -- {(period, node, field, segment, timestamp): {'period': period,
                                                             'val': val,
                                                             'by': by,
                                                             'how': how,
                                                             'found': found,
                                                             'timestamp': timestamp,
                                                             'node': node,
                                                             'field': field}}
    """
    threads, cache = [], {}
    db = db if db else sec_context.tenant_db

    if timestamp:
        dlf_ts, deal_ts = 0, 0
    else:
        period = time_context.deal_period if not isinstance(time_context.deal_period, list) else time_context.fm_period
        dlf_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
        deal_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))

    if config.debug:
        for descendants, field, segment in product(descendants_list, fields, segments):
                if segment == 'all_deals' or descendants[0] in eligible_nodes_for_segs.get(segment, []):
                    fetch_fm_rec(time_context,
                                 descendants,
                                 field,
                                 segment,
                                 config,
                                 (timestamp, dlf_ts, deal_ts, timestamp - recency_window if recency_window else None),
                                 db,
                                 cache)
    else:
        # instead of spinning up a million threads and waiting a bunch, make a pool of 200 and cycle through em
        # theres some threshold of how many is too many threads where we wind up waiting too much
        # from my expirementing locally it looks like we only hit it on this bulk fetch for exports and not on the snapshots
        # but if that changes, may want to take this approach in fetch_fm_recs and fetch_fm_recs_history
        # NOTE: if this ever gets upgraded to python3, we should really be using concurrent.futures ThreadPoolExecutor, not this
        count = 0
        actual_count = 0
        pool = ThreadPool(MAX_THREADS)
        fm_rec_params = []
        for descendants, field, segment in product(descendants_list, fields, segments):
                count += 1
                if segment == 'all_deals' or descendants[0] in eligible_nodes_for_segs.get(segment, []):
                    actual_count += 1
                    fm_rec_params.append((time_context,
                                  descendants,
                                  field,
                                  segment,
                                  config,
                                  (timestamp, dlf_ts, deal_ts, timestamp - recency_window if recency_window else None),
                                  db,
                                  cache))
        pool.map(_fetch_fm_rec, tuple(fm_rec_params))
        pool.close()
        pool.join()
        logger.info('actual comibnations: %s, threads executed count : %s', count, actual_count)

    return cache


def bulk_fetch_recs_by_timestamp(time_context,
                                 descendants,
                                 fields,
                                 segments,
                                 config,
                                 eligible_nodes_for_segs,
                                 timestamps,
                                 db=None,
                                 round_val=False,
                                 node=None,
                                 is_pivot_special=False,
                                 call_from=None,
                                 time_context_list = [],
                                 recalc_UE_fields = [],
                                 recalc_DR_fields = []
                                 ):
    fm_recs = {}
    for timestamp in timestamps:
        fm_recs.update(bulk_fetch_recs_for_data_export(time_context,
                                                       descendants,
                                                       fields,
                                                       segments,
                                                       config,
                                                       eligible_nodes_for_segs,
                                                       timestamp,
                                                       db,
                                                       round_val=round_val,
                                                       is_pivot_special=is_pivot_special,
                                                       call_from=call_from,
                                                       time_context_list = time_context_list,
                                                       recalc_UE_fields = recalc_UE_fields,
                                                       recalc_DR_fields = recalc_DR_fields
                                                       ))
    unique_fields = set(fields)
    for timestamp, field, segment, descendant in product(timestamps, unique_fields, segments, descendants):
        field_type = config.fields[field]['type']
        node, children, _ = descendant
        if field_type == 'NC':
            mgr_field, rep_field = config.fields[field]['source']
            val = {}
            if children:
                val = fm_recs.get((time_context.fm_period, node, mgr_field, segment, timestamp))
            else:
                val = fm_recs.get((time_context.fm_period, node, rep_field, segment, timestamp))
            if val:
                fm_recs[(time_context.fm_period, node, field, segment, timestamp)] = val


    return fm_recs

def bulk_fetch_recs_by_timestamp_cq(time_context,
                                 descendants,
                                 fields,
                                 segments,
                                 config,
                                 eligible_nodes_for_segs,
                                 timestamps,
                                 db=None,
                                 round_val=False,
                                 node=None
                                 ):
    fm_recs = {}
    #for timestamp in timestamps:
    fm_recs.update(bulk_fetch_recs_for_data_export_new(time_context,
                                                       descendants,
                                                       fields,
                                                       segments,
                                                       config,
                                                       eligible_nodes_for_segs,
                                                       timestamps,
                                                       db,
                                                       round_val=round_val,
                                                       node=node))
    unique_fields = set(fields)
    for timestamp, field, segment, descendant in product(timestamps, unique_fields, segments, descendants):
        field_type = config.fields[field]['type']
        node, children, _ = descendant
        if field_type == 'NC':
            mgr_field, rep_field = config.fields[field]['source']
            val = {}
            if children:
                val = fm_recs.get((time_context.fm_period, node, mgr_field, segment, timestamp))
            else:
                val = fm_recs.get((time_context.fm_period, node, rep_field, segment, timestamp))
            if val:
                fm_recs[(time_context.fm_period, node, field, segment, timestamp)] = val


    return fm_recs

def get_fields_by_type(fields, config, time_context, special_cs_fields=[]):
    fields_by_type = {}
    for field in set(fields):
        if 'hist_field' in config.fields[field] and 'h' in time_context.relative_periods:
            # historic period, switch to using true up field if it exists
            field_type = 'PC'
            if config.quarter_editable:
                if not all(elem == 'h' for elem in time_context.relative_periods):
                    field_type = config.fields[field]['type']
        else:
            field_type = config.fields[field]['type']
        if field_type == 'NC':
            mgr_field, rep_field = config.fields[field]['source']

            mgr_field_type = config.fields[mgr_field]['type']
            rep_field_type = config.fields[rep_field]['type']

            if mgr_field_type not in fields_by_type:
                fields_by_type[mgr_field_type] = []
            fields_by_type[mgr_field_type].append(mgr_field)
            if mgr_field_type == 'CS':
                source_fields = config.fields[mgr_field]['source']
                eager_fields_by_type, special_cs_fields = get_fields_by_type(source_fields, config, time_context, special_cs_fields)
                for type in eager_fields_by_type:
                    if type not in fields_by_type:
                        fields_by_type[type] = []
                    fields_by_type[type].extend(eager_fields_by_type[type])

            if rep_field_type not in fields_by_type:
                fields_by_type[rep_field_type] = []
            fields_by_type[rep_field_type].append(rep_field)
            if rep_field_type == 'CS':
                source_fields = config.fields[rep_field]['source']
                eager_fields_by_type, special_cs_fields = get_fields_by_type(source_fields, config, time_context, special_cs_fields)
                for type in eager_fields_by_type:
                    if type not in fields_by_type:
                        fields_by_type[type] = []
                    fields_by_type[type].extend(eager_fields_by_type[type])

        elif field_type == 'CS':
            source_fields = config.fields[field]['source']
            field_type_ = config.fields[source_fields[0]]['type']
            special_case = False
            if 'hist_field' in config.fields[source_fields[0]] and 'h' in time_context.relative_periods:
                # historic period, switch to using true up field if it exists
                field_type_ = 'PC'
                if config.quarter_editable:
                    if not all(elem == 'h' for elem in time_context.relative_periods):
                        field_type_ = config.fields[source_fields[0]]['type']
            if field_type_ == 'NC':
                mgr_field, rep_field = config.fields[source_fields[0]]['source']
                if config.fields[mgr_field]['type'] == 'CS' or config.fields[rep_field]['type'] == 'CS':
                    special_cs_fields.append(field)
                    special_case = True
            elif field_type_ == 'PC':
                special_cs_fields.append(field)
                special_case = True

            if not special_case:
                if field_type not in fields_by_type:
                    fields_by_type[field_type] = []
                fields_by_type[field_type].append(field)
                eager_fields_by_type, special_cs_fields = get_fields_by_type(source_fields, config, time_context, special_cs_fields)
                for type in eager_fields_by_type:
                    if type not in fields_by_type:
                        fields_by_type[type] = []
                    fields_by_type[type].extend(eager_fields_by_type[type])
        else:
            if field_type not in fields_by_type:
                fields_by_type[field_type] = []
            fields_by_type[field_type].append(field)
    return fields_by_type, special_cs_fields


def bulk_fetch_recs_for_data_export_new(time_context,
                                    descendants,
                                    fields,
                                    segments,
                                    config,
                                    eligible_nodes_for_segs,
                                    timestamps=None,
                                    db=None,
                                    round_val=True,
                                    node=None
                                    ):
    cache = {}

    db = db if db else sec_context.tenant_db

    '''if timestamp and round_val:
        dlf_ts, deal_ts = 0, 0
    else:
        period = time_context.deal_period if not isinstance(time_context.deal_period, list) else time_context.fm_period
        dlf_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
        deal_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))'''

    special_cs_fields = []
    fields_by_type, special_cs_fields = get_fields_by_type(fields, config, time_context, special_cs_fields)

    eager_retrieval_fields = []
    if 'FN' in fields_by_type:
        for fn_field in fields_by_type['FN']:
            eager_retrieval_fields.extend(config.fields[fn_field]['source'])

    eager_fields_by_type, special_cs_fields = get_fields_by_type(eager_retrieval_fields, config, time_context, special_cs_fields)

    for type in eager_fields_by_type:
        if type not in fields_by_type:
            fields_by_type[type] = []
        fields_by_type[type].extend(eager_fields_by_type[type])

    for type, fields in fields_by_type.items():
        fields_by_type[type] = list(set(fields))

    # AV-14059 Commenting below as part of the log fix
    # logger.info("fields for bulk data fetch are %s" % fields_by_type)

    if len(segments) == 1 and 'all_deals' in segments and config.has_segments:
        segments = config.segments

    segments = [segment for segment in segments]

    if any([segment in config.FN_segments for segment in segments]):
        segments += add_FN_dependent_segments(segments, config, [])
        segments = list(set(segments))

    '''timestamp_info = (timestamp, dlf_ts, deal_ts,None)'''

    threads = 0
    pool = ThreadPool(MAX_THREADS)

    if 'UE' in fields_by_type:
        UE_params = []
        for timestamp in timestamps:
            if timestamp and round_val:
                dlf_ts, deal_ts = 0, 0
            else:
                period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                    list) else time_context.fm_period
                dlf_ts = try_float(
                    sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                deal_ts = try_float(
                    sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
            timestamp_info = (timestamp, dlf_ts, deal_ts, None)
            UE_params.append((time_context,
                          descendants,
                          fields_by_type['UE'],
                          segments,
                          config,
                          timestamp_info,
                          db,
                          cache,
                          True,
                          round_val))
            threads += 1
        pool.map(_bulk_fetch_user_entered_recs_pool, tuple(UE_params))

    if 'DR' in fields_by_type:
        DR_params = []
        config.debug = True
        try:
            dr_from_fm_coll_for_snapshot = sec_context.details.get_flag('deal_rollup', 'snapshot_dr_from_fm_coll',
                                                                        False)
        except:
            dr_from_fm_coll_for_snapshot = False
        if config.deal_config.segment_field or not config.has_segments or dr_from_fm_coll_for_snapshot:
            for timestamp in timestamps:
                if timestamp and round_val:
                    dlf_ts, deal_ts = 0, 0
                else:
                    period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                        list) else time_context.fm_period
                    dlf_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                    deal_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
                timestamp_info = (timestamp, dlf_ts, deal_ts, None)
                DR_params.append((config,
                         fields_by_type['DR'],
                         time_context,
                         descendants,
                         segments,
                         timestamp_info,
                         db,
                         cache,
                         round_val,
                         timestamp,
                         eligible_nodes_for_segs,
                         True,
                         True
                         ))
                threads += 1
            pool.map(handle_dr_fields_pool, tuple(DR_params))
        elif not config.deal_config.segment_field and config.has_segments:
            config.debug = True
            for field, segment, descendant, timestamp in product(fields_by_type['DR'], segments, descendants, timestamps):
                if segment == 'all_deals' or descendants[0] in eligible_nodes_for_segs.get(segment, []):
                    if timestamp and round_val:
                        dlf_ts, deal_ts = 0, 0
                    else:
                        period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                            list) else time_context.fm_period
                        dlf_ts = try_float(
                            sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                        deal_ts = try_float(
                            sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
                    DR_params.append((time_context,
                             descendant,
                             field,
                             segment,
                             config,
                             (timestamp, dlf_ts, deal_ts, None),
                             db,
                             cache))
                    threads += 1
            pool.map(_fetch_fm_rec, tuple(DR_params))

    if 'print_DR' in fields_by_type:
        print_DR_params = []
        for timestamp in timestamps:
            if timestamp and round_val:
                dlf_ts, deal_ts = 0, 0
            else:
                period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                    list) else time_context.fm_period
                dlf_ts = try_float(
                    sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                deal_ts = try_float(
                    sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
            timestamp_info = (timestamp, dlf_ts, deal_ts, None)
            print_DR_params.append((config,
                                 fields_by_type['prnt_DR'],
                                 time_context,
                                 descendants,
                                 segments,
                                 timestamp_info,
                                 db,
                                 cache,
                                 timestamp,
                                 eligible_nodes_for_segs,
                                 round_val,
                                 True))
            threads += 1
        pool.map(_bulk_fetch_prnt_dr_recs_pool, tuple(print_DR_params))

    if 'AI' in fields_by_type:
        AI_params = []
        for timestamp in timestamps:
            if timestamp and round_val:
                dlf_ts, deal_ts = 0, 0
            else:
                period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                    list) else time_context.fm_period
                dlf_ts = try_float(
                    sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                deal_ts = try_float(
                    sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
            timestamp_info = (timestamp, dlf_ts, deal_ts, None)
            AI_params.append((time_context,
                            descendants,
                            fields_by_type['AI'],
                            segments,
                            config,
                            timestamp_info,
                            db,
                            cache,
                            False,
                            round_val))
            threads += 1
        pool.map(_bulk_fetch_ai_recs_pool, tuple(AI_params))

    descendants_by_segments = {}
    rollup_segments = config.rollup_segments
    for segment in segments:
        descendants_list = []
        if segment == 'all_deals':
            descendants_list = descendants
        else:
            for node in descendants:
                if node[0] in eligible_nodes_for_segs.get(segment, []):
                    descendants_list.append(node)
        descendants_by_segments[segment] = descendants_list
        if 'PC' in fields_by_type:
            PC_params = []
            for timestamp in timestamps:
                if timestamp and round_val:
                    dlf_ts, deal_ts = 0, 0
                else:
                    period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                        list) else time_context.fm_period
                    dlf_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                    deal_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
                timestamp_info = (timestamp, dlf_ts, deal_ts, None)
                PC_params.append((time_context,
                                      descendants_list,
                                      fields_by_type['PC'],
                                      segment,
                                      config,
                                      timestamp_info,
                                      db,
                                      cache,
                                      round_val))
                threads += 1
            pool.map(_bulk_period_conditional_rec_pool, tuple(PC_params))

    for segment in segments:
        descendants_list = descendants_by_segments[segment]
        if 'FN' in fields_by_type:
            FN_params = []
            for timestamp in timestamps:
                if timestamp and round_val:
                    dlf_ts, deal_ts = 0, 0
                else:
                    period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                        list) else time_context.fm_period
                    dlf_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                    deal_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
                timestamp_info = (timestamp, dlf_ts, deal_ts, None)
                FN_params.append((time_context,
                                  descendants_list,
                                  fields_by_type['FN'],
                                  segment,
                                  config,
                                  timestamp_info,
                                  db,
                                  cache))
                threads += 1
            pool.map(_bulk_fetch_formula_recs_pool, tuple(FN_params))

    if 'CS' in fields_by_type:
        CS_derived_fields, _ = get_fields_by_type(fields_by_type['CS'], config, time_context)
        CS_derived_UE_fields = CS_derived_fields.get('UE', [])
        CS_derived_DR_fields = CS_derived_fields.get('DR', [])
        CS_derived_FN_fields = CS_derived_fields.get('FN', [])
        descendants_list = descendants
        child_descendants = []
        for descendant in descendants_list:
            node, children, grandchildren = descendant
            for child in children:
                grandkids = {grandkid: parent for grandkid, parent in grandchildren.iteritems()
                    if parent == child}
                child_descendant = (child, grandkids, {})
                child_descendants.append(child_descendant)
                for grandkid in grandkids:
                    grandkid_descendant = (grandkid, {}, {})
                    child_descendants.append(grandkid_descendant)
        cs_cache = {}
        CS_UE_params = []
        for timestamp in timestamps:
            if timestamp and round_val:
                dlf_ts, deal_ts = 0, 0
            else:
                period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                    list) else time_context.fm_period
                dlf_ts = try_float(
                    sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                deal_ts = try_float(
                    sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
            timestamp_info = (timestamp, dlf_ts, deal_ts, None)
            CS_UE_params.append((time_context,
                                      child_descendants,
                                      CS_derived_UE_fields,
                                      segments,
                                      config,
                                      timestamp_info,
                                      db,
                                      cs_cache,
                                      True,
                                      round_val))
            threads += 1
        pool.map(_bulk_fetch_user_entered_recs_pool, tuple(CS_UE_params))
        if CS_derived_DR_fields:
            CS_DR_params = []
            for timestamp in timestamps:
                if timestamp and round_val:
                    dlf_ts, deal_ts = 0, 0
                else:
                    period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                        list) else time_context.fm_period
                    dlf_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                    deal_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
                timestamp_info = (timestamp, dlf_ts, deal_ts, None)
                config.debug = True
                CS_DR_params.append((config,
                             CS_derived_DR_fields,
                             time_context,
                             child_descendants,
                             segments,
                             timestamp_info,
                             db,
                             cs_cache,
                             round_val,
                             timestamp,
                             eligible_nodes_for_segs,
                             True,
                             True
                             ))
                threads += 1
            pool.map(handle_dr_fields_pool, tuple(CS_DR_params))
        if CS_derived_FN_fields:
            CS_FN_params = []
            for segment in segments:
                for timestamp in timestamps:
                    if timestamp and round_val:
                        dlf_ts, deal_ts = 0, 0
                    else:
                        period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                            list) else time_context.fm_period
                        dlf_ts = try_float(
                            sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                        deal_ts = try_float(
                            sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
                    timestamp_info = (timestamp, dlf_ts, deal_ts, None)
                    CS_FN_params.append((time_context,
                                         child_descendants,
                                         CS_derived_FN_fields,
                                         segment,
                                         config,
                                         timestamp_info,
                                         db,
                                         cs_cache))
                    threads += 1
            pool.map(_bulk_fetch_formula_recs_pool, tuple(CS_FN_params))

        for segment in segments:
            descendants_list = descendants_by_segments[segment]
            if special_cs_fields:
                for timestamp in timestamps:
                    if timestamp and round_val:
                        dlf_ts, deal_ts = 0, 0
                    else:
                        period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                            list) else time_context.fm_period
                        dlf_ts = try_float(
                            sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                        deal_ts = try_float(
                            sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
                    timestamp_info = (timestamp, dlf_ts, deal_ts, None)
                    _bulk_fetch_child_sum_rec_special(time_context,
                                          descendants_list,
                                          special_cs_fields,
                                          segment,
                                          config,
                                          timestamp_info,
                                          db,
                                          cache,
                                          round_val=round_val)
        for segment in segments:
            descendants_list = descendants_by_segments[segment]
            for timestamp in timestamps:
                if timestamp and round_val:
                    dlf_ts, deal_ts = 0, 0
                else:
                    period = time_context.deal_period if not isinstance(time_context.deal_period,
                                                                        list) else time_context.fm_period
                    dlf_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
                    deal_ts = try_float(
                        sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
                timestamp_info = (timestamp, dlf_ts, deal_ts, None)
                cache.update(_bulk_fetch_child_sum_rec(time_context,
                                      child_descendants,
                                      fields_by_type['CS'],
                                      segment,
                                      config,
                                      timestamp_info,
                                      db,
                                      cs_cache,
                                      round_val=round_val))

                cache.update(_bulk_fetch_child_sum_rec(time_context,
                                      descendants_list,
                                      fields_by_type['CS'],
                                      segment,
                                      config,
                                      timestamp_info,
                                      db,
                                      cs_cache,
                                      round_val=round_val))
        if len(config.segments) > 1:
            by_val = "system"
            latest_timestamp = 0
            period, comp_periods = time_context.fm_period, time_context.submission_component_periods
            cs_field_with_fn_source=[]
            for field in fields_by_type['CS']:
                source_field = config.fields[field]['source'][0]
                if config.fields[source_field]['type'] == 'FN':
                    cs_field_with_fn_source.append(field)
            cs_field_without_fn_source = [x for x in fields_by_type['CS'] if x not in cs_field_with_fn_source]
            for descendants, field in product(descendants_list, cs_field_without_fn_source):
                node, children, grandchildren = descendants
                val = 0
                for segment in segments:
                    if segment != "all_deals":
                        ch_key = (period, node, field, segment, timestamp)
                        if segment in rollup_segments:
                            val += try_float(get_nested(cache, [ch_key, 'val'], 0))
                        seg_timestamp = get_nested(cache, [ch_key, 'timestamp'], 0)
                        if seg_timestamp > latest_timestamp:
                            latest_timestamp = seg_timestamp
                            by_val = get_nested(cache, [ch_key, 'by'], "system")
                res = {'period': period,
                       'segment': config.primary_segment,
                       'val': val,
                       'by': by_val,
                       'how': 'sum_of_children',
                       'found': True if val else False,
                       'timestamp': latest_timestamp,
                       'node': node,
                       'field': field}
                if cache is not None:
                    cache[(period, node, field, config.primary_segment, timestamp)] = res
    pool.close()
    pool.join()

    for timestamp in timestamps:
        if timestamp and round_val:
            dlf_ts, deal_ts = 0, 0
        else:
            period = time_context.deal_period if not isinstance(time_context.deal_period, list) else time_context.fm_period
            dlf_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
            deal_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))

        timestamp_info = (timestamp, dlf_ts, deal_ts,None)
        all_fields = set([])
        for _, type_fields in fields_by_type.items():
            all_fields.update(type_fields)
        populate_FN_segments(time_context,
                            all_fields,
                            config.FN_segments,
                            descendants_by_segments,
                            cache,
                            timestamp,
                            [],
                            config,
                            fields_by_type,
                            timestamp_info)

    return cache

def bulk_fetch_recs_for_data_export_only_UE(time_context,
                                    descendants,
                                    fields,
                                    segments,
                                    config,
                                    eligible_nodes_for_segs,
                                    timestamp=None,
                                    db=None,
                                    round_val=True,
                                    node=None,
                                    includefuturetimestamp=False,
                                    is_pivot_special=False,
                                    call_from=None,
                                    time_context_list = [],
                                    exclude_empty = None,
                                    updated_since = None,
                                    skip=None,
                                    limit=None):
    cache = {}

    db = db if db else sec_context.tenant_db

    if timestamp and round_val:
        dlf_ts, deal_ts = 0, 0
    else:
        period = time_context.deal_period if not isinstance(time_context.deal_period, list) else time_context.fm_period
        dlf_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
        deal_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))

    special_cs_fields = []
    fields_by_type, special_cs_fields = get_fields_by_type(fields, config, time_context, special_cs_fields)

    is_special_pivot_segmented = is_pivot_special
    if node:
        is_special_pivot_segmented = config.is_special_pivot_segmented(node)
    if len(segments) == 1 and 'all_deals' in segments and config.has_segments and is_special_pivot_segmented:
        segments = config.segments

    segments = [segment for segment in segments]

    if any([segment in config.FN_segments for segment in segments]):
        segments += add_FN_dependent_segments(segments, config, [])
        segments = list(set(segments))

    timestamp_info = (timestamp, dlf_ts, deal_ts,None)


    if 'UE' in fields_by_type:
        records = _bulk_fetch_user_entered_recs_v2(time_context,
                                      descendants,
                                      list(set(fields_by_type['UE'])),
                                      segments,
                                      config,
                                      timestamp_info,
                                      db,
                                      cache,
                                      get_all_segments=True,
                                      round_val=round_val,
                                      includefuturetimestamp=includefuturetimestamp,
                                      exclude_empty=exclude_empty,
                                      updated_since=updated_since,
                                      skip=skip,
                                      limit=limit)

    return records

def bulk_fetch_recs_for_data_export(time_context,
                                    descendants,
                                    fields,
                                    segments,
                                    config,
                                    eligible_nodes_for_segs,
                                    timestamp=None,
                                    db=None,
                                    round_val=True,
                                    node=None,
                                    includefuturetimestamp=False,
                                    is_pivot_special=False,
                                    call_from=None,
                                    time_context_list = [],
                                    recalc_UE_fields = [],
                                    recalc_DR_fields = []
                                    ):
    cache = {}

    db = db if db else sec_context.tenant_db

    if timestamp and round_val:
        dlf_ts, deal_ts = 0, 0
    else:
        period = time_context.deal_period if not isinstance(time_context.deal_period, list) else time_context.fm_period
        dlf_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
        deal_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))

    special_cs_fields = []
    fields_by_type, special_cs_fields = get_fields_by_type(fields, config, time_context, special_cs_fields)

    for type_key in ('DR', 'UE'):
        if type_key in fields_by_type:

            if type_key == 'UE':
                fields_by_type[type_key].extend(recalc_UE_fields)
            elif type_key == 'DR':
                fields_by_type[type_key].extend(recalc_DR_fields)

            fields_by_type[type_key] = list(set(fields_by_type[type_key]))


    bucket_forecast_field = config.config.get('bucket_forecast_field')
    bucket_fields = config.config.get('bucket_fields', [])

    eager_retrieval_fields = []
    if 'FN' in fields_by_type:
        for fn_field in fields_by_type['FN']:
            eager_retrieval_fields.extend(config.fields[fn_field]['source'])

    eager_fields_by_type, special_cs_fields = get_fields_by_type(eager_retrieval_fields, config, time_context, special_cs_fields)

    FN_field_prioritize = []
    for type in eager_fields_by_type:
        if type == 'FN':
            FN_field_prioritize.extend(eager_fields_by_type[type])
            continue
        if type not in fields_by_type:
            fields_by_type[type] = []
        fields_by_type[type].extend(eager_fields_by_type[type])

    for type, fields in fields_by_type.items():
        fields_by_type[type] = list(set(fields))

    for fn_field in FN_field_prioritize:
        if fn_field in fields_by_type['FN']:
            fields_by_type['FN'].remove(fn_field)

    # AV-14059 Commenting below as part of the log fix
    logger.info("fields for bulk data fetch are %s special_cs_fields %s" % (fields_by_type,
                                                                            special_cs_fields))
    is_special_pivot_segmented = is_pivot_special
    if node:
        is_special_pivot_segmented = config.is_special_pivot_segmented(node)
    if len(segments) == 1 and 'all_deals' in segments and config.has_segments and is_special_pivot_segmented:
        segments = config.segments

    segments = [segment for segment in segments]

    if any([segment in config.FN_segments for segment in segments]):
        segments += add_FN_dependent_segments(segments, config, [])
        segments = list(set(segments))

    timestamp_info = (timestamp, dlf_ts, deal_ts,None)
    FN_derived_UE_fields = []
    FN_derived_DR_fields = []
    if FN_field_prioritize:
        FN_derived_fields, _ = get_fields_by_type(FN_field_prioritize, config, time_context)
        FN_derived_UE_fields = FN_derived_fields.get('UE', [])
        FN_derived_DR_fields = FN_derived_fields.get('DR', [])

    if 'UE' in fields_by_type:
        if not config.month_to_qtr_rollup:
            _bulk_fetch_user_entered_recs(time_context,
                                          descendants,
                                          list(set(fields_by_type['UE'] + FN_derived_UE_fields)),
                                          segments,
                                          config,
                                          timestamp_info,
                                          db,
                                          cache,
                                          get_all_segments=True,
                                          round_val=round_val,
                                          includefuturetimestamp=includefuturetimestamp)
        else:
            for field_ in config.weekly_data:
                if field_ in fields_by_type['UE']:
                    fields_by_type['UE'].remove(field_)
                if field_ in FN_derived_UE_fields:
                    FN_derived_UE_fields.remove(field_)
                if field_ in config.quarterly_high_low_fields:
                    FN_derived_UE_fields.remove(field_)

            if len(list(set(fields_by_type['UE'] + FN_derived_UE_fields))) >= 1:
                _bulk_fetch_user_entered_recs(time_context,
                                              descendants,
                                              list(set(fields_by_type['UE'] + FN_derived_UE_fields)),
                                              segments,
                                              config,
                                              timestamp_info,
                                              db,
                                              cache,
                                              get_all_segments=True,
                                              round_val=round_val,
                                              includefuturetimestamp=includefuturetimestamp)

            if config.weekly_data + config.quarterly_high_low_fields:
                _bulk_fetch_user_entered_recs(time_context,
                                              descendants,
                                              config.weekly_data + config.quarterly_high_low_fields,
                                              segments,
                                              config,
                                              timestamp_info,
                                              db,
                                              cache,
                                              get_all_segments=True,
                                              round_val=round_val,
                                              includefuturetimestamp=includefuturetimestamp)


    if 'DR' in fields_by_type:
        handle_dr_fields(config,
                         list(set(fields_by_type['DR'] + FN_derived_DR_fields)),
                         time_context,
                         descendants,
                         segments,
                         timestamp_info,
                         db,
                         cache,
                         round_val,
                         timestamp,
                         eligible_nodes_for_segs,
                         call_from=call_from,
                         time_context_list = time_context_list
                         )

    if 'prnt_DR' in fields_by_type:
        _bulk_fetch_prnt_dr_recs(config,
                                 fields_by_type['prnt_DR'],
                                 time_context,
                                 descendants,
                                 segments,
                                 timestamp_info,
                                 db,
                                 cache,
                                 timestamp,
                                 eligible_nodes_for_segs,
                                 round_val=round_val,
                                 get_all_segments=True,
                                 node=node)

    if 'AI' in fields_by_type:
        _bulk_fetch_ai_recs(time_context,
                            descendants,
                            fields_by_type['AI'],
                            segments,
                            config,
                            timestamp_info,
                            db,
                            cache,
                            get_all_segments=True,
                            round_val=round_val,
                            bucket_fields=bucket_fields,
                            bucket_forecast_field=bucket_forecast_field)

    descendants_by_segments = {}
    rollup_segments = config.rollup_segments
    for segment in segments:
        descendants_list = []
        if segment == 'all_deals':
            descendants_list = descendants
        else:
            for node in descendants:
                if node[0] in eligible_nodes_for_segs.get(segment, []):
                    descendants_list.append(node)
        descendants_by_segments[segment] = descendants_list
        if 'PC' in fields_by_type:
            _bulk_period_conditional_rec(time_context,
                                      descendants_list,
                                      fields_by_type['PC'],
                                      segment,
                                      config,
                                      timestamp_info,
                                      db,
                                      cache,
                                      round_val=round_val)

    for segment in segments:
        descendants_list = descendants_by_segments[segment]
        if FN_field_prioritize:
            _bulk_fetch_formula_recs(time_context,
                                     descendants_list,
                                     FN_field_prioritize,
                                     segment,
                                     config,
                                     timestamp_info,
                                     db,
                                     cache)
    for segment in segments:
        descendants_list = descendants_by_segments[segment]
        if 'FN' in fields_by_type:
            _bulk_fetch_formula_recs(time_context,
                                  descendants_list,
                                  fields_by_type['FN'],
                                  segment,
                                  config,
                                  timestamp_info,
                                  db,
                                  cache)

    if 'CS' in fields_by_type:
        CS_derived_fields, _ = get_fields_by_type(fields_by_type['CS'], config, time_context)
        CS_derived_UE_fields = CS_derived_fields.get('UE', [])
        CS_derived_DR_fields = CS_derived_fields.get('DR', [])
        CS_derived_FN_fields = list(set(CS_derived_fields.get('FN', [])))
        eager_CS_FN_fields_by_type, _ = get_fields_by_type(CS_derived_FN_fields, config,
                                                     time_context, [])

        # # cs derived fn fields which are dependent on other FN fields should be prioritized
        CS_FN_field_prioritize = []
        for type in eager_CS_FN_fields_by_type:
            if type == 'FN':
                CS_FN_field_prioritize.extend(eager_CS_FN_fields_by_type[type])
        CS_FN_derived_UE_fields = []
        CS_FN_derived_DR_fields = []
        if CS_FN_field_prioritize:
            CS_FN_derived_fields, _ = get_fields_by_type(CS_FN_field_prioritize, config, time_context)
            CS_FN_derived_UE_fields = CS_FN_derived_fields.get('UE', [])
            CS_FN_derived_DR_fields = CS_FN_derived_fields.get('DR', [])
        for fn_field_ in list(set(CS_FN_field_prioritize)):
            if fn_field_ in CS_derived_FN_fields:
                CS_derived_FN_fields.remove(fn_field_)

        descendants_list = descendants
        child_descendants = []
        segments_enabled_list = None
        for descendant in descendants_list:
            node, children, grandchildren = descendant
            if node and segments_enabled_list is None:
                segments_enabled_list = config.get_segments(epoch().as_epoch(), node)
            for child in children:
                grandkids = {grandkid: parent for grandkid, parent in grandchildren.iteritems()
                    if parent == child}
                child_descendant = (child, grandkids, {})
                child_descendants.append(child_descendant)
                for grandkid in grandkids:
                    grandkid_descendant = (grandkid, {}, {})
                    child_descendants.append(grandkid_descendant)
        cs_cache = {}
        if not config.month_to_qtr_rollup:
            _bulk_fetch_user_entered_recs(time_context,
                                          child_descendants,
                                          list(set(CS_derived_UE_fields + CS_FN_derived_UE_fields)),
                                          segments,
                                          config,
                                          timestamp_info,
                                          db,
                                          cs_cache,
                                          get_all_segments=True,
                                          round_val=round_val)
        else:
            for field_ in config.weekly_data:
                if field_ in CS_derived_UE_fields:
                    CS_derived_UE_fields.remove(field_)
                if field_ in CS_FN_derived_UE_fields:
                    CS_FN_derived_UE_fields.remove(field_)
                if field_ in config.quarterly_high_low_fields:
                    CS_derived_UE_fields.remove(field_)
            if len(list(set(CS_derived_UE_fields + CS_FN_derived_UE_fields))) >= 1:
                _bulk_fetch_user_entered_recs(time_context,
                                              child_descendants,
                                              list(set(CS_derived_UE_fields + CS_FN_derived_UE_fields)),
                                              segments,
                                              config,
                                              timestamp_info,
                                              db,
                                              cs_cache,
                                              get_all_segments=True,
                                              round_val=round_val)
            if config.weekly_data + config.quarterly_high_low_fields:
                _bulk_fetch_user_entered_recs(time_context,
                                              child_descendants,
                                              config.weekly_data + config.quarterly_high_low_fields,
                                              segments,
                                              config,
                                              timestamp_info,
                                              db,
                                              cs_cache,
                                              get_all_segments=True,
                                              round_val=round_val)

        if CS_derived_DR_fields:
            handle_dr_fields(config,
                             list(set(CS_derived_DR_fields + CS_FN_derived_DR_fields)),
                             time_context,
                             child_descendants,
                             segments,
                             timestamp_info,
                             db,
                             cs_cache,
                             round_val,
                             timestamp,
                             eligible_nodes_for_segs
                             )
        if CS_derived_FN_fields:
            if CS_FN_field_prioritize:
                for segment in segments:
                    if segment in segments_enabled_list:
                        _bulk_fetch_formula_recs(time_context,
                                                 child_descendants,
                                                 CS_FN_field_prioritize,
                                                 segment,
                                                 config,
                                                 timestamp_info,
                                                 db,
                                                 cs_cache,
                                                 main_cache=cache)
            for segment in segments:
                if segment in segments_enabled_list:
                    _bulk_fetch_formula_recs(time_context,
                                             child_descendants,
                                             CS_derived_FN_fields,
                                             segment,
                                             config,
                                             timestamp_info,
                                             db,
                                             cs_cache,
                                             main_cache=cache)

        for segment in segments:
            descendants_list_local = descendants_by_segments[segment]
            if special_cs_fields:
                _bulk_fetch_child_sum_rec_special(time_context,
                                          descendants_list_local,
                                          special_cs_fields,
                                          segment,
                                          config,
                                          timestamp_info,
                                          db,
                                          cache,
                                          round_val=round_val)
        for segment in segments:
            descendants_list_local = descendants_by_segments[segment]
            cache.update(_bulk_fetch_child_sum_rec(time_context,
                                      child_descendants,
                                      fields_by_type['CS'],
                                      segment,
                                      config,
                                      timestamp_info,
                                      db,
                                      cs_cache,
                                      round_val=round_val,
                                      main_cache=cache))

            cache.update(_bulk_fetch_child_sum_rec(time_context,
                                      descendants_list_local,
                                      fields_by_type['CS'],
                                      segment,
                                      config,
                                      timestamp_info,
                                      db,
                                      cs_cache,
                                      round_val=round_val,
                                                   main_cache=cache))
        if len(config.segments) > 1:
            by_val = "system"
            latest_timestamp = 0
            period, comp_periods = time_context.fm_period, time_context.submission_component_periods
            cs_field_with_fn_source=[]
            for field in fields_by_type['CS']:
                source_field = config.fields[field]['source'][0]
                if config.fields[source_field]['type'] == 'FN':
                    cs_field_with_fn_source.append(field)
            cs_field_without_fn_source = [x for x in fields_by_type['CS'] if x not in cs_field_with_fn_source]
            for descendants, field in product(descendants_list, cs_field_without_fn_source):
                node, children, grandchildren = descendants
                val = 0
                for segment in segments:
                    if segment != "all_deals":
                        ch_key = (period, node, field, segment, timestamp)
                        if segment in rollup_segments:
                            val += try_float(get_nested(cache, [ch_key, 'val'], 0))
                        seg_timestamp = get_nested(cache, [ch_key, 'timestamp'], 0)
                        if seg_timestamp > latest_timestamp:
                            latest_timestamp = seg_timestamp
                            by_val = get_nested(cache, [ch_key, 'by'], "system")
                if not config.is_special_pivot_segmented(node):
                    ch_key = (period, node, field, 'all_deals', timestamp)
                    val = try_float(get_nested(cache, [ch_key, 'val'], 0))
                if config.forecast_service_editable_fields:
                    source_fields = get_all_source_fields(config, field)
                    if any(item in config.forecast_service_editable_fields for item in source_fields):
                        ch_key = (period, node, field, 'all_deals', timestamp)
                        val = try_float(get_nested(cache, [ch_key, 'val'], 0))
                res = {'period': period,
                       'segment': config.primary_segment,
                       'val': val,
                       'by': by_val,
                       'how': 'sum_of_children',
                       'found': True if val else False,
                       'timestamp': latest_timestamp,
                       'node': node,
                       'field': field}
                if cache is not None:
                    cache[(period, node, field, config.primary_segment, timestamp)] = res

    all_fields = set([])

    for fn_field in FN_field_prioritize:
        if 'FN' in fields_by_type and fn_field not in fields_by_type['FN']:
            if isinstance(fields_by_type['FN'], set):
                fields_by_type['FN'].add(fn_field)
            elif isinstance(fields_by_type['FN'], list):
                fields_by_type['FN'].append(fn_field)

    for _, type_fields in fields_by_type.items():
        all_fields.update(type_fields)


    time_context_list = time_context_list if time_context_list else [time_context]
    for _time_context in time_context_list:
        _period = _time_context.deal_period if not isinstance(_time_context.deal_period, list) else _time_context.fm_period
        _dlf_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(_period), 0))
        _deal_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(_period), 0))
        _timestamp_info = (timestamp, _dlf_ts, _deal_ts, None)
        populate_FN_segments(_time_context,
                            all_fields,
                            config.FN_segments,
                            descendants_by_segments,
                            cache,
                            timestamp,
                            [],
                            config,
                            fields_by_type,
                            _timestamp_info)

    return cache

def populate_FN_segments(time_context,
                         all_fields,
                         FN_segments,
                         descendants_by_segments,
                         cache,
                         timestamp,
                         segs_populated,
                         config,
                         fields_by_type,
                         timestamp_info):
    missing_fields = set()
    for key in cache.keys():
        if isinstance(key, tuple) and len(key) == 5:
            field = key[2]
            if field not in all_fields:
                missing_fields.add(field)

    if missing_fields:
        if isinstance(all_fields, set):
            all_fields.update(missing_fields)
        elif isinstance(all_fields, list):
            all_fields.extend(missing_fields)

    for field_key in set(all_fields):
        if config.fields.get(field_key, {}).get('ignore_func_segment', False):
            all_fields.remove(field_key)

    for segment, segment_func in FN_segments.items():
        if segment in segs_populated:
            continue
        descendants_list = descendants_by_segments.get(segment, [])
        period = time_context.fm_period
        for descendants, field in product(descendants_list, all_fields):
            node, _, _ = descendants
            source = []
            for source_seg in segment_func.get('source'):
                if source_seg in FN_segments:
                    populate_FN_segments(time_context,
                                         all_fields,
                                         {source_seg: FN_segments[source_seg]},
                                         descendants_by_segments,
                                         cache,
                                         timestamp,
                                         segs_populated,
                                         config,
                                         fields_by_type,
                                         timestamp_info)
                source.append(cache.get((period, node, field, source_seg, timestamp), {}).get('val', 0))
            try:
                val = eval(segment_func.get('func'))
            except ZeroDivisionError:
                val = 0
            except Exception:
                val = 0
            res = {'period': period,
                   'segment': segment,
                   'val': val,
                   'by': "system",
                   'how': 'derived_segment',
                   'found': True if val else False,
                   'timestamp': timestamp,
                   'node': node,
                   'field': field}
            if cache is not None:
                cache[(period, node, field, segment, timestamp)] = res

        ## Need to re-calculate some FN fields as values may change after FN segment operations based on config
        if 'FN' in fields_by_type:
            recalc_fields = []
            for field in fields_by_type['FN']:
                is_recalc = config.fields[field].get('recalc', False)
                if is_recalc:
                    recalc_fields.append(field)
            _bulk_fetch_formula_recs(time_context,
                                     descendants_list,
                                     recalc_fields,
                                     segment,
                                     config,
                                     timestamp_info,
                                     sec_context.tenant_db,
                                     cache)
        segs_populated.append(segment)

def add_FN_dependent_segments(segments, config, segs_populated):
    try:
        FN_segments = config.FN_segments
        for segment in segments:
            if segment in segs_populated:
                continue
            if segment in FN_segments:
                for source_seg in FN_segments[segment].get('source'):
                    if source_seg not in segments:
                        segs_populated.append(source_seg)
                    if source_seg in FN_segments:
                        segs_populated += add_FN_dependent_segments(segs_populated, config, [])
        return list(set(segs_populated))
    except Exception as e:
        logger.error("Error while fetching source segments, error is %s", str(e))
        return []



def handle_dr_fields(config,
                     dr_fields,
                     time_context,
                     descendants,
                     segments,
                     timestamp_info,
                     db,
                     cache,
                     round_val,
                     timestamp,
                     eligible_nodes_for_segs,
                     dr_from_fm_coll_for_snapshot=None,
                     found=None,
                     call_from=None,
                     dlf_ts=None,
                     deal_ts=None,
                     trend=False,
                     time_context_list = []
                     ):
    # if segment_field is defined or tenant does not have segments use bulk fetch for dr records
    # for tenant like honeywell we have segment field named pulsesegment so we can group by segment.
    try:
        dr_from_fm_coll_for_snapshot = dr_from_fm_coll_for_snapshot if dr_from_fm_coll_for_snapshot is not None else sec_context.details.get_flag('deal_rollup', 'snapshot_dr_from_fm_coll', False)
    except:
        dr_from_fm_coll_for_snapshot = False
    try:
        found = found if found is not None else False
    except:
        found = False

    if config.deal_config.segment_field or not config.has_segments or dr_from_fm_coll_for_snapshot:
        dlf_fields = []
        fm_fields_ = []
        instant_dlf_update_fields = []
        fm_dlf_instant_update_fields = config.config.get('fm_dlf_instant_update_fields', [])
        for field in dr_fields:
            if field in fm_dlf_instant_update_fields:
                instant_dlf_update_fields.append(field)
            elif config.deal_rollup_fields[field].get('dlf') or any(filt.get('op') == 'dlf' if isinstance(filt, dict) else \
                                                                          False for filt in
                                                                  config.deal_rollup_fields[field]['filter']):
                dlf_fields.append(field)
            else:
                fm_fields_.append(field)

        if dlf_fields:
            _bulk_fetch_deal_rollup_recs(time_context,
                                         descendants,
                                         dlf_fields,
                                         segments,
                                         config,
                                         timestamp_info,
                                         db,
                                         cache,
                                         round_val=round_val,
                                         get_all_segments=True,
                                         is_dr_deal_fields=False,
                                         dr_from_fm_coll_for_snapshot=dr_from_fm_coll_for_snapshot,
                                         found=found,
                                         eligible_nodes_for_segs=eligible_nodes_for_segs,
                                         call_from=call_from,
                                         trend=trend
                                         )

        if fm_fields_:
            _bulk_fetch_deal_rollup_recs(time_context,
                                         descendants,
                                         fm_fields_,
                                         segments,
                                         config,
                                         timestamp_info,
                                         db,
                                         cache,
                                         round_val=round_val,
                                         get_all_segments=True,
                                         is_dr_deal_fields=True,
                                         dr_from_fm_coll_for_snapshot=dr_from_fm_coll_for_snapshot,
                                         found=found,
                                         eligible_nodes_for_segs=eligible_nodes_for_segs,
                                         call_from=call_from,
                                         trend=trend
                                         )

        if instant_dlf_update_fields:
            fetch_fm_recs_history(time_context,
                                  descendants,
                                  instant_dlf_update_fields,
                                  segments,
                                  config,
                                  [timestamp],
                                  db=db,
                                  cache=cache,
                                  eligible_nodes_for_segs=eligible_nodes_for_segs,
                                  call_from=call_from,
                                  dlf_ts=dlf_ts,
                                  deal_ts=deal_ts,
                                  time_context_list = time_context_list
                                  )
    # if segment_field is not defined for tenants having segments get dr records one by one for each segment.
    # for tenant like github we don't have a unique way to identify segment in deals info so,
    # retrieving data one by one for each segment.
    elif not config.deal_config.segment_field and config.has_segments:
        fetch_fm_recs_history(time_context,
                              descendants,
                              dr_fields,
                              segments,
                              config,
                              [timestamp],
                              db=db,
                              cache=cache,
                              eligible_nodes_for_segs=eligible_nodes_for_segs,
                              call_from=call_from,
                              time_context_list = time_context_list
                              )


def fetch_fm_recs(time_context,
                  descendants_list,
                  fields,
                  segments,
                  config,
                  timestamp=None,
                  recency_window=None,
                  db=None,
                  eligible_nodes_for_segs={}
                  ):
    """
    fetch fm records from db for multiple nodes/fields
    you are 100% guaranteed to get records for all the params you request

    Arguments:
        time_context {time_context} -- fm period, components of fm period
                                       deal period, close periods of deal period
                                       deal expiration timestamp,
                                       relative periods of component periods
                                       ('2020Q2', ['2020Q2'],
                                        '2020Q2', ['201908', '201909', '201910'],
                                        1556074024910,
                                        ['h', 'c', 'f'])
        descendants_list {list} -- list of tuples of
                                   (node, [children], [grandchildren])
                                    fetches data for each node
                                    using children/grandkids to compute sums
                                    [('A', ['B, 'C'], ['D', 'E'])]
        fields {list} -- list of field names
                         ['commit', 'best_case']
        segments {list} -- list of segment names
                          ['all_deals', 'new', 'upsell']
        config {FMConfig} -- instance of FMConfig

    Keyword Arguments:
        timestamp {int} -- epoch timestamp to get data as of (default: {None})
                           if None, gets most recent record
                           1556074024910
        recency_window {int} -- window to reject data from before timestamp - recency_window  (default: {None})
                                if None, will accept any record, regardless of how stale
                                ONE_DAY
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        dict -- {(period, node, field, segment, timestamp): {'period': period,
                                                             'val': val,
                                                             'by': by,
                                                             'how': how,
                                                             'found': found,
                                                             'timestamp': timestamp,
                                                             'node': node,
                                                             'field': field}}
    """
    threads, cache = [], {}
    db = db if db else sec_context.tenant_db

    if timestamp:
        dlf_ts, deal_ts = 0, 0
    else:
        period = time_context.deal_period if not isinstance(time_context.deal_period, list) else time_context.fm_period
        dlf_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
        deal_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))

    if any([segment in config.FN_segments for segment in segments]):
        segments += add_FN_dependent_segments(segments, config, [])
        segments = list(set(segments))

    for descendants, field, segment in product(descendants_list, fields, segments):
        fetch_fm_rec(time_context,
                     descendants,
                     field,
                     segment,
                     config,
                     (timestamp, dlf_ts, deal_ts, timestamp - recency_window if recency_window else None),
                     db,
                     cache)

    if config.FN_segments:
        descendants_by_segments = {}
        for segment in segments:
            descendants_list = []
            for node in descendants:
                if node in eligible_nodes_for_segs.get(segment, []) or segment == 'all_deals':
                    descendants_list.append((node, {}, {}))
            descendants_by_segments[segment] = descendants_list
        all_fields = copy.deepcopy(fields)
        timestamp_info = (timestamp, dlf_ts, deal_ts, None)
        for field_key in set(all_fields):
            if config.fields.get(field_key, {}).get('ignore_func_segment', False):
                all_fields.remove(field_key)
        fields_by_type, _ = get_fields_by_type(fields, config, time_context)
        populate_FN_segments(time_context,
                             all_fields,
                             config.FN_segments,
                             descendants_by_segments,
                             cache,
                             timestamp,
                             [],
                             config,
                             fields_by_type,
                             timestamp_info)
    return cache


def fetch_fm_recs_history(time_context,
                          descendants_list,
                          fields,
                          segments,
                          config,
                          timestamps,
                          recency_window=None,
                          db=None,
                          cache={},
                          ignore_recency_for=[],
                          eligible_nodes_for_segs={},
                          call_from=None,
                          dlf_ts=None,
                          deal_ts=None,
                          time_context_list = []
                          ):
    """
    fetch history of fm records for multiple dbs/fields
    you are 100% guaranteed to get records for all the params you request

    Arguments:
        time_context {time_context} -- fm period, components of fm period
                                       deal period, close periods of deal period
                                       deal expiration timestamp,
                                       relative periods of component periods
                                       ('2020Q2', ['2020Q2'],
                                        '2020Q2', ['201908', '201909', '201910'],
                                        1556074024910,
                                        ['h', 'c', 'f'])
        descendants_list {list} -- list of tuples of
                                   (node, [children], [grandchildren])
                                    fetches data for each node
                                    using children/grandkids to compute sums
                                    [('A', ['B, 'C'], ['D', 'E'])]
        fields {list} -- list of field names
                         ['commit', 'best_case']
        segments {list} -- list of segment names
                          ['all_deals', 'new', 'upsell']
        config {FMConfig} -- instance of FMConfig
        timestamps {list} -- list of epoch timestamps to get data as of
                             [1556074024910, 1556075853732, ...]

    Keyword Arguments:
        recency_window {int} -- window to reject data from before timestamp - recency_window  (default: {None})
                            if None, will accept any record, regardless of how stale
                            ONE_DAY
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        dict -- {(period, node, field, segment, timestamp): {'period': period,
                                                             'val': val,
                                                             'by': by,
                                                             'how': how,
                                                             'found': found,
                                                             'timestamp': timestamp,
                                                             'node': node,
                                                             'field': field}}
    """
    # WARN: performance for this might suck, is threading the right call?
    original_recency_window = recency_window
    if cache is None:
        cache = {}
    db = db if db else sec_context.tenant_db
    period = time_context.deal_period if not isinstance(time_context.deal_period, list) else time_context.fm_period
    dlf_ts = dlf_ts if dlf_ts is not None else try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
    deal_ts = deal_ts if deal_ts is not None else try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
    try:
        t = sec_context.details
        year_accepted_from = int(t.get_flag('fm_latest_migration', 'year', 0))
    except:
        year_accepted_from = 0

    if not eligible_nodes_for_segs:
        for segment in config.segments:
            if segment == config.primary_segment:
                continue
            eligible_nodes_for_segs[segment] = [dd['node'] for dd in fetch_eligible_nodes_for_segment(time_context.now_timestamp, segment)]

    if config.debug:
        for timestamp, field, segment, descendants in product(timestamps, fields, segments, descendants_list):
            if field in ignore_recency_for:
                recency_window = None
            else:
                recency_window = original_recency_window
            if segment == 'all_deals' or descendants[0] in eligible_nodes_for_segs.get(segment, []):
                fetch_fm_rec(time_context,
                             descendants,
                             field,
                             segment,
                             config,
                             (timestamp, dlf_ts, deal_ts, timestamp - recency_window if recency_window and timestamp else None),
                             db=db,
                             cache=cache,
                             call_from=call_from)
    else:
        count = 0
        pool = ThreadPool(MAX_THREADS)
        fm_rec_params = []
        threads = 0
        user = sec_context.login_user_name
        time_context_list = time_context_list if time_context_list else [time_context]

        for timestamp, field, segment, descendants, _time_context in product(timestamps, fields, segments, descendants_list, time_context_list):
            # TODO: bug here ... with deal and dlf expirations not reflecting correct value to dashboard
            if field in ignore_recency_for:
                recency_window = None
            else:
                recency_window = original_recency_window
            count = count + 1
            _period = _time_context.deal_period if not isinstance(_time_context.deal_period, list) else _time_context.fm_period
            _dlf_ts = dlf_ts if dlf_ts is not None else try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(_period), 0))
            _deal_ts = deal_ts if deal_ts is not None else try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(_period), 0))

            if segment == 'all_deals' or descendants[0] in eligible_nodes_for_segs.get(segment,[]):
                fm_rec_params.append((_time_context,
                                        descendants,
                                        field,
                                        segment,
                                        config,
                                        (timestamp, _dlf_ts, _deal_ts, timestamp - recency_window if recency_window and timestamp else None),
                                        db,
                                        cache,
                                        None,
                                        call_from,
                                        year_accepted_from,
                                        user))
                threads += 1

        pool.map(_fetch_fm_rec, tuple(fm_rec_params))
        pool.close()
        pool.join()

        # AV-14059 Commenting below as part of the log fix
        # logger.info('actual comibnations: %s, threads executed count : %s', count, threads)

    return cache


def bulk_fetch_fm_recs_history(time_context,
                          descendants_list,
                          fields,
                          segments,
                          config,
                          timestamps,
                          recency_window=None,
                          db=None,
                          cache={},
                          ignore_recency_for=[],
                          eligible_nodes_for_segs={},
                          fields_by_type={},
                          get_all_segments=True
                          ):

    try:
        # WARN: performance for this might suck, is threading the right call?
        original_recency_window = recency_window
        if cache is None:
            cache = {}
        fm_recs = {}
        db = db if db else sec_context.tenant_db
        period = time_context.deal_period if not isinstance(time_context.deal_period, list) else time_context.fm_period
        dlf_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_dlf_expiration_timestamp'.format(period), 0))
        deal_ts = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(period), 0))
        if not eligible_nodes_for_segs:
            for segment in config.segments:
                if segment == config.primary_segment:
                    continue
                eligible_nodes_for_segs[segment] = [dd['node'] for dd in
                                                    fetch_eligible_nodes_for_segment(time_context.now_timestamp, segment)]
        if any([segment in config.FN_segments for segment in segments]):
            segments += add_FN_dependent_segments(segments, config, [])
            segments = list(set(segments))

        threads = 0
        count = 0
        pool = ThreadPool(MAX_THREADS)
        try:
            dr_from_fm_coll_for_snapshot = sec_context.details.get_flag('deal_rollup', 'snapshot_dr_from_fm_coll', False)
        except:
            dr_from_fm_coll_for_snapshot = False
        for field_type, fields in fields_by_type.items():
            final_timestamps = {}
            if field_type == 'UE' or field_type == 'AI' or field_type == 'DR' or field_type == 'prnt_DR':
                for timestamp, field in product(timestamps, fields):
                    if field in ignore_recency_for:
                        recency_window = None
                    else:
                        recency_window = original_recency_window
                    final_timestamp = (
                    timestamp, dlf_ts, deal_ts, timestamp - recency_window if recency_window and timestamp else None)

                    if final_timestamp not in final_timestamps:
                        final_timestamps[final_timestamp] = [field]
                    if field not in final_timestamps[final_timestamp]:
                        final_timestamps[final_timestamp].append(field)

            if field_type == 'UE':
                fm_rec_params = []
                all_segments = config.segments
                all_segments = [segment for segment in all_segments]
                rollup_segments = [segment for segment in config.rollup_segments]
                if 'all_deals' not in rollup_segments:
                    rollup_segments.append('all_deals')
                node_children_check = {node: any(node_children) for node, node_children, _ in descendants_list}
                nodes = node_children_check.keys()
                nodes_and_eligible_segs_len = {}
                if config.partially_segmented:
                    for node in nodes:
                        nodes_and_eligible_segs_len[node] = len(config.get_segments(epoch().as_epoch(), node))
                if 'newrelic' in sec_context.login_tenant_name:
                    #temporarily added for newewlic
                    try:
                        for final_timestamp, fields in final_timestamps.iteritems():
                            fm_recs = _bulk_fetch_user_entered_recs(time_context,
                                                                    descendants_list,
                                                                    fields,
                                                                    all_segments,
                                                                    config,
                                                                    final_timestamp,
                                                                    db,
                                                                    cache,
                                                                    True,
                                                                    True,
                                                                    True,
                                                                    nodes_and_eligible_segs_len)
                    except Exception as e:
                        logger.error(e)
                        raise e
                else:
                    for final_timestamp, fields in final_timestamps.iteritems():
                        count = count + 1
                        # TODO: bug here ... with deal and dlf expirations not reflecting correct value to dashboard
                        fm_rec_params.append((time_context,
                                              descendants_list,
                                              fields,
                                              rollup_segments if (len(segments) == 1 \
                                                          and segments[0] == 'all_deals') else segments,
                                              config,
                                              final_timestamp,
                                              db,
                                              cache,
                                              True,
                                              True,
                                              True,
                                              nodes_and_eligible_segs_len))
                        threads += 1
                    pool.map(_bulk_fetch_user_entered_recs_pool, tuple(fm_rec_params))
            elif field_type == 'AI':
                fm_rec_params = []
                bucket_fields = FMConfig().config.get('bucket_fields', [])
                if any([field in bucket_fields for field in fields]):
                    bucket_forecast_field = FMConfig().config.get('bucket_forecast_field') if FMConfig().config.get(
                        'bucket_forecast_field') else []
                else:
                    bucket_fields = []
                    bucket_forecast_field = []


                for final_timestamp, fields in final_timestamps.iteritems():
                    count = count + 1
                    # TODO: bug here ... with deal and dlf expirations not reflecting correct value to dashboard
                    fm_rec_params.append((time_context,
                                          descendants_list,
                                          fields,
                                          segments,
                                          config,
                                          final_timestamp,
                                          db,
                                          cache,
                                          get_all_segments,
                                          True,
                                          bucket_fields,
                                          bucket_forecast_field))
                    threads += 1

                pool.map(_bulk_fetch_ai_recs_pool, tuple(fm_rec_params))
            elif field_type == 'DR':
                try:
                    dr_from_fm_coll_for_snapshot = sec_context.details.get_flag('deal_rollup', 'snapshot_dr_from_fm_coll',  False)
                except:
                    dr_from_fm_coll_for_snapshot = False
                fm_rec_params = []
                fm_params = []
                config.debug = True
                if config.deal_config.segment_field or not config.has_segments or dr_from_fm_coll_for_snapshot:
                    for final_timestamp, fields in final_timestamps.iteritems():
                        count = count + 1
                        found = True    # passed directly as the last arg
                        fm_rec_params.append((config,
                                          fields,
                                          time_context,
                                          descendants_list,
                                          segments,
                                          final_timestamp,
                                          db,
                                          cache,
                                          True,
                                          final_timestamp[0],
                                          eligible_nodes_for_segs,
                                          True,
                                          True,
                                          None,
                                          dlf_ts,
                                          deal_ts,
                                          True))
                        threads += 1
                    pool.map(handle_dr_fields_pool, tuple(fm_rec_params))
                elif not config.deal_config.segment_field and config.has_segments:
                    config.debug = True
                    for timestamp, field, segment, descendants in product(timestamps, fields, segments, descendants_list):
                        # TODO: bug here ... with deal and dlf expirations not reflecting correct value to dashboard
                        if field in ignore_recency_for:
                            recency_window = None
                        else:
                            recency_window = original_recency_window
                        count = count + 1
                        if segment == 'all_deals' or descendants[0] in eligible_nodes_for_segs.get(segment, []):
                            fm_params.append((time_context,
                                                  descendants,
                                                  field,
                                                  segment,
                                                  config,
                                                  (timestamp, dlf_ts, deal_ts,
                                                   timestamp - recency_window if recency_window and timestamp else None),
                                                  db,
                                                  cache))
                            threads += 1
                    pool.map(_fetch_fm_rec, tuple(fm_params))
            elif field_type == 'prnt_DR':
                fm_rec_params = []
                prev_periods = prev_periods_allowed_in_deals()
                for final_timestamp, fields in final_timestamps.iteritems():
                    count = count + 1
                    fm_rec_params.append((config,
                                          fields,
                                          time_context,
                                          descendants_list,
                                          rollup_segments if (len(segments) == 1 \
                                                          and segments[0] == 'all_deals') else segments,
                                          final_timestamp,
                                          db,
                                          cache,
                                          final_timestamp[0],
                                          eligible_nodes_for_segs,
                                          True,
                                          True,
                                          None,
                                          prev_periods))
                    threads += 1
                pool.map(_bulk_fetch_prnt_dr_recs_pool, tuple(fm_rec_params))
            else:
                fm_rec_params = []
                for timestamp, field, segment, descendants in product(timestamps, fields, segments, descendants_list):
                    # TODO: bug here ... with deal and dlf expirations not reflecting correct value to dashboard
                    if field in ignore_recency_for:
                        recency_window = None
                    else:
                        recency_window = original_recency_window
                    count = count + 1
                    if segment == 'all_deals' or descendants[0] in eligible_nodes_for_segs.get(segment, []):
                        fm_rec_params.append((time_context,
                                              descendants,
                                              field,
                                              segment,
                                              config,
                                              (timestamp, dlf_ts, deal_ts,
                                               timestamp - recency_window if recency_window and timestamp else None),
                                              db,
                                              cache))
                        threads += 1
                pool.map(_fetch_fm_rec, tuple(fm_rec_params))
        pool.close()
        pool.join()

        all_fields = set([])
        for _, type_fields in fields_by_type.items():
            all_fields.update(type_fields)
        descendants_by_segments = {}
        for segment in segments:
            descendants_by_segments[segment] = descendants_list

        for timestamp in timestamps:
            populate_FN_segments(time_context,
                                 all_fields,
                                 config.FN_segments,
                                 descendants_by_segments,
                                 cache,
                                 timestamp,
                                 [],
                                 config,
                                 fields_by_type,
                                 timestamp_info=(timestamp, dlf_ts, deal_ts, None)
                                 )

        logger.info('actual comibnations: %s, threads executed count : %s', count, threads)
        return cache
    except Exception as e:
        logger.exception(e)
        raise e


def fetch_fm_rec(time_context,
                 descendants,
                 field,
                 segment,
                 config,
                 timestamp_info=None,
                 db=None,
                 cache=None,
                 prnt_node=None,
                 call_from=None,
                 year_accepted_from=None,
                 user=None
                 ):
    """
    fetch a single fm record for a node/field

    Arguments:
        time_context {time_context} -- fm period, components of fm period
                                       deal period, close periods of deal period
                                       deal expiration timestamp,
                                       relative periods of component periods
                                       ('2020Q2', ['2020Q2'],
                                        '2020Q2', ['201908', '201909', '201910'],
                                        1556074024910,
                                        ['h', 'c', 'f'])
        descendants {tuple} -- (node, [children], [grandchildren])
                                fetches data for node
                                using children/grandkids to compute sums
                                ('A', ['B, 'C'], ['D', 'E'])
        field {str} -- field name
                      'commit'
        segment {str} -- segment name
                         'all deals'
        config {FMConfig} -- instance of FMConfig

    Keyword Arguments:
        timestamp_info {tuple} -- (ts to get data as of,
                                   ts dlf DR recs expired as of,
                                   ts deal DR recs expired as of,
                                   ts to reject data from before (None to accept any level of staleness))
                                   if None passed, gets most recent record
                                   (default: {None})
                                   (1556074024910, 1556074024910, 1556074024910)
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one
                       (pass in to avoid overhead when fetching many times)
        cache {dict} -- dict to hold records fetched by (default: {None})
                        (used to memoize fetching many recs)
        prnt_node {str} -- parent of the particular node. Value will same as node when it's already the
                            parent. prnt_node is required for prnt_DR fields

    Returns:
        dict -- a single fm record
    """
    node, _, _ = descendants
    period = time_context.fm_period
    if not timestamp_info:
        timestamp_info = (None, 0, 0, None)
    if not db:
        db = sec_context.tenant_db
    timestamp, _, _, _ = timestamp_info

    # TODO: dont love this implementation...
    # should PC be defined in config, instead of down here
    # that would be cleaner, but make the code slower probably
    if ('hist_field' in config.fields[field]
            and 'h' in time_context.relative_periods and 'overwrite_field' not in config.fields[field]):
        # historic period, switch to using true up field if it exists
        fetch_type = 'PC'
        if config.quarter_editable:
            #If quarter editable, use PC type only when the quarter is a previous quarter
            if not all(elem == 'h' for elem in time_context.relative_periods):
                fetch_type = config.fields[field]['type']
    else:
        fetch_type = config.fields[field]['type']


    if cache:
        try:
            return cache[(period, node, field, segment, timestamp)]
        except KeyError:
            pass

    fetch_map = {
        'UE': _fetch_user_entered_rec,
        'US': _fetch_child_sum_rec,  # TODO: deprecate this option
        'DR': _fetch_deal_rollup_rec,
        'FN': _fetch_formula_rec,
        'AI': _fetch_ai_rec,
        'NC': _fetch_node_conditional_rec,
        'CS': _fetch_child_sum_rec,
        'PC': _fetch_period_conditional_rec,  # TODO: i dont like the name for this
        'prnt_DR': fetch_deal_rollup_for_prnt_dr_rec #Not in use here because of if condition below, added for readability
    }
    if fetch_type == 'prnt_DR':
        res = fetch_deal_rollup_for_prnt_dr_rec(time_context, descendants,
                                field,
                                segment,
                                config,
                                timestamp_info,
                                db,
                                cache,
                                prnt_node)
    else:
        if fetch_type == 'DR':
            res = fetch_map[fetch_type](time_context, descendants, field, segment, config, timestamp_info, db, cache,
                                        year_accepted_from=year_accepted_from, user=user)
        else:
            res = fetch_map[fetch_type](time_context,
                                        descendants,
                                        field,
                                        segment,
                                        config,
                                        timestamp_info,
                                        db,
                                        cache,
                                        year_accepted_from=year_accepted_from)

    if cache is not None:
        cache[(period, node, field, segment, timestamp)] = res

    return res


def fetch_fm_records_from_time_range(period,
                                     time_range,
                                     nodes=None,
                                     fields=None,
                                     db=None):
    """
    fetch fm records that occured within time range, and have had no updates since

    Arguments:
        period {str} -- ???? uh
        time_range {tuple} -- begin timestamp, end timestamp                    (1573440113408, 1573540213408)

    Keyword Arguments:
        nodes {list} -- list of nodes to limit query to (default: {None})
        fields {list} -- list of fm fields to limit query to (default: {None})
        db {pymongo.database.Database} -- instance of tenant_db
                                    (default: {None})
                                    if None, will create one

    Returns:
        list -- [fm records]
    """
    # TODO: bunch of logic
    # like can only be component period, and can only be fields that are actually saved in db, not computed
    fm_data_collection = db[FM_COLL] if db else sec_context.tenant_db[FM_COLL]

    match = {'period': period}
    if fields:
        match['field'] = {'$in': fields}
    if nodes:
        match['node'] = {'$in': nodes}

    sort = {'timestamp': 1}

    group = {'_id': {'period': '$period',
                     'node': '$node',
                     'field': '$field'},
             'timestamp': {'$last': '$timestamp'},
             'doc': {'$last': '$$ROOT'}}

    rematch = {'timestamp': {'$gte': time_range[0], '$lte': time_range[1]}}

    project = {'doc': 1, '_id': 0}

    pipeline = [{'$match': match},
                {'$sort': sort},
                {'$group': group},
                {'$match': rematch},
                {'$project': project}]

    return fm_data_collection.aggregate(pipeline, allowDiskUse=True)


def fetch_latest_dr_count(periods, db=None):
    if not isinstance(periods, list):
        periods = [periods]
    if not db:
        db = sec_context.tenant_db
    fm_latest_data_collection = db[FM_LATEST_COLL]
    return fm_latest_data_collection.count_documents({'period': {'$in': periods}})

def fetch_latest_deal_rollups(period,
                              nodes,
                              fields,
                              segments,
                              config,
                              db=None,
                              ):
    """
    fetch latest deal rollup values from fm_latest_data_collection

    Arguments:
        period {str} -- component period mnemonic (month if monthly fm, quarter if quarterly)
                        '2020Q2'
        nodes {list} -- list of nodes to fetch values for
                        ['0050000FLN2C9I2',]
        fields {str} -- list of field names, must be deal rollup fields
                        ['commit', 'best_case']
        config {FMConfig} -- instance of FMConfig

    Keyword Arguments:
        db {pymongo.database.Database} -- instance of tenant_db
                                          (default: {None})
                                          if None, will create one
    Returns:
        dict -- {(segment, field, node): val}
    """
    if not db:
        db = sec_context.tenant_db
    fm_latest_data_collection = db[FM_LATEST_COLL]

    match = {'period': period,
             'segment': {'$in': list(segments)},
             'node': {'$in': list(nodes)},
             'field': {'$in': list(fields)}}
    project = {'period': 1, 'segment': 1, 'node': 1, 'field': 1, 'val': 1}
    pipeline = [{'$match': match},
                {'$project': project}]

    if config.debug:
        logger.info("pipeline for latest deal rollups is %s " % pipeline)

    return {(rec['segment'], rec['field'], rec['node']): rec['val']
            for rec in fm_latest_data_collection.aggregate(pipeline, allowDiskUse=True)}


def fetch_throwback_totals(node, segment, save_history, db=None):
    """
    get the historical throwback data computed in insight task

    Arguments:
        week_num {int} -- number of week

    Returns:
        dict -- proportion of each week wins. Values sum up to 1
    """
    fi_collection = db['forecast_explanation_insights'] if db else sec_context.tenant_db['forecast_explanation_insights']
    if save_history:
        timestamp = list(fi_collection.aggregate([{ "$sort" : { "timestamp" : -1 } }, { "$project": {"timeMax": {"$max": "$timestamp"}} }]))[0]["timeMax"]
    else:
        timestamp = 0
    criteria = {'$and':[{'timestamp': timestamp,
                            'node': node,
                            'segment': segment}]}
    return list(fi_collection.find(criteria))


def _fetch_user_entered_rec(time_context,
                            descendants,
                            field,
                            segment,
                            config,
                            timestamp_info,
                            db,
                            cache,
                            year_accepted_from=None
                            ):
    """
    fetch record that came from user entering data in aviso
    """
    # NOTE: much of this logic is duplicated in _bulk_fetch_user_entered_recs
    # if you are changing logic here, be sure to also change in the bulk version of the function
    node, children, _ = descendants
    segments = config.segment_map[segment]
    # below code is commented as this is creating CS-21073 issue. 'segment_rep' looks legacy code which is not present anymore
    # if issue arises, try removing the code, but handle CS-21073 as well.
    if 'consider_only_segment_value' in config.fields[field]:  #or ((not children and segment == config.primary_segment) and 'segment_rep' not in config.fields[field]):
        segments = [segment]
    if config.primary_segment == segment and config.config_based_segments_rollup:
        segments = config.rollup_segments
    period, comp_periods = time_context.fm_period, time_context.submission_component_periods
    timestamp, _, _, cutoff_timestamp = timestamp_info
    how = 'sum_of_users' if comp_periods[0] != period or len(segments) != 1 else 'fm_upload'
    found = True
    FM_COLL = read_from_collection(period[:4], timestamp=timestamp, call_from='_fetch_user_entered_rec', config=config,
                                   year_accepted_from=year_accepted_from)
    fm_data_collection = db[FM_COLL]

    if field in config.quarterly_high_low_fields and "Q" in period:
        comp_periods = [period]

    match = {'period': {'$in': comp_periods},
             'segment': {'$in': segments},
             'node': node,
             'field': field}
    if timestamp is not None:
        match['timestamp'] = {'$lte': timestamp}
    if len(comp_periods) == 1 and cutoff_timestamp is not None:
        # BUG: can only support cutoff_timestamp for non aggregated :(
        match['timestamp']['$gt'] = cutoff_timestamp
    sort = {'timestamp': 1}
    group = {'_id': {'period': '$period',
                    'node': '$node',
                    'segment': '$segment',
                    'field': '$field'},
            'timestamp': {'$last': '$timestamp'},
            'by': {'$last': '$by'},
            'val': {'$last': '$val'}}
    regroup = {'_id': None,
               'timestamp': {'$last': '$timestamp'},
               'by': {'$last': '$by'},
               'val': {'$sum': '$val'}}
    if (segment == config.primary_segment and len(config.segment_map[config.primary_segment]) > 1) or (len(comp_periods)>1):
        pipeline = [{'$match': match},
                    {'$sort': sort},
                    {'$group': group},
                    {'$sort': sort},
                    {'$group': regroup}]
    else:
        pipeline = [{'$match': match},
                    {'$sort': sort},
                    {'$group': group},
                    {'$group': regroup}]

    if FM_COLL == FM_LATEST_DATA_COLL:
        regroup = {'_id': None,
                   'timestamp': {'$last': '$timestamp'},
                   'by': {'$last': '$by'},
                   'val': {'$sum': '$val'}}
        pipeline = [{'$match': match},
                    {'$group': regroup}]


    if config.debug:
        logger.info('fetch_user_entered_rec pipeline: %s reading from %s', (pipeline, FM_COLL))

    try:
        aggs = list(fm_data_collection.aggregate(pipeline, allowDiskUse=True))
        val, by, ts = aggs[0]['val'], aggs[0]['by'], aggs[0]['timestamp']
    except IndexError:
        val, by, ts, found = 0, 'system', 0, False

    overwritten_val = overwrite_fields(config, field, comp_periods, period, node, timecontext=time_context)
    if overwritten_val is not None:
        val = overwritten_val

    return {'period': period,
            'segment': segment,
            'val': val,
            'by': by,
            'how': how,
            'found': found,
            'timestamp': ts,
            'node': node,
            'field': field}


def _fetch_deal_rollup_rec(time_context,
                           descendants,
                           field,
                           segment,
                           config,
                           timestamp_info,
                           db,
                           cache,
                           call_from=None,
                           year_accepted_from=None,
                           user=None,
                           period_and_close_periods=None
                           ):
    """
    fetch record that is sum of deal amount field for deals matching a filter
    """
    # NOTE: much of this logic is duplicated in _bulk_fetch_deal_rollup_recs
    # if you are changing logic here, be sure to also change in the bulk version of the function
    node, _, _ = descendants
    segments = [segment]
    current_segment_dtls = config.segments[segment]
    period, comp_periods = time_context.fm_period, time_context.component_periods
    timestamp, dlf_expiration, deal_expiration, _ = timestamp_info
    FM_COLL = read_from_collection(period[:4], timestamp=timestamp, field_type='DR', call_from='_fetch_deal_rollup_rec',
                                   config=config, year_accepted_from=year_accepted_from)
    fm_data_collection = db[FM_COLL]
    expiration_timestamp = dlf_expiration if config.fields[field].get('dlf') else deal_expiration
    if timestamp and timestamp < expiration_timestamp:
        query_expiration_timestamp = 0
    else:
        query_expiration_timestamp = expiration_timestamp
    found = True
    # BUG: when rollup updates some but not all of the component periods totals for a node/field combo
    # the updated records will pass the timestamp criteria, but the others will not
    # so we only sum for the ones that got updated, missing the older values
    # this crept up when we stopped saving duping records in Decemberish
    val = None
    config_based_segments_rollup = config.config_based_segments_rollup
    use_all_deals_for_past_data_rollup = config.use_all_deals_for_past_data_rollup
    is_pivot_special = node and node.split('#')[0] in config.deal_config.not_deals_tenant.get('special_pivot', [])

    segments_sum_field = []
    if segment == 'all_deals':
        segments = []
        # Checking if segments are present or not
        try:
            as_of = timestamp if timestamp else get_period_as_of(period)
            if timestamp and use_all_deals_for_past_data_rollup:
                segments = [segment]
            else:
                for x in config.rollup_segments:
                    if node_aligns_to_segment(as_of, node, x, config) and x not in segments:
                        segments.append(x)
        except:
            segments = config.rollup_segments

        # for checking if segments have same sum_field or different for rollup.
        for seg in segments:
            sum_field = config.segment_amount_fields.get(seg, config.fields[field]['sum_field'])
            if sum_field not in segments_sum_field:
                segments_sum_field.append(sum_field)

    is_dlf_field = config.deal_rollup_fields[field].get('dlf', False)
    field_filters = config.deal_rollup_fields[field]['filter']
    if not is_dlf_field:
        for filt in field_filters:
            is_dlf_field = is_dlf_field or (filt.get('op') == 'dlf' if isinstance(filt, dict) else False)

    if is_dlf_field and call_from == 'UploadStatus' and field in config.fm_status_fields:
        is_dlf_field = False
    # If sum_field_override is been used in the segment config then we are overrding the field and fetching it directly from deals
    ts = None
    if 'sum_field_override' in current_segment_dtls and field in current_segment_dtls['sum_field_override'] \
            and not (timestamp and not is_same_day(EpochClass(), epoch(timestamp))):
        val = None
    elif segment == 'all_deals' and len(segments_sum_field) > 1 and \
            not config_based_segments_rollup and \
            not config.config.get("segments", {}).get("rollup_segments", {}) and \
            (not timestamp or (timestamp and is_same_day(EpochClass(), epoch(timestamp)))):
        val = None
    elif (timestamp and not is_same_day(EpochClass(), epoch(timestamp))) or (len(period) == 4 and \
                                                                             config.read_from_fm_collection_for_year):
        if timestamp and not is_same_day(EpochClass(), epoch(timestamp)) \
                and len(segments) > 1 and segment == 'all_deals':
            segments = [seg for seg in segments if seg != 'all_deals'] if not is_pivot_special else segments
        if config.fields[field].get('is_cumulative', False) and len(comp_periods) > 1:
            comp_periods = [max(comp_periods)]
        match = {'period': {'$in': comp_periods},
                 'node': node,
                 'segment': {'$in': segments},
                 'field': field}
        if timestamp is not None:
            # for view past forecast, when query timestamp is end of quarter and we check for end quarter \
            # we don't need to pass greater than condition
            if 'Q' not in period:
                end_timestamp = get_eom(epoch(timestamp))
            else:
                period_info = time_context.period_info
                end_timestamp = get_eoq_for_period(period, period_info)
            if query_expiration_timestamp and str(epoch(int(query_expiration_timestamp)))[:19] ==  str(end_timestamp)[:19]:
                match['timestamp'] = {}
            if 'timestamp' not in match:
                match['timestamp'] = {}
            match['timestamp'].update({'$lte': timestamp})
        sort = {'timestamp': 1}
        group = {'_id': {'period': '$period',
                         'node': '$node',
                         'segment': '$segment',
                         'field': '$field'},
                 'val': {'$last': '$val'},
                 'timestamp': {'$last': '$timestamp'}}
        regroup = {'_id': None,
                   'val': {'$sum': '$val'},
                   'timestamp': {'$last': '$timestamp'}}
        if call_from == "UploadStatus" and is_pivot_special:
            additional_fields = {'by': {'$last': '$by'}, 'how': {'$last': '$how'}}
            group.update(additional_fields)
            regroup.update(additional_fields)

        pipeline = [{'$match': match},
                    {'$sort': sort},
                    {'$group': group},
                    {'$group': regroup}]
        if FM_COLL == FM_LATEST_COLL:
            regroup = {'_id': None,
                       'val': {'$sum': '$val'},
                       'timestamp': {'$last': '$timestamp'}}
            pipeline = [{'$match': match},
                        {'$group': regroup}]

        if config.debug:
            logger.info('fetch_deal_rollup_rec pipeline: %s from %s' %  (pipeline, FM_COLL))

        try:
            aggs = list(fm_data_collection.aggregate(pipeline, allowDiskUse=True))
            val = aggs[0]['val']
            ts = aggs[0]['timestamp']
            if call_from == "UploadStatus" and is_pivot_special:
                by = aggs[0]['by']
                how = aggs[0]['how']
        except IndexError:
            # make value 0 to avoid reading from deals collection in case of past date.
            if timestamp and not is_same_day(EpochClass(), epoch(timestamp)):
                val = 0
            else:
                val = None

    if val is None:
        sum_field = config.segment_amount_fields.get(segment, config.fields[field]['sum_field'])
        seg_filter = config.segment_filters.get(segment)
        name, crit, ops = config.fields[field]['crit_and_ops']
        is_cumulative_field = config.fields[field].get('is_cumulative', False)
        close_periods = time_context.close_periods if not is_cumulative_field else time_context.cumulative_close_periods
        crit = {'$and': [crit, seg_filter]}
        if segment and segment != 'all_deals' and sum_field != config.fields[field]['sum_field']:
            ops = [(sum_field, sum_field, '$sum')]
        # Overriding the segment field with the overrided field in config
        try:
            if len(period) == 4:
                period_and_close_periods = time_context.period_and_close_periods_for_year or \
                                           get_period_and_close_periods(period, False, deal_svc=True)
            else:
                period_and_close_periods = [(time_context.deal_period, time_context.close_periods)]
        except Exception as e:
            logger.exception(e)
        if 'sum_field_override' in current_segment_dtls and field in current_segment_dtls['sum_field_override']:
            if segment != "all_deals":
                ovveride_field = current_segment_dtls['sum_field_override'][field]
                name, crit, ops = config.fields[ovveride_field]['crit_and_ops']
                sum_field = config.fields[ovveride_field]['sum_field']
                crit = {'$and': [crit, seg_filter]}
            else:
                total_value = 0
                # If sum_field_override in all_deals segment, then we are overriding the field with sum of all the segment values for that field.
                for seg,segment_dtls in config.segments.items():
                    if seg != "all_deals":
                        seg_filter = config.segment_filters.get(seg)
                        ovveride_field = segment_dtls['sum_field_override'][field] if 'sum_field_override' in segment_dtls else field
                        name, crit, ops = config.fields[ovveride_field]['crit_and_ops']
                        sum_field = config.fields[ovveride_field]['sum_field'] if 'sum_field_override' in segment_dtls else segment_dtls['sum_field']
                        crit = {'$and': [crit, seg_filter]}
                        resp = fetch_deal_totals(period_and_close_periods,
                                                node,
                                                ops,
                                                config.deal_config,
                                                filter_criteria=crit,
                                                timestamp=time_context.deal_timestamp,
                                                db=db,
                                                cache=cache,
                                                prev_periods=time_context.prev_periods,
                                                user=user,
                                                sfdc_view=True if len(period) == 4 else False,
                                                actual_view=True if len(period)== 4 else False
                                                )
                        val = resp.get(sum_field, 0)
                        if (ts and ts < resp.get("timestamp", 0)) or not ts:
                            ts = resp.get("timestamp")
                        total_value += len(val) if isinstance(val,list) else val
                res = {'period': period,
                        'segment': segment,
                        'val': total_value,
                        'by': 'system',
                        'how': 'deal_rollup',
                        'found': found,
                        'timestamp': ts,
                        'node': node,
                        'field': field}
                if cache is not None:
                    cache[(period, node, field, segment, timestamp)] = res
                return res
        if config.debug:
            logger.info('fetch_deal_rollup_rec field: %s, deal svc crit: %s, ops: %s, ts: %s, exp_ts: %s, sum field %s',
                        field, crit, ops, timestamp, expiration_timestamp, sum_field)
        # looking for a historic record that doesnt exist, maybe still a bug though
        # TODO: this needs a unit test

        try:
            if segment != "all_deals" or\
                    (segment == "all_deals" and (not segments or \
                                                 (len(segments) == 1 and segments[0] == 'all_deals') or\
                                                 (len(segments_sum_field) > 1 and \
                                                  not config_based_segments_rollup and \
                                                  not config.config.get("segments", {}).get("rollup_segments", {})))):
                pivot_node = node.split("#")[0]
                if pivot_node == 'CRR':
                    resp = fetch_crr_deal_totals(period_and_close_periods,
                                                 node,
                                                 ops,
                                                 config.deal_config,
                                                 filter_criteria=crit,
                                                 timestamp=time_context.deal_timestamp,
                                                 db=db,
                                                 cache=cache
                                                 )
                else:
                    resp = fetch_deal_totals(period_and_close_periods,
                                        node,
                                        ops,
                                        config.deal_config,
                                        filter_criteria=crit,
                                        timestamp=time_context.deal_timestamp,
                                        db=db,
                                        cache=cache,
                                        prev_periods=time_context.prev_periods,
                                        user=user,
                                        sfdc_view=True if len(period) == 4 else False,
                                        actual_view=True if len(period) == 4 else False
                                        )
                val = resp.get(sum_field, 0)
                ts = resp.get("timestamp")
            else:
                total_value = 0
                for seg, segment_dtls in config.segments.items():
                    if seg != "all_deals" and seg in segments:
                        seg_filter = config.segment_filters.get(seg)
                        overide_field = segment_dtls['sum_field_override'][
                            field] if 'sum_field_override' in segment_dtls and \
                                      field in segment_dtls['sum_field_override'] else field
                        name, crit, ops = config.fields[overide_field]['crit_and_ops']
                        sum_field_ = config.segment_amount_fields.get(seg, config.fields[overide_field]['sum_field'])
                        sum_field = config.fields[overide_field][
                            'sum_field'] if 'sum_field_override' in segment_dtls and \
                                            overide_field in segment_dtls[
                                                'sum_field_override'] else sum_field_
                        if sum_field != config.fields[overide_field]['sum_field']:
                            ops = [(sum_field, sum_field, '$sum')]
                        crit = {'$and': [crit, seg_filter]}
                        resp = fetch_deal_totals(period_and_close_periods,
                                                node,
                                                ops,
                                                config.deal_config,
                                                filter_criteria=crit,
                                                timestamp=time_context.deal_timestamp,
                                                db=db,
                                                cache=cache,
                                                prev_periods=time_context.prev_periods,
                                                user=user,
                                                sfdc_view=True if len(period) == 4 else False,
                                                actual_view=True if len(period) == 4 else False
                                                )
                        val = resp.get(sum_field, 0)
                        if (ts and ts < resp.get("timestamp", 0)) or not ts:
                            ts = resp.get("timestamp")
                        total_value += len(val) if isinstance(val, list) else val
                res = {'period': period,
                       'segment': segment,
                       'val': total_value,
                       'by': 'system',
                       'how': 'deal_rollup',
                       'found': found,
                       'timestamp': ts,
                       'node': node,
                       'field': field}
                if cache is not None:
                    cache[(period, node, field, segment, timestamp)] = res
                    return res
        except:
            val, found = 0, False

    overwritten_val = overwrite_fields(config, field, comp_periods, period, node, timecontext=time_context)
    if overwritten_val is not None:
        val = overwritten_val

    if call_from == "UploadStatus" and is_pivot_special:
        res = {'period': period,
               'segment': segment,
               'val': len(val) if isinstance(val,list) else val,
               'by': by,
               'how': how,
               'found': found,
               'timestamp': ts,
               'node': node,
               'field': field}
    else:
        res = {'period': period,
                'segment': segment,
                'val': len(val) if isinstance(val,list) else val,
                'by': 'system',
                'how': 'deal_rollup',
                'found': found,
                'timestamp': ts,
                'node': node,
                'field': field}
    if cache is not None:
        cache[(period, node, field, segment, timestamp)] = res
    return res


def overwrite_fields(_config, _field, _comp_periods, _period, _nodes, timecontext=None):
    # -- AV-7725 --
    overwrite_field = None
    pipeline = None
    period_in_past = False
    try:
        if 'Q' in _period and all(elem == 'h' for elem in timecontext.relative_periods):
            period_in_past = True
        elif 'h' in timecontext.relative_periods:
            period_in_past = True
            if not all(elem == 'h' for elem in timecontext.relative_periods):
                period_in_past = False
        if _config.fields[_field].get('overwrite_field') and period_in_past:
            # HINT: overwrite_field = 'ACT_CRR.value'
            overwrite_field = str(_config.fields[_field].get('overwrite_field'))
            _col = sec_context.tenant_db[GBM_CRR_COLL]
            if not isinstance(_nodes, list):
                _nodes = [_nodes]
            match = {'monthly_period': {'$in': _comp_periods},
                     '__segs': {'$in': _nodes}}
            projection = {'_id': 0, overwrite_field: 1}
            group = {'_id': 0, _field: {"$sum": "$" + overwrite_field}}
            pipeline = [{'$match': match},
                        {'$project': projection},
                        {'$group': group}]
            aggs = list(_col.aggregate(pipeline, allowDiskUse=True))
            val = aggs[0][_field] if aggs else 0
            # AV-14059 Commenting below as part of the log fix
            # logger.info(
            #     "overwriting {} with {} for period {} pipeline {}".format(_field,
            #                                                               overwrite_field,
            #                                                               _period,
            #                                                               pipeline))
            return val
    except Exception as _err:
        logger.exception(
            "Failed overwriting {} with {} for period {} {} {}".format(_field, overwrite_field, _period, pipeline,
                                                                       _err))
    # -x- AV-7725 -x-

def _bulk_fetch_formula_recs(time_context,
                              descendants_list,
                              fields,
                              segment,
                              config,
                              timestamp_info,
                              db,
                              cache,
                              main_cache=None):
    """
    fetch many user entered records for multiple nodes and fields
    returns nothing, just puts records in cache
    """
    # NOTE: much of this logic is duplicated in _fetch_formula_rec
    # if you are changing logic here, be sure to also change in the non bulk version of the function
    year_accepted_from = 0
    try:
        t = sec_context.details
        year_accepted_from = int(t.get_flag('fm_latest_migration', 'year', 0))
    except:
        year_accepted_from = 0
    for descendants, field in product(descendants_list, fields):
        _fetch_formula_rec(time_context,
                           descendants,
                           field,
                           segment,
                           config,
                           timestamp_info,
                           db,
                           cache,
                           year_accepted_from=year_accepted_from,
                           main_cache=main_cache)
    return cache

def _fetch_formula_rec(time_context,
                       descendants,
                       field,
                       segment,
                       config,
                       timestamp_info,
                       db,
                       cache,
                       year_accepted_from=None,
                       main_cache=None
                       ):
    """
    fetch record that is result of a specified function applied to other fm fields
    """
    node, _, _ = descendants
    period, comp_periods = time_context.fm_period, time_context.component_periods
    source_fields = config.fields[field]['source']
    timestamp, _, _, _ = timestamp_info

    threads = []
    if cache is None:
        cache = {}

    for source_field in source_fields:
        ch_key = (period, node, source_field, segment, timestamp)
        if ch_key not in cache and ((main_cache and ch_key not in main_cache) or main_cache is None or not main_cache):
            t = threading.Thread(target=fetch_fm_rec,
                                 args=(time_context, descendants, source_field, segment,
                                       config, timestamp_info, db, cache, None,
                                           None,
                                           year_accepted_from))
            threads.append(t)
            t.start()
    for t in threads:
        t.join()

    # source is actually used as a local variable by the eval
    source = []
    ts = None
    for source_field in source_fields:
        val = cache.get((period, node, source_field, segment, timestamp), {'val': 0}).get('val')
        ts_ = cache.get((period, node, source_field, segment, timestamp), {'timestamp': 0}).get('timestamp', 0)
        field_type = config.fields[source_field]['type']
        by = 'system'
        if field_type == 'UE':
            by = cache.get((period, node, source_field, segment, timestamp), {'by': 'system'}).get('by', 'system')
        by_ = 'system'
        if val == 0 and main_cache:
            val = main_cache.get((period, node, source_field, segment, timestamp), {'val': 0}).get('val')
            ts_ = main_cache.get((period, node, source_field, segment, timestamp), {'timestamp': 0}).get('timestamp', 0)
            if field_type == 'UE':
                by_ = main_cache.get((period, node, source_field, segment, timestamp), {'by': 'system'}).get('by', 'system')
        source.append(val)
        if (ts and ts < ts_) or not ts:
            ts = ts_
            by = by_
    if config.debug:
        logger.info('fetch_formula_rec field: %s source : %s node %s', field, source, node)
    try:
        val = eval(config.fields[field]['func'])
    except:
        val = 0

    if 'or' in config.fields[field]['func']:
        source = []
        for source_field in source_fields:
            field_type = config.fields[source_field]['type']
            by = cache.get((period, node, source_field, segment, timestamp), {'by': 'system'}).get('by', 'system')
            source.append(by)
        try:
            by = eval(config.fields[field]['func'])
        except:
            by = 'system'

    res = {'period': period,
           'segment': segment,
           'val': val,
           'by': by,
           'how': 'formula',
           'found': True,
           'timestamp': ts,
           'node': node,
           'field': field}
    if cache is not None:
        cache[(period, node, field, segment, timestamp)] = res
    return res

def _fetch_ai_rec(time_context,
                  descendants,
                  field,
                  segment,
                  config,
                  timestamp_info,
                  db,
                  cache,
                  year_accepted_from=None
                  ):
    """
    fetch record that is generated from aviso predictions
    """
    # NOTE: much of this logic is duplicated in _bulk_fetch_ai_recs
    # if you are changing logic here, be sure to also change in the bulk version of the function
    # TODO: think, make sure this is safe
    node, _, _ = descendants
    period, comp_periods = time_context.fm_period, time_context.component_periods
    timestamp, _, _, cutoff_timestamp = timestamp_info
    FM_COLL = read_from_collection(period[:4], timestamp=timestamp, call_from='_fetch_ai_rec', config=config,
                                   year_accepted_from=year_accepted_from)
    fm_data_collection = db[FM_COLL]
    found = True

    # NOTE: taking a step backwards here until we have augustus monthly
    # ai numbers are just taken straight from gbm, and are no longer the sum of component periods
    # this might cause some inconsistencies for tenants with monthly predictions, so we should be cautious here
    # Match query
    match = {'period': {'$in': [period]},
             'node': node,
             'field': field}

    # Check if the segment has a 'segment_func' in config
    segment_config = config.segments.get(segment, None)

    if segment_config and 'segment_func' in segment_config:
        # Segment has a segment_func, so we use its 'source' list for aggregation
        segments = segment_config['segment_func']['source']
        match['segment'] = {"$in": segments}  # Include all relevant sub-segments
    else:
        # No segment_func, process as a single segment
        match['segment'] = segment

    if timestamp is not None:
        match['timestamp'] = {'$lte': timestamp}
    if cutoff_timestamp is not None:
        # BUG: this doesnt work with comp periods
        match['timestamp']['$gt'] = cutoff_timestamp
    sort = {'timestamp': -1}
    group = {'_id': {'period': '$period',
                     'node': '$node',
                     'field': '$field',
                     'segment': '$segment'},
             'timestamp': {'$last': '$timestamp'},
             'by': {'$last': '$by'},
             'val': {'$first': '$val'}}
    regroup = {'_id': None,
               'timestamp': {'$last': '$timestamp'},
               'by': {'$last': '$by'},
               'val': {'$sum': '$val'}}
    pipeline = [{'$match': match},
                {'$sort': sort},
                {'$group': group},
                {'$group': regroup}]
    if FM_COLL == FM_LATEST_DATA_COLL:
        regroup = {'_id': None,
                   'timestamp': {'$last': '$timestamp'},
                   'by': {'$last': '$by'},
                   'val': {'$sum': '$val'}}
        pipeline = [{'$match': match},
                    {'$group': regroup}]

    if config.debug:
        logger.info('fetch_ai_rec pipeline: %s', pipeline)

    try:
        aggs = list(fm_data_collection.aggregate(pipeline, allowDiskUse=True))
        val, by, ts = aggs[0]['val'], aggs[0]['by'], aggs[0]['timestamp']
    except IndexError:
        val, by, ts, found = 0, 'system', timestamp, False

    return {'period': period,
            'segment': segment,
            'val': val,
            'by': by,
            'how': 'ai',
            'found': found,
            'timestamp': ts,
            'node': node,
            'field': field}


def _fetch_node_conditional_rec(time_context,
                                descendants,
                                field,
                                segment,
                                config,
                                timestamp_info,
                                db,
                                cache,
                                year_accepted_from=None
                                ):
    """
    fetch record that is either a mgr field or a rep field depending on node
    """
    node, children, _ = descendants
    timestamp, _, _, _ = timestamp_info

    mgr_field, rep_field = config.fields[field]['source']

    if not children:  # TODO: think and make sure this is guaranteed
        return fetch_fm_rec(time_context,
                            descendants,
                            rep_field,
                            segment,
                            config,
                            timestamp_info,
                            db=db,
                            cache=cache,
                            year_accepted_from=year_accepted_from
                            )

    return fetch_fm_rec(time_context,
                        descendants,
                        mgr_field,
                        segment,
                        config,
                        timestamp_info,
                        db=db,
                        cache=cache,
                        year_accepted_from=year_accepted_from
                        )

def _bulk_fetch_child_sum_rec(time_context,
                              descendants_list,
                              fields,
                              segment,
                              config,
                              timestamp_info,
                              db,
                              cache,
                              round_val=True,
                              main_cache=None):
    """
    fetch many child sum records for multiple nodes and fields
    returns nothing, just puts records in cache
    """
    # NOTE: much of this logic is duplicated in _fetch_formula_rec
    # if you are changing logic here, be sure to also change in the non bulk version of the function
    if main_cache is None:
        main_cache = {}

    def get_actioned_user_and_timestamp(ch_key, latest_timestamp, by_val):
        """
        :param ch_key:
        :param latest_timestamp:
        :param by_val:
        :return:

        This functionality will be useful to retrive latest timestamp of FM Status Record and the user who did the activity.
        """
        cs_timestamp = get_nested(cache, [ch_key, 'timestamp'], 0) or get_nested(main_cache, [ch_key, 'timestamp'], 0)
        if cs_timestamp > latest_timestamp:
            latest_timestamp = cs_timestamp
        cs_actioned_user = get_nested(cache, [ch_key, 'by'], "system") or get_nested(main_cache, [ch_key, 'by'], "system")
        if cs_actioned_user != "system":
            by_val = cs_actioned_user

        return latest_timestamp, by_val

    period, comp_periods = time_context.fm_period, time_context.submission_component_periods
    timestamp, _, _, _ = timestamp_info
    for descendants, field in product(descendants_list, fields):
        node, children, grandchildren = descendants
        source_fields = config.fields[field]['source']
        val = 0
        latest_timestamp = 0
        by_val = 'system'
        if not children:
            for source_field in source_fields:
                field_type = config.fields[source_field]['type']
                if field_type == 'NC':
                    mgr_field, rep_field = config.fields[source_field]['source']
                    ch_key = (period, node, rep_field, segment, timestamp)
                    val = try_float(get_nested(cache, [ch_key, 'val'], 0)) or try_float(get_nested(main_cache, [ch_key, 'val'], 0))
                    latest_timestamp, by_val = get_actioned_user_and_timestamp(ch_key, latest_timestamp, by_val)
                else:
                    ch_key = (period, node, source_field, segment, timestamp)
                    val = try_float(get_nested(cache, [ch_key, 'val'], 0)) or try_float(get_nested(main_cache, [ch_key, 'val'], 0))
                    latest_timestamp, by_val = get_actioned_user_and_timestamp(ch_key, latest_timestamp, by_val)
        else:
            for source_field, child in product(source_fields, children):
                field_type = config.fields[source_field]['type']
                if field_type == 'NC':
                    grandkids = [grandkid for grandkid, parent in grandchildren.iteritems() if parent == child]
                    mgr_field, rep_field = config.fields[source_field]['source']
                    if grandkids:
                        ch_key = (period, child, mgr_field, segment, timestamp)
                        val += try_float(get_nested(cache, [ch_key, 'val'], 0)) or try_float(get_nested(main_cache, [ch_key, 'val'], 0))
                        latest_timestamp, by_val = get_actioned_user_and_timestamp(ch_key, latest_timestamp, by_val)
                    else:
                        ch_key = (period, child, rep_field, segment, timestamp)
                        val += try_float(get_nested(cache, [ch_key, 'val'], 0)) or try_float(get_nested(main_cache, [ch_key, 'val'], 0))
                        latest_timestamp, by_val = get_actioned_user_and_timestamp(ch_key, latest_timestamp, by_val)
                else:
                    ch_key = (period, child, source_field, segment, timestamp)
                    val += try_float(get_nested(cache, [(period, child, source_field, segment, timestamp), 'val'], 0)) or \
                           try_float(get_nested(main_cache, [(period, child, source_field, segment, timestamp), 'val'], 0))
                    latest_timestamp, by_val = get_actioned_user_and_timestamp(ch_key, latest_timestamp, by_val)
        if round_val:
            val = round(val)
        overwritten_val = overwrite_fields(config, field, comp_periods, period, node, timecontext=time_context)
        if overwritten_val is not None:
            val = overwritten_val
        res = {'period': period,
               'segment': segment,
               'val': val,
               'by': by_val,
               'how': 'sum_of_children',
               'found': True if val else False,
               'timestamp': timestamp if timestamp else latest_timestamp,
               'node': node,
               'field': field}
        if cache is not None:
            cache[(period, node, field, segment, timestamp)] = res
    return cache

def _bulk_fetch_child_sum_rec_special(time_context,
                                      descendants_list,
                                      fields,
                                      segment,
                                      config,
                                      timestamp_info,
                                      db,
                                      cache,
                                      round_val=True):
    """
    fetch many child sum records for multiple nodes and fields
    returns nothing, just puts records in cache
    """
    # NOTE: much of this logic is duplicated in _fetch_formula_rec
    # if you are changing logic here, be sure to also change in the non bulk version of the function
    period, comp_periods = time_context.fm_period, time_context.submission_component_periods
    timestamp, _, _, _ = timestamp_info
    try:
        t = sec_context.details
        year_accepted_from = int(t.get_flag('fm_latest_migration', 'year', 0))
    except:
        year_accepted_from = 0
    nodes_all = []
    for descendants, field in product(descendants_list, fields):
        node, children, grandchildren = descendants
        nodes_all += children if not config.fields[field].get('grandkids') else grandchildren
    nodes_and_eligible_segs_len = {}
    if nodes_all and config.partially_segmented:
        for node in nodes_all:
            nodes_and_eligible_segs_len[node] = len(config.get_segments(epoch().as_epoch(), node))

    for descendants, field in product(descendants_list, fields):
        node, _, _ = descendants
        res = _fetch_child_sum_rec(time_context,
                                   descendants,
                                   field,
                                   segment,
                                   config,
                                   timestamp_info,
                                   db,
                                   cache,
                                   year_accepted_from=year_accepted_from,
                                   nodes_and_eligible_segs_len=nodes_and_eligible_segs_len)
        if round_val:
            res['val'] = round(res['val'])
        if cache is not None:
            cache[(period, node, field, segment, timestamp)] = res
    return cache


def _fetch_child_sum_rec(time_context,
                         descendants,
                         field,
                         segment,
                         config,
                         timestamp_info,
                         db,
                         cache,
                         year_accepted_from=None,
                         nodes_and_eligible_segs_len=None
                         ):
    """
    fetch record that is the sum of childrens fm fields
    """
    node_parent, children, grandchildren = descendants
    segments = config.segment_map[segment]
    period, comp_periods = time_context.fm_period, time_context.submission_component_periods
    source_fields = config.fields[field]['source']
    timestamp, _, _, cutoff_timestamp = timestamp_info
    found = True

    node_children_check = {node: any(node_children) for node, node_children, _ in [descendants]}

    for child in children:
        grandkids = {grandkid: parent for grandkid, parent in grandchildren.iteritems()
                     if parent == child}
        node_children_check[child] = any(grandkids)

    field_type = config.fields[source_fields[0]]['type']
    if 'hist_field' in config.fields[source_fields[0]] and 'h' in time_context.relative_periods and not config.show_raw_data_in_trend:
        # historic period, switch to using true up field if it exists
        field_type = 'PC'
        if config.quarter_editable:
            if not all(elem == 'h' for elem in time_context.relative_periods):
                field_type = config.fields[source_fields[0]]['type']


    if year_accepted_from is None:
        try:
            t = sec_context.details
            year_accepted_from = int(t.get_flag('fm_latest_migration', 'year', 0))
        except:
            year_accepted_from = 0

    if field_type != 'PC' and all(source_field in config.user_entered_fields for source_field in source_fields):
        if segment == 'all_deals':
            segments = [segment for segment in config.segments]
        else:
            segments = config.segment_map[segment]
        nodes = children if not config.fields[field].get('grandkids') else grandchildren
        val = sum([try_float(get_nested(cache, [(period, child, source_field, segment, timestamp), 'val'], 0)) for
                   source_field, child in product(source_fields, nodes)])
        if val:
            return {'period': period,
                    'segment': segment,
                    'val': val,
                    'by': 'system',
                    'how': 'sum_of_children',
                    'found': found,
                    'timestamp': timestamp,
                    'node': node_parent,
                    'field': field}

        FM_COLL = read_from_collection(period[:4], timestamp=timestamp, call_from='_fetch_child_sum_rec', config=config,
                                       year_accepted_from=year_accepted_from)
        fm_data_collection = db[FM_COLL]
        by_val = 'system'
        match = {'period': {'$in': comp_periods},
                 'segment': {'$in': segments},
                 'node': {'$in': nodes.keys() if nodes else [node_parent]},
                 'field': {'$in': config.fields[field]['source']}}
        if timestamp is not None:
            match['timestamp'] = {'$lte': timestamp}
        sort = {'timestamp': 1}
        group = {'_id': {'period': '$period',
                         'node': '$node',
                         'segment': '$segment',
                         'field': '$field'},
                 'val': {'$last': '$val'},
                 'by': {'$last': '$by'},
                 'timestamp': {'$last': '$timestamp'}}
        regroup = {'_id': {'node': '$_id.node',
                           'field': '$_id.field'},
                   'val': {'$sum': '$val'},
                   'by': {'$last': '$by'},
                   'timestamp': {
                           '$last': '$timestamp'
                        },
                   'comments':{'$last': '$comments'}
                   }

        group['_id']['segment'] = '$segment'
        regroup['_id']['segment'] = '$_id.segment'
        pipeline = [{'$match': match},
                    {'$sort': sort},
                    {'$group': group},
                    {'$sort': sort},
                    {'$group': regroup}]
        if FM_COLL == FM_LATEST_DATA_COLL:
            regroup = {'_id': {'node': '$node',
                               'field': '$field'},
                       'val': {'$sum': '$val'},
                       'by': {'$last': '$by'},
                       'timestamp': {
                           '$last': '$timestamp'
                       }}
            regroup['_id']['segment'] = '$segment'
            pipeline = [{'$match': match},
                        {'$group': regroup}]
        if config.debug:
            logger.info('fetch child sum pipeline: %s from %s' % (pipeline, FM_COLL))

        recs = fm_data_collection.aggregate(pipeline, allowDiskUse=True)

        db_recs = {(rec['_id']['node'], rec['_id']['field'], rec['_id']['segment']): rec
                   for rec in recs}
        rollup_segments = config.rollup_segments
        if nodes_and_eligible_segs_len is None:
            nodes_and_eligible_segs_len = {}
            if config.partially_segmented:
                for node_ in nodes:
                    nodes_and_eligible_segs_len[node_] = len(config.get_segments(epoch().as_epoch(), node_))

        res_val = 0
        res_timestamp = 0
        for node_ in set(nodes):
            aggregated_val = 0
            len_of_segments = len(segments)
            # if the node has only one segment, aggregated_val condition should be passed. THis is in case the tenant has segments,
            # but the current node doesn't have segments
            if len_of_segments == 1 and 'all_deals' in segments:
                # value in all_deals is summation of segments. Hence all segments need to be looped through
                # for non-segmented tenants ['all_deals'] will be retained by default
                segments = [segment_ for segment_ in config.segments]
                len_of_segments = len(segments)
            len_of_eligible_segments = nodes_and_eligible_segs_len.get(node_)
            segment_values_dict = {}
            for segment_ in segments:
                val = get_nested(db_recs, [(node_, config.fields[field]['source'][0], segment_), 'val'], 0)
                overwritten_val = overwrite_fields(config, config.fields[field]['source'][0], comp_periods, period, node_, timecontext=time_context)
                if overwritten_val is not None:
                    val = overwritten_val
                if ((not node_children_check[node_]) or (len_of_segments == 1) or (len_of_eligible_segments == 1) or (len_of_segments > 1 and segment_ != "all_deals")) and segment_ in rollup_segments:
                    aggregated_val += val
                new_timestamp = get_nested(db_recs, [(node_, config.fields[field]['source'][0], segment), 'timestamp'], 0)
                if res_timestamp is None or res_timestamp < new_timestamp:
                    res_timestamp = get_nested(db_recs, [(node_, config.fields[field]['source'][0], segment), 'timestamp'], 0)
                if not ('all_deals' in segments and config.is_special_pivot_segmented(node_)):
                    res_val += val
                segment_values_dict[segment_] = val

            if 'all_deals' in segments and config.is_special_pivot_segmented(node_):
                prev_val = segment_values_dict.get("all_deals", 0)
                val = 0.0 if (config.segment_percentage_rollup and config.fields[config.fields[field]['source'][0]].get('format') == 'percentage') else aggregated_val

                if (prev_val is not None and config.fields[field]['source'][0] in config.forecast_service_editable_fields):
                    val = prev_val
                res_val += val
        return {'period': period,
                'segment': segment,
                'val': res_val,
                'by': 'system',
                'how': 'sum_of_children',
                'found': found,
                'timestamp': timestamp if timestamp is not None else res_timestamp,
                'node': node_parent,
                'field': field}

    threads = []
    if cache is None:
        cache = {}

    #CS-9233 if the node does not have children, get the value of source field for the node itself
    if not children:
        return fetch_fm_rec(time_context,
                     descendants,
                     config.fields[field]['source'][0],
                     segment,
                     config,
                     timestamp_info,
                     db=db,
                     cache=cache,
                     year_accepted_from=year_accepted_from
                     )


    for source_field, child in product(source_fields, children):
        child_descendants = (child, {grandkid: parent for grandkid, parent in grandchildren.iteritems()
                                     if parent == child}, {})
        t = threading.Thread(target=fetch_fm_rec,
                             args=(time_context,
                                   child_descendants,
                                   source_field,
                                   segment,
                                   config,
                                   timestamp_info,
                                   db,
                                   cache,
                                   None,
                                   None,
                                   year_accepted_from
                                   ))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    val = sum([try_float(get_nested(cache, [(period, child, source_field, segment, timestamp), 'val'], 0)) for
               source_field, child in product(source_fields, children)])

    return {'period': period,
            'segment': segment,
            'val': val,
            'by': 'system',
            'how': 'sum_of_children',
            'found': found,
            'timestamp': timestamp,
            'node': node_parent,
            'field': field}


def _bulk_period_conditional_rec(time_context,
                              descendants_list,
                              fields,
                              segment,
                              config,
                              timestamp_info,
                              db,
                              cache,
                              round_val=True):
    """
    fetch many child sum records for multiple nodes and fields
    returns nothing, just puts records in cache
    """
    # NOTE: much of this logic is duplicated in _fetch_formula_rec
    # if you are changing logic here, be sure to also change in the non bulk version of the function
    period, comp_periods = time_context.fm_period, time_context.submission_component_periods
    timestamp, _, _, _ = timestamp_info
    try:
        t = sec_context.details
        year_accepted_from = int(t.get_flag('fm_latest_migration', 'year', 0))
    except:
        year_accepted_from = 0
    for descendants, field in product(descendants_list, fields):
        node, _, _ = descendants
        res = _fetch_period_conditional_rec(time_context,
                                   descendants,
                                   field,
                                   segment,
                                   config,
                                   timestamp_info,
                                   db,
                                   cache,
                                   year_accepted_from=year_accepted_from)
        if round_val:
            res['val'] = round(res['val'])
        if cache is not None:
            cache[(period, node, field, segment, timestamp)] = res
    return cache

def _fetch_period_conditional_rec(time_context,
                                  descendants,
                                  field,
                                  segment,
                                  config,
                                  timestamp_info,
                                  db,
                                  cache,
                                  year_accepted_from=None
                                  ):
    """
    fetch record that is either a regular field or historic field depending on if period is historic
    """
    node, _, _ = descendants
    period, comp_periods = time_context.fm_period, time_context.component_periods
    relative_periods = time_context.relative_periods
    timestamp, _, _, _ = timestamp_info
    hist_field = config.fields[field]['hist_field']
    period_info = time_context.period_info
    all_quarters = time_context.all_quarters
    if year_accepted_from is None:
        try:
            t = sec_context.details
            year_accepted_from = int(t.get_flag('fm_latest_migration', 'year', 0))
        except:
            year_accepted_from = 0

    def get_comp_and_fields_condition(comp_period):
        timestamp_condtion = False
        if not timestamp:
            #timestamp none means, get me the last value in that quarter, i.e get me the value on the eoq date
            timestamp_condtion = True
        else:
            #when the timestamp is not None, we check if the timestamp is the eoq timestamp.
            #If it is the eoq timestamp and rel_period is 'h', we use the hist field, otherwise we don't.
            # [:19] because some epochs have milliseconds and some don't
            epoch_timestamp = epoch(timestamp)
            if ((len(comp_periods) > 1 or 'Q' in comp_period)) and (get_eoq_for_period(period, period_info)[:19] == str(epoch_timestamp)[:19]):
                if str(get_eom(epoch_timestamp))[:19] == str(epoch_timestamp)[:19]:
                    timestamp_condtion = False
                else:
                    timestamp_condtion = True
            elif 'Q' not in comp_period:
                component_periods_boundries = time_context.component_periods_boundries
                eom_timestamp = component_periods_boundries[comp_period]['end']
                # For a monthly tenant, suppose the months are may, june and july
                # we want to check quarterly view as of 20th july, so, we will get hist_field for may and june, and field for july
                # then eom_timestamp of may will be lesser than 20th july i.e. int(eom_timestamp) <= int(timestamp)
                # if we are checking for july month
                if int(eom_timestamp) <= int(timestamp) or str(epoch(eom_timestamp))[:19] == str(epoch_timestamp)[:19]:
                    timestamp_condtion = True
        return timestamp_condtion

    comps_and_fields = [(comp_period, hist_field if rel_period == 'h' and get_comp_and_fields_condition(comp_period) else field)
                        for comp_period, rel_period in zip(comp_periods, relative_periods)]

    hist_field_config = config.fields.get(hist_field, {})
    hist_field_is_cumulative = hist_field_config.get('is_cumulative', False)
    if hist_field_is_cumulative and len(comps_and_fields) > 1:
        updated_comp_period = ""
        updated_comp_field = ""
        for i, (comp_period, comp_field) in enumerate(comps_and_fields):
            if comp_period > updated_comp_period:
                updated_comp_period = comp_period
                updated_comp_field = comp_field

        comps_and_fields = [(updated_comp_period, updated_comp_field)]

    threads = []
    if cache is None:
        cache = {}

    for i, (comp_period, comp_field) in enumerate(comps_and_fields):
        comp_qtr=all_quarters[i]
        # TODO: holy shit this is gross and too much time knowledge in the fm service
        tc = time_context._asdict()
        tc.update({'fm_period': comp_period,
                   'component_periods': [comp_period],
                   'submission_component_periods': [comp_period],
                   'close_periods': [comp_period],
                   'deal_period': comp_qtr,
                   'relative_periods': [],
                   'period_info': period_info})
        comp_context = time_context_tuple(**tc)
        t = threading.Thread(target=fetch_fm_rec,
                             args=(comp_context,
                                   descendants,
                                   comp_field,
                                   segment,
                                   config,
                                   timestamp_info,
                                   db,
                                   cache,
                                   None,
                                   None,
                                   year_accepted_from
                                   ))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    val = sum([try_float(get_nested(cache, [(comp_period, node, comp_field, segment, timestamp), 'val'], 0))
               for comp_period, comp_field in comps_and_fields])
    overwritten_val = overwrite_fields(config, field, comp_periods, period, node, timecontext=time_context)
    if overwritten_val is not None:
        val = overwritten_val
    return {'period': period,
            'segment': segment,
            'val': val,
            'by': 'system',
            'how': 'true_up',
            'found': True,
            'timestamp': timestamp,
            'node': node,
            'field': field}

def _bulk_fetch_user_entered_recs_v2(time_context,
                                  descendants_list,
                                  fields,
                                  segments,
                                  config,
                                  timestamp_info,
                                  db,
                                  cache,
                                  get_all_segments=False,
                                  round_val=True,
                                  found=None,
                                  nodes_and_eligible_segs_len=None,
                                  includefuturetimestamp=False,
                                  exclude_empty=None,
                                  updated_since=None,
                                  skip=None,
                                  limit=None):
        """
            fetch many user entered records for multiple nodes and fields
            returns nothing, just puts records in cache
            """
        # NOTE: much of this logic is duplicated in _fetch_user_entered_rec
        # if you are changing logic here, be sure to also change in the non bulk version of the function
        try:
            found = found if found is not None else False
        except:
            found = False
        try:
            node_children_check = {node: any(node_children) for node, node_children, _ in descendants_list}
            nodes = node_children_check.keys()
            period, comp_periods = time_context.fm_period, time_context.submission_component_periods
            timestamp, _, _, cutoff_timestamp = timestamp_info
            FM_COLL = read_from_collection(period[:4], timestamp=timestamp, call_from='_bulk_fetch_user_entered_recs',
                                           config=config)
            fm_data_collection = db[FM_COLL]
            how = 'sum_of_users' if comp_periods[0] != period else 'fm_upload'

            if len(period) == 4 and config.quarter_editable:  # add quarters as well incase of monthly tenants
                comp_periods = time_context.all_quarters
            match = {'period': {'$in': comp_periods},
                     'node': {'$in': nodes},
                     'field': {'$in': fields}}
            if updated_since is not None:
                match['timestamp'] = {'$gte': int(updated_since)}
            if exclude_empty is not None and exclude_empty is True:
                match['val'] = {'$nin': [0.0, 0, None]}
            if not get_all_segments:
                match['segment'] = {'$in': segments}
            if timestamp is not None:
                match['timestamp'] = {'$lte': timestamp}
                if includefuturetimestamp:
                    match['timestamp'] = {'$gte': timestamp}

            sort = {'timestamp': 1}
            group = {'_id': {'period': '$period',
                             'node': '$node',
                             'field': '$field'},
                     'timestamp': {'$last': '$timestamp'},
                     'by': {'$last': '$by'},
                     'val': {'$last': '$val'},
                     'comments': {'$last': '$comments'}}

            regroup = {'_id': {'node': '$_id.node',
                               'field': '$_id.field'},
                       'val': {'$sum': '$val'},
                       'by': {'$last': '$by'},
                       'timestamp': {
                           '$last': '$timestamp'
                       },
                       'comments': {'$last': '$comments'}
                       }

            group['_id']['segment'] = '$segment'
            regroup['_id']['segment'] = '$_id.segment'
            pipeline = [{'$match': match},
                        {'$sort': sort},
                        {'$group': group},
                        {'$sort': sort},
                        {'$group': regroup}]
            if FM_COLL == FM_LATEST_DATA_COLL:
                regroup = {'_id': {'node': '$node',
                                   'field': '$field'},
                           'val': {'$sum': '$val'},
                           'by': {'$last': '$by'},
                           'timestamp': {
                               '$last': '$timestamp'
                           }}
                regroup['_id']['segment'] = '$segment'
                pipeline = [{'$match': match},
                            {'$group': regroup}]
            if skip is not None or limit is not None:
                pipeline.append({'$skip': int(skip) if skip else 0})
                if limit:
                    pipeline.append({'$limit': int(limit)})
            if config.debug:
                logger.info('bulk_fetch_user_entered_rec_v2 pipeline: %s from %s' % (pipeline, FM_COLL))

            recs = fm_data_collection.aggregate(pipeline, allowDiskUse=True)
            return recs

        except Exception as e:
            logger.exception(e)
            raise e

def _bulk_fetch_user_entered_recs(time_context,
                                  descendants_list,
                                  fields,
                                  segments,
                                  config,
                                  timestamp_info,
                                  db,
                                  cache,
                                  get_all_segments=False,
                                  round_val=True,
                                  found=None,
                                  nodes_and_eligible_segs_len=None,
                                  includefuturetimestamp=False):
    """
    fetch many user entered records for multiple nodes and fields
    returns nothing, just puts records in cache
    """
    # NOTE: much of this logic is duplicated in _fetch_user_entered_rec
    # if you are changing logic here, be sure to also change in the non bulk version of the function
    try:
        found = found if found is not None else False
    except:
        found = False
    try:
        node_children_check = {node: any(node_children) for node, node_children, _ in descendants_list}
        nodes = node_children_check.keys()
        period, comp_periods = time_context.fm_period, time_context.submission_component_periods
        timestamp, _, _, cutoff_timestamp = timestamp_info
        FM_COLL = read_from_collection(period[:4], timestamp=timestamp, call_from='_bulk_fetch_user_entered_recs',
                                       config=config)
        fm_data_collection = db[FM_COLL]
        how = 'sum_of_users' if comp_periods[0] != period else 'fm_upload'

        if len(period) == 4 and config.quarter_editable: # add quarters as well incase of monthly tenants
            comp_periods = time_context.all_quarters

        if set(fields).intersection(set(config.weekly_data)):
            comp_periods = [period]

        if set(fields).intersection(set(config.quarterly_high_low_fields)) and "Q" in period:
            comp_periods = [period]

        match = {'period': {'$in': comp_periods},
                 'node': {'$in': nodes},
                 'field':  {'$in': fields}}
        if not get_all_segments:
            match['segment'] = {'$in': segments}
        if timestamp is not None:
            match['timestamp'] = {'$lte': timestamp}
            if includefuturetimestamp:
                match['timestamp'] = {'$gte': timestamp}

        sort = {'timestamp': 1}
        group = {'_id': {'period': '$period',
                         'node': '$node',
                         'field': '$field'},
                 'timestamp': {'$last': '$timestamp'},
                 'by': {'$last': '$by'},
                 'val': {'$last': '$val'},
                 'comments':{'$last': '$comments'}}

        regroup = {'_id': {'node': '$_id.node',
                           'field': '$_id.field'},
                   'val': {'$sum': '$val'},
                   'by': {'$last': '$by'},
                   'timestamp': {
                           '$last': '$timestamp'
                        },
                   'comments':{'$last': '$comments'}
                   }

        group['_id']['segment'] = '$segment'
        regroup['_id']['segment'] = '$_id.segment'
        pipeline = [{'$match': match},
                    {'$sort': sort},
                    {'$group': group},
                    {'$sort': sort},
                    {'$group': regroup}]
        if FM_COLL == FM_LATEST_DATA_COLL:
            regroup = {'_id': {'node': '$node',
                               'field': '$field'},
                       'val': {'$sum': '$val'},
                       'by': {'$last': '$by'},
                       'timestamp': {
                           '$last': '$timestamp'
                       }}
            regroup['_id']['segment'] = '$segment'
            pipeline = [{'$match': match},
                        {'$group': regroup}]

        if config.debug:
            logger.info('bulk_fetch_user_entered_rec pipeline: %s from %s' % (pipeline, FM_COLL))

        recs = fm_data_collection.aggregate(pipeline, allowDiskUse=True)

        db_recs = {(rec['_id']['node'], rec['_id']['field'], rec['_id']['segment']): rec
                   for rec in recs}
        rollup_segments = config.rollup_segments
        if nodes_and_eligible_segs_len is None:
            nodes_and_eligible_segs_len = {}
            if config.partially_segmented:
                for node in nodes:
                    nodes_and_eligible_segs_len[node] = len(config.get_segments(epoch().as_epoch(), node))
        for node, field in product(nodes, fields):
            aggregated_val = 0
            len_of_segments = len(segments)
            # if the node has only one segment, aggregated_val condition should be passed. THis is in case the tenant has segments,
            # but the current node doesn't have segments
            if len_of_segments == 1 and 'all_deals' in segments:
                # value in all_deals is summation of segments. Hence all segments need to be looped through
                # for non-segmented tenants ['all_deals'] will be retained by default
                segments = [segment for segment in config.segments]
                len_of_segments = len(segments)
            len_of_eligible_segments = nodes_and_eligible_segs_len.get(node)
            for segment in segments:

                val = get_nested(db_recs, [(node, field, segment), 'val'], 0)
                overwritten_val = overwrite_fields(config, field, comp_periods, period, node, timecontext=time_context)
                if overwritten_val is not None:
                    val = overwritten_val
                if ((not node_children_check[node]) or (len_of_segments == 1) or (len_of_eligible_segments == 1) or (len_of_segments > 1 and segment != "all_deals")) and segment in rollup_segments:
                    aggregated_val += val
                cache[(period, node, field, segment, timestamp)] = {'period': period,
                                                                    'segment': segment,
                                                                    'val': round(val) if round_val and not config.fields[field].get('format') == 'percentage' else val,
                                                                    'by': get_nested(db_recs, [(node, field, segment), 'by'], 'system'),
                                                                    'how': how,
                                                                    'found': found if found else (node, field, segment) in db_recs,
                                                                    'timestamp': get_nested(db_recs, [(node, field, segment), 'timestamp'], 0),
                                                                    'node': node,
                                                                    'field': field}
                if config.fields[field].get('commentable') == True:
                    comments = get_nested(db_recs, [(node, field, segment), 'comments'], '')
                    cache[(period, node, field, segment, timestamp)]['comments'] = comments

            if 'all_deals' in segments and config.is_special_pivot_segmented(node):
                prev_val = cache[(period, node, field, 'all_deals', timestamp)]['val']
                cache[(period, node, field, 'all_deals', timestamp)]['val'] = 0.0 if \
                    (config.segment_percentage_rollup and config.fields[field].get('format') == 'percentage') \
                    else (round(aggregated_val)
                          if round_val and not config.fields[field].get('format') == 'percentage' else aggregated_val)
                if (prev_val is not None and field in config.forecast_service_editable_fields):
                    cache[(period, node, field, 'all_deals', timestamp)]['val'] = prev_val
        return cache
    except Exception as e:
        logger.exception(e)
        raise e

def _bulk_fetch_deal_rollup_recs(time_context,
                                 descendants_list,
                                 fields,
                                 segments,
                                 config,
                                 timestamp_info,
                                 db,
                                 cache,
                                 round_val=True,
                                 get_all_segments=False,
                                 is_dr_deal_fields=False,
                                 dr_from_fm_coll_for_snapshot=None,
                                 found=None,
                                 eligible_nodes_for_segs=None,
                                 call_from=None,
                                 trend=False
                                 ):
    """
    fetch many deal rollup records for multiple nodes and fields
    returns nothing, just puts records in cache
    """
    # NOTE: much of this logic is duplicated in _fetch_deal_rollup_rec
    # if you are changing logic here, be sure to also change in the non bulk version of the function
    nodes = [node for node, _, _ in descendants_list]
    seg_sum_fields = config.segment_amount_fields
    seg_filter = {}
    period, comp_periods = time_context.fm_period, time_context.component_periods
    timestamp, dlf_expiration, deal_expiration, _ = timestamp_info
    FM_COLL = read_from_collection(period[:4], timestamp=timestamp, field_type='DR',
                                   call_from='bulk_fetch_deal_rollup_recs', config=config)
    fm_data_collection = db[FM_COLL]
    monthly_period_realtime_update = config.monthly_period_realtime_update


    fields_not_found = {}
    try:
        dr_from_fm_coll_for_snapshot = dr_from_fm_coll_for_snapshot if dr_from_fm_coll_for_snapshot is not None else sec_context.details.get_flag('deal_rollup', 'snapshot_dr_from_fm_coll', False)
    except:
        dr_from_fm_coll_for_snapshot = False

    try:
        found = found if found is not None else False
    except:
        found = False

    is_monthly = config.periods_config.monthly_fm
    if (((len(comp_periods) == 1 and len(segments) == 1 and (not is_monthly or (
            'Q' not in comp_periods[0] and not monthly_period_realtime_update)) and is_dr_deal_fields) or \
            (dr_from_fm_coll_for_snapshot)) and trend) or \
            (timestamp and not is_same_day(EpochClass(), epoch(timestamp))) or \
            (len(period) == 4 and config.read_from_fm_collection_for_year):
        dlf_fields = []
        fm_fields_ = []
        for field in fields:
            if config.deal_rollup_fields[field].get('dlf') or any(filt.get('op') == 'dlf' if isinstance(filt, dict) else \
                                                                False for filt in config.deal_rollup_fields[field]['filter']):
                dlf_fields.append(field)
            else:
                fm_fields_.append(field)
        for field_type in ['dlf', 'deal_fields']:
            fields_ = None
            if field_type == 'dlf':
                fields_ = dlf_fields
                expiration_timestamp = dlf_expiration
            else:
                fields_ = fm_fields_
                expiration_timestamp = deal_expiration
            if fields_:
                if timestamp and timestamp < expiration_timestamp:
                    query_expiration_timestamp = 0
                else:
                    query_expiration_timestamp = expiration_timestamp
                match = {'period': {'$in': comp_periods},
                        'node': {'$in': nodes},
                        'field': {'$in': fields_}}
                if not get_all_segments:
                    match['segment'] = {'$in': segments}
                if timestamp is not None:
                    match['timestamp'] = {'$lte': timestamp}
                sort = {'timestamp': 1}
                group = {'_id': {'period': '$period',
                                'node': '$node',
                                'segment': '$segment',
                                'field': '$field'},
                        'val': {'$last': '$val'}}
                regroup = {'_id': {'node': '$_id.node',
                                'field': '$_id.field',
                                'segment': '$_id.segment'},
                        'val': {'$sum': '$val'}}
                pipeline = [{'$match': match},
                            {'$sort': sort},
                            {'$group': group},
                            {'$group': regroup}]
                if FM_COLL == FM_LATEST_COLL:
                    regroup = {'_id': {'node': '$node',
                                       'field': '$field',
                                       'segment': '$segment'},
                               'val': {'$sum': '$val'}}
                    pipeline = [{'$match': match},
                                {'$group': regroup}]

                if config.debug:
                    logger.info('bulk fetch_deal_rollup_rec pipeline: %s from %s' % (pipeline, FM_COLL))
                recs = fm_data_collection.aggregate(pipeline, allowDiskUse=True)

                db_recs = {(rec['_id']['node'], rec['_id']['field'], rec['_id']['segment']): rec
                        for rec in recs}
                rollup_segments = config.rollup_segments

                for node, field in product(nodes, fields_):
                    aggregated_val = 0
                    for segment in segments:
                        val = get_nested(db_recs, [(node, field, segment), 'val'], None)
                        if val is None:
                            fields_not_found[(node, field)] = expiration_timestamp
                            val = 0
                        overwritten_val = overwrite_fields(config, field, comp_periods, period, node, timecontext=time_context)
                        if overwritten_val is not None:
                            val = overwritten_val
                        if segment in rollup_segments:
                            aggregated_val += val
                        cache[(period, node, field, segment, timestamp)] = {'period': period,
                                                                            'segment': segment,
                                                                            'val': round(val) if round_val else val,
                                                                            'by': get_nested(db_recs, [(node, field, segment), 'by'], 'system'),
                                                                            'how': 'deal_rollup',
                                                                            'found': found if found else (node, field, segment) in db_recs,
                                                                            'timestamp': get_nested(db_recs, [(node, field, segment), 'timestamp'], timestamp),
                                                                            'node': node,
                                                                            'field': field}
                    if 'all_deals' in segments:
                        prev_val = cache[(period, node, field, 'all_deals', timestamp)]['val']
                        cache[(period, node, field, 'all_deals', timestamp)]['val'] = round(aggregated_val) if round_val else aggregated_val

                        if (prev_val is not None and field in config.forecast_service_editable_fields):
                            cache[(period, node, field, 'all_deals', timestamp)]['val'] = prev_val
        return cache

    fields_and_nodes = {}
    if fields_not_found:
        for node, field in fields_not_found.keys():
            expiration_timestamp = fields_not_found.get((node,field), 0)
            if timestamp and expiration_timestamp and abs(timestamp - time_context.now_timestamp) > ONE_DAY:
                pass
            else:
                if not fields_and_nodes.get(field):
                    fields_and_nodes[field] = []
                fields_and_nodes[field].append(node)
        if not fields_and_nodes:
            return cache
    elif timestamp and not is_same_day(EpochClass(), epoch(timestamp)):
        return cache
    prev_periods = prev_periods_allowed_in_deals()

    if fields_and_nodes:
        for field in fields_and_nodes:
            nodes = fields_and_nodes[field]
            fetch_dr_from_deals(config, seg_filter, seg_sum_fields, segments, [field], nodes, time_context,
                                period, timestamp, round_val=round_val, cache=cache, db=db,
                                prev_periods=prev_periods)
        return cache

    return fetch_dr_from_deals(config, seg_filter, seg_sum_fields, segments, fields, nodes, time_context,
                               period, timestamp, round_val=round_val, cache=cache, db=db,
                               prev_periods=prev_periods)


def fetch_dr_from_deals(config,
                        seg_filter,
                        seg_sum_fields,
                        segments,
                        fields,
                        nodes,
                        time_context,
                        period,
                        timestamp,
                        round_val=True,
                        cache=None,
                        db=None,
                        prev_periods=[]) :
    crits_and_ops = [schema['crit_and_ops']
                     for field, schema in config.deal_rollup_fields.iteritems() if field in fields]

    seg_crit_and_ops = [
        (filt_name,
         {'$and': [filt, seg_filter]},
         list(set([(seg_sum_fields.get(segment) if seg_sum_fields.get(segment) else sum_field, rendered_field, op)
                   for (sum_field, rendered_field, op) in ops
                   for segment in segments])),
        config.deal_rollup_fields[filt_name]['filter'],
        any(filt.get('op') == 'dlf' if isinstance(filt, dict) else False for filt in config.deal_rollup_fields[filt_name]['filter']) or \
        config.deal_rollup_fields[filt_name].get('dlf'))
        for filt_name, filt, ops in crits_and_ops
    ]
    if len(period) == 4:
        period_and_close_periods = time_context.period_and_close_periods_for_year or \
                                   get_period_and_close_periods(period, False, deal_svc=True)
    else:
        period_and_close_periods = [(time_context.deal_period, time_context.close_periods)]

    totals = fetch_many_dr_from_deals(period_and_close_periods,
                                           nodes,
                                           seg_crit_and_ops,
                                           config.deal_config,
                                           db=db,
                                           return_seg_info=True,
                                           timestamp=time_context.deal_timestamp,
                                           prev_periods=prev_periods or time_context.prev_periods,
                                           actual_view=True if len(period)==4 else False)

    for segment in segments:
        segment_ = segment
        if segment != 'all_deals':
            segment_filter_ = config.segment_filters.get(segment)
            deal_config = config.deal_config
            seg_field = deal_config.segment_field
            if seg_field and segment_filter_ and segment_filter_.get(seg_field) and segment_filter_.get(seg_field).get(
                    "$in"):
                segment_ = segment_filter_.get(seg_field).get("$in")
        for node, field in product(nodes, fields):
            val = 0
            if isinstance(segment_,list):
                for seg in segment_:
                    if seg_sum_fields.get(segment):
                        val += get_nested(totals, [(field, seg, node), seg_sum_fields[segment]],0)
                    else:
                        val += get_nested(totals, [(field, seg, node), config.fields[field]['sum_field']], 0)
            else:
                val += get_nested(totals, [(field, segment_, node), seg_sum_fields[segment] if seg_sum_fields.get(segment)
                                            else config.fields[field]['sum_field']], 0)
            cache[(period, node, field, segment, timestamp)] = {'period': period,
                                                                'segment': segment,
                                                                'val': round(val) if round_val else val,
                                                                'by': 'system',
                                                                'how': 'deal_rollup',
                                                                'found': True,
                                                                'timestamp': timestamp,
                                                                'node': node,
                                                                'field': field}

    return cache


def _bulk_fetch_prnt_dr_recs(config,
                             fields,
                             time_context,
                             descendants_list,
                             segments,
                             timestamp_info,
                             db,
                             cache,
                             timestamp,
                             eligible_nodes_for_segs,
                             round_val=True,
                             get_all_segments=True,
                             node=None,
                             prev_periods=[]):
    """
    fetch records for each segment, field, node, timestamp  for deals matching a filter.
    Fetching of recording happens one at a time and the fetched records and added in the cache.
    At the end we return the cache

        Arguments:
            time_context {time_context} -- fm period, components of fm period
                                           deal period, close periods of deal period
                                           deal expiration timestamp,
                                           relative periods of component periods
                                           ('2020Q2', ['2020Q2'],
                                            '2020Q2', ['201908', '201909', '201910'],
                                            1556074024910,
                                            ['h', 'c', 'f'])
            descendants_list {list} -- list of tuples of
                                       (node, [children], [grandchildren])
                                        fetches data for each node
                                        using children/grandkids to compute sums
                                        [('A', ['B, 'C'], ['D', 'E'])]
            fields {list} -- list of field names
                             ['commit', 'best_case']
            segments {list} -- list of segment names
                              ['all_deals', 'new', 'upsell']
            config  -- instance of Config
            timestamps {list} -- list of epoch timestamps to get data as of
                                 [1556074024910, 1556075853732, ...]

        Keyword Arguments:
            recency_window {int} -- window to reject data from before timestamp - recency_window  (default: {None})
                                if None, will accept any record, regardless of how stale
                                ONE_DAY
            db {object} -- instance of tenant_db (default: {None})
                           if None, will create one

        Returns:
            cache
    """
    prnt_node= descendants_list[0][0]
    if node:
        nodes = [node]
        prnt_node = node
    nodes = [node for node, _, _ in descendants_list]
    seg_sum_fields = config.segment_amount_fields
    seg_filter = {}
    period, comp_periods = time_context.fm_period, time_context.component_periods
    timestamp, dlf_expiration, deal_expiration, _ = timestamp_info
    FM_COLL = read_from_collection(period[:4], timestamp=timestamp, field_type='DR',
                                   call_from='_bulk_fetch_prnt_dr_recs', config=config)
    fm_data_collection = db[FM_COLL]
    if cache is None:
        cache = {}
    db = db if db else sec_context.tenant_db
    if timestamp and not is_same_day(EpochClass(), epoch(timestamp)):
        dlf_fields = []
        fm_fields_ = []
        for field in fields:
            if config.prnt_deal_rollup_fields[field].get('dlf') or \
                    any(filt.get('op') == 'dlf' if isinstance(filt, dict) else False
                        for filt in config.prnt_deal_rollup_fields[field]['filter']):
                dlf_fields.append(field)
            else:
                fm_fields_.append(field)
        for field_type in ['dlf', 'deal_fields']:
            fields_ = None
            if field_type == 'dlf':
                fields_ = dlf_fields
                expiration_timestamp = dlf_expiration
            else:
                fields_ = fm_fields_
                expiration_timestamp = deal_expiration
            if fields_:
                if timestamp and timestamp < expiration_timestamp:
                    query_expiration_timestamp = 0
                else:
                    query_expiration_timestamp = expiration_timestamp
                match = {'period': {'$in': comp_periods},
                         'node': {'$in': nodes},
                         'field': {'$in': fields_}}
                if not get_all_segments:
                    match['segment'] = {'$in': segments}
                if timestamp is not None:
                    match['timestamp'] = {'$lte': timestamp}
                sort = {'timestamp': 1}
                group = {'_id': {'period': '$period',
                                 'node': '$node',
                                 'segment': '$segment',
                                 'field': '$field'},
                         'val': {'$last': '$val'}}
                regroup = {'_id': {'node': '$_id.node',
                                   'field': '$_id.field',
                                   'segment': '$_id.segment'},
                           'val': {'$sum': '$val'}}
                pipeline = [{'$match': match},
                            {'$sort': sort},
                            {'$group': group},
                            {'$group': regroup}]
                if FM_COLL == FM_LATEST_COLL:
                    regroup = {'_id': {'node': '$node',
                                       'field': '$field',
                                       'segment': '$segment'},
                               'val': {'$sum': '$val'}}
                    pipeline = [{'$match': match},
                                {'$group': regroup}]

                if config.debug:
                    logger.info('bulk fetch_prnt_dr_recs pipeline: %s from %s' % (pipeline, FM_COLL))
                recs = fm_data_collection.aggregate(pipeline, allowDiskUse=True)

                db_recs = {(rec['_id']['node'], rec['_id']['field'], rec['_id']['segment']): rec for rec in recs}
                rollup_segments = config.rollup_segments

                for node, field in product(nodes, fields_):
                    aggregated_val = 0
                    for segment in segments:
                        val = get_nested(db_recs, [(node, field, segment), 'val'], None)
                        if val is None:
                            val = 0
                        if segment in rollup_segments  and (segment != 'all_deals' or \
                                                            (segment == 'all_deals' and len(segments) == 1)):
                            aggregated_val += val
                        cache[(period, node, field, segment, timestamp)] = {'period': period,
                                                                            'segment': segment,
                                                                            'val': round(val) if round_val else val,
                                                                            'by': get_nested(db_recs, [(node, field, segment), 'by'], 'system'),
                                                                            'how': 'deal_rollup',
                                                                            'found': True,
                                                                            'timestamp': get_nested(db_recs, [(node, field, segment), 'timestamp'], timestamp),
                                                                            'node': node,
                                                                            'field': field}
                    if 'all_deals' in segments:
                        prev_val = cache[(period, node, field, 'all_deals', timestamp)]['val']
                        cache[(period, node, field, 'all_deals', timestamp)]['val'] = round(aggregated_val) if round_val else aggregated_val

                        if (prev_val is not None and field in config.forecast_service_editable_fields):
                            cache[(period, node, field, 'all_deals', timestamp)]['val'] = prev_val
        return cache
    else:
        timestamps = [None]
        if not eligible_nodes_for_segs:
            for segment in config.segments:
                if segment == config.primary_segment:
                    continue
                eligible_nodes_for_segs[segment] = [dd['node'] for dd in fetch_eligible_nodes_for_segment(time_context.now_timestamp, segment,
                                                                                                          db=db)]

        prev_periods = prev_periods if prev_periods else prev_periods_allowed_in_deals()
        for timestamp, field, segment, descendants in product(timestamps, fields, segments, descendants_list):
            if segment == 'all_deals' or descendants[0] in eligible_nodes_for_segs.get(segment, []):
                fetch_deal_rollup_for_prnt_dr_rec(time_context,
                                                  descendants,
                                                  field,
                                                  segment,
                                                  config,
                                                  timestamp_info,
                                                  db,
                                                  cache,
                                                  prnt_node,
                                                  prev_periods=prev_periods)

        return cache


def fetch_deal_rollup_for_prnt_dr_rec(time_context,descendants,field,segment,config,timestamp_info,db,cache,prnt_node,
                                      prev_periods=[]):
    """
    fetch record that is sum of deal amount field for deals matching a filter.
    We will use the parent of the node for fetching the information and the filter will be
    applied to the parent of the node. If the node itself is a parent node then the filter will
    be applied to that particular node only

        Arguments:
        time_context {time_context} -- fm period, components of fm period
                                       deal period, close periods of deal period
                                       deal expiration timestamp,
                                       relative periods of component periods
                                       ('2020Q2', ['2020Q2'],
                                        '2020Q2', ['201908', '201909', '201910'],
                                        1556074024910,
                                        ['h', 'c', 'f'])
        descendants {tuple} -- (node, [children], [grandchildren])
                                fetches data for node
                                using children/grandkids to compute sums
                                ('A', ['B, 'C'], ['D', 'E'])
        field {str} -- field name
                      'commit'
        segment {str} -- segment name
                         'all deals'
        config {Config} -- instance of Config

    Keyword Arguments:
        timestamp_info {tuple} -- (ts to get data as of,
                                   ts dlf DR recs expired as of,
                                   ts deal DR recs expired as of,
                                   ts to reject data from before (None to accept any level of staleness))
                                   if None passed, gets most recent record
                                   (default: {None})
                                   (1556074024910, 1556074024910, 1556074024910)
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one
                       (pass in to avoid overhead when fetching many times)
        cache {dict} -- dict to hold records fetched by (default: {None})
                        (used to memoize fetching many recs)
        prnt_node {str} -- parent of the particular node. Value will same as node when it's already the
                            parent. prnt_node is required for prnt_DR fields

    Returns:
        dict -- a single fm record
    """
    #
    node, _, _ = descendants
    current_segment_dtls = config.segments[segment]
    period, comp_periods = time_context.fm_period, time_context.component_periods
    timestamp, dlf_expiration, deal_expiration, _ = timestamp_info
    found = True
    sum_field = config.segment_amount_fields.get(segment, config.fields[field]['sum_field'])
    seg_filter = config.segment_filters.get(segment)
    name, crit, ops = config.fields[field]['crit_and_ops']
    crit = {'$and': [crit, seg_filter]}
    if segment and segment != 'all_deals' and sum_field != config.fields[field]['sum_field']:
        ops = [(sum_field, sum_field, '$sum')]
    # Overriding the segment field with the overrided field in config
    if 'sum_field_override' in current_segment_dtls and field in current_segment_dtls['sum_field_override']:
        if segment != "all_deals":
            ovveride_field = current_segment_dtls['sum_field_override'][field]
            name, crit, ops = config.fields[ovveride_field]['crit_and_ops']
            sum_field = config.fields[ovveride_field]['sum_field']
            crit = {'$and': [crit, seg_filter]}
        else:
            total_value = 0
            # If sum_field_override in all_deals segment, then we are overriding the field with sum of all the segment values for that field.
            for seg, segment_dtls in config.segments.items():
                if seg != "all_deals":
                    seg_filter = config.segment_filters.get(seg)
                    ovveride_field = segment_dtls['sum_field_override'][
                        field] if 'sum_field_override' in segment_dtls else field
                    name, crit, ops = config.fields[ovveride_field]['crit_and_ops']
                    sum_field = config.fields[ovveride_field]['sum_field'] if 'sum_field_override' in segment_dtls else \
                    segment_dtls['sum_field']
                    crit = {'$and': [crit, seg_filter]}
                    val = fetch_prnt_DR_deal_totals((time_context.deal_period, time_context.close_periods),
                                            node,
                                            ops,
                                            config.deal_config,
                                            prnt_node,
                                            filter_criteria=crit,
                                            timestamp=timestamp if timestamp else time_context.deal_timestamp,
                                            db=db,
                                            cache=cache,
                                            prev_periods=prev_periods or time_context.prev_periods).get(sum_field, 0)
                    total_value += len(val) if isinstance(val, list) else val
            res = {'period': period,
                   'segment': segment,
                   'val': total_value,
                   'by': 'system',
                   'how': 'deal_rollup',
                   'found': found,
                   'timestamp': timestamp,
                   'node': node,
                   'field': field}
            if cache is not None:
                cache[(period, node, field, segment, timestamp)] = res
            return res

    try:
        val = fetch_prnt_DR_deal_totals((time_context.deal_period, time_context.close_periods),
                                    node,
                                    ops,
                                    config.deal_config,
                                    prnt_node,
                                    filter_criteria=crit,
                                    timestamp=None,
                                    db=db,
                                    cache=cache,
                                    prev_periods=prev_periods or time_context.prev_periods).get(sum_field, 0)
    except:
        val, found = 0, False

    res = {'period': period,
            'segment': segment,
            'val': len(val) if isinstance(val,list) else val,
            'by': 'system',
            'how': 'deal_rollup',
            'found': found,
            'timestamp': timestamp,
            'node': node,
            'field': field}
    if cache is not None:
        cache[(period, node, field, segment, timestamp)] = res
    return res

def _bulk_fetch_ai_recs(time_context,
                        descendants_list,
                        fields,
                        segments,
                        config,
                        timestamp_info,
                        db,
                        cache,
                        get_all_segments=False,
                        round_val=True,
                        bucket_fields=[],
                        bucket_forecast_field=[]):
    """
    fetch many prediction records for multiple nodes and fields
    returns nothing, just puts records in cache
    """
    # NOTE: much of this logic is duplicated in _fetch_ai_rec
    # if you are changing logic here, be sure to also change in the non bulk version of the function
    try:
        nodes = [node for node, _, _ in descendants_list]
        period = time_context.fm_period
        timestamp, _, _, _ = timestamp_info
        FM_COLL = read_from_collection(period[:4], timestamp=timestamp, call_from='_bulk_fetch_ai_recs',
                                       config=config)
        fm_data_collection = db[FM_COLL]

        # NOTE: taking a step backwards here until we have augustus monthly
        # ai numbers are just taken straight from gbm, and are no longer the sum of component periods
        # this might cause some inconsistencies for tenants with monthly predictions, so we should be cautious here
        if bucket_forecast_field and len(bucket_forecast_field) > 0:
            match = {'period': {'$in': [period]},
                     'node': {'$in': nodes},
                     'field': {'$in': bucket_forecast_field}}
            if not get_all_segments:
                match['segment'] = {'$in': segments}
            if timestamp is not None:
                match['timestamp'] = {'$lte': timestamp}
            sort = {'timestamp': 1}
            group = {'_id': {'period': '$period',
                             'node': '$node',
                             'segment': '$segment',
                             'field': '$field'},
                     'timestamp': {'$last': '$timestamp'},
                     'by': {'$last': '$by'},
                     'val': {'$last': '$val'}}
            regroup = {'_id': {'node': '$_id.node',
                               'field': '$_id.field'},
                       'timestamp': {'$last': '$timestamp'},
                       'by': {'$last': '$by'},
                       'val': {'$sum': '$val'}}

            regroup['_id']['segment'] = '$_id.segment'
        else:
            match = {'period': {'$in': [period]},
                     'node': {'$in': nodes},
                     'field':  {'$in': fields}}
            if not get_all_segments:
                match['segment'] = {'$in': segments}
            if timestamp is not None:
                match['timestamp'] = {'$lte': timestamp}
            sort = {'timestamp': 1}
            group = {'_id': {'period': '$period',
                             'node': '$node',
                             'segment': '$segment',
                             'field': '$field'},
                     'timestamp': {'$last': '$timestamp'},
                     'by': {'$last': '$by'},
                     'val': {'$last': '$val'}}
            regroup = {'_id': {'node': '$_id.node',
                               'field': '$_id.field'},
                       'timestamp': {'$last': '$timestamp'},
                       'by': {'$last': '$by'},
                       'val': {'$sum': '$val'}}

            regroup['_id']['segment'] = '$_id.segment'

        pipeline = [{'$match': match},
                    {'$sort': sort},
                    {'$group': group},
                    {'$group': regroup}]
        if FM_COLL == FM_LATEST_DATA_COLL:
            regroup = {'_id': {'node': '$node',
                               'field': '$field'},
                       'timestamp': {'$last': '$timestamp'},
                       'by': {'$last': '$by'},
                       'val': {'$sum': '$val'}}
            regroup['_id']['segment'] = '$segment'
            pipeline = [{'$match': match},
                        {'$group': regroup}]
        if config.debug:
            logger.info('bulk_fetch_ai_rec pipeline: %s from %s' % (pipeline, FM_COLL))

        db_recs = {(rec['_id']['node'], rec['_id']['field'], rec['_id']['segment']): rec
                   for rec in fm_data_collection.aggregate(pipeline, allowDiskUse=True)}
        rollup_segments = config.rollup_segments

        for node, field in product(nodes, fields):
            aggregated_val = 0
            for segment in segments:
                val = get_nested(db_recs, [(node, field, segment), 'val'], 0)
                #nan check
                val = val if val==val else 0
                if field in bucket_fields:
                    val = get_nested(db_recs, [(node, bucket_forecast_field[0], field), 'val'], 0)
                if segment in rollup_segments:
                    aggregated_val += val
                cache[(period, node, field, segment, timestamp)] = {'period': period,
                                                                    'segment': segment,
                                                                    'val': round(val) if round_val else val,
                                                                    'by': get_nested(db_recs, [(node, field, segment), 'by'], 'system'),
                                                                    'how': 'ai',
                                                                    'found': (node, field, segment) in db_recs,
                                                                    'timestamp': get_nested(db_recs, [(node, field, segment), 'timestamp'], 0),
                                                                    'node': node,
                                                                    'field': field}

            if cache.get((period, node, field, 'all_deals', timestamp), None) is None or config.rollup_ai_segments:
                cache[(period, node, field, 'all_deals', timestamp)] = {}
                cache[(period, node, field, 'all_deals', timestamp)]['val'] = round(aggregated_val) if round_val else aggregated_val
        return cache
    except Exception as e:
        logger.exception(e)
        raise e

def fetch_fm_rec_in_period(descendants,
                 fields,
                 segments,
                 config,
                 eligible_nodes_for_segs,
                 fm_rec_period,
                 cache,
                 timestamp = None,
                 time_context_list = []
                 ):

    logger.info('Processing the data for the period: {}.....'.format(fm_rec_period))
    time_context = get_time_context(fm_rec_period, config=None, quarter_editable=config.quarter_editable)
    db = sec_context.tenant_db
    res = bulk_fetch_recs_by_timestamp(time_context,
            descendants,
            fields,
            segments,
            config,
            eligible_nodes_for_segs,
            [timestamp],
            db = db,
            time_context_list = time_context_list
        )

    cache.update(res)
    return cache

def _fetch_fm_rec(args):
    # dumb overhead function because pool.map only takes a single argument and cant do lambdas
    return fetch_fm_rec(*args)

def _bulk_fetch_user_entered_recs_pool(args):
    # overhead function because pool.map only takes a single argument and cant do lambdas
    return _bulk_fetch_user_entered_recs(*args)

def _bulk_fetch_ai_recs_pool(args):
    # overhead function because pool.map only takes a single argument and cant do lambdas
    return _bulk_fetch_ai_recs(*args)

def handle_dr_fields_pool(args):
    # overhead function because pool.map only takes a single argument and cant do lambdas
    return handle_dr_fields(*args)

def _bulk_fetch_prnt_dr_recs_pool(args):
    # overhead function because pool.map only takes a single argument and cant do lambdas
    return _bulk_fetch_prnt_dr_recs(*args)

def _bulk_period_conditional_rec_pool(args):
    # overhead function because pool.map only takes a single argument and cant do lambdas
    return _bulk_period_conditional_rec(*args)

def _bulk_fetch_formula_recs_pool(args):
    # overhead function because pool.map only takes a single argument and cant do lambdas
    return _bulk_fetch_formula_recs(*args)

# fetch cached records from snapshot
def fetch_cached_snapshot(period, node, mobile=None, config=None, db=None, segment=None, force_retrieve_cache=False,
                          timestamp=None):
    if not db:
        db = sec_context.tenant_db
    config = config if config else FMConfig()
    allowed_periods = config.snapshot_config.get("allowed_periods", [])
    yearly_view = config.snapshot_config.get('yearly_view', False)
    if yearly_view:
        if len(period) == 6 and 'Q' not in period:
            # This code can be used to find the year to which it belongs in case we have month as a period
            for quarter, months in get_available_quarters_and_months(PeriodsConfig()).iteritems():
                if period in months:
                    qtr = quarter
            current_year = qtr[0:4]
        elif len(period) == 6:
            # This code can be used to find the year to which it belongs in case we have quarter as a period
            current_year = period[0:4]
        else:
            current_year = period
        if period != current_year:
            quarter_and_its_component_period = get_period_and_close_periods(current_year, deal_svc=True)
            qtr_comp_period_lst = []
            for sublist in quarter_and_its_component_period:
                qtr_comp_period_lst.append(sublist[0])
                for mth in sublist[1]:
                    qtr_comp_period_lst.append(mth)
            allowed_periods += qtr_comp_period_lst
        else:
            allowed_periods.append(period)
    if period not in allowed_periods:
        periods_to_prefetch = get_periods_editable(config.periods_config,
                                                   future_qtr_editable=config.future_qtrs_prefetch_count,
                                                   past_qtr_editable=config.past_qtrs_prefetch_count)
        for per in periods_to_prefetch:
            allowed_periods.append(per)
            _, component_periods = get_period_and_component_periods(per)
            allowed_periods += component_periods
            if config.deal_config.weekly_fm:
                weeks = [week_prd.mnemonic for week_prd in weekly_periods(per)]
                allowed_periods += weeks
    is_cached = False
    if period in allowed_periods:
        is_cached = True
    if not is_cached:
        return {}
    coll = SNAPSHOT_ROLE_COLL if config.has_roles else SNAPSHOT_COLL
    if timestamp and config.snapshot_config.get("snapshot_historical", False):
        coll = SNAPSHOT_HIST_ROLE_COLL if config.has_roles else SNAPSHOT_HIST_COLL
    snapshot_coll = db[coll]
    if mobile:
        coll = MOBILE_SNAPSHOT_ROLE_COLL if config.has_roles else MOBILE_SNAPSHOT_COLL
        snapshot_coll = db[coll]
    if segment:
        criteria = {"node": node, "period": period, "segment": segment}
    else:
        criteria = {"node": node, "period": period, "segment": 'all_deals'}
    if mobile:
        projections = {"mobile_snapshot_data": 1, "last_updated_time": 1, "mobile_snapshot_stale": 1, "how": 1}
    else:
        projections = {"snapshot_data": 1, "last_updated_time": 1, "snapshot_stale": 1, "how": 1}
    if not mobile and timestamp and config.snapshot_config.get("snapshot_historical", False):
        try:
            timestamp = int(timestamp)
            dt = epoch(timestamp).as_datetime()
            as_of_date = dt.strftime("%Y-%m-%d")
        except:
            as_of_date = ''
        criteria['as_of_date'] = as_of_date
    if config.has_roles:
        user_role = sec_context.get_current_user_role()
        if user_role not in config.get_roles:
            user_role = DEFAULT_ROLE
        criteria.update({'role': user_role})
    response = snapshot_coll.find_one(criteria, projections)
    result = {}
    if response:
        # mobile cached data
        if force_retrieve_cache:
            if response.get('snapshot_data'):
                result = response['snapshot_data']
                result['updated_at'] = response['last_updated_time']
                return result

        if mobile:
            if response.get('mobile_snapshot_stale', False):
                if response.get('how', 'chipotle') == 'ui_update':
                    return {}
            if response.get('mobile_snapshot_data'):
                result = add_collection_nodes(response['mobile_snapshot_data'], config)
                result['updated_at'] = response['last_updated_time']
            return result

        if response.get('how', 'chipotle') == 'UE_update' and \
                not response.get('snapshot_stale'):
            if response.get('snapshot_data'):
                result = response['snapshot_data']
                result['updated_at'] = response['last_updated_time']
                logger.info("returning from cache as data was regenerated as part of conditional snapshot for UE update")
            return result

        # web cached data
        if response.get('snapshot_stale', False):
            if response.get('how', 'chipotle') == 'ui_update' and \
                    not config.snapshot_config.get('show_cached_only', False):
                return {}
        if response.get('snapshot_data'):
            result = response['snapshot_data']
            result['updated_at'] = response['last_updated_time']
            if response.get('how', 'chipotle') == 'ui_update' and response.get('snapshot_stale', False):
                result['data_stale'] = True
    return result



def add_collection_nodes(mobile_snapshot_data, config):
    data = {}
    column_data = mobile_snapshot_data.get('data')
    for column in config.columns:
        data[column] = column_data

    mobile_snapshot_data['data'] = data
    return mobile_snapshot_data


def remove_collection_nodes(resp):
    cached_resp = {}
    if len(resp.get('data', {}).values()) > 0:
        data = resp.get('data', {}).values().pop()
        cached_resp['data'] = data
        cached_resp['metadata'] = resp.get('metadata')
    return cached_resp

# fetch cached records for forecast_summary from snapshot
def fetch_cached_forecast_summary(period, node, segment, config=None, db=None,
                                  current_data_only=False, new_home_page=False):
    if not db:
        db = sec_context.tenant_db
    config = config if config else FMConfig()
    allowed_periods = config.snapshot_config.get("allowed_periods", [])
    show_stale_limit = config.snapshot_config.get("show_stale_limit", 15)
    yearly_view = config.snapshot_config.get('yearly_view', False)
    if yearly_view:
        if len(period) == 6 and 'Q' not in period:
            # This code can be used to find the year to which it belongs in case we have month as a period
            for quarter, months in get_available_quarters_and_months(PeriodsConfig()).iteritems():
                if period in months:
                    qtr = quarter
            current_year = qtr[0:4]
        elif len(period) == 6:
            # This code can be used to find the year to which it belongs in case we have quarter as a period
            current_year = period[0:4]
        else:
            current_year = period
        if period != current_year:
            quarter_and_its_component_period = get_period_and_close_periods(current_year, deal_svc=True)
            qtr_comp_period_lst = []
            for sublist in quarter_and_its_component_period:
                qtr_comp_period_lst.append(sublist[0])
                for mth in sublist[1]:
                    qtr_comp_period_lst.append(mth)
            allowed_periods += qtr_comp_period_lst
        else:
            allowed_periods.append(period)
    if period not in allowed_periods:
        periods_to_prefetch = get_periods_editable(config.periods_config,
                                                   future_qtr_editable=config.future_qtrs_prefetch_count,
                                                   past_qtr_editable=config.past_qtrs_prefetch_count)
        for per in periods_to_prefetch:
            allowed_periods.append(per)
            _, component_periods = get_period_and_component_periods(per)
            allowed_periods += component_periods
    is_cached = False
    if period in allowed_periods:
        is_cached = True
    if not is_cached:
        return {}
    coll = SNAPSHOT_ROLE_COLL if config.has_roles else SNAPSHOT_COLL
    snapshot_coll = db[coll]
    criteria = {"node": node, "period": period, "segment": segment}
    if config.has_roles:
        user_role = sec_context.get_current_user_role()
        if user_role not in config.get_roles:
            user_role = DEFAULT_ROLE
        criteria.update({'role': user_role})
    projections = {"forecast_summary_data": 1, "last_updated_time": 1, "forecast_summary_stale": 1, "how": 1}
    if new_home_page:
        projections = {"new_homepage_fs": 1, "last_updated_time": 1, "new_homepage_fs_stale": 1, "how": 1}
    if config.debug:
        logger.info("fetch_cached_forecast_summary criteria %s projection %s" % (criteria,
                                                                                 projections))
    response = snapshot_coll.find_one(criteria, projections)
    result = {}
    if response:
        if new_home_page:
            if response.get('new_homepage_fs_stale', False):
                if response.get('how', 'chipotle') == 'ui_update':
                    return {}
                elif ((get_now().as_datetime() - epoch(
                        response['last_updated_time']).as_datetime()).total_seconds()) / 60 > int(show_stale_limit):
                    return {}
            if response.get('new_homepage_fs'):
                result = response['new_homepage_fs']
                result['updated_at'] = response['last_updated_time']
        else:
            if response.get('forecast_summary_stale', False):
                if response.get('how', 'chipotle') == 'ui_update':
                    return {}
                elif ((get_now().as_datetime() - epoch(response['last_updated_time']).as_datetime()).total_seconds()) / 60 > int(show_stale_limit):
                    return {}
            if response.get('forecast_summary_data'):
                result = response['forecast_summary_data']
                # to be removed on additing data for new_home_page
                if result.get("views_data") and not (result.get("current") and result.get("changes")):
                    return {}
                # this is to fix the records.
                if not current_data_only and not result.get("changes"):
                    return {}
                result['update_at'] = response['last_updated_time']
    return result


def get_all_source_fields(config, field):
    source_fields = config.fields[field].get('source', [])
    child_source_fields = []

    for child_field in source_fields:
        child_source_fields.extend(get_all_source_fields(config, child_field))

    return source_fields + child_source_fields


def fetch_nextq_pipeline(timestamp,
                        node,
                        fields=None,
                        db=None):
    """
    fetch next quarter pipeline data that occured.
    Arguments:
        node {str} -- Global#00538000005ac6T
        timestamp {str} -- 2023-10-30
    Keyword Arguments:
        db {pymongo.database.Database} -- instance of tenant_db
                                    (default: {None})
                                    if None, will create one
    Returns:
        next quarter pipeline aggregation
    """
    nextq_collection = db[NEXTQ_COLL] if db else sec_context.tenant_db[NEXTQ_COLL]

    match = {'node' : node,  'timestamp': timestamp}
    pipeline = [{'$match': match}]
    logger.info("PIPELINE FOR NEXT QUARTER : {}".format(pipeline))
    result = nextq_collection.aggregate(pipeline, allowDiskUse=True)
    logger.info("result {}".format(result))

    return result

def fetch_waterfall_fm_sync_data(periods, nodes, field, segment):
    fm_data_collection = sec_context.tenant_db[FM_COLL]
    criteria = {
        'period': {'$in': periods},
        'node': {'$in': nodes},
        'field': field,
        'segment': segment
    }
    sort = {
        'timestamp': 1
    }
    group = {'_id': {'period': '$period',
                     'node': '$node',
                     'segment': '$segment',
                     'field': '$field'},
             'timestamp': {'$last': '$timestamp'},
             'by': {'$last': '$by'},
             'val': {'$last': '$val'}}
    # regroup = {'_id': {'node': '$_id.node',
    #                    'field': '$_id.field'},
    #            'timestamp': {'$last': '$timestamp'},
    #            'by': {'$last': '$by'},
    #            'val': {'$sum': '$val'}}
    #
    # regroup['_id']['segment'] = '$_id.segment'

    pipeline = [{'$match': criteria},
                {'$sort': sort},
                {'$group': group},
                ]

    return {(rec['_id']['period'], rec['_id']['node'], rec['_id']['field'], rec['_id']['segment']): rec['val']
               for rec in fm_data_collection.aggregate(pipeline, allowDiskUse=True)}

def convert_excel_date_to_eod_timestamp(excel_date):
    time = xl2datetime_ttz(excel_date)
    final_date = time.replace(hour=23, minute=59, second=59)
    timestamp = datetime2epoch(final_date)
    return timestamp

def fetch_fields_by_type(config, time_context, descendants, fields, ignore_recency_for):
    adhoc_field_mapping = {}
    fields_by_type = {}
    for field in set(fields):
            if 'hist_field' in config.fields[field] and 'h' in time_context.relative_periods and not config.show_raw_data_in_trend:
                # historic period, switch to using true up field if it exists
                field_type = 'PC'
                if config.quarter_editable:
                    if not all(elem == 'h' for elem in time_context.relative_periods):
                        field_type = config.fields[field]['type']
            else:
                field_type = config.fields[field]['type']
            if field_type == 'NC' and 'hist_field' in config.fields[field] and config.show_raw_data_in_trend: # get raw field in case of NC
                if descendants:
                    raw_field, _ = config.fields[field]['source']
                else:
                    _,raw_field = config.fields[field]['source']
                adhoc_field_mapping[raw_field]=field
                field_type = config.fields[raw_field]['type']
                field = raw_field

            if field_type not in fields_by_type:
                fields_by_type[field_type] = []
            if field not in fields_by_type[field_type]:
                fields_by_type[field_type].append(field)
            #else:
            #    fields_by_type[field_type].append(field)

    if adhoc_field_mapping:
            for raw_field,field in adhoc_field_mapping.items():
                if field in fields:
                    fields.remove(field)
                if field in ignore_recency_for:
                    ignore_recency_for.remove(field)
                    ignore_recency_for.append(raw_field)
                fields.append(raw_field)

    if config.config.get('bucket_fields'):
        field_set, remove_set = set(fields_by_type['AI']), set(config.config.get('bucket_fields'))
        fields_by_type['AI'] = list(field_set - remove_set)

    return fields_by_type


def fetch_qtd_actual_data(period, node, as_of, config, time_context, read_weeks, historical_timestamp = None):
    week_nums = len(read_weeks)
    qtd_actual_timestamps = {}
    if week_nums == 0:
        return {}, qtd_actual_timestamps
    waterfall_config = config.config.get('waterfall_config', {})
    QTDActual = waterfall_config.get('QTDActual', {})
    waterfall_pivot = node.split('#')[0] if '#' in node else None
    if not waterfall_pivot or (waterfall_pivot not in QTDActual):
        return default_dynamics_data(week_nums), qtd_actual_timestamps

    pivot_config = QTDActual[waterfall_pivot]
    segment = pivot_config.get('segment', None)
    field = pivot_config.get('field', None)
    multiplier = pivot_config.get('multiplier', 1000)
    if not segment or not field:
        return default_dynamics_data(week_nums), qtd_actual_timestamps

    descendants = fetch_descendants(as_of, [node], levels=2, include_children=False)
    descendants = [(rec['node'], rec['descendants'][0], rec['descendants'][1]) for rec in descendants]
    ignore_recency_for = config.ignore_recency_for_fields
    fields_by_type = fetch_fields_by_type(config, time_context, descendants, [field], ignore_recency_for)
    cache = {}
    fm_recs = {}
    timestamps = []
    data = {}
    epoch_now = epoch().as_epoch()
    current_year, current_month, current_day = epoch2datetime(epoch_now).timetuple()[:3]
    current_eod_xl = get_eod(epoch(current_year, current_month, current_day)).as_xldate()
    if historical_timestamp:
        current_eod_xl = epoch(historical_timestamp).as_xldate()
    for week_range in read_weeks:
        begin_date_timestamp = convert_excel_date_to_eod_timestamp(week_range['begin'])
        timestamps.append(begin_date_timestamp)
        if week_range['begin'] >= current_eod_xl:
            break

    if len(timestamps) == len(read_weeks):
        timestamps.append(epoch(read_weeks[-1]['end']).as_epoch())

    try:
        fm_recs = bulk_fetch_fm_recs_history(time_context,
                                        descendants,
                                        [field],
                                        [segment],
                                        config,
                                        timestamps,
                                        cache=cache,
                                        ignore_recency_for=ignore_recency_for,
                                        fields_by_type=fields_by_type,
                                        get_all_segments=False)
    except Exception as e:
        logger.error("Failed! %s" % (e))

    no_of_req_weeks = len(timestamps)
    for week_range in read_weeks:
        if no_of_req_weeks:
           begin_date_timestamp = timestamps[len(timestamps)-no_of_req_weeks]
           data[week_range['label']] = fm_recs[(period, node, field, segment, begin_date_timestamp)]['val'] / multiplier
           qtd_actual_timestamps[week_range['label']] = fm_recs[(period, node, field, segment, begin_date_timestamp)]['timestamp']
           no_of_req_weeks -= 1
        else:
           data[week_range['label']] = 0

    if no_of_req_weeks:
        end_date_timestamp = timestamps[len(timestamps) - no_of_req_weeks]
        data['week14'] = fm_recs[(period, node, field, segment, end_date_timestamp)]['val'] / multiplier
        qtd_actual_timestamps['week14'] = fm_recs[(period, node, field, segment, end_date_timestamp)][
            'timestamp']

    return data, qtd_actual_timestamps

def fetch_prev_qtd_data(period, node, config):
    weeks = weekly_periods(period)
    weeks.sort(key = lambda x:x.begin)
    week_nums = len(weeks)

    waterfall_config = config.config.get('waterfall_config', {})
    Prev_QTD = waterfall_config.get('Prev_QTD', {})
    waterfall_pivot = node.split('#')[0] if '#' in node else None
    if not waterfall_pivot or (waterfall_pivot not in Prev_QTD):
        return default_dynamics_data(week_nums)

    pivot_config = Prev_QTD[waterfall_pivot]
    segment = pivot_config.get('segment', None)
    field = pivot_config.get('field', None)
    multiplier = pivot_config.get('multiplier', 1000)

    cache = {}
    for week in weeks:
        week_mnemonic = week.mnemonic
        time_context = get_time_context(week_mnemonic)
        descendants = (node, [], [])
        fetch_fm_rec(time_context, descendants,field, segment, config, cache=cache)

    res = {}
    for i in range(1, 14):
        week_key = "week"+str(i)
        try:
            val = float(cache[(weeks[min(i, 12)].mnemonic, node, field, segment, None)]['val'])
        except Exception:
            val = 0
        res[week_key] =  val / multiplier

    return res


def fetch_immediate_childs_status(immediate_childs, nodes_label, period, y_week, field, db = None):
    try:
        waterfall_data_collection = db[WATERFALL_COLL] if db else sec_context.tenant_db[WATERFALL_COLL]
        criteria = {
            'node':{'$in': immediate_childs},
            'period': period,
            'y_week': y_week,
            'field': field
        }
        projection = {
            'node': 1,
            'is_submitted':1
        }
        pipeline = [
            {'$match':criteria},
            {'$project': projection},
            {
                '$group':{
                    '_id': '$node',
                    'is_submitted': {'$first': '$is_submitted'}
                }
            }
        ]

        documents = list(waterfall_data_collection.aggregate(pipeline))
        final_data = {}
        for doc in documents:
            node_id = doc.get('_id')
            is_submitted = doc.get('is_submitted', False)
            if is_submitted is None:
                is_submitted = False
            final_data[node_id] = {
            'label': nodes_label.get(node_id),
            'is_submitted': is_submitted
            }
        for node_id , label in  nodes_label.items():
            if node_id not in final_data:
                final_data[node_id] = {
                    'label': label,
                    'is_submitted': False
                }
        return final_data
    except Exception as ex:
        logger.exception(ex)
        return None


def fetch_immediate_child_overview(node, period, y_week, field, as_of, db = None):
    try:
        descendants = fetch_descendants(as_of,  [node], levels=2, include_children=False)
        descendants = [(rec['node'], rec['descendants'][0], rec['descendants'][1], rec['parent']) for rec in descendants]
        if not descendants[0][2]:
            return []
        immediate_childs = list(descendants[0][1].keys())
        nodes_label = fetch_labels(as_of, immediate_childs)
        final_data = fetch_immediate_childs_status(immediate_childs, nodes_label, period, y_week, field)

        final_output = [{'node': node_id, 'label': data['label'], 'is_submitted': data['is_submitted'] }
                        for node_id , data in final_data.items()]

        return final_output
    except Exception as ex:
        logger.exception(ex)
        return None

def fetch_outweek_data_submit_status(node, period, y_week, field, db = None):
    try:
        result_node = node.rsplit('#', 1)[1]
        if result_node == 'Global' or result_node == '!':
            return {'is_submitted': None}

        waterfall_data_collection = db[WATERFALL_COLL] if db else sec_context.tenant_db[WATERFALL_COLL]
        criteria = {
            'node':node,
            'period': period,
            'y_week': y_week,
            'field': field,
            'is_submitted': {'$exists': True}
        }
        result = waterfall_data_collection.find_one(criteria, {'is_submitted': 1})
        is_submitted_status = result.get('is_submitted') if result else False
        if is_submitted_status not in [True, False]:
            return {'is_submitted': False}
        return {'is_submitted': is_submitted_status}
    except Exception as ex:
        logger.exception(ex)
        return None

def fetch_track_data(period, nodes, db = None, track_field= 'track'):
    try:
        waterfall_data_collection = db[WATERFALL_COLL] if db else sec_context.tenant_db[WATERFALL_COLL]
        criteria = {'period': period, 'field': track_field, 'node':{'$in': nodes}, 'y_week': None}
        cursor = waterfall_data_collection.find(criteria, { '_id': 0, 'node': 1, 'val': 1, 'x_week': 1} )
        default_track_data = default_dynamics_data()
        data = {}
        for doc in cursor:
            node_id = doc['node']
            value = doc['val']
            week  = doc['x_week']
            if node_id not in data:
                data[node_id] = default_track_data.copy()
            if week not in default_track_data:
                continue
            data[node_id][week] = value

        return data
    except Exception as ex:
        logger.exception(ex)
        return None


def fetch_waterfall_data(period, node, as_of,  db=None, read_weeks=[],
                         config={}, time_context={},
                         week_nums=13, is_mx_track=False,
                         historical_timestamp=None):
    waterfall_data_collection = db[WATERFALL_COLL] if db else sec_context.tenant_db[WATERFALL_COLL]
    if historical_timestamp:
        waterfall_data_collection = db[WATERFALL_HISTORY_COLL] if db else sec_context.tenant_db[WATERFALL_HISTORY_COLL]
    criteria = {'period': period, 'node': node}
    if historical_timestamp:
        criteria.update({
            'last_updated_at': {'$lte': historical_timestamp}
        })
        sort = {
            'last_updated_at' : -1
        }
        group = {
            "_id": {
                "node": "$node",
                "period": "$period",
                "field": "$field",
                "x_week": "$x_week",
                "y_week": "$y_week"
            },
            "latest_record": {"$first": "$$ROOT"}
        }
        replace_root = {"$replaceRoot": {"newRoot": "$latest_record"}}

        pipeline = [
            {'$match': criteria},
            {'$sort': sort},
            {'$group' : group},
            replace_root
        ]

        logger.info("pipeline for waterfall historical fetch is %s", pipeline)
        cursor = waterfall_data_collection.aggregate(pipeline)
    else:
        cursor = waterfall_data_collection.find(criteria)
    data = {'track': {}, 'weeksData': {}, 'rollover': {}}
    if is_mx_track:
        data.update({'mx_track': {}})

    # Fetch timestamp of each weekdata record separately
    weekdata_last_updated = {}
    weekdata_is_submitted = {}

    for doc in cursor:
        field = doc['field']
        x_week = doc['x_week']
        y_week = doc['y_week']
        val = doc['val']
        last_updated = doc['last_updated_at']
        is_submitted = doc.get('is_submitted', False)

        if field == 'track':
            data['track'][x_week] = val
        elif field == 'mx_track':
            if is_mx_track:
                data['mx_track'][x_week] = val
        elif field == 'weeksData':
            if y_week not in data['weeksData']:
                data['weeksData'][y_week] = {}
            if y_week not in weekdata_last_updated:
                weekdata_last_updated[y_week] = {}
            if y_week not in weekdata_is_submitted:
                weekdata_is_submitted[y_week] = {}
            data['weeksData'][y_week][x_week] = val
            weekdata_last_updated[y_week][x_week] = last_updated
            weekdata_is_submitted[y_week][x_week] = is_submitted
        elif field == 'rollover':
            data['rollover'][y_week] = val

    logger.info("FINAL DATA TO RESTRUCTURE : {}".format(data))

    restructured_data = {'track': {}, 'weeksData': {}, 'rollover': {}}
    if is_mx_track:
        restructured_data.update({'mx_track': {}})
    qtd_actual_data, qtd_actual_timestamps = fetch_qtd_actual_data(period, node, as_of, config, time_context,
                                                           read_weeks=read_weeks,
                                                           historical_timestamp = historical_timestamp)
    restructured_data['QTDActual'] = qtd_actual_data
    last_updated_qtd_actual_timestamp = None

    prev_qtd_data = fetch_prev_qtd_data(period, node, config)
    restructured_data['prev_QTD'] = prev_qtd_data

    for i in range(week_nums):
        week_key = "week{}".format(i+1)
        if week_key in qtd_actual_timestamps and qtd_actual_timestamps[week_key] > epoch(read_weeks[i]['begin']).as_epoch():
            last_updated_qtd_actual_timestamp = qtd_actual_timestamps[week_key]

    if last_updated_qtd_actual_timestamp:
        last_updated_qtd_actual_timestamp = epoch(last_updated_qtd_actual_timestamp).as_epoch()

    # Initialize track with zeros for all weeks
    for i in range(1, week_nums + 1):  # weeks range from 1 to 13
        week_key = "week{}".format(i)
        restructured_data['track'][week_key] = data['track'].get(week_key, 0)

    # Initialize mx_track with zeros for all weeks
    if is_mx_track:
        for i in range(1, week_nums + 1):  # weeks range from 1 to 13
            week_key = "week{}".format(i)
            restructured_data['mx_track'][week_key] = data['mx_track'].get(week_key, 0)

    # Initialize rollover with zeros for all weeks
    for i in range(week_nums + 1):  # weeks range from 1 to 13
        week_key = "week{}".format(i)
        restructured_data['rollover'][week_key] = data['rollover'].get(week_key, 0)

    # declare var to check whether current intersection is editable
    is_current_intersection_editable = True

    # Restructure weeksData

    for y_idx in range(week_nums + 1):
        y_week = "week{}".format(y_idx)
        restructured_week = OrderedDict()
        start_week = 1
        end_week = week_nums + 1

        # week numbers start from  1
        for x_idx in range(start_week, end_week):
            week_key = "week{}".format(x_idx)
            if x_idx < y_idx:
                y_prev_week = "week{}".format(y_idx-1)
                restructured_week[week_key] = data['weeksData'][y_prev_week][week_key]
                continue
            restructured_week[week_key] = data['weeksData'].get(y_week, {}).get(week_key, 0)

            # Intersection cell manipulation
            if y_idx in range(1, week_nums + 1) and y_week == week_key:
                now = get_now()
                wk_len = len(read_weeks)
                if y_idx <= wk_len:
                    # Current week is always Thursday to next Wednesday
                    active_week_begin = epoch(read_weeks[y_idx - 1]['begin']).as_datetime() + timedelta(days=3)
                    active_week_end = epoch(read_weeks[min(y_idx, wk_len-1)]['end']).as_datetime() - timedelta(days=4)
                    if y_idx == wk_len:
                        active_week_end = epoch(read_weeks[-1]['end']).as_datetime()
                    is_active_week = active_week_begin <= now.as_datetime() <= active_week_end
                    if historical_timestamp:
                        is_active_week = active_week_begin <= epoch(historical_timestamp).as_datetime() <= active_week_end

                    # If value is already submitted, continue
                    if weekdata_is_submitted.get(y_week, {}).get(week_key, False):
                        if is_active_week:
                            is_current_intersection_editable = False

                if y_idx <= len(read_weeks) and now.as_xldate() > read_weeks[y_idx-1]['end']:
                    # Find timestamps of data
                    week_data_timestamp = weekdata_last_updated.get(y_week, {}).get(week_key, 0)
                    next_week_key = "week{}".format(x_idx + 1)
                    currentQTD = restructured_data['QTDActual'].get(week_key, 0)
                    nextQTD = restructured_data['QTDActual'].get(next_week_key, 0)
                    nextQTD_timestamp = qtd_actual_timestamps.get(next_week_key, 0)
                    nextMonday_timestamp = epoch(read_weeks[min(y_idx, wk_len-1)]['begin']).as_epoch()
                    if y_idx == wk_len:
                        nextMonday_timestamp = epoch(read_weeks[min(y_idx, wk_len - 1)]['begin']).as_epoch() + 7*24*3600*1000

                    # If weeksData value is overridden after nextQTD value population
                    # we need to show weeksData value directly, however, shouldn't be editable
                    now_epoch = now.as_epoch()
                    if historical_timestamp:
                        now_epoch = historical_timestamp
                    if nextMonday_timestamp <= nextQTD_timestamp < now_epoch and nextQTD > 0 and week_data_timestamp > nextQTD_timestamp:
                        if is_active_week:
                            is_current_intersection_editable = False
                            continue
                    # if "file is loaded", i.e., if QTD value for next Monday is present,
                    # show the difference between next week start and current week start
                    # make intersection non-editable if it is current week
                    elif nextMonday_timestamp <= nextQTD_timestamp < now_epoch and nextQTD > 0:
                        # For week1, value has to be ignored
                        if week_key == 'week1':
                            currentQTD = 0
                        restructured_week[week_key] = nextQTD - currentQTD
                        if is_active_week:
                            is_current_intersection_editable = False
                    elif y_idx == wk_len:
                        restructured_week[week_key] = nextQTD - currentQTD

        data['weeksData'][y_week] = restructured_week
        restructured_data['weeksData'][y_week] = data['weeksData'][y_week]


    config = FMConfig()
    waterfall_config = config.config.get('waterfall_config', {})
    fm_to_waterfall_sync = waterfall_config.get('fm_to_waterfall_sync', {})
    waterfall_pivot = node.split('#')[0] if '#' in node else None
    if not waterfall_pivot or (waterfall_pivot not in fm_to_waterfall_sync):

        restructured_data['dynamicsData'] = default_dynamics_data(week_nums)
        return restructured_data, is_current_intersection_editable, last_updated_qtd_actual_timestamp

    pivot_config = fm_to_waterfall_sync[waterfall_pivot]
    segment = pivot_config.get('segment', None)
    field = pivot_config.get('field', None)
    multiplier = pivot_config.get('multiplier', 1000)
    if not segment or not field:
        restructured_data['dynamicsData'] = default_dynamics_data(week_nums)
        return restructured_data, is_current_intersection_editable, last_updated_qtd_actual_timestamp

    sum_field = config.segment_amount_fields.get(segment)
    seg_filter = config.segment_filters.get(segment)
    _, crit, _ = config.fields[field]['crit_and_ops']
    crit = {'$and': [crit, seg_filter]}
    close_date_field = config.deal_config.close_date_field

    restructured_data['dynamicsData'] = get_waterfall_weekly_totals(period, node, read_weeks, close_date_field, sum_field, crit, multiplier)

    return restructured_data, is_current_intersection_editable, last_updated_qtd_actual_timestamp

def default_dynamics_data(week_nums=13):
    data = {}
    for i in range(1, week_nums+1):
        data['week{}'.format(i)] = 0
    return data

def transform_week_string(input_str):
    # Match the week number in the string
    import re
    match = re.match(r'week(\d+)', input_str)
    if match:
        week_number = int(match.group(1))
        return 'week{}'.format(week_number - 1)
    else:
        raise ValueError("Input string does not match the expected format")



def get_forecast_schedule(nodes, db=None, get_dict={}):
    try:
        forecast_schedule_coll = db[FORECAST_SCHEDULE_COLL] if db else sec_context.tenant_db[FORECAST_SCHEDULE_COLL]
        criteria = {'node_id': {'$in': nodes}}
        projections = {"recurring": 1,
                       "unlockPeriod": 1,
                       "unlockFreq": 1,
                       "unlockDay": 1,
                       "unlocktime": 1,
                       "lockPeriod": 1,
                       "lockFreq": 1,
                       "lockDay": 1,
                       "locktime": 1,
                       "timeZone": 1,
                       "node_id": 1,
                       "lock_on": 1,
                       "unlock_on": 1,
                       "status": 1,
                       "non_recurring_timestamp": 1,
                       "status_non_recurring": 1
                       }
        response = forecast_schedule_coll.find(criteria, projections)
        if get_dict:
            return {res['node_id']: res for res in response}
        return [res for res in response]
    except Exception as ex:
        logger.exception(ex)
        return {'success': False, 'Error': str(ex)}


def get_requests(user_id=None, user_node=None, db=None):
    try:
        forecast_req_coll = db[FORECAST_UNLOCK_REQUESTS] if db else sec_context.tenant_db[FORECAST_UNLOCK_REQUESTS]
        criteria = {}
        if user_id is not None:
            criteria = {"user_id": user_id}
        if user_node:
            criteria['user_node'] = user_node
        response = forecast_req_coll.find(criteria, {"user_id": 1, "user_node": 1,
                                                     'email': 1, 'action': 1})
        return {res['user_id']: res for res in response}
    except Exception as ex:
        logger.exception(ex)
        return {'success': False, 'Error': str(ex)}


def get_user_level_schedule(user_id=None, user_node=None, db=None, get_user_by_list=None):
    try:
        user_level_schedule = db[USER_LEVEL_SCHEDULE] if db else sec_context.tenant_db[USER_LEVEL_SCHEDULE]
        criteria = {}
        if user_id is not None:
            criteria = {"user_id": user_id}
        if user_node:
            criteria['node_id'] = user_node
        response = user_level_schedule.find(criteria, {"user_id": 1, "email": 1,
                                                       'node_id': 1, 'forecastTimestamp': 1,
                                                       'forecastWindow': 1})
        result = []
        if get_user_by_list:
            for res in response:
                if res['user_id'] not in result:
                    result[res['user_id']] = [res]
                else:
                    result[res['user_id']].append(res)
            return result

        return {res['user_id'] + "_" +  res['node_id']: res for res in response}
    except Exception as ex:
        logger.exception(ex)
        return {'success': False, 'Error': str(ex)}

def is_valid_week_string(input_str):
    import re
    match = re.match(r'week(\d+)', input_str)
    if not match:
        raise ValueError("Input string does not match the expected format")

def find_req_periods(period, week_period = None):
    req_periods = []
    if week_period is not None:
        req_periods.append(week_period)
    else:
        quarter_weeks_range =  weekly_periods(period)
        for week_range in quarter_weeks_range:
            req_periods.append(week_range.mnemonic)
    return req_periods

def find_recs_from_weekly_forecast_fm_coll(nodes, period, week_period, segments, field, db=None, coll_suffix=None):
    coll_name = WEEKLY_FORECAST_FM_COLL
    fm_weekly_forecast_coll = '_'.join([coll_name, coll_suffix]) if coll_suffix else coll_name
    fm_weekly_forecast_collection = db[coll_name] if db else sec_context.tenant_db[fm_weekly_forecast_coll]
    req_periods = find_req_periods(period, week_period)

    criteria = {
        'node': {'$in': nodes},
        'period': {'$in': req_periods},
        'segment' : {'$in': segments},
        'field': field
    }
    projection = {
        'node': 1,
        'segment': 1,
        'val': 1,
        'period': 1,
        '_id': 0
    }
    pipeline = [
        {'$match':criteria},
        {'$project': projection}
    ]
    documents = list(fm_weekly_forecast_collection.aggregate(pipeline))
    return documents


def update_fm_weekly_snapshot_for_historical_avg(nodes, period, week_period, config, fm_weekly_segments, fm_weekly_snapshot):
    historical_field = 'historical_avg'
    fm_weekly_forecast_waterfall_config = config.config.get('weekly_forecast_waterfall_config', {})
    field_label = fm_weekly_forecast_waterfall_config.get('historical_avg', {}).get('label', historical_field)
    field_format = fm_weekly_forecast_waterfall_config.get('historical_avg', {}).get('format', 'percentage')
    field_editable = False
    documents = find_recs_from_weekly_forecast_fm_coll(nodes, period, week_period, fm_weekly_segments, historical_field)
    for doc in documents:
        required_keys = ['segment', 'val', 'period']
        if not all(k in doc for k in required_keys):
            continue
        seg = doc['segment']
        value = doc['val']
        mnemonic_period = doc['period']
        node_key = doc['node']
        node_seg_key = node_key + '_' + seg
        if historical_field not in fm_weekly_snapshot[node_seg_key]['fields']:
            fm_weekly_snapshot[node_seg_key]['fields'][historical_field] = {
                'weeks_period_data_val': {},
                'editable': field_editable,
                'format': field_format,
                'label': field_label
            }
        fm_weekly_snapshot[node_seg_key]['fields'][historical_field]['weeks_period_data_val'][mnemonic_period] = value


def find_previous_quarters(current_quarter, prev_years):
    year = int(current_quarter[:4])
    quarter = current_quarter[4:]
    previous_quarters = ["{}{}".format(year - i, quarter) for i in range(1, prev_years + 1)]
    return previous_quarters

def find_previous_week_quarters(current_week_quarter, prev_years):
    year = int(current_week_quarter[:4])
    week_quarter = current_week_quarter[4:]
    previous_week_quarters = ["{}{}".format(year - i, week_quarter) for i in range(1, prev_years + 1)]
    return previous_week_quarters


def get_crm_schedule(db=None):
    try:
        crm_schedule = db[CRM_SCHEDULE] if db else sec_context.tenant_db[CRM_SCHEDULE]
        criteria = {}
        response = crm_schedule.find(criteria)
        if response:
            return [res for res in response]
        else:
            return []
    except Exception as ex:
        logger.exception(ex)
        return {'success': False, 'Error': str(ex)}


def get_regional_admin_details(node_id, db=None):
    try:
        admin_mapping_coll = db[ADMIN_MAPPING] if db else sec_context.tenant_db[ADMIN_MAPPING]
        criteria = {'node_id': node_id}
        response = admin_mapping_coll.find(criteria)
        return {res['node_id']: res for res in response}
    except Exception as ex:
        logger.exception(ex)
        return {'success': False, 'Error': str(ex)}

def get_fm_weekly_records(period, descendants, config, eligible_nodes_for_segs, fm_weekly_fields, fm_weekly_segments, cache = {}):
    req_periods = find_req_periods(period)
    non_DR_fields, cur_DR_fields  = [], []
    time_context = get_time_context(period, config=None, quarter_editable=config.quarter_editable)
    fields_by_type, _ = get_fields_by_type(fm_weekly_fields, config, time_context)

    for _field in fm_weekly_fields:
        if 'DR' in fields_by_type and _field in fields_by_type['DR']:
            cur_DR_fields.append(_field)
        else:
            non_DR_fields.append(_field)

    if cur_DR_fields:
        time_context_list = [get_time_context(fm_rec_period, config=None, quarter_editable=config.quarter_editable) for fm_rec_period in req_periods]
        fetch_fm_rec_in_period(descendants, cur_DR_fields, fm_weekly_segments, config, eligible_nodes_for_segs, req_periods[0], cache, time_context_list = time_context_list)

    if non_DR_fields:
        for fm_rec_period in req_periods:
            fetch_fm_rec_in_period(descendants, non_DR_fields, fm_weekly_segments, config, eligible_nodes_for_segs, fm_rec_period, cache)

    return cache


def get_segments_in_weekly_forecast_for_user(config, node, period, segment = None):
    fm_weekly_forecast_waterfall_config = config.config.get('weekly_forecast_waterfall_config',{})
    as_of = get_period_as_of(period)
    is_pivot_special = False
    if node and node.split('#')[0] in config.get_special_pivots:
        is_pivot_special = True
    effective_user = sec_context.get_effective_user()
    user_edit_dims = effective_user.edit_dims if effective_user else None
    user_segments = config.get_segments(as_of, node, is_pivot_special= is_pivot_special)
    weekly_segments_list = list(fm_weekly_forecast_waterfall_config.get('segments', []))
    req_segments = []

    for _segment in weekly_segments_list:
        if _segment in user_segments and (not user_edit_dims or _segment in user_edit_dims):
            req_segments.append(_segment)

    if segment:
        req_segments = [segment]
        if 'segment_tree' in fm_weekly_forecast_waterfall_config:
            segment_keys_path = segment.split(".")
            req_segment_dict = get_nested(fm_weekly_forecast_waterfall_config["segment_tree"], segment_keys_path)
            req_segments = get_all_keys(req_segment_dict)

    return req_segments


def get_weekly_forecast_parameters(config, req_segments):
    fm_weekly_forecast_waterfall_config = config.config.get('weekly_forecast_waterfall_config',{})
    fm_weekly_fields =  list(fm_weekly_forecast_waterfall_config.get('weekly_fields_source', []))
    required_fields =   list(fm_weekly_forecast_waterfall_config.get('fields_order', []))
    fields_execution_order =   list(fm_weekly_forecast_waterfall_config.get('backend_fields_order', []))
    req_segments_map  = {segment: config.segments.get(segment, {}) for segment in (req_segments or [])}
    fm_quarter_fields = list(fm_weekly_forecast_waterfall_config.get('quarter_fields_source', []))
    total_weeks_fields = list(fm_weekly_forecast_waterfall_config.get('total_weeks_source', []))
    return fm_weekly_fields, fm_quarter_fields, required_fields,  req_segments_map, fields_execution_order, total_weeks_fields


def find_recursive_fm_weekly_require_field_val(mnemonic_period,
                                               node_id,
                                               require_field,
                                               weekly_segment,
                                               timestamp,
                                               weekly_fields_source,
                                               quarter_fields_source,
                                               total_weeks_source,
                                               backend_restore_weekly_field_source,
                                               calculation_cache,
                                               weekly_waterfall_config,
                                               sorted_weekly_periods_data,
                                               fm_weekly_recs,
                                               is_leaf):


    cache_key = (mnemonic_period, node_id, require_field, weekly_segment, timestamp)
    if cache_key in calculation_cache:
        return calculation_cache[cache_key]
    # Fetch field configuration
    field_config = weekly_waterfall_config.get('fields', {}).get(require_field, {})
    require_field_function = field_config.get('function', '')

    #this sources are used to calculate the require field value

    if 'schema_dependent_function' in field_config:
        target_schema = "rep" if is_leaf else "grid"
        require_field_function = field_config['schema_dependent_function'][target_schema]['function']
        is_default_function = 'default' in field_config['schema_dependent_function'][target_schema]
        default_function = field_config['schema_dependent_function'][target_schema].get('default', '')
        is_relative_period = 'relative_period' in field_config['schema_dependent_function'][target_schema]
        relative_periods = field_config['schema_dependent_function'][target_schema].get('relative_period', [])
    else:
        require_field_function = field_config.get('function', '')
        is_default_function = 'default' in field_config
        default_function = field_config.get('default', '')
        is_relative_period = 'relative_period' in field_config
        relative_periods = field_config.get('relative_period', [])

    derived_grid_source = []

    # Handle derived grid source
    if 'derived_grid_source' in field_config:
        for derived_grid_field in field_config['derived_grid_source']:
            derived_grid_field_val = find_recursive_fm_weekly_require_field_val(
                mnemonic_period, node_id, derived_grid_field, weekly_segment, timestamp,
                weekly_fields_source, quarter_fields_source, total_weeks_source, backend_restore_weekly_field_source,
                calculation_cache, weekly_waterfall_config, sorted_weekly_periods_data, fm_weekly_recs, is_leaf
            )
            derived_grid_source.append(derived_grid_field_val)

    # Evaluate the require_field function
    try:
        require_field_val = eval(require_field_function)
    except ZeroDivisionError:
        require_field_val = 0

    # Determine relative period
    cur_relative_period = sorted_weekly_periods_data.get(mnemonic_period, {}).get('relative_period', None)
    is_editable = field_config.get('editable', False)
    is_target_field_found = True if require_field_val != 0 else False
    if is_editable:
        target_field = field_config.get('target_field', require_field)
        is_target_field_found = fm_weekly_recs.get((mnemonic_period, node_id, target_field, weekly_segment, timestamp), {}).get('found', False)


    if is_default_function and (
        (not is_target_field_found and not is_relative_period) or
        (is_relative_period and cur_relative_period in relative_periods)
    ):

        try:
            require_field_val = eval(default_function)
        except ZeroDivisionError:
            require_field_val = 0


    # Cache and return the result
    calculation_cache[cache_key] = require_field_val
    return require_field_val


def get_weekly_periods_in_months(qtr_period):
    all_monthly_periods = find_monthly_periods(qtr_period)
    all_weekly_periods = find_req_periods(qtr_period)
    month_week_map = {}
    req_data = {}
    for week_period in all_weekly_periods:
        month_no = int(week_period[4:6])
        month_week_map.setdefault(month_no, []).append(week_period)

    for month_period in all_monthly_periods:
        month_no = int(month_period[4:6])
        req_data[month_period] = month_week_map[month_no]

    return req_data


def get_source_list_for_weekly_forecast(req_period, node_id, weekly_segment, timestamp, fm_recs, fm_fields, all_fields_source):
    fields_source = [0] * len(all_fields_source)

    for fm_field in fm_fields:
        fm_weekly_field_val = fm_recs.get((req_period, node_id, fm_field, weekly_segment, timestamp), {}).get('val', 0)
        field_index = all_fields_source.index(fm_field)
        fields_source[field_index] = fm_weekly_field_val

    return fields_source


def get_source_list_for_aggregate_fields_in_periods(req_periods, node_id, weekly_segment, timestamp, fm_recs, fm_fields, all_fields_source):
    fields_source = [0] * len(all_fields_source)

    for fm_field in fm_fields:

        period_sum = 0
        for _period in req_periods:
            cache_key = (_period, node_id, fm_field, weekly_segment, timestamp)
            period_sum += fm_recs.get(cache_key, 0)

        field_index = all_fields_source.index(fm_field)
        fields_source[field_index] = period_sum

    return fields_source

def get_children_of_nodes(nodes, as_of_timestamp, levels = 2):
    parent_child_map = {}
    descendants = fetch_descendants(as_of_timestamp, nodes , levels)
    descendants = [(rec['node'], rec['descendants'][0], rec['descendants'][1]) for rec in descendants]
    for key, dict1, _ in descendants:
        childs = list(dict1.keys())
        parent_child_map[key] = childs

    return parent_child_map


def is_weekly_forecast_field_editable(weekly_segment, field, weekly_waterfall_config, config):
    is_editable =  weekly_waterfall_config.get('fields', {}).get(field, {}).get('editable', False)
    segment_editable = 'segment_func' not in config.segments.get(weekly_segment, {})
    segment_dependent_editability = weekly_waterfall_config.get('fields', {}).get(field, {}).get('segment_dependent_editability', False)

    if not is_editable:
        return False

    # If the field is editable and its editability is segment-dependent, check for disabled edit fields or segment_func
    if segment_dependent_editability:
        if not segment_editable:
            return False

        target_field = weekly_waterfall_config.get('fields', {}).get(field, {}).get('target_field', field)
        disable_edit_fields = config.segments.get(weekly_segment, {}).get('disable_edit_fields', [])
        if target_field in disable_edit_fields:
            return False

    return True


def process_weekly_snapshot_field(node_id, weekly_segment, mnemonic_period, cur_field, cur_field_val, fm_weekly_snapshot,
                                  all_quarter_fields_source, quarter_fields_source, weekly_waterfall_config, config, sorted_weekly_periods_data):

    segment_label = config.segments.get(weekly_segment, {}).get('label', weekly_segment)
    node_segment_key = node_id + "_" + weekly_segment
    require_field_label =  weekly_waterfall_config.get('fields', {}).get(cur_field, {}).get('label','').format(segment= segment_label)
    require_field_format =  weekly_waterfall_config.get('fields', {}).get(cur_field, {}).get('format', 'amount')
    require_field_editable =  is_weekly_forecast_field_editable(weekly_segment, cur_field, weekly_waterfall_config, config)

    if cur_field not in fm_weekly_snapshot[node_segment_key]['fields']:
        fm_weekly_snapshot[node_segment_key]['fields'][cur_field] = {
            'weeks_period_data_val': {},
            'editable': require_field_editable,
            'format': require_field_format,
            'label': require_field_label
        }

    if 'monthly_forecast' in weekly_waterfall_config.get('fields', {}).get(cur_field, {}):
        monthly_forecast_field = weekly_waterfall_config.get('fields', {}).get(cur_field, {}).get('monthly_forecast')
        field_index = all_quarter_fields_source.index(monthly_forecast_field)
        monthly_forecast_val = quarter_fields_source[field_index]
        fm_weekly_snapshot[node_segment_key]['fields'][cur_field]['monthly_forecast'] = monthly_forecast_val

    if 'clickable_field' in weekly_waterfall_config.get('fields', {}).get(cur_field, {}):
        clickable_field = weekly_waterfall_config.get('fields', {}).get(cur_field, {}).get('clickable_field')
        fm_weekly_snapshot[node_segment_key]['fields'][cur_field]['clickable_field'] = clickable_field


    relative_period_val = sorted_weekly_periods_data.get(mnemonic_period, {}).get('relative_period', None)
    render_periods = weekly_waterfall_config.get('fields', {}).get(cur_field, {}).get('render_period', ['h', 'f', 'c'])
    if relative_period_val in render_periods:
        fm_weekly_snapshot[node_segment_key]['fields'][cur_field]['weeks_period_data_val'][mnemonic_period] = cur_field_val


def is_leaf_node(nodes, period):
    is_leaf_map = {}
    for _node_id in nodes:
        as_of = get_period_as_of(period)
        children = [x['node'] for x in fetch_children(as_of, [_node_id])]
        is_leaf = not children
        is_leaf_map[_node_id] =  is_leaf
    return is_leaf_map

def process_weekly_forecast_cumulative_field(node, segment,  restore_field, weekly_periods_val, fm_weekly_recs, sorted_weekly_periods_data):

    weekly_periods = list(sorted_weekly_periods_data.keys())
    running_sum = 0
    for week_period in weekly_periods:
        running_sum += weekly_periods_val[week_period]

        fm_weekly_recs[(week_period, node, restore_field, segment, None)] = {'period': week_period,
                                                                    'segment': segment,
                                                                    'val': running_sum,
                                                                    'by': 'system',
                                                                    'how': 'backend process',
                                                                    'found': True,
                                                                    'timestamp': None,
                                                                    'node': node,
                                                                    'field': restore_field}



def get_weekly_segment_snapshot_values_cache(all_nodes, fields_execution_order, req_periods, req_segments, period,
                                             fm_weekly_recs, fm_weekly_fields, total_weeks_fields,  fm_quarterly_recs,
                                             fm_quarter_fields, config, calculation_cache, weekly_waterfall_config):

    all_weekly_fields_source = list(weekly_waterfall_config.get('weekly_fields_source', []))
    all_quarter_fields_source = list(weekly_waterfall_config.get('quarter_fields_source', []))
    all_total_weeks_source =  list(weekly_waterfall_config.get('total_weeks_source', []))
    all_backend_restore_weekly_field_source = list(weekly_waterfall_config.get('backend_restore_weekly_field_source', []))
    backend_restore_fields = list(weekly_waterfall_config.get('backend_restore_weekly_field_source', []))
    sorted_weekly_periods_data = fetch_weekly_periods(period)
    is_leaf_map = is_leaf_node(all_nodes, period)

    # Process nodes and segments to calculate required field values
    for node_id, weekly_segment, timestamp in product(all_nodes, req_segments, [None]):
        is_leaf = is_leaf_map[node_id]
        total_weeks_source = [0] * len(all_total_weeks_source)
        # Process each field in execution order
        for cur_field in fields_execution_order:
            total_quarter_val = 0
            weekly_periods_val = {}
            field_config = weekly_waterfall_config.get('fields', {}).get(cur_field, {})
            is_cumulative_field = 'cumulative_process' in field_config and 'backend_restore_field' in field_config.get('cumulative_process', {})
            # Iterate over required periods to calculate field values
            for mnemonic_period in req_periods:
                # Fetch data sources for weekly and quarterly fields

                weekly_fields_source = get_source_list_for_weekly_forecast(
                    mnemonic_period, node_id, weekly_segment, timestamp, fm_weekly_recs, fm_weekly_fields, all_weekly_fields_source)

                quarter_fields_source = get_source_list_for_weekly_forecast(
                    period, node_id, weekly_segment, timestamp, fm_quarterly_recs, fm_quarter_fields, all_quarter_fields_source)

                backend_restore_weekly_field_source = get_source_list_for_weekly_forecast(
                    mnemonic_period, node_id, weekly_segment, timestamp, fm_weekly_recs, backend_restore_fields, all_backend_restore_weekly_field_source
                )

                cur_field_val = find_recursive_fm_weekly_require_field_val(
                    mnemonic_period, node_id, cur_field, weekly_segment, timestamp,
                    weekly_fields_source, quarter_fields_source, total_weeks_source,  backend_restore_weekly_field_source, calculation_cache,
                    weekly_waterfall_config, sorted_weekly_periods_data, fm_weekly_recs, is_leaf)

                total_quarter_val += cur_field_val
                weekly_periods_val[mnemonic_period] = cur_field_val

            if cur_field in total_weeks_fields:
                field_index = all_total_weeks_source.index(cur_field)
                total_weeks_source[field_index] = total_quarter_val

            if is_cumulative_field:
                restore_field = field_config.get('cumulative_process').get('backend_restore_field')
                process_weekly_forecast_cumulative_field(node_id, weekly_segment, restore_field, weekly_periods_val, fm_weekly_recs, sorted_weekly_periods_data)


def create_fm_weekly_snapshot(nodes, req_segments, req_periods, required_fields, period, fm_quarterly_recs, fm_quarter_fields ,
                              include_children_data,  parent_child_map,  nodes_labels,   calculation_cache,
                              fm_weekly_snapshot, config):

    weekly_waterfall_config = config.config.get('weekly_forecast_waterfall_config',{})
    all_quarter_fields_source = list(weekly_waterfall_config.get('quarter_fields_source', []))
    sorted_weekly_periods_data = fetch_weekly_periods(period)
    for node_id, weekly_segment, timestamp in product(nodes, req_segments, [None]):
        segment_label = config.segments.get(weekly_segment, {}).get('label', weekly_segment)
        node_segment_key = node_id + "_" + weekly_segment

        if node_segment_key not in fm_weekly_snapshot:
            fm_weekly_snapshot[node_segment_key] = {'fields': {}, 'segment': weekly_segment, 'label': segment_label}

        for cur_field in required_fields:
            childs_rollup_weekly =  {}
            is_rollup_field = 'child_rollup' in weekly_waterfall_config.get('fields', {}).get(cur_field, {})

            for mnemonic_period in req_periods:

                cur_field_val = calculation_cache.get((mnemonic_period, node_id, cur_field, weekly_segment, timestamp), 0)

                quarter_fields_source = get_source_list_for_weekly_forecast(
                    period, node_id, weekly_segment, timestamp, fm_quarterly_recs, fm_quarter_fields, all_quarter_fields_source)

                require_field_format = weekly_waterfall_config.get('fields', {}).get(cur_field, {}).get('format', 'amount')
                process_weekly_snapshot_field(
                    node_id, weekly_segment, mnemonic_period, cur_field, cur_field_val, fm_weekly_snapshot,
                    all_quarter_fields_source, quarter_fields_source, weekly_waterfall_config, config, sorted_weekly_periods_data)


                if is_rollup_field and include_children_data:
                    cur_child_field = weekly_waterfall_config.get('fields', {}).get(cur_field, {}).get('child_rollup', {}).get('field', cur_field)
                    if 'child_data' not in fm_weekly_snapshot[node_segment_key]['fields'][cur_field]:
                        fm_weekly_snapshot[node_segment_key]['fields'][cur_field]['child_data'] = {'editable': False, 'format': require_field_format}


                    for cur_child in parent_child_map[node_id]:
                        child_segment_key = cur_child + "_" + weekly_segment
                        if child_segment_key not in fm_weekly_snapshot[node_segment_key]['fields'][cur_field]['child_data']:
                            fm_weekly_snapshot[node_segment_key]['fields'][cur_field]['child_data'][child_segment_key] = {
                                'label': nodes_labels[cur_child], 'weeks_period_data_val': {}}

                        cur_child_val = calculation_cache.get((mnemonic_period, cur_child, cur_child_field, weekly_segment, timestamp), 0)

                        if mnemonic_period not in childs_rollup_weekly:
                            childs_rollup_weekly[mnemonic_period] = 0
                        childs_rollup_weekly[mnemonic_period] += cur_child_val

                        fm_weekly_snapshot[node_segment_key]['fields'][cur_field]['child_data'][child_segment_key]['weeks_period_data_val'][mnemonic_period] = cur_child_val

            if is_rollup_field and include_children_data:
                fm_weekly_snapshot[node_segment_key]['fields'][cur_field]['child_rollup_data'] = childs_rollup_weekly


def get_parent_child_metadata(nodes, as_of_timestamp, include_children_data = False):
    parent_child_map = {}
    if include_children_data:
        parent_child_map = get_children_of_nodes(nodes, as_of_timestamp)

    all_nodes = list(nodes)
    for key, value in parent_child_map.items():
        all_nodes.extend(value)

    return parent_child_map, all_nodes


def get_weekly_forecast_cache(period,
                            nodes ,
                            fm_weekly_fields,
                            fm_quarter_fields,
                            fields_execution_order,
                            total_weeks_fields,
                            req_segments,
                            req_segments_map,
                            req_periods,
                            as_of_timestamp,
                            config,
                            eligible_nodes_for_segs,
                            weekly_waterfall_config,
                            week_period = None,
                            include_children_data = False):

    # Initialize required data structures for storing records and cache
    fm_quarterly_recs, fm_weekly_recs, calculation_cache  = {}, {}, {}
    parent_child_map, all_nodes = get_parent_child_metadata(nodes, as_of_timestamp, include_children_data)
    descendants = [(node_id, {}, {}) for node_id in all_nodes]


    get_fm_weekly_records(period, descendants, config, eligible_nodes_for_segs,
                               fm_weekly_fields, req_segments_map, fm_weekly_recs)

    fetch_fm_rec_in_period(descendants, fm_quarter_fields, req_segments_map, config,
                                eligible_nodes_for_segs, period, fm_quarterly_recs)

    get_weekly_segment_snapshot_values_cache(all_nodes, fields_execution_order, req_periods, req_segments, period,
                                             fm_weekly_recs, fm_weekly_fields, total_weeks_fields,  fm_quarterly_recs,
                                             fm_quarter_fields, config, calculation_cache, weekly_waterfall_config)

    return fm_quarterly_recs, fm_weekly_recs, calculation_cache


def fetch_weekly_segment_snapshot(period,
                                  nodes ,
                                  fm_quarter_fields,
                                  required_fields,
                                  req_segments,
                                  fm_quarterly_recs,
                                  fm_weekly_recs,
                                  calculation_cache,
                                  req_periods,
                                  as_of_timestamp,
                                  config,
                                  week_period = None,
                                  include_children_data = False):

    # Initialize required data structures for storing records and cache
    fm_weekly_snapshot  = {}

    is_historical_avg = False
    if 'historical_avg' in required_fields:
        is_historical_avg = True
        required_fields.remove('historical_avg')

    parent_child_map, all_nodes = get_parent_child_metadata(nodes, as_of_timestamp, include_children_data)
    nodes_labels = fetch_labels(as_of_timestamp, all_nodes)

    create_fm_weekly_snapshot(nodes, req_segments, req_periods, required_fields, period, fm_quarterly_recs, fm_quarter_fields ,
                              include_children_data,  parent_child_map,  nodes_labels,   calculation_cache,
                              fm_weekly_snapshot, config)

    if is_historical_avg and 'historical_avg' in config.config.get('weekly_forecast_waterfall_config', {}):
        update_fm_weekly_snapshot_for_historical_avg(nodes, period, week_period, config, req_segments, fm_weekly_snapshot)

    return {'data': fm_weekly_snapshot}


def update_weekly_segment_snapshot_metadata(node, req_segments, segment,  data, as_of_timestamp, include_children_data, config):
    config_data = config.config
    fm_weekly_config = config_data.get('weekly_forecast_waterfall_config', {})

    # Generate segment order based on config and available data
    display_segments = config_data.get('segments', {}).get('display', [])
    segment_order = ["{}_{}".format(node, seg) for seg in display_segments if "{}_{}".format(node, seg) in data.get('data', {})]

    # Extract ordered fields present in snapshot data
    node_segment_key = "{}_{}".format(node, req_segments[0])
    config_fields_order = fm_weekly_config.get('fields_order', [])
    fields_in_snapshot = data.get('data', {}).get(node_segment_key, {}).get('fields', {}).keys()
    fields_order = [field for field in config_fields_order if field in fields_in_snapshot]

    weekly_snapshot_metadata = {'column_order': fields_order, 'segment_order': segment_order}

    if include_children_data:
        parent_child_map = get_children_of_nodes([node], as_of_timestamp)
        weekly_snapshot_metadata['child_key'] = parent_child_map.get(node, [])

    if 'segment_tree' in fm_weekly_config:
        segment_keys_path = (segment or 'segment_tree').split(".")
        req_segment_dict = get_nested(fm_weekly_config['segment_tree'], segment_keys_path, fm_weekly_config['segment_tree'])
        weekly_snapshot_metadata['segment_tree'] = modify_dict_keys(req_segment_dict, node)

    data['metadata'] = weekly_snapshot_metadata


def find_months_from_weekly_forecast_periods(req_periods):
    """Extract unique months from the data."""
    months = set()
    for key in req_periods:
        if len(key) == 9 and 'W' in key:
            month = int(key[4:6])
            months.add(month)

    months = sorted(months)
    start_index = 0
    for index in range(len(months)-1):
        if months[index+1]-months[index]>1:
            start_index = index+1
            break
    sorted_month_order = months[start_index:] + months[:start_index]
    return sorted_month_order


def create_key_from_weekly_mnemonic(period, mnemonic_key, custom_month_order):

    if len(mnemonic_key) == 9 and 'W' in  mnemonic_key:
        year = int(period[0:4])
        month_no = int(mnemonic_key[4:6])
        month = custom_month_order[month_no-1].capitalize()
        month_key = month[0:3]
        week_no = mnemonic_key[7:]
        return "FY{}-{}-WK{}".format(year, month_key, week_no)

    elif mnemonic_key in custom_month_order:
        return mnemonic_key.capitalize()

    return mnemonic_key



def find_field_names_from_weekly_forecast_export_coll(period, cursor, current_months, custom_month_order):
    """Generate field names based on available months and weeks."""
    fieldnames = ['Name', 'label']
    weeks_in_months = {}

    # Process the first record to identify weeks
    for record in cursor:
        for key in record:
            if len(key) == 9 and 'W' in key:
                month_no = int(key[4:6])
                new_key = create_key_from_weekly_mnemonic(period, key, custom_month_order)
                if  month_no not in weeks_in_months:
                    weeks_in_months[month_no] = []
                weeks_in_months[month_no].append(new_key)
        break

    # Add week and month field names
    for month_no in current_months:
        weeks_in_months[month_no] = sorted(weeks_in_months[month_no])
        month_key = custom_month_order[month_no-1].capitalize()
        fieldnames.extend(weeks_in_months[month_no])
        fieldnames.append(month_key)

    fieldnames.extend([
        'quarter-total',
        'Monthly Forecast',
        'Difference (Monthly-Weekly)'
    ])

    return fieldnames


def reorder_weekly_forecast_segment_rows(node, period, segment_rows, ordered_segment_rows, config):
    segment_order = config.config.get('segments', {}).get('display', {})
    lookup_dict = {}
    for key in segment_rows.keys():
        segment = key.split('_')[-1]
        lookup_dict[segment] = key

    for segment in segment_order:
        if segment in lookup_dict:
            key = lookup_dict[segment]
            ordered_segment_rows.append((key, segment_rows[key]))

    waterfall_config = config.config.get('weekly_forecast_waterfall_config', {})
    as_of_timestamp = get_period_as_of(period)
    all_fields = list(waterfall_config.get('fields_order', []))
    req_segments = get_segments_in_weekly_forecast_for_user(config, node, period)
    parent_child_map = get_children_of_nodes([node], as_of_timestamp)
    child_labels = fetch_labels(as_of_timestamp, parent_child_map.get(node, []))

    fields_label_order = {}
    for seg in req_segments:
        seg_key = "{}_{}".format(node, seg)
        segment_label = config.segments.get(seg, {}).get('label', seg)
        for field in all_fields:
            if field in waterfall_config.get('fields', {}):
                field_label = waterfall_config['fields'].get(field, {}).get('label', field).format(segment=segment_label)
                fields_label_order.setdefault(seg_key, []).append(field_label)
                if 'child_rollup' in waterfall_config['fields'][field]:
                    fields_label_order[seg_key].append("{}_Team Rollup".format(field_label))
                    for _child in parent_child_map.get(node, []):
                        fields_label_order[seg_key].append(child_labels.get(_child, _child))
            elif field in waterfall_config:
                field_label = waterfall_config.get(field, {}).get('label', field)
                fields_label_order.setdefault(seg_key, []).append(field_label)

    for key, seg_rows in ordered_segment_rows:
        ordered_labels = fields_label_order.get(key, [])
        label_index_map = {label: i for i, label in enumerate(ordered_labels)}
        seg_rows[:] = sorted(
            seg_rows,
            key=lambda x: label_index_map.get(x["label"], float("inf")),
        )



def reformat_weekly_forecast_export_data(node, period, cursor, custom_month_order, segment_header, ordered_segment_rows , other_rows, as_of, config):
    """Reformat forecast data to include appropriate headers and formatting."""
    segment_rows = {}
    for record in cursor:
        rec_copy = record.copy()
        rec_format = rec_copy.get('quarter-total-format', 'amount')
        node_key = rec_copy.get('node')
        labels = fetch_labels(as_of, [node_key])
        record['Name'] = labels[node_key]
        for key, value in rec_copy.items():
            # Process weekly data
            if len(key) == 9 and 'W' in key:
                value = value * 100 if rec_format == 'percentage' else value
                val = round(float(value), 2)
                new_key = create_key_from_weekly_mnemonic(period, key, custom_month_order)
                del record[key]
                record[new_key] = val

            # Process  monthly data
            elif key in custom_month_order:
                value = value * 100 if rec_format == 'percentage' else value
                val = round(float(value), 2)
                new_key = key.capitalize()
                del record[key]
                record[new_key] = val

            # Process quarter total
            elif key == 'quarter-total':
                value = value * 100 if rec_format == 'percentage' else value
                val = round(float(value), 2)
                record[key] = val

            elif key == 'monthly_forecast':
                new_key = 'Monthly Forecast'
                val = round(float(value), 2)
                del record[key]
                record[new_key] = val

            elif key == 'monthly_difference_in_amount':
                new_key = 'Difference (Monthly-Weekly)'
                val = round(float(value), 2)
                del record[key]
                record[new_key] = val

        # Categorize the processed record
        if 'segment' in rec_copy:
            if rec_copy['type'] == 'segment-header':
                segment_header[rec_copy['segment']] = record
            else:
                segment_rows.setdefault(rec_copy['segment'], []).append(record)
        else:
            other_rows.append(record)

    reorder_weekly_forecast_segment_rows(node, period, segment_rows, ordered_segment_rows, config)

def fetch_weekly_forecast_export_coll(node, period, req_segments, db=None):
    node_segments_key = []
    for segment in req_segments:
        node_segments_key.append(node+ "_" +segment)
    weekly_forecast_export_coll = db[WEEKLY_FORECAST_EXPORT_COLL] if db else sec_context.tenant_db[WEEKLY_FORECAST_EXPORT_COLL]
    criteria = {
        'period': period,
        'node': node,
        '$or': [
            {'segment': {'$in': node_segments_key}},
            {'segment': {'$exists': False}}
        ]
    }
    cursor = list(weekly_forecast_export_coll.find(criteria))
    return cursor


def fetch_cached_export(period, timestamp=None, config=None, db=None):
    as_of = timestamp if timestamp is not None else get_period_as_of(period)
    dt = epoch(as_of).as_datetime()
    as_of_date = dt.strftime("%Y-%m-%d")
    coll = EXPORT_ALL + "_" + str(period)
    export_all_coll = sec_context.tenant_db[coll]
    criteria = {'export_date': {'$lte': as_of_date}}
    file_name = list(export_all_coll.find(criteria).sort([('export_date', -1)]).limit(1))[0]['file_name']
    return file_name


def create_csv_and_save_in_bucket(period, bucket_name, fieldnames, data_rows, file_name):

    current_timestamp = epoch().as_epoch()
    csv_buffer = BytesIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()

    for row in data_rows:
        writer.writerow({key: row.get(key, None) for key in fieldnames})

    s3.put_object(
        Bucket = bucket_name,
        Key = file_name,
        Body = csv_buffer.getvalue(),  # Get the content from the buffer
        ContentType ='text/csv'
    )
    logger.info("File '{}' with {} total rows created successfully and saved to S3 bucket '{}' for period {}.".format(file_name, len(data_rows), bucket_name, period))

    file_path = "https://{}.s3.us-east-1.amazonaws.com/{}".format(bucket_name, file_name)
    return {"success": True, "data": {"updated_at": current_timestamp, "file_path": file_path}}


def get_weekly_edw_data(period = None):
    try:
        return sec_context.details.get_flag('fm_svc', WEEKLY_EDW_DATA, {})
    except :
        return None


def get_weekly_edw_process_status(period = None):
    try:
        return sec_context.details.get_flag('fm_svc', WEEKLY_EDW_PROCESS_STATUS, "Not Started")
    except:
        return None


def get_edw_data():
    try:
        return sec_context.details.get_flag('fm_svc',EDW_DATA, [])
    except:
        return None


def set_edw_data(edw_data):
    try:
        logger.info("EDW flag has been set",edw_data)
        return sec_context.details.set_flag('fm_svc',EDW_DATA,edw_data)
    except:
        logger.error("An error occurred while setting the edw_data value.")
        return None


def get_edw_process_update():
    try:
        return sec_context.details.get_flag('fm_svc',EDW_PROCESS_UPDATE, 'Started')
    except:
        return None


def set_edw_process_update(edw_update):
    try:
        logger.info("EDW flag has been set",edw_update)
        return sec_context.details.set_flag('fm_svc',EDW_PROCESS_UPDATE,edw_update)
    except:
        logger.error("An error occurred while setting the edw_data value.")
        return None

def get_weekly_edw_process_start_time():
    try:
        timestamp = epoch().as_epoch()
        return sec_context.details.get_flag('fm_svc', WEEKLY_EDW_PROCESS_START_TIME, timestamp)
    except:
        return None

def get_weekly_forecast_export_all(period):
    try:
        return sec_context.details.get_flag('fm_svc', WEEKLY_FORECAST_EXPORT_ALL.format(period), {})
    except:
        return None

def get_all_keys(data):
    """Recursively collects all keys in the dictionary."""
    keys = []
    for key, value in data.items():
        keys.append(key)
        if isinstance(value, dict):
            keys.extend(get_all_keys(value))
    return keys

def modify_dict_keys(data, prefix_key):
    if isinstance(data, dict):
        return {
            "{}_{}".format(prefix_key, key): modify_dict_keys(value, prefix_key)
            for key, value in data.items()
        }
    return data

def fetch_weekly_periods(period):
    periodconfig = PeriodsConfig()
    quarter_week_range =  weekly_periods(period)
    now = get_now(periodconfig)
    now_dt = now.as_datetime()
    weekly_periods_data = {}
    for week_range in quarter_week_range:
        week_info = render_period(week_range, now_dt, periodconfig)
        weekly_periods_data[week_range.mnemonic] = week_info

    sorted_weekly_periods_data = OrderedDict(sorted(weekly_periods_data.items(), key=lambda item: item[1]['end']))
    return sorted_weekly_periods_data


def fetch_expired_nodes(node, as_of, period):
    exp_from_date , exp_to_date = get_period_begin_end(period)
    exp_boundary_dates = (exp_from_date, exp_to_date)
    exp_child_order = [x['node'] for x in fetch_children(as_of, [node], period = period,  boundary_dates = exp_boundary_dates)]
    latest_child_nodes = [x['node'] for x in fetch_children(as_of, [node], period = period)]
    latest_child_nodes = list(set(latest_child_nodes))
    expired_nodes = set(exp_child_order) - set(latest_child_nodes)
    expired_nodes = list(expired_nodes)
    return expired_nodes


def get_children_periods_mapping(node, period):
    children_periods_map = {}
    # Get all the periods for the quarter
    qtr_periods = get_quarter_period(period)

    # Get the boundary dates for the quarter for fetching the children
    for qtr in qtr_periods:
        qtr_as_of = get_period_as_of(qtr)
        qtr_from_date, _ = fetch_boundry(qtr_as_of, drilldown=True)
        _, qtr_to_date = get_period_begin_end(qtr)
        qtr_boundary_dates = (qtr_from_date, qtr_to_date)
        children = [x['node'] for x in fetch_children(qtr_as_of, [node], period = qtr,  boundary_dates=qtr_boundary_dates)]
        children = list(set(children))
        children_periods_map[qtr] = children

    return children_periods_map


def fetch_cached_snapshot_in_period(period, node, config=None, db=None, segment=None):
    if not db:
        db = sec_context.tenant_db

    config = config if config else FMConfig()

    coll = SNAPSHOT_ROLE_COLL if config.has_roles else SNAPSHOT_COLL
    snapshot_coll = db[coll]
    if segment:
        criteria = {"node": node, "period": period, "segment": segment}
    else:
        criteria = {"node": node, "period": period, "segment": 'all_deals'}

    projections = {"snapshot_data": 1, "last_updated_time": 1, "snapshot_stale": 1, "how": 1}

    if config.has_roles:
        user_role = sec_context.get_current_user_role()
        if user_role not in config.get_roles:
            user_role = DEFAULT_ROLE
        criteria.update({'role': user_role})

    response = snapshot_coll.find_one(criteria, projections)
    result = {}
    if response:
        if response.get('snapshot_data'):
            result = response['snapshot_data']
            result['updated_at'] = response['last_updated_time']
    return result


def find_field_format(field, segment, config):
    field_format = config.config.get('fields', {}).get(field, {}).get('format', None)
    if field_format:
        return field_format
    percentage_override_segments = config.percentage_override_segments
    count_override_segments = config.count_override_segments
    if segment in count_override_segments:
        field_format = 'count'
    elif segment in percentage_override_segments:
        field_format = 'percentage'
    else:
        field_format = 'amount'

    return field_format


def fetch_active_nodes(waterfall_config, waterfall_pivot, node, period):
    waterfall_pivot_max_depth = waterfall_config.get('max_depth', {})
    leaf_level = 12
    if waterfall_pivot in waterfall_pivot_max_depth:
        leaf_level = waterfall_pivot_max_depth[waterfall_pivot]['leaf_depth']
    root_node = node.rsplit('#', 1)[0] + '#!'
    as_of = get_period_as_of(period)
    subtree_height = find_all_subtree_height(as_of, root_node, leaf_level)

    descendants = set()
    excluded_node_set = set()
    if waterfall_pivot in waterfall_pivot_max_depth:
        if 'exclude_node_and_descendants' in waterfall_pivot_max_depth[waterfall_pivot]:
            exclude_node_and_descendants = waterfall_pivot_max_depth[waterfall_pivot]['exclude_node_and_descendants']
            excluded_node_set.update(exclude_node_and_descendants)
            excluded_descendants = fetch_descendants(as_of,
                                                     nodes=exclude_node_and_descendants,
                                                     levels=12,
                                                     include_hidden=False,
                                                     include_children=False,
                                                     drilldown=True)
            for rec in excluded_descendants:
                if rec.get("descendants"):
                    for desc in rec['descendants']:
                        if desc:
                            for cnode, _ in desc.items():
                                excluded_node_set.add(cnode)

    for node, height in subtree_height.items():
        if height >= 1 and node not in excluded_node_set:
            descendants.add(node)

    return list(descendants)


def fetch_qtd_performance_gard_parameters(config, req_segments, period):
    qtd_gard_config = config.config.get('qtd_performance_config', {})
    weekly_forecast_config = qtd_gard_config.get('weekly_forecast_config', {})
    qtd_weekly_fields = list(weekly_forecast_config.get('weekly_fields_source', []))
    fields_execution_order = list(weekly_forecast_config.get('backend_fields_order', []))
    req_segments_map = {segment: config.segments.get(segment, {}) for segment in req_segments}
    qtd_quarter_fields = list(weekly_forecast_config.get('quarter_fields_source', []))
    total_weeks_fields = list(weekly_forecast_config.get('total_weeks_source', []))
    as_of_timestamp = get_period_as_of(period)
    weekly_periods = find_req_periods(period)
    req_fields = list(qtd_gard_config.get('field_schema', {}).keys())
    return qtd_weekly_fields, qtd_quarter_fields, req_segments_map, fields_execution_order, \
        total_weeks_fields, as_of_timestamp, weekly_periods, req_fields


def fetch_nodes_for_gard_dashboard(node, node_level, subnode_level, as_of):
    depth = int(subnode_level) - int(node_level)
    descendants = list(fetch_descendants(as_of, [node], depth))


    children = {rec['node']: list(rec['descendants'][0].keys()) for rec in descendants}.get(node, [])
    lth_grand_children = {rec['node']: list(rec['descendants'][depth - 1].keys()) for rec in descendants}.get(node, [])

    return lth_grand_children + (children if depth > 1 else []) + [node]


def fetch_hierarchy_order_for_gard_dashboard(node, node_level, subnode_level, as_of, period, boundary_dates=None):
    hierarchy_order = [{node: []}]
    depth = int(subnode_level) - int(node_level)
    children = [x['node'] for x in
                   fetch_children(as_of, [node], period, boundary_dates=boundary_dates)]

    if depth == 1:
        return hierarchy_order + [{child: []} for child in children]
    descendants = list(fetch_descendants(as_of, [node], depth))

    root_node = node.rsplit('#', 1)[0] + '#!'
    lth_grand_children = {rec['node']: list(rec['descendants'][depth - 1].keys()) for rec in descendants}.get(node, [])
    map_data = find_map_in_nodes_and_lth_grand_children(as_of, root_node, children, lth_grand_children)

    return hierarchy_order + [{child: map_data.get(child, [])} for child in children]


def get_monthly_sum_from_weekly(node_id, weekly_periods, weekly_field, segment, timestamp,
                                 month_rank, qtd_gard_weekly_recs, qtd_gard_calculation_cache):

    sorted_months = find_months_from_weekly_forecast_periods(weekly_periods)
    monthly_sum = {}

    for week_period in weekly_periods:
        cache_key = (week_period, node_id, weekly_field, segment, timestamp)
        week_val = qtd_gard_calculation_cache.get(cache_key, 0)
        month = int(week_period[4:6])
        monthly_sum[month] = monthly_sum.get(month, 0) + week_val

    #rank is 1 based
    if len(sorted_months) >= month_rank:
        return monthly_sum[sorted_months[month_rank - 1]]
    else:
        return 0

def fetch_qtd_performance_gard_snapshot(req_nodes,
                                        req_fields,
                                        period,
                                        segment,
                                        weekly_periods,
                                        qtd_gard_weekly_recs,
                                        qtd_gard_quarterly_recs,
                                        qtd_gard_calculation_cache,
                                        config,
                                        labels):

    qtd_gard_snapshot = {}
    field_schema = config.config.get('qtd_performance_config', {}).get('field_schema', {})

    for node_id , field, timestamp in product(req_nodes, req_fields, [None]):
        field_config = field_schema.get(field, {})
        val = 0
        if node_id not in qtd_gard_snapshot:
            qtd_gard_snapshot[node_id] = {'fields': {}}

        if 'value_type' in field_config and field_config['value_type'] == 'quarterly':
            val = qtd_gard_quarterly_recs.get((period, node_id, field, segment, timestamp), {}).get('val', 0)

        elif 'value_type' in field_config and field_config['value_type'] == 'monthly_sum' and 'weekly_field' in field_config and 'month_rank' in field_config:
            weekly_field = field_config['weekly_field']
            month_rank = field_config['month_rank']
            val = get_monthly_sum_from_weekly(node_id, weekly_periods, weekly_field, segment, timestamp,
                                               month_rank, qtd_gard_weekly_recs, qtd_gard_calculation_cache)

        qtd_gard_snapshot[node_id]['fields'][field] = {'value': val}
        if 'label' not in qtd_gard_snapshot[node_id]:
            qtd_gard_snapshot[node_id]['label'] = labels.get(node_id)

    return {'data':qtd_gard_snapshot}



def fetch_qtd_performance_product_snapshot(node_id,
                                        req_fields,
                                        period,
                                        segments,
                                        weekly_periods,
                                        qtd_gard_weekly_recs,
                                        qtd_gard_quarterly_recs,
                                        qtd_gard_calculation_cache,
                                        config):

    qtd_product_snapshot = {}
    field_schema = config.config.get('qtd_performance_config', {}).get('field_schema', {})

    for segment, field, timestamp in product(segments, req_fields, [None]):
        field_config = field_schema.get(field, {})
        val = 0
        if segment not in qtd_product_snapshot:
            qtd_product_snapshot[segment] = {'fields': {}}

        if 'value_type' in field_config and field_config['value_type'] == 'quarterly':
            val = qtd_gard_quarterly_recs.get((period, node_id, field, segment, timestamp), {}).get('val', 0)

        elif 'value_type' in field_config and field_config['value_type'] == 'monthly_sum' and 'weekly_field' in field_config and 'month_rank' in field_config:
            weekly_field = field_config['weekly_field']
            month_rank = field_config['month_rank']
            val = get_monthly_sum_from_weekly(node_id, weekly_periods, weekly_field, segment, timestamp,
                                               month_rank, qtd_gard_weekly_recs, qtd_gard_calculation_cache)

        qtd_product_snapshot[segment]['fields'][field] = {'value': val}
        if 'label' not in qtd_product_snapshot[segment]:
            qtd_product_snapshot[segment]['label'] = config.segments[segment].get('label', segment)

    return {'data':qtd_product_snapshot}


def update_qtd_performance_gard_metadata(data, hierarchy_order, config):
    qtd_gard_metadata = {}
    qtd_gard_config = config.config.get('qtd_performance_config', {})
    qtd_gard_metadata["column_order"] = qtd_gard_config.get('column_order', [])
    field_schema  = {}
    for field, field_config in qtd_gard_config.get('field_schema', {}).items():
        field_schema[field] = {
            'label': field_config.get('label', field),
            'format': field_config.get('format', 'amount')
        }
    qtd_gard_metadata["field_schema"] = field_schema
    qtd_gard_metadata['hierarchy_order'] = hierarchy_order

    data['metadata'] = qtd_gard_metadata


def update_qtd_performance_product_metadata(data, config):
    qtd_product_metadata = {}
    qtd_product_config = config.config.get('qtd_performance_config', {})
    qtd_product_metadata["column_order"] = qtd_product_config.get('column_order', [])
    field_schema  = {}
    for field, field_config in qtd_product_config.get('field_schema', {}).items():
        field_schema[field] = {
            'label': field_config.get('label', field),
            'format': field_config.get('format', 'amount')
        }
    qtd_product_metadata["field_schema"] = field_schema
    segments_order = qtd_product_config.get('segment_order', [])
    qtd_product_metadata['segment_order'] = segments_order
    segments = []
    for segment_order in segments_order:
        for i in segment_order:
            segments.append(i)
            segments += segment_order[i]
    segments_breakdown = []
    for segment in segments:
        segments_breakdown.append({"key": segment,
                                   "segment": segment,
                                   "label": config.segments[segment].get('label', segment),
                                   'segment_id': config.segments[segment].get('filter')})
    qtd_product_metadata['segment_breakdown'] = segments_breakdown
    data['metadata'] = qtd_product_metadata


def fetch_pipeline_gard_parameters(config, req_segments, period):
    pipeline_config = config.config.get('pipeline_config', {})
    req_fields = pipeline_config.get('fields', {})
    req_segments_map = {segment: config.segments.get(segment, {}) for segment in req_segments}
    as_of_timestamp = get_period_as_of(period)
    weekly_periods = find_req_periods(period)
    return req_segments_map, as_of_timestamp, weekly_periods, req_fields


def get_pipeline_data(time_context,
                      period,
                      nodes ,
                      fields,
                      req_segments,
                      as_of_timestamp,
                      config,
                      eligible_nodes_for_segs,
                      include_children_data = False):

    parent_child_map, all_nodes = get_parent_child_metadata(nodes, as_of_timestamp, include_children_data)
    descendants = [(node_id, {}, {}) for node_id in all_nodes]
    fm_recs = {}
    fm_recs.update(bulk_fetch_recs_by_timestamp(time_context,
                                                descendants,
                                                fields,
                                                req_segments,
                                                config,
                                                eligible_nodes_for_segs,
                                                [None]))
    return fm_recs


def fetch_pipeline_gard_snapshot(req_nodes,
                                 req_fields,
                                 period,
                                 segment,
                                 fm_recs,
                                 labels):

    pipeline_gard_snapshot = {}

    for node_id , field, timestamp in product(req_nodes, req_fields, [None]):
        if node_id not in pipeline_gard_snapshot:
            pipeline_gard_snapshot[node_id] = {'fields': {}}

        val = fm_recs.get((period, node_id, field, segment, timestamp), {}).get('val', 0)

        pipeline_gard_snapshot[node_id]['fields'][field] = {'value': val}
        if 'label' not in pipeline_gard_snapshot[node_id]:
            pipeline_gard_snapshot[node_id]['label'] = labels.get(node_id)

    return {'data': pipeline_gard_snapshot}


def update_pipeline_gard_metadata(data, hierarchy_order, config):
    pipeline_gard_metadata = {}
    pipeline_config = config.config.get('pipeline_config', {})
    pipeline_gard_metadata["column_order"] = pipeline_config.get('column_order', [])
    field_schema  = {}
    for field, field_config in pipeline_config.get('field_schema', {}).items():
        field_schema[field] = {
            'label': field_config.get('label', field),
            'format': field_config.get('format', 'amount')
        }
    pipeline_gard_metadata["field_schema"] = field_schema
    pipeline_gard_metadata['hierarchy_order'] = hierarchy_order

    data['metadata'] = pipeline_gard_metadata


def fetch_pipeline_product_snapshot(node_id,
                                    config,
                                    req_fields,
                                    period,
                                    segments,
                                    fm_recs):

    pipeline_product_snapshot = {}

    for segment, field, timestamp in product(segments, req_fields, [None]):
        if segment not in pipeline_product_snapshot:
            pipeline_product_snapshot[segment] = {'fields': {}}

        val = fm_recs.get((period, node_id, field, segment, timestamp), {}).get('val', 0)

        pipeline_product_snapshot[segment]['fields'][field] = {'value': val}
        if 'label' not in pipeline_product_snapshot:
            pipeline_product_snapshot[segment]['label'] = config.segments[segment].get('label', segment)

    return {'data': pipeline_product_snapshot}


def update_pipeline_product_metadata(data, config):
    pipeline_product_metadata = {}
    pipeline_config = config.config.get('pipeline_config', {})
    pipeline_product_metadata["column_order"] = pipeline_config.get('column_order', [])
    field_schema  = {}
    for field, field_config in pipeline_config.get('field_schema', {}).items():
        field_schema[field] = {
            'label': field_config.get('label', field),
            'format': field_config.get('format', 'amount')
        }
    pipeline_product_metadata["field_schema"] = field_schema
    segments_order = pipeline_config.get('segment_order', [])
    pipeline_product_metadata['segment_order'] = segments_order
    segments = []
    for segment_order in segments_order:
        for i in segment_order:
            segments.append(i)
            segments += segment_order[i]
    segments_breakdown = []
    for segment in segments:
        segments_breakdown.append({"key": segment,
                                   "segment": segment,
                                   "label": config.segments[segment].get('label', segment),
                                   'segment_id': config.segments[segment].get('filter')})
    pipeline_product_metadata['segment_breakdown'] = segments_breakdown
    data['metadata'] = pipeline_product_metadata


def find_monthly_periods_info(q_period):

    periodconfig = PeriodsConfig()
    quarter_month_range =  monthly_periods(q_period)
    now = get_now(periodconfig)
    now_dt = now.as_datetime()
    monthly_periods_data = {}
    for month_range in quarter_month_range:
        month_info = render_period(month_range, now_dt, periodconfig)
        monthly_periods_data[month_range.mnemonic] = month_info

    return monthly_periods_data


def find_quarterly_periods_info(q_periods):

    avail_qs = [prd for prd in get_all_periods__m('Q') if prd.mnemonic in q_periods]
    periodconfig = PeriodsConfig()
    now = get_now(periodconfig)
    now_dt = now.as_datetime()

    quarterly_periods_data = {}
    for quarter_range in avail_qs:
        quarter_range_info = render_period(quarter_range, now_dt, periodconfig)
        quarterly_periods_data[quarter_range.mnemonic] = quarter_range_info

    return quarterly_periods_data


def find_monthly_periods(q_period):
    req_periods = []
    quarter_month_range =  monthly_periods(q_period)
    for month_range in quarter_month_range:
        req_periods.append(month_range.mnemonic)
    return req_periods


def get_fm_monthly_records(period, descendants, config, eligible_nodes_for_segs, fm_monthly_fields, segment_map, cache = {}):

    monthly_periods = find_monthly_periods(period)
    non_DR_fields, cur_DR_fields  = [], []
    time_context = get_time_context(period, config=None, quarter_editable=config.quarter_editable)
    fields_by_type, _ = get_fields_by_type(fm_monthly_fields, config, time_context)

    for _field in fm_monthly_fields:
        if 'DR' in fields_by_type and _field in fields_by_type['DR']:
            cur_DR_fields.append(_field)
        else:
            non_DR_fields.append(_field)

    if cur_DR_fields:
        time_context_list = [get_time_context(fm_rec_period, config=None, quarter_editable=config.quarter_editable) for fm_rec_period in monthly_periods]
        fetch_fm_rec_in_period(descendants, cur_DR_fields, segment_map, config, eligible_nodes_for_segs, monthly_periods[0], cache, time_context_list = time_context_list)

    if non_DR_fields:
        for fm_rec_period in monthly_periods:
            fetch_fm_rec_in_period(descendants, non_DR_fields, segment_map, config, eligible_nodes_for_segs, fm_rec_period, cache)

    return cache

def find_recursive_fm_monthly_require_field_val(mnemonic_period,
                                               node_id,
                                               require_field,
                                               weekly_segment,
                                               timestamp,
                                               weekly_forecast_config,
                                               sorted_monthly_periods_data,
                                               monthly_fields_source,
                                               quarter_fields_source,
                                               weekly_aggregate_monthly_source,
                                               monthly_calculation_cache,
                                               is_leaf):


    cache_key = (mnemonic_period, node_id, require_field, weekly_segment, timestamp)

    if cache_key in monthly_calculation_cache:
        return monthly_calculation_cache[cache_key]

    field_config = weekly_forecast_config.get('fields', {}).get(require_field, {})

    if 'schema_based' in field_config['monthly_info']:
        target_schema = "rep" if is_leaf else "grid"
        require_field_function = field_config['monthly_info']['schema_based'][target_schema]['function']
        is_default_function = 'default' in field_config['monthly_info']['schema_based'][target_schema]
        default_function = field_config['monthly_info']['schema_based'][target_schema].get('default', '')
        is_relative_period = 'relative_period' in field_config['monthly_info']['schema_based'][target_schema]
        relative_period_values = field_config['monthly_info']['schema_based'][target_schema].get('relative_period', [])

    # Evaluate the require_field function
    try:
        require_field_val = eval(require_field_function)
    except ZeroDivisionError:
        require_field_val = 0

    # Determine relative period
    cur_relative_period = sorted_monthly_periods_data.get(mnemonic_period, {}).get('relative_period', None)

    if is_default_function and is_relative_period and cur_relative_period in relative_period_values:

        try:
            require_field_val = eval(default_function)
        except ZeroDivisionError:
            require_field_val = 0


    # Cache and return the result
    monthly_calculation_cache[cache_key] = require_field_val
    return require_field_val


def get_monthly_forecast_cache(period,
                               nodes,
                               req_segments,
                               fields_execution_order,
                               fm_monthly_fields,
                               segment_map,
                               config,
                               eligible_nodes_for_segs,
                               weekly_calculation_cache,
                               fm_quarterly_recs,
                               weekly_forecast_config):

    fm_monthly_recs, monthly_calculation_cache = {}, {}
    descendants = [(node_id, {}, {}) for node_id in nodes]
    get_fm_monthly_records(period, descendants, config, eligible_nodes_for_segs, fm_monthly_fields, segment_map, fm_monthly_recs)
    sorted_monthly_periods_data = find_monthly_periods_info(period)
    all_monthly_fields_source = weekly_forecast_config.get('monthly_fields_source', [])
    monthly_fields = weekly_forecast_config.get('monthly_fields_source', [])
    all_quarter_fields_source = weekly_forecast_config.get('quarter_fields_source', [])
    quarter_fields = weekly_forecast_config.get('quarter_fields_source', [])
    all_weekly_aggregate_monthly_source = weekly_forecast_config.get('weekly_aggregate_monthly_source', [])
    weekly_aggregate_fields = weekly_forecast_config.get('weekly_aggregate_monthly_source', [])
    weeks_in_month = get_weekly_periods_in_months(period)
    monthly_periods = find_monthly_periods(period)
    is_leaf_map = is_leaf_node(nodes, period)


    for node_id, weekly_segment in product(nodes, req_segments):
        is_leaf = is_leaf_map[node_id]
        for require_field in fields_execution_order:
            field_config = weekly_forecast_config.get('fields', {}).get(require_field, {})
            is_monthly_info_field =  'monthly_info' in field_config
            if not is_monthly_info_field:
                continue

            for month_mnemonic in monthly_periods:

                req_weeks_periods = weeks_in_month[month_mnemonic]
                monthly_fields_source = get_source_list_for_weekly_forecast(month_mnemonic, node_id, weekly_segment, None,
                                            fm_monthly_recs, monthly_fields, all_monthly_fields_source)

                quarter_fields_source = get_source_list_for_weekly_forecast(period, node_id, weekly_segment, None,
                                            fm_quarterly_recs, quarter_fields, all_quarter_fields_source)

                weekly_aggregate_monthly_source  = get_source_list_for_aggregate_fields_in_periods(req_weeks_periods, node_id, weekly_segment, None,
                                            weekly_calculation_cache, weekly_aggregate_fields, all_weekly_aggregate_monthly_source)

                find_recursive_fm_monthly_require_field_val(month_mnemonic,
                                               node_id,
                                               require_field,
                                               weekly_segment,
                                               None,
                                               weekly_forecast_config,
                                               sorted_monthly_periods_data,
                                               monthly_fields_source,
                                               quarter_fields_source,
                                               weekly_aggregate_monthly_source,
                                               monthly_calculation_cache,
                                               is_leaf)



    return fm_monthly_recs, monthly_calculation_cache


def get_monthly_forecast_parameters(config, q_period):
    fm_monthly_fields = config.config.get('weekly_forecast_waterfall_config', {}).get('monthly_fields_source')
    monthly_periods = find_monthly_periods(q_period)
    return fm_monthly_fields, monthly_periods


def is_monthly_forecast_field_editable(weekly_segment, target_schema, month_status, field_config, weekly_waterfall_config, config):
    is_editable =  month_status in field_config['monthly_info']["schema_based"][target_schema].get('editable', [])

    if not is_editable:
        return False

    target_field = field_config['monthly_info']['schema_based'][target_schema].get(month_status)
    segment_editable = 'segment_func' not in config.segments.get(weekly_segment, {})
    segment_dependent_editability = field_config.get('segment_dependent_editability', False)


    # If the field is editable and its editability is segment-dependent, check for disabled edit fields or segment_func
    if segment_dependent_editability:
        if not segment_editable:
            return False

        disable_edit_fields = config.segments.get(weekly_segment, {}).get('disable_edit_fields', [])
        if target_field in disable_edit_fields:
            return False

    return True



def update_months_data_in_weekly_snapshot(
        period, nodes, required_fields, req_segments,
        monthly_calculation_cache, monthly_periods, snapshot_data, config):

    monthly_periods_info = find_monthly_periods_info(period)
    weekly_waterfall_config = config.config.get('weekly_forecast_waterfall_config', {})
    field_configs = weekly_waterfall_config.get('fields', {})
    is_leaf_map = is_leaf_node(nodes, period)

    for node_id, segment_id in product(nodes, req_segments):
        node_seg_key = "{}_{}".format(node_id, segment_id)
        is_leaf = is_leaf_map[node_id]
        node_data = snapshot_data['data'].setdefault(node_seg_key, {}).setdefault('fields', {})

        for field_id in required_fields:
            field_config = field_configs.get(field_id, {})
            monthly_info_config = field_config.get('monthly_info', {})

            if not monthly_info_config:
                continue

            field_data = node_data.setdefault(field_id, {})

            for month_mnemonic in monthly_periods:
                month_status = monthly_periods_info.get(month_mnemonic, {}).get('relative_period')

                if 'schema_based' in monthly_info_config:
                    target_schema = "rep" if is_leaf else "grid"
                    cache_key = (month_mnemonic, node_id, field_id, segment_id, None)
                    value = monthly_calculation_cache.get(cache_key, 0)

                    is_editable = is_monthly_forecast_field_editable(
                        segment_id, target_schema, month_status, field_config,
                        weekly_waterfall_config, config
                    )

                    if month_status in monthly_info_config.get('render_period', ['h', 'f', 'c']):
                        months_data = field_data.setdefault('monthly_info', {})
                        months_data[month_mnemonic] = {'value': value, 'editable': is_editable}
                        if is_editable:
                            target_field = monthly_info_config['schema_based'][target_schema].get(month_status)
                            months_data[month_mnemonic]['target_editable_field'] = target_field



def find_recursive_fm_quarterly_require_field_val(mnemonic_period,
                                            node_id,
                                            require_field,
                                            weekly_segment,
                                            timestamp,
                                            weekly_forecast_config,
                                            quarter_period_info,
                                            total_weeks_source,
                                            total_months_source,
                                            quarter_fields_source,
                                            quarterly_calculation_cache,
                                            is_leaf):



    cache_key = (mnemonic_period, node_id, require_field, weekly_segment, timestamp)

    if cache_key in quarterly_calculation_cache:
        return quarterly_calculation_cache[cache_key]

    field_config = weekly_forecast_config.get('fields', {}).get(require_field, {})

    if 'schema_based' in field_config['quarterly_info']:
        target_schema = "rep" if is_leaf else "grid"
        require_field_function = field_config['quarterly_info']['schema_based'][target_schema]['function']
        is_default_function = 'default' in field_config['quarterly_info']['schema_based'][target_schema]
        default_function = field_config['quarterly_info']['schema_based'][target_schema].get('default', '')
        is_relative_period = 'relative_period' in field_config['quarterly_info']['schema_based'][target_schema]
        relative_period_values = field_config['quarterly_info']['schema_based'][target_schema].get('relative_period', [])

    # Evaluate the require_field function
    try:
        require_field_val = eval(require_field_function)
    except ZeroDivisionError:
        require_field_val = 0

    # Determine relative period
    cur_relative_period = quarter_period_info.get(mnemonic_period, {}).get('relative_period', None)

    if is_default_function and is_relative_period and cur_relative_period in relative_period_values:

        try:
            require_field_val = eval(default_function)
        except ZeroDivisionError:
            require_field_val = 0


    # Cache and return the result
    quarterly_calculation_cache[cache_key] = require_field_val
    return require_field_val


def get_quarterly_forecast_cache(period,
                               nodes,
                               req_segments,
                               fields_execution_order,
                               config,
                               weekly_calculation_cache,
                               monthly_calculation_cache,
                               fm_quarterly_recs,
                               weekly_forecast_config):

    quarterly_calculation_cache = {}
    all_total_months_source = weekly_forecast_config.get('total_months_source', [])
    total_months_fields = weekly_forecast_config.get('total_months_source', [])
    all_quarter_fields_source = weekly_forecast_config.get('quarter_fields_source', [])
    quarter_fields = weekly_forecast_config.get('quarter_fields_source', [])
    all_total_weeks_source = weekly_forecast_config.get('total_weeks_source', [])
    total_weeks_fields = weekly_forecast_config.get('total_weeks_source', [])
    req_weeks_periods = find_req_periods(period)
    monthly_periods = find_monthly_periods(period)
    quarter_period_info = find_quarterly_periods_info([period])
    is_leaf_map = is_leaf_node(nodes, period)


    for node_id, weekly_segment in product(nodes, req_segments):
        is_leaf = is_leaf_map[node_id]
        for require_field in fields_execution_order:

            field_config = weekly_forecast_config.get('fields', {}).get(require_field, {})
            is_quarterly_info_field =  'quarterly_info' in field_config
            if not is_quarterly_info_field:
                continue

            quarter_fields_source = get_source_list_for_weekly_forecast(period, node_id, weekly_segment, None,
                                        fm_quarterly_recs, quarter_fields, all_quarter_fields_source)

            total_weeks_source  = get_source_list_for_aggregate_fields_in_periods(req_weeks_periods, node_id, weekly_segment, None,
                                        weekly_calculation_cache, total_weeks_fields, all_total_weeks_source)

            total_months_source = get_source_list_for_aggregate_fields_in_periods(monthly_periods, node_id, weekly_segment, None,
                                        monthly_calculation_cache, total_months_fields, all_total_months_source)

            find_recursive_fm_quarterly_require_field_val(period,
                                            node_id,
                                            require_field,
                                            weekly_segment,
                                            None,
                                            weekly_forecast_config,
                                            quarter_period_info,
                                            total_weeks_source,
                                            total_months_source,
                                            quarter_fields_source,
                                            quarterly_calculation_cache,
                                            is_leaf)

    return quarterly_calculation_cache


def update_quarterly_data_in_weekly_snapshot(period, nodes, required_fields, req_segments,
        quarterly_calculation_cache, snapshot_data, config):


    weekly_waterfall_config = config.config.get('weekly_forecast_waterfall_config', {})
    quarterly_periods_info = find_quarterly_periods_info([period])
    field_configs = weekly_waterfall_config.get('fields', {})

    for node_id, segment_id in product(nodes, req_segments):
        node_seg_key = "{}_{}".format(node_id, segment_id)
        node_data = snapshot_data['data'].setdefault(node_seg_key, {}).setdefault('fields', {})

        for field_id in required_fields:
            field_config = field_configs.get(field_id, {})
            quarterly_info_config = field_config.get('quarterly_info', {})

            if not quarterly_info_config:
                continue

            field_data = node_data.setdefault(field_id, {})
            quarter_status = quarterly_periods_info.get(period, {}).get('relative_period')
            cache_key = (period, node_id, field_id, segment_id, None)
            value = quarterly_calculation_cache.get(cache_key, 0)

            if quarter_status in quarterly_info_config.get('render_period', ['h', 'f', 'c']):
                field_data['quarter-total'] = value


def get_snapshot_window_feature_data(node, config):
    status = None
    forecastTimestamp = None
    user_det = sec_context.get_effective_user()
    userid = user_det.user_id
    user_level_schedule = get_user_level_schedule(user_id=userid, user_node=node)
    as_of = epoch().as_epoch()
    fm_schedule = get_forecast_schedule([node], get_dict=True)
    forecastWindow = "unlock"
    user_node = node
    if not fm_schedule:
        return {"forecast_status": config.forecast_window_default,
                "forecastTimestamp": as_of}
    if fm_schedule.get(user_node).get("recurring"):
        lockPeriod = fm_schedule[user_node].get('lockPeriod', 'month')
        lockFreq = fm_schedule[user_node].get('lockFreq', 'month')
        unlockDay = int(fm_schedule[user_node].get('unlockDay', 0))
        unlocktime = fm_schedule[user_node]['unlocktime'].split(":") if 'unlocktime' in fm_schedule[user_node] \
            else ["00", "00"]
        lockDay = int(fm_schedule[user_node]['lockDay']) if 'lockDay' in fm_schedule[user_node] \
            else 0
        locktime = fm_schedule[user_node]['locktime'].split(":") if 'locktime' in fm_schedule[user_node] \
            else ["00", "00"]
        timeZone_original = fm_schedule.get(user_node)['timeZone'] if 'timeZone' in fm_schedule[user_node] \
            else "PST"
        from micro_fm_app.fm_service.api.forecast_schedule import timezone_map
        timeZone = timezone_map[timeZone_original.upper()]
        lock_on = None
        unlock_on = None
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month
        if lockPeriod == 'month' or (lockPeriod == 'quarter' and lock_on is None):
            unlock_on = datetime.datetime(current_year, current_month, unlockDay,
                                            int(unlocktime[0]), int(unlocktime[1]), 0)
            zone = pytz.timezone(timeZone)
            unlock_on = zone.localize(unlock_on)
            if lockFreq == 'month':
                lock_on = datetime.datetime(current_year, current_month, lockDay,
                                            int(locktime[0]), int(locktime[1]), 0)
                zone = pytz.timezone(timeZone)
                lock_on = zone.localize(lock_on)
            elif lockFreq == 'days' or lockFreq == 'weekdays':
                lock_on = unlock_on + datetime.timedelta(lockDay)

        if as_of >= epoch(lock_on).as_epoch():
            forecastWindow = "lock"
            forecastTimestamp = epoch(lock_on).as_epoch()
        if as_of >= epoch(unlock_on).as_epoch() and epoch(unlock_on).as_epoch() > epoch(lock_on).as_epoch():
            forecastWindow = "unlock"
            forecastTimestamp = epoch(unlock_on).as_epoch()
    else:
        forecastWindow = fm_schedule.get(user_node).get("status_non_recurring")
        forecastTimestamp = fm_schedule.get(user_node).get("non_recurring_timestamp")
    user_key = userid + "_" + user_node
    if user_level_schedule and user_key in user_level_schedule:
        if forecastTimestamp and forecastTimestamp <= user_level_schedule[user_key]['forecastTimestamp']:
            forecastWindow = user_level_schedule[user_key]['forecastWindow']
            forecastTimestamp = user_level_schedule[user_key]['forecastTimestamp']
    return {"forecast_status": forecastWindow,
            "forecastTimestamp": forecastTimestamp}


def get_node_descendants(node, as_of):
    node_descendants = {
        node_id for nth_level in list(fetch_descendants(as_of, [node], levels=20, drilldown=True))[0]['descendants']
        for node_id in nth_level.keys()
    }
    node_descendants.add(node)
    return list(node_descendants)

def _append_transformed_export_data(req_period, as_of_date, node_label, segment_label, field_label,
                                    value, fieldnames, transformed_export_data, node_level = None):

    row_data = {
        'as_of_date': as_of_date,
        'Name': node_label,
        'Segment': segment_label,
        'Period': req_period,
        'Field': field_label,
        'Value': value
    }

    if 'Level' in fieldnames and node_level:
        row_data.update({'Level': 'Level {}'.format(node_level)})

    transformed_export_data.append(row_data)


def transform_weekly_snapshot_data_into_export_format(q_period, custom_month_order, nodes_labels, nodes_level,
                                                    as_of_date, config, fieldnames, weekly_segment_snapshot):
    """
    Transform and store weekly snapshot data into export format.
    """
    transformed_export_data = []
    for key, dic in weekly_segment_snapshot['data'].items():
        node_key, segment_key = key.rsplit('_', 1)[0], dic['segment']
        node_label = nodes_labels.get(node_key)
        node_level = nodes_level[node_key]
        segment_label = config.segments.get(segment_key, {}).get('label')

        for field, field_dict in dic['fields'].items():
            total_period_data = {}
            field_format = field_dict.get('format', 'amount')

            for week_period, val in field_dict['weeks_period_data_val'].items():
                # Format value based on field format
                value = round(float(val * 100 if field_format == 'percentage' else val), 2)
                req_period = create_key_from_weekly_mnemonic(q_period, week_period, custom_month_order)
                _append_transformed_export_data(
                    req_period, as_of_date, node_label, segment_label,
                    field_dict['label'], value, fieldnames, transformed_export_data, node_level
                )

                # Get month name from week period and accumulate values
                month_no = int(week_period[4:6])
                month_key = custom_month_order[month_no - 1]
                total_period_data[month_key] = total_period_data.get(month_key, 0) + val
                total_period_data['quarter-total'] = total_period_data.get('quarter-total', 0) + val

            for cur_period, cur_val in total_period_data.items():
                value = round(float(cur_val * 100 if field_format == 'percentage' else cur_val), 2)
                req_period = create_key_from_weekly_mnemonic(q_period, cur_period, custom_month_order)
                _append_transformed_export_data(
                    req_period, as_of_date, node_label, segment_label,
                    field_dict['label'], value, fieldnames, transformed_export_data, node_level
                )

    return transformed_export_data
