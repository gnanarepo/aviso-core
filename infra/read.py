import copy
import logging
import re
import threading
from collections import OrderedDict, namedtuple
from datetime import datetime
from functools import wraps
from itertools import product

import numpy as np
import pandas as pd
from utils.metriclogger import NOOPMetricSet
from aviso.settings import sec_context
from bson.objectid import ObjectId
from dateutil.rrule import WEEKLY, rrule

from config.deal_config import DealConfig
from infra import (DEALS_COLL, FAVS_COLL, GBM_CRR_COLL, NEW_DEALS_COLL,
                   QUARTER_COLL)
from infra.fetch_helper import modify_hint_field
from infra.filters import contextualize_filter, parse_filters
from infra.mongo_utils import \
    _determine_period_field_name as get_key_for_close_periods
from tasks.hierarchy.hierarchy_utils import get_user_permissions
from utils.date_utils import (current_period, epoch, get_bom, get_boq, get_bow,
                              get_eod, get_nested_with_placeholder,
                              get_nextq_mnem_safe, get_prevq_mnem_safe,
                              get_week_ago, get_yest, monthly_periods,
                              next_period, period_details,
                              period_details_range, period_rng_from_mnem,
                              prev_period, prev_periods_allowed_in_deals,
                              weekly_periods)
from utils.misc_utils import (contextualize_field, flatten_to_list, get_nested,
                              is_lead_service, iter_chunks, merge_dicts,
                              try_float, use_df_for_dlf_rollup,
                              use_dlf_fcst_coll_for_rollups)
from utils.relativedelta import relativedelta

from . import DRILLDOWN_COLL, DRILLDOWN_LEADS_COLL, HIER_COLL, HIER_LEADS_COLL

logger = logging.getLogger('gnana.%s' % __name__)


time_context = namedtuple("time_context", ["fm_period", "component_periods",
                                           "deal_period", "close_periods",
                                           "deal_timestamp", "relative_periods",
                                           "now_timestamp", "submission_component_periods", "period_info",
                                           "component_periods_boundries", "cumulative_close_periods",
                                           "prev_periods","all_quarters", "period_and_close_periods_for_year"])

ONE_HOUR = 60 * 60 * 1000
SIX_HOURS = 6 * ONE_HOUR
ONE_DAY = 24 * ONE_HOUR
# it's been
ONE_WEEK = 7 * ONE_DAY


def _get_timestamp(period):
    try:
        return sec_context.details.get_flag(DEALS_COLL, '{}_update_date'.format(period))
    except:
        return 0

def fetch_distinct_periods(db=None):
    deals_collection = db[DEALS_COLL] if db else sec_context.tenant_db[DEALS_COLL]
    distinct_periods = deals_collection.distinct("period")
    if distinct_periods: return distinct_periods
    deals_collection = db[NEW_DEALS_COLL] if db else sec_context.tenant_db[NEW_DEALS_COLL]
    distinct_periods.extend(deals_collection.distinct("period"))
    return distinct_periods


def render_period(period_tuple,
                  now_dt,
                  config,
                  ):
    """
    render period and some metadata

    Arguments:
        period_tuple {period} -- named period tuple from dateUtils               period(mnem='2020Q2', begin=, end=)
        now_dt {datetime} -- time of now                                         datetime(2019, 6, 7, 10, 43, 51, 48004)
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        dict -- {period dtls}
    """
    rel_prd = relative_period(now_dt, period_tuple.begin, period_tuple.end)

    return {'begin': epoch(period_tuple.begin).as_epoch(),
            'end': epoch(period_tuple.end).as_epoch(),
            'relative_period': rel_prd,
            'has_forecast': period_has_forecast(period_tuple.mnemonic, rel_prd == 'f', config)}


def period_has_forecast(period,
                        future_period=None,
                        config=None):
    # totes cheating to get across services
    # TODO need to understand and implement accordingly for weekly forecast
    if 'W' in period:
        return not future_period
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()
    if not future_period:
            prd_begin, prd_end = [epoch(x) for x in period_rng_from_mnem(period)]
            now = get_now(config)
            future_period = relative_period(now, prd_begin, prd_end) == 'f'
    return 'Q' in period or (config.monthly_predictions and not future_period and len(period) != 4)


def relative_period(now,
                    begin,
                    end
                    ):
    """
    figure out if period is h(istoric), c(urrent), or f(uture)

    Arguments:
        now {datetime} -- time of now                                            datetime(2019, 6, 7, 10, 43, 51, 48004)
        begin {datetime} -- time of beginning of period                          datetime(2019, 5, 1, 00, 00, 00, 00000)
        end {datetime} -- time of end of period                                  datetime(2019, 8, 1, 00, 00, 00, 00000)

    Returns:
        str -- period descriptor
    """
    if begin <= now <= end:
        return 'c'
    else:
        return 'f' if now < begin else 'h'


def future_period(period,
                  config=None):
    """
    check if period is in the future

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        bool -- period is future
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    prd_begin, prd_end = [epoch(x) for x in period_rng_from_mnem(period)]
    now = get_now(config)
    return relative_period(now, prd_begin, prd_end) == 'f'


def component_periods(period,
                      config=None, period_type='Q'
                      ):
    """
    get periods that roll up to period

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        list -- list of component mnemonic
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()
    if period[-2] != 'Q' or not config.monthly_fm:
        return [period]
    return [mnemonic for (mnemonic, _beg_dt, _end_dt) in monthly_periods(period, period_type)]


def active_periods(config, quarter_editable=False, component_periods_editable=False,
                   past_quarter_editable=0, future_quarter_editable=1, week_period_editability='W'):
    """
    get active forecasting periods (current, and future)

    Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        list -- list of current and future periods
    """
    now_dt = get_now(config).as_datetime()
    custom_quarter_editable = config.is_custom_quarter_editable
    curr_mnem, next_mnem = current_period(now_dt).mnemonic, next_period(now_dt).mnemonic
    active_qtrs = []
    if past_quarter_editable or future_quarter_editable > 1:
        avail_prds = get_periods_editable(config,
                                          future_qtr_editable=future_quarter_editable,
                                          past_qtr_editable=past_quarter_editable)
        active_qtrs = [prd for prd in avail_prds]
    if not config.monthly_fm:
        if active_qtrs:
            return active_qtrs
        return [curr_mnem, next_mnem]

    weeks = []
    if config.weekly_fm:
        if week_period_editability == 'Q':
            weekly_begin_boundary = current_period(now_dt).begin
        elif week_period_editability == 'M':
            weekly_begin_boundary = get_current_month(config).begin
        else:
            weekly_begin_boundary = get_current_week(config).begin
        weeks = [week_prd.mnemonic for prd in (active_qtrs if active_qtrs else [curr_mnem, next_mnem])
                            for week_prd in weekly_periods(prd) if week_prd.begin >= weekly_begin_boundary]

    if component_periods_editable:
        if active_qtrs:
            months = [mnth_prd.mnemonic for prd in active_qtrs for mnth_prd in monthly_periods(prd)]
            return months + (active_qtrs if (quarter_editable or custom_quarter_editable) else []) + weeks
        return [mnem for period in [curr_mnem, next_mnem] for (mnem, _, end_dt)
                in monthly_periods(period)] + ([curr_mnem, next_mnem] if (quarter_editable or custom_quarter_editable) else []) + weeks

    return [mnem for period in [curr_mnem, next_mnem] for (mnem, _, end_dt)
            in monthly_periods(period) if end_dt >= now_dt] + ([curr_mnem, next_mnem] if (quarter_editable or custom_quarter_editable) else []) + weeks


def period_is_active(period,
                     config=None,
                     quarter_editable=False,
                     component_periods_editable=False,
                     past_quarter_editable=0,
                     future_quarter_editable=1,
                     week_period_editability='W'
                     ):
    """
    determine if period is active

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Optional:
        quarter_editable -- If fm_monthly tenant has quarter editability option  True/False  Deafults to False
        Value and impacts for forecast monthly tenants(True : Considers Current and Next Quarters and thier months as active periods,
                           False : Considers only Months of current and next quarters but not quarters themselves)

    Returns:
        bool -- True if active, False if not
    """
    # totes cheating to get across services
    quarter_editable = quarter_editable and 'Q' in period
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()
    return period in active_periods(config, quarter_editable=quarter_editable,
                                    component_periods_editable=component_periods_editable,
                                    past_quarter_editable=past_quarter_editable,
                                    future_quarter_editable=future_quarter_editable,
                                    week_period_editability=week_period_editability)


def get_now(config=None):
    """
    get time of now in app

    Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        epoch -- epoch of apps current time
    """

    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    if config.use_sys_time:
        return epoch()
    else:
        try:
            context_details = sec_context.details
            return epoch(context_details.get_flag('prd_svc', 'now_date'))
        except:
            return epoch()


def validate_period(period,
                    config=None):
    """
    check mnemonic is valid

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        bool -- True if valid
    """
    # Returns true if the period is a valid period name.
    # Should start with 4 numbers, should end with a number.
    # totes cheating to get across services
    #from config.periods_config import PeriodsConfig
    #config = config if config is not None else PeriodsConfig()

    try:
        int(period[:4])
        if len(period) == 9:
            return 'w' in period.lower()
        return len(period) == 6 or len(period) == 4
    except ValueError:
        return False


def special_pivot_latest_refresh_date(config):
    try:
        col = sec_context.tenant_db[GBM_CRR_COLL]
        last_executed_timestamp = list(col.find({'updated_by': 'gbm_crr_results_task'}))
        if last_executed_timestamp:
            return last_executed_timestamp[0].get('last_execution_time')
        return get_now(config).as_epoch()
    except Exception as _err:
        logger.warning("Exception in special_pivot_latest_refresh_date {}".format(_err))
        return get_now(config).as_epoch()

def latest_refresh_timestamp_leads(config):
    if config.use_sys_time:
        try:
            #This is the time when the chipotle was completed
            return int(sec_context.details.get_flag('leads', 'last_execution_time'))
        except (ValueError, TypeError):
            pass

    return get_now(config).as_epoch()

def latest_refresh_date(config):
    """
    get epoch timestamp of last model run

    Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        int -- epoch timestamp
    """

    if config.use_sys_time:
        try:
            #This is the time when the chipotle was completed
            return int(sec_context.details.get_flag('molecule_status',
                                                    'rtfm', {}).get('last_execution_time', sec_context.details.get_flag('molecule_status',
                                                    'rtfm', {}).get('ui_display_time')))
        except (ValueError, TypeError):
            pass

    return get_now(config).as_epoch()


def latest_eoq_refresh_date(config, period, now_dt, relative_period):
    """
    get epoch timestamp of last model run

    Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        int -- epoch timestamp
    """

    if config.use_sys_time:
        if relative_period == 'h':
            last_minute_of_quarter = render_period(current_period(period.begin), now_dt, config).get('end')
            try:
                # This is the time when the eoq was completed
                return int(sec_context.details.get_flag('molecule_status',
                                                        'rtfm', {}).get(period.mnemonic + '_eoq_time',
                                                                        last_minute_of_quarter))
            except:
                pass
        else:
            prev_prd = prev_period(period.begin)
            last_minute_of_quarter = render_period(prev_prd, now_dt, config).get('end')
            try:
            #This is the time when the chipotle was completed
                return int(sec_context.details.get_flag('molecule_status',
                                         'rtfm', {}).get('ui_display_time', last_minute_of_quarter))
            except:
                pass

    return get_now(config).as_epoch()


def get_available_periods(config):
    """
    available periods in app

    Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        set -- {period mnemonics}
    """
    now_dt = get_now(config).as_datetime()
    required_periods = {prev_period(now_dt)[0],
                        current_period(now_dt)[0]}
    required_periods |= {next_period(now_dt, skip=i+1)[0] for i in range(config.future_periods_count)}
    result_periods = set(fetch_distinct_periods())
    return required_periods | result_periods

def get_close_periods_cumulative(close_periods):
    if isinstance(close_periods, list):
        if len(close_periods) == 1:
            close_period = close_periods[0]
            if len(close_period) == 6:
                return make_cumulative_close_periods(close_period)

    return close_periods

def make_cumulative_close_periods(close_period):
    _, close_period_end = period_rng_from_mnem(close_period)
    quarter = get_quarter_period(close_period)
    close_periods = []
    if quarter:
        quarter_period_begin, _ = period_rng_from_mnem(quarter)
        for month_period in monthly_periods(quarter):
            if month_period.begin >= epoch(quarter_period_begin).as_datetime() and month_period.end <= epoch(close_period_end).as_datetime():
                close_periods.append(month_period.mnemonic)

    return close_periods


def get_periods_editable(config=None, future_qtr_editable=1, past_qtr_editable=0):
    """
    get_periods_editable in app

    Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        set -- {period mnemonics}
    """
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    now_dt = get_now(config).as_datetime()
    required_periods = {current_period(now_dt)[0]}
    if past_qtr_editable:
        required_periods |= {prev_period(now_dt, skip=i+1)[0] for i in range(past_qtr_editable)}
    required_periods |= {next_period(now_dt, skip=i+1)[0] for i in range(future_qtr_editable)}
    return required_periods

def get_quarter_period(period):
    from config.periods_config import PeriodsConfig
    period_config = PeriodsConfig()
    available_periods = get_available_quarters_and_months(period_config)
    if period and len(period) == 9 and 'w' in period.lower():
        # defaulting to month, as quarter is same for month and week
        period = period[:6]
    if period:
        if 'Q' in list(period):
            return period
        elif len(period) == 4:
            quarters = [period + 'Q' + str(x) for x in range(1, 5)]
            return quarters
        else:
            for quarter, months in available_periods.items():
                if period in months:
                    return quarter

def get_period_and_close_periods(period,
                                 out_of_period=False,
                                 config=None,
                                 deal_svc=False
                                 ):
    """
    get period, and closing periods from a period

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        out_of_period {bool} -- dont limit close periods (default: False)
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        tuple -- (period, [close periods])
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    prd_begin, prd_end = [epoch(x) for x in period_rng_from_mnem(period)]
    now = get_now(config)
    future_period = relative_period(now, prd_begin, prd_end) == 'f'

    if future_period:
        fetch_period = period_details(now.as_datetime()).mnemonic
    elif period[-2] != 'Q':
        fetch_period = period_details(prd_begin.as_datetime()).mnemonic
    else:
        fetch_period = period

    # If BOQ is in progress, current period should be adjusted so that empty deals are not displayed
    # check if the current period has update_date flag
    deal_load_time = sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(fetch_period), 0)
    if not deal_load_time and fetch_period == current_period().mnemonic:
        fetch_period = prev_period().mnemonic

    if out_of_period:
        return fetch_period, []

    if not config.monthly_fm:
        if deal_svc and config.yearly_fm and len(period) == 4:
            pass
        else:
            return fetch_period, [period]

    if period[-2] == 'Q':
        return fetch_period, [mnemonic for (mnemonic, _, _) in monthly_periods(period)]

    if deal_svc and config.yearly_fm and len(period) == 4:
        qtrs = []
        for mnth_prd in monthly_periods(period, period_type='Y'):
            q = current_period(mnth_prd.begin).mnemonic
            if q not in qtrs:
                qtrs.append(q)
        qtrs_details = []
        for qtr in qtrs:
            qtrs_details.append(get_period_and_close_periods(qtr,
                                                             out_of_period=out_of_period,
                                                             config=config,
                                                             deal_svc=deal_svc
                                                             ))
        periods_and_close_periods = {}
        for qtr, close_periods in qtrs_details:
            if qtr not in periods_and_close_periods:
                periods_and_close_periods[qtr] = []
            periods_and_close_periods[qtr] += close_periods

        return [(k, v) for k, v in periods_and_close_periods.items()]

    return fetch_period, [period]


def get_crr_period_and_monthly_period(period, is_dlf=False):
    from config.periods_config import PeriodsConfig
    period_config = PeriodsConfig()
    available_periods = get_available_quarters_and_months(period_config)
    period_and_monthly_period = []
    if period:
        if 'Q' in list(period):
            quarter_months = available_periods.get(period) or []
            if is_dlf and quarter_months:
                return period, [max(quarter_months)]
            return period, quarter_months
        elif len(period) == 4:
            quarters = [period + 'Q' + str(x) for x in range(1, 5)]
            for quarter in quarters:
                quarter_months = available_periods.get(quarter) or []
                period_and_monthly_period.append((quarter, quarter_months))
        else:
            for quarter, months in available_periods.items():
                if period in months:
                    return quarter, [period]
    return period_and_monthly_period

def get_period_and_close_periods_yearly(period,
                                 out_of_period=False,
                                 config=None,
                                 deal_svc=False
                                 ):
    """
    get period, and closing periods from a year

    Arguments:
        period {str} -- period mnemonic                                          '2020'

    Keyword Arguments:
        out_of_period {bool} -- dont limit close periods (default: False)
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        tuple -- (period, [close periods])
    """
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    prd_begin, prd_end = [epoch(x) for x in period_rng_from_mnem(period)]
    now = get_now(config)
    future_period = relative_period(now, prd_begin, prd_end) == 'f'

    if future_period:
        fetch_period = period_details(now.as_datetime()).mnemonic
    elif period[-2] != 'Q':
        fetch_period = period_details(prd_begin.as_datetime()).mnemonic
    else:
        fetch_period = period

    deal_load_time = sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(fetch_period), 0)
    if not deal_load_time and fetch_period == current_period().mnemonic:
        fetch_period = prev_period().mnemonic

    if config.yearly_fm and len(period) == 4:
        qtrs = []
        for mnth_prd in monthly_periods(period, period_type='Y'):
            q = current_period(mnth_prd.begin).mnemonic
            if q not in qtrs:
                qtrs.append(q)
        qtrs_details = []
        for qtr in qtrs:
            qtrs_details.append(get_period_and_close_periods(qtr,
                                                             out_of_period=out_of_period,
                                                             config=config,
                                                             deal_svc=deal_svc
                                                             ))
        periods_and_close_periods = {}
        for qtr, close_periods in qtrs_details:
            if qtr not in periods_and_close_periods:
                periods_and_close_periods[qtr] = []
            periods_and_close_periods[qtr] += close_periods

        return [(k, v) for k, v in periods_and_close_periods.items()]

    return fetch_period, [period]


def get_period_and_component_periods(period,
                                     config=None):
    """
    get period and its component periods

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        tuple -- (period, [close periods])
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    if len(period) == 4:
        if config.monthly_fm:
            return period, [mnemonic for (mnemonic, _, _) in monthly_periods(period, 'Y')]
        else:
            return period, list(
                set([current_period(begin).mnemonic for (mnemonic, begin, _) in monthly_periods(period, 'Y')]))

    if not config.monthly_fm or period[-2] != 'Q':
        return period, [period]

    return period, [mnemonic for (mnemonic, _, _) in monthly_periods(period)]


def get_period_range(period,
                     range_type='weeks',
                     end_timestamp=None,
                     adjustment=True):
    """
    get date ranges in epoch timestamps for a period

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        range_type {str} -- range type to fetch (default: 'weeks')               'weeks'
                        supports days and weeks
        end_timestamp {int} -- get range up to this timestamp

    Returns:
        list -- epoch timestamps for range
    """
    prd_begin, prd_end = period_rng_from_mnem(period)
    if adjustment:
        prd_begin += SIX_HOURS  # HACK to deal with results timestamps being 3AM
    step_size = ONE_WEEK if range_type == 'weeks' else ONE_DAY

    period_range = [ts for ts in np.arange(prd_begin,
                                           prd_end,
                                           step_size)
                    if not end_timestamp or ts <= end_timestamp]
    if not end_timestamp and range_type == 'days':
        period_range[-1] = prd_end  # HACK to get end of period timestamp at midnight
    return period_range

def get_period_range_with_day_of_start(period,
                     range_type='weeks',
                     end_timestamp=None,
                     adjustment=True,
                     day_of_start=None):
    """
    get date ranges in epoch timestamps for a period with user-defined day of start

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        range_type {str} -- range type to fetch (default: 'weeks')               'weeks'
                        supports days and weeks
        end_timestamp {int} -- get range up to this timestamp
        day_of_start {int} -- mention from which day you want to start the week     '0'
                            0-Monday, 1-Tuesday etc.

    Returns:
        list -- epoch timestamps for range
    """
    prd_begin, prd_end = period_rng_from_mnem(period)
    if adjustment:
        prd_begin += SIX_HOURS  # HACK to deal with results timestamps being 3AM
    step_size = ONE_WEEK if range_type == 'weeks' else ONE_DAY
    period_range = []
    if day_of_start != datetime.fromtimestamp(epoch(prd_begin).as_epoch() / 1000).weekday():
        period_range.append(prd_begin)
        prd_begin = epoch(rrule(freq=WEEKLY, dtstart=epoch(prd_begin).as_datetime(), byweekday=day_of_start, count=1)[0]).as_epoch()

    for ts in np.arange(prd_begin, prd_end, step_size):
        if not end_timestamp or ts <= end_timestamp:
            period_range.append(ts)

    if not end_timestamp and range_type == 'days':
        period_range[-1] = prd_end  # HACK to get end of period timestamp at midnight
    return period_range

def get_period_eod_timestamps(period):

    prd_begin, prd_end = period_rng_from_mnem(period)
    prd_begin = get_eod(epoch(prd_begin))
    prd_end = get_eod(epoch(prd_end))
    e = prd_begin
    timestamps = []
    while e <= prd_end:
        timestamps.append(e.as_epoch())
        e += relativedelta(days=1)
    return timestamps

def get_timestamps_by_start_end_prd_ts(start_prd_ts,end_prd_ts):

    prd_begin = get_eod(epoch(start_prd_ts))
    prd_end = get_eod(epoch(end_prd_ts))
    e = prd_begin
    timestamps= []
    while e <= prd_end:
        timestamps.append(e.as_epoch())
        e += relativedelta(days=1)
    return timestamps

def get_period_as_of(period,
                     as_of=None,
                     config=None,
                     prd_begin_for_h=False):
    """
    get as_of for period
    if as_of provided, confirm its in period, and return
    otherwise return current time for period

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        as_of {int} epoch timestamp
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        epoch -- epoch of apps current time for period, or valid as_of
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    if as_of:
        prd_begin, prd_end = period_rng_from_mnem(period)
        if epoch(prd_begin).as_epoch() <= as_of <= epoch(prd_end).as_epoch():
            return as_of

    # for historic periods, return end of period
    prd_begin, prd_end = [epoch(x) for x in period_rng_from_mnem(period)]
    now = get_now(config)
    if relative_period(now, prd_begin, prd_end) == 'h':
        return prd_end.as_epoch() if not prd_begin_for_h else prd_begin.as_epoch()

    try:
        return epoch(sec_context.details.get_flag('prd_svc', '{}_now_date'.format(period))).as_epoch()
    except:
        return now.as_epoch()


def get_period_infos(period,
                     as_of=None,
                     config=None,
                     ):
    """
    get period information for given period and next two periods

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        as_of {int} epoch timestamp
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        list -- [{prd_dtls}]
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    # TODO: this may be wrong...
    now = epoch(get_period_as_of(period, as_of))
    now_dt, now_xl = now.as_datetime(), now.as_xldate()

    comp_periods = component_periods(period)
    future_comp_periods = [component_periods(next_period(now_dt).mnemonic),
                           component_periods(next_period(now_dt, skip=2).mnemonic)]

    period_infos = []
    for i, comp_period in enumerate(comp_periods):
        # ok to lose some precision here, this is only being used to generate test data
        begin, end = [int(epoch(x).as_xldate()) for x in period_rng_from_mnem(comp_period)]
        period_info = {'name': comp_period,
                       'begin': begin,
                       'end': end,
                       'relative_period': relative_period(now_xl, begin, end)}
        for y, future_comps in enumerate(future_comp_periods):
            period_str = '_'.join(['plus', str(y + 1)])
            future_period = future_comps[i]
            begin, end = [int(epoch(x).as_xldate()) for x in period_rng_from_mnem(future_period)]
            period_info.update({'_'.join([period_str, 'name']): future_period,
                                '_'.join([period_str, 'begin']): begin,
                                '_'.join([period_str, 'end']): end})
        period_infos.append(period_info)

    return period_infos


def get_as_of_dates(period,
                    as_of=None,
                    include_yest=False,
                    config=None,
                    include_bmon=False,
                    include_boq=False
                    ):
    """
    get as of dates for period in multiple date formats

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        as_of {int} -- epoch timestamp
        include_yest {bool} -- include yest as_of if True (default: False)
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        dict -- {as_of_str: {'ep': epoch, 'xl': xldate, 'str': string description}}
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    # TODO: this may be wrong...
    now = epoch(get_period_as_of(period, as_of, config=config))
    bow = get_bow(now)
    bom = get_bom(now)
    week_ago = get_week_ago(now)

    as_ofs = {'now': {'ep': now.as_epoch() + SIX_HOURS,
                      'xl': int(now.as_xldate()),
                      'str': 'Now'},
              'bow': {'ep': week_ago.as_epoch() + SIX_HOURS,
                      'xl': int(week_ago.as_xldate()),
                      'str': 'Since Last Week',
                      'eod': get_eod(week_ago).as_epoch()},
              'bom': {'ep': bom.as_epoch() + SIX_HOURS,
                      'xl': int(bom.as_xldate()),
                      'str': 'Since Beginning of Month',
                      'eod': get_eod(bom).as_epoch()}}
    if include_bmon:
        bmon = get_eod(get_bow(now))
        as_ofs['bmon'] = {'ep': bmon.as_epoch(),
                          'xl': int(bmon.as_xldate()),
                          'str': 'Since Monday'}
    if include_yest:
        yest = get_yest(now)
        as_ofs['yest'] = {'ep': yest.as_epoch() + SIX_HOURS,
                          'xl': int(yest.as_xldate()),
                          'str': 'Since Yesterday',
                          'eod': get_eod(yest).as_epoch()}

    # monthly   |   is period quarter   |   should add quarter
    # True      |   True                |   True
    # True      |   False               |   False
    # False     |   False               |   False
    # False     |   False               |   True
    if config.monthly_predictions:
        if 'Q' not in period and not include_boq:
            return as_ofs
    elif 'Q' not in period and not include_boq:
        return as_ofs

    boq = get_boq(now)
    as_ofs['boq'] = {'ep': boq.as_epoch() + SIX_HOURS,
                     'xl': int(boq.as_xldate()),
                     'str': 'Since Beginning of Quarter',
                     'eod': get_eod(boq).as_epoch()}

    return as_ofs


def get_as_of_dates_for_load_changes(period,
                                     as_of=None,
                                     include_yest=False,
                                     config=None,
                                     include_bmon=False):
    """
    get as of dates for period in multiple date formats

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        as_of {int} -- epoch timestamp
        include_yest {bool} -- include yest as_of if True (default: False)
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        dict -- {as_of_str: {'ep': epoch, 'xl': xldate, 'str': string description}}
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    # TODO: this may be wrong...
    now = epoch(get_period_as_of(period, as_of, config=config))
    bow = get_bow(now)
    bom = get_bom(now)
    week_ago = get_week_ago(now)
    boq = get_boq(now)
    if boq > week_ago:
        week_ago = boq

    as_ofs = {'now': {'ep': now.as_epoch() + SIX_HOURS,
                      'xl': int(now.as_xldate()),
                      'str': 'Now',
                      'date': now.as_datetime().strftime("%Y-%m-%d")
                      },
              'bow': {'ep': week_ago.as_epoch() + SIX_HOURS,
                      'xl': int(week_ago.as_xldate()),
                      'str': 'Since Last Week',
                      'date': week_ago.as_datetime().strftime("%Y-%m-%d")},
              'bom': {'ep': bom.as_epoch() + SIX_HOURS,
                      'xl': int(bom.as_xldate()),
                      'str': 'Since Beginning of Month',
                      'date': bom.as_datetime().strftime("%Y-%m-%d")}}
    if include_bmon:
        bmon = get_eod(get_bow(now))
        as_ofs['bmon'] = {'ep': bmon.as_epoch(),
                          'xl': int(bmon.as_xldate()),
                          'str': 'Since Monday',
                          'date': bmon.as_datetime().strftime("%Y-%m-%d")}
    if include_yest:
        yest = get_yest(now)
        if boq > yest:
            yest = boq
        as_ofs['yest'] = {'ep': yest.as_epoch() + SIX_HOURS,
                          'xl': int(yest.as_xldate()),
                          'str': 'Since Yesterday',
                          'date': yest.as_datetime().strftime("%Y-%m-%d")}

    # monthly   |   is period quarter   |   should add quarter
    # True      |   True                |   True
    # True      |   False               |   False
    # False     |   False               |   False
    # False     |   False               |   True
    if config.monthly_predictions:
        if 'Q' not in period:
            return as_ofs
    elif 'Q' not in period:
        return as_ofs

    as_ofs['boq'] = {'ep': boq.as_epoch() + SIX_HOURS,
                     'xl': int(boq.as_xldate()),
                     'str': 'Since Beginning of Quarter',
                     'date': boq.as_datetime().strftime("%Y-%m-%d")}

    return as_ofs



def get_period_boundaries(period,
                          config=None):
    """
    get period boundaries for periods surrounding given period

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        list -- [(prd beg ts, prd end ts, prd mnem)] sorted by prd beg ts
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    backwards = 24
    forwards = 24

    prev_periods = [[component_periods(get_prevq_mnem_safe(period, skip=x+1))] for x in range(backwards)]
    curr_periods = [component_periods(period)]
    next_periods = [[component_periods(get_nextq_mnem_safe(period, skip=x+1))] for x in range(forwards)]

    period_boundaries = []
    for period in flatten_to_list(prev_periods + curr_periods + next_periods):
        beg, end = period_rng_from_mnem(period)
        period_boundaries.append((epoch(beg).as_xldate(), epoch(end).as_xldate(), period))

    return sorted(period_boundaries, key=lambda x: x[0])

def get_period_boundaries_weekly(period):
    """
    get period boundaries for periods surrounding given period

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Returns:
        list -- [(prd beg ts, prd end ts, prd mnem)] sorted by prd beg ts
    """

    backwards = 24
    forwards = 24

    prev_periods = [[get_prevq_mnem_safe(period, skip=x+1)] for x in range(backwards)]
    curr_periods = [period]
    next_periods = [[get_nextq_mnem_safe(period, skip=x+1)] for x in range(forwards)]

    period_boundaries = []
    for period in flatten_to_list(prev_periods + curr_periods + next_periods):
        beg, end = period_rng_from_mnem(period)
        weekly_periods = period_details_range(epoch(beg).as_datetime(), epoch(end).as_datetime())
        for weekly_period in weekly_periods:
            period_boundaries.append((epoch(weekly_period.begin).as_xldate(), epoch(weekly_period.end).as_xldate(),
                                      weekly_period.mnemonic))

    return sorted(period_boundaries, key=lambda x: x[0])


def get_period_boundaries_monthly(period):
    """
    get period boundaries for periods surrounding given period

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Returns:
        list -- [(prd beg ts, prd end ts, prd mnem)] sorted by prd beg ts
    """

    backwards = 24
    forwards = 24

    prev_periods = [[get_prevq_mnem_safe(period, skip=x+1)] for x in range(backwards)]
    curr_periods = [period]
    next_periods = [[get_nextq_mnem_safe(period, skip=x+1)] for x in range(forwards)]

    period_boundaries = []
    for period in flatten_to_list(prev_periods + curr_periods + next_periods):
        beg, end = period_rng_from_mnem(period)
        monthly_periods = period_details_range(epoch(beg).as_datetime(), epoch(end).as_datetime(), period_type='M')
        for monthly_period in monthly_periods:
            period_boundaries.append((epoch(monthly_period.begin).as_xldate(), epoch(monthly_period.end).as_xldate(),
                                      monthly_period.mnemonic))

    return sorted(period_boundaries, key=lambda x: x[0])


def get_time_context(period,
                     config=None,
                     quarter_editable=False
                     ):
    """
    get all the time context required for fm service

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        time_context -- namedtuple (fm_period, component_periods, deal_period,
                                    close_periods, deal_timestamp, relative_periods)
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()
    fm_period, comp_periods = get_period_and_component_periods(period, config)
    period_and_close_periods_for_year = []
    if len(period) != 4:
        deal_period, close_periods = get_period_and_close_periods(period, config=config)
    else:
        deal_period = []
        close_periods = []
        deal_period, close_periods = map(list, zip(*get_period_and_close_periods_yearly(period, config=config)))
        period_and_close_periods_for_year = get_period_and_close_periods(period, False, deal_svc=True)
    deal_timestamp = _get_timestamp(deal_period if not isinstance(deal_period, list) else period)
    submission_component_periods = [period] if ('Q' in period and quarter_editable) else comp_periods
    rel_prds = []
    now = get_now(config)
    component_periods_boundries = {}
    all_quarters = []
    for period_ in comp_periods:
        prd_begin, prd_end = [epoch(x) for x in period_rng_from_mnem(period_)]
        # for period conditional fields we need to know the period boundries.
        component_periods_boundries[period_] = {'begin': prd_begin.as_epoch(),
                                               'end': prd_end.as_epoch()}
        rel_prds.append(relative_period(now, prd_begin, prd_end))
        all_quarters.append(current_period(prd_begin.as_datetime()).mnemonic)
    period_info = period_details(period_type='Q', count=10)
    cumulative_close_periods = get_close_periods_cumulative(close_periods)
    if len(period) == 4 and quarter_editable:
        submission_component_periods = list(set(all_quarters))
    return time_context(fm_period, comp_periods, deal_period,
                        close_periods, deal_timestamp,
                        rel_prds, now.as_epoch(), submission_component_periods, period_info,
                        component_periods_boundries, cumulative_close_periods, prev_periods_allowed_in_deals(),all_quarters,
                        period_and_close_periods_for_year)


def get_snapshot_range(period,
                       config=None):
    """
    return available snapshot range for a period in ISO-8601 format
    snapshot range is beginning of prev period to end of this period

    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        tuple -- begin date, end date
    """
    # totes cheating to get across services
    # TODO: bad, this is fm logic in the periods service ...
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()
    if period[-2] != 'Q':
        prd_begin, prd_end = [epoch(x) for x in period_rng_from_mnem(period)]
        period = period_details(prd_begin.as_datetime(),period_type = "Y").mnemonic if len(period) == 4 else period_details(prd_begin.as_datetime(),period_type = "Y").mnemonic
    prev_period = get_prevq_mnem_safe(period)

    _, end = period_rng_from_mnem(period)
    begin, _ = period_rng_from_mnem(prev_period)
    return epoch(begin).as_datetime().date().isoformat(), epoch(end).as_datetime().date().isoformat()


def get_current_period(config=None):
    """
    get mnemonic of current app period

    Keyword Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        str -- period mnemonic
    """
    # totes cheating to get across services
    from config.periods_config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    # TODO: should there be months??
    return current_period(get_now(config).as_datetime()).mnemonic




def get_period_begin_end(period):
    """
    Arguments:
        period {str} -- period mnemonic                                          '2020Q2'

    Keyword Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        epoch -- epoch of apps begin and end time for period
    """

    prd_begin, prd_end = [epoch(x) for x in period_rng_from_mnem(period)]

    return prd_begin.as_epoch(), prd_end.as_epoch()

def get_available_quarters_and_months(config):
    available_periods = get_available_periods(config)
    quarters_and_months = {}
    for period in available_periods:
        quarters_and_months[period] = component_periods(period, config)
    return quarters_and_months


def adjust_current_period(periods_data, now_dt):
    """

    iterate over all periods and check if the current period has

    Arguments:
        periods {obj} -- periods obj
        Example - {
            "2020Q1": {
                "has_forecast": true,
                "begin": 1577865600000,
                "relative_period": "h",
                "end": 1585724399999
            },
            "2020Q3": {
                "has_forecast": true,
                "begin": 1593586800000,
                "relative_period": "c",
                "end": 1601535599999
            }
        }
    """

    try:
        current_prd = current_period(now_dt).mnemonic
        current_period_update_date = try_float(sec_context.details.get_flag('deal_svc', '{}_deal_expiration_timestamp'.format(current_prd), 0))
        if current_period_update_date == 0:
            prev_prd = prev_period(now_dt)

            periods_data[prev_prd.mnemonic]["relative_period"] = "c"
            periods_data[current_prd]["relative_period"] = "f"

            # considering previous period's end as now_dt for further processing
            new_now = epoch(periods_data[prev_prd.mnemonic]["end"]).as_datetime()
            return periods_data, new_now
    except Exception:
        logger.info("previous period does not exist, current period {}".format(now_dt))

    return periods_data, now_dt

def get_current_month(config):
    now_dt = get_now(config).as_datetime()
    curr_mnem = current_period(now_dt).mnemonic
    for month in monthly_periods(curr_mnem):
        if month.begin <= now_dt <= month.end:
            return month

    return now_dt

def get_current_week(config):
    now_dt = get_now(config).as_datetime()
    curr_mnem = current_period(now_dt).mnemonic
    for week in weekly_periods(curr_mnem):
        if week.begin <= now_dt <= week.end:
            return week

    return now_dt

def get_current_weeks(period):
    peirod_begin,period_end = get_period_begin_end(period)
    peirod_begin, period_end = epoch(peirod_begin).as_datetime(),epoch(period_end).as_datetime()
    curr_mnem = current_period(peirod_begin).mnemonic
    weeks = []
    for week in weekly_periods(curr_mnem):
        if peirod_begin <= week.begin <= period_end:
            weeks.append(week)
    return weeks

def is_period_in_past(period):
    """
    Determines if a given period is in the past.

    Args:
        period (str): The period to check.

    Returns:
        bool: True if the period is in the past, False otherwise.
    """

    from config.periods_config import PeriodsConfig
    period_in_past = False
    if PeriodsConfig().monthly_fm:
        if 'Q' not in period and len(period) != 4:
            time_context = get_time_context(period)
            if 'h' in time_context.relative_periods:
                period_in_past = True
    return period_in_past


def versioned_func_switch(fn):
    @wraps(fn)
    def switched_fn(*args, **kwargs):
        if sec_context.details.get_config(category='micro_app',
                                          config_name='hierarchy').get('versioned',False):
            return eval(fn.__name__ + '_versioned')(*args, **kwargs)
        return fn(*args, **kwargs)
    return switched_fn


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_node(as_of,
               node,
               include_hidden=False,
               drilldown=True,
               fields=[],
               db=None,
               period=None,
               boundary_dates=None
               ):
    """
    fetch node record for single node

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        dict -- node record
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'node': node,
                '$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    projection = {}
    if fields:
        for field in fields:
            projection[field] = 1
    else:
        projection['_id'] = 0

    return hier_collection.find(criteria, projection).next()


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_node_versioned(as_of,
                        node,
                        include_hidden=False,
                        drilldown=True,
                        fields=[],
                        db=None,
                        period=None,
                        boundary_dates=None
                        ):
    """
    fetch node record for single node

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        dict -- node record
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    if boundary_dates:
        from_date, to_date = boundary_dates
    else:
        if period:
            as_of = get_period_as_of(period)
            from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
            _, to_date = get_period_begin_end(period)
        else:
            from_date = to_date = as_of
    criteria = {'node': node,
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    projection = {}
    if fields:
        for field in fields:
            projection[field] = 1
    else:
        projection['_id'] = 0

    return hier_collection.find(criteria, projection).next()

# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_many_nodes(as_of,
                    nodes,
                    include_hidden=False,
                    drilldown=True,
                    db=None,
                    period=None
                    ):
    """
    fetch node records for many nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        nodes {list} -- list of nodes to find records for                        ['0050000FLN2C9I2', ]

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of dicts of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    criteria = {'$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}
    if nodes:
        criteria['node'] = {'$in': nodes}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return hier_collection.find(criteria, {'_id': 0})

# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_many_nodes_versioned(as_of,
                            nodes,
                            include_hidden=False,
                            drilldown=True,
                            db=None,
                            period=None
                            ):
    """
    fetch node records for many nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        nodes {list} -- list of nodes to find records for                        ['0050000FLN2C9I2', ]

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of dicts of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    if period:
        as_of = get_period_as_of(period)
        from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
        _, to_date = get_period_begin_end(period)
    else:
        from_date = to_date = as_of
    criteria = {'$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if nodes:
        criteria['node'] = {'$in': nodes}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return hier_collection.find(criteria, {'_id': 0})

# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_eligible_nodes_for_segment(as_of,
                                    segment,
                                    drilldown=True,
                                    db=None,
                                    period=None,
                                    boundary_dates=None):
    """
    fetches the nodes which are eligible for given segment

    returns nodes which are eligible
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    hier_collection.ensure_index([('normal_segs', -1), ('from', -1), ('to', -1)])

    criteria = {'normal_segs': {'$in': [segment]},
                '$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}

    return hier_collection.find(criteria, {'_id':0, 'node': 1})

# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_eligible_nodes_for_segment_versioned(as_of,
                                            segment,
                                            drilldown=True,
                                            db=None,
                                            period=None,
                                            boundary_dates = None):
    """
    fetches the nodes which are eligible for given segment

    returns nodes which are eligible
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    hier_collection.ensure_index([('normal_segs', -1), ('from', -1), ('to', -1)])

    if boundary_dates:
        from_date, to_date = boundary_dates
    else:
        if period:
            as_of = get_period_as_of(period)
            from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
            _, to_date = get_period_begin_end(period)
        else:
            from_date = to_date = as_of

    criteria = {'normal_segs': {'$in': [segment]},
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}

    return hier_collection.find(criteria, {'_id':0, 'node': 1})

def check_children(node, drilldown=True, db=None):
    coll = DRILLDOWN_COLL if drilldown else HIER_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    count = hier_collection.count({"parent": node})
    return count

# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_hidden_nodes(as_of,
                       drilldown=True,
                       signature='',
                       db=None,
                       period=None,
                       service=None):
    """
    fetch nodes that are hidden

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        signature {str} -- if provided, fetch only hidden nodes with how
                           matching signature, (default: {''})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        list -- list of dicts of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'$and': [
        {'$or': [{'from': None},
                 {'from': {'$lte': as_of}}]},
        {'$or': [{'to': None},
                 {'to': {'$gte': as_of}}]}
    ]}

    hide_criteria = {
            'hidden_from': 0,
            "$or": [{"hidden_to": {"$exists": False}}, {"hidden_to": None}]  # Node not unhidden yet
        }

    criteria = {'$and': [criteria, hide_criteria]}

    if signature:
        criteria['how'] = signature

    return hier_collection.find(criteria, {'_id': 0})

# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_hidden_nodes_versioned(as_of,
                                 drilldown=True,
                                 signature='',
                                 db=None,
                                 period=None,
                                 service=None):
    """
    fetch nodes that are hidden

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        signature {str} -- if provided, fetch only hidden nodes with how
                           matching signature, (default: {''})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        list -- list of dicts of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if period:
        as_of = get_period_as_of(period)
        from_date, to_date = fetch_boundry(as_of, drilldown=drilldown, service=service)
    else:
        from_date = to_date = as_of

    if 'Q' in period:
        prev_prd = prev_period(period_type='Q')[0]
        prev_as_of = get_period_as_of(prev_prd)
        prev_timestamp, prev_end_timestamp = fetch_boundry(prev_as_of, drilldown=drilldown, service=service)
    else:
        prev_prd = str(int(period[:4]) - 1) + str(int(period[4:]) + 11) if int(period[4:]) - 1 == 0 else str(int(period) - 1)
        prev_as_of = get_period_as_of(prev_prd)
        prev_timestamp, prev_end_timestamp = fetch_boundry(prev_as_of, drilldown=drilldown, service=service)

    how_criteria = {'$or': [{'how': {'$regex': '_hide'}},
                            {'how': {'$regex': '^hide', '$options': 'm'}}]}

    previous_period_hidden_criteria = {'from': {'$lt': from_date}}

    previous_period_hidden_criteria.update(how_criteria)

    #logger.info("previous_period_hidden_criteria: %s",previous_period_hidden_criteria)

    current_period_nodes_criteria = {'from': {'$gte': prev_timestamp}}

    if to_date is None:
        current_period_nodes_criteria['$or'] = [{"to": None},
                                                {"to": {'$lte': prev_end_timestamp}}]
    else:
        current_period_nodes_criteria['to'] = {'$lte':to_date}

    current_period_hidden_criteria= copy.deepcopy(current_period_nodes_criteria)
    current_period_hidden_criteria.update(how_criteria)

    #logger.info("current_period_hidden_criteria: %s", current_period_hidden_criteria)

    hidden_nodes = {}

    previous_period_hidden_nodes = hier_collection.find(previous_period_hidden_criteria, {'_id': 0})
    current_period_hidden_nodes = hier_collection.find(current_period_hidden_criteria, {'_id': 0})
    current_period_nodes = hier_collection.find(current_period_nodes_criteria, {'_id': 0})

    for node in previous_period_hidden_nodes:
        hidden_nodes[node['node']] = node

    for node in current_period_hidden_nodes:
        hidden_nodes[node['node']] = node

    for node in current_period_nodes:
        if "_hide" in node['how'] or node['how'].startswith('hide'):
            continue

        if node['node'] in hidden_nodes and node['from'] > hidden_nodes[node['node']]['from']:
            del hidden_nodes[node['node']]

    #logger.info("%s hidden nodes found",len(hidden_nodes))

    return hidden_nodes.values()

# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_root_nodes(as_of,
                     include_hidden=False,
                     drilldown=True,
                     true_roots=False,
                     db=None,
                     period=None,
                     service=None
                     ):
    """
    fetch node records for root nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        true_roots {bool} -- if True, fetch the true admin root nodes
                             otherwise fetch the visible root node in app
                             (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        list -- list of dicts of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'parent': None,
                '$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}

    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}
    roots = list(hier_collection.find(criteria, {'_id': 0}))
    if true_roots or not drilldown:
        return roots

    # TODO: ok 1, this is dumb and can probably do done in a single query
    # but also 2, is this safe and does it make sense?
    criteria = {'parent': {'$in': [x['node'] for x in roots]},
                '$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}

    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return [node for node in hier_collection.find(criteria, {'_id': 0}) if 'not_in_hier' not in node['node']]


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_root_nodes_versioned(as_of,
                               include_hidden=False,
                               drilldown=True,
                               true_roots=False,
                               db=None,
                               period=None,
                               service=None
                               ):
    """
    fetch node records for root nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        true_roots {bool} -- if True, fetch the true admin root nodes
                             otherwise fetch the visible root node in app
                             (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        list -- list of dicts of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if period:
        as_of = get_period_as_of(period)
        from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
        _, to_date = get_period_begin_end(period)
    else:
        from_date = to_date = as_of

    criteria = {'parent': None,
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}

    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}
    pipeline = [{'$match': criteria},
                {'$project': {'_id':0}}]

    roots = hier_collection.aggregate(pipeline)
    if true_roots or not drilldown:
        return roots

    # TODO: ok 1, this is dumb and can probably do done in a single query
    # but also 2, is this safe and does it make sense?
    criteria = {'parent': {'$in': [x['node'] for x in roots]},
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}

    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    pipeline = [{'$match': criteria},
                {'$project': {'_id':0, 'from':0, 'to':0}}]

    return [node for node in hier_collection.aggregate(pipeline) if 'not_in_hier' not in node['node']]


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_children(as_of,
                   nodes,
                   include_hidden=False,
                   drilldown=True,
                   db=None,
                   period=None,
                   boundary_dates=None,
                   service=None
                   ):
    """
    fetch node records for children of given nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        nodes {list} -- node to get children on                                 ['0050000FLN2C9I2', ]

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'parent': {'$in': nodes},
                '$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    # return sorted from highest priority to lowest priority, then break ties alphabetically by node id
    return hier_collection.find(criteria, {'_id': 0}).sort([('priority', -1), ('node', 1)])


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_children_versioned(as_of,
                             nodes,
                             include_hidden=False,
                             drilldown=True,
                             db=None,
                             period=None,
                             boundary_dates=None,
                             service=None
                             ):
    """
    fetch node records for children of given nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        nodes {list} -- node to get children on                                 ['0050000FLN2C9I2', ]

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if boundary_dates:
        from_date, to_date = boundary_dates
    else:
        if period:
            as_of = get_period_as_of(period)
            from_date, _ = fetch_boundry(as_of, drilldown=drilldown, service=service)
            _, to_date = get_period_begin_end(period)
        else:
            from_date = to_date = as_of

    criteria = {'parent': {'$in': nodes},
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    group = {'_id':
                {'node': '$node',
                'parent': '$parent',
                'label': '$label',
                'how': '$how'},
            'node':{'$last':'$node'},
            'parent':{'$last':'$parent'},
            'label':{'$last':'$label'},
            'how':{'$last':'$how'},
            'priority':{'$last':'$priority'}}

    project = {'_id': 0}

    sort = {'priority': -1, 'node': 1}

    pipeline = [{'$match':criteria}, {'$group': group}, {'$project': project}, {'$sort':sort}]

    # return sorted from highest priority to lowest priority, then break ties alphabetically by node id
    return hier_collection.aggregate(pipeline)


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_ancestors(as_of,
                    nodes=None,
                    include_hidden=False,
                    include_children=False,
                    drilldown=True,
                    limit_to_visible=False,
                    db=None,
                    period=None,
                    boundary_dates=None,
                    service=None,
                    unattached=False,
                    is_edw_write=False
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
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        limit_to_visible {bool} if True, only return ancestors up to visible root if not admin user
                                if admin user, return ancestors up to admin root
                                if False, return all ancestors regardless of visibility
                                (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of ({node: node, parent: parent, ancestors: [ancestors from root to bottom]})
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if limit_to_visible and is_edw_write == False:
        user_access = get_user_permissions(sec_context.get_effective_user(), 'results')
        exclude_admin_root = '!' not in user_access
    else:
        exclude_admin_root = False

    as_of = 0

    criteria = {'$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
                ]}

    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    if nodes:
        if not isinstance(nodes, list):
            nodes = list(nodes)
        node_criteria = {'node': {'$in': nodes}}
        if include_children:
            node_criteria = {'$or': [node_criteria, {'parent': {'$in': nodes}}]}
        match = merge_dicts(criteria, node_criteria)
    else:
        match = criteria

    anc_criteria = copy.deepcopy(criteria)
    if unattached:
        anc_criteria.update({
            'unattached': True
        })
    lookup = {'from': coll,
              'startWith': '$parent',
              'connectFromField': 'parent',
              'connectToField': 'node',
              'depthField': 'level',
              'restrictSearchWithMatch': anc_criteria,
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

    return _anc_yielder(hier_collection.aggregate(pipeline, allowDiskUse=True), exclude_admin_root)


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_ancestors_versioned(as_of,
                              nodes=None,
                              include_hidden=False,
                              include_children=False,
                              drilldown=True,
                              limit_to_visible=False,
                              db=None,
                              period=None,
                              boundary_dates=None,
                              service=None,
                              unattached=False,
                              is_edw_write=False
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
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        limit_to_visible {bool} if True, only return ancestors up to visible root if not admin user
                                if admin user, return ancestors up to admin root
                                if False, return all ancestors regardless of visibility
                                (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of ({node: node, parent: parent, ancestors: [ancestors from root to bottom]})
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if boundary_dates:
        from_date, to_date = boundary_dates
    else:
        if period:
            as_of = get_period_as_of(period)
            from_date, _ = fetch_boundry(as_of, drilldown=drilldown, service=service)
            _, to_date = get_period_begin_end(period)
        else:
            from_date = to_date = as_of

    if limit_to_visible and is_edw_write == False:
        user_access = get_user_permissions(sec_context.get_effective_user(), 'results')
        exclude_admin_root = '!' not in user_access
    else:
        exclude_admin_root = False

    criteria = {'$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    if nodes:
        node_criteria = {'node': {'$in': list(nodes)}}
        if include_children:
            node_criteria = {'$or': [node_criteria, {'parent': {'$in': list(nodes)}}]}
        match = merge_dicts(criteria, node_criteria)
    else:
        match = criteria

    anc_criteria = copy.deepcopy(criteria)
    if unattached:
        anc_criteria.update({
            'unattached': True
        })
    lookup = {'from': coll,
              'startWith': '$parent',
              'connectFromField': 'parent',
              'connectToField': 'node',
              'depthField': 'level',
              'restrictSearchWithMatch': anc_criteria,
              'as': 'ancestors'}
    project = {'node': 1,
               'parent': 1,
               'label': 1,
               '_id': 0,
               'ancestors': {'$map': {'input': '$ancestors',
                                      'as': 'ancestor',
                                      'in': {'node': '$$ancestor.node',
                                             'level': '$$ancestor.level'}}}}

    group = {'_id':
                {'node': '$node',
                'parent': '$parent',
                'label': '$label',
                'how': '$how'},
            'node':{'$last':'$node'},
            'parent':{'$last':'$parent'},
            'label':{'$last':'$label'},
            'how':{'$last':'$how'}}

    pipeline = [{'$match': match},
                {'$group': group},
                {'$graphLookup': lookup},
                {'$project': project}]

    return _anc_yielder_versioned(hier_collection.aggregate(pipeline), exclude_admin_root)


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_descendants(as_of,
                      nodes=None,
                      levels=1,
                      include_hidden=False,
                      include_children=False,
                      drilldown=True,
                      db=None,
                      period=None,
                      boundary_dates=None,
                      leads=False,
                      ):
    """
    fetch descendants of many hierarchy nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        nodes {list} -- nodes to find descendants for                           ['0050000FLN2C9I2', ]

    Keyword Arguments:
        nodes {list} -- nodes to find ancestors for, if None grabs all
                    (default: {None})
        levels {int} -- how many levels of tree to traverse down
                        (default: {1})
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        include_children {bool} -- if True, fetch children of nodes in nodes
                                   (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of ({node: node, parent: parent, descendants: ({children}, {grandchildren}, ... ,)})
    """
    if leads: coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    else: coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    criteria = {'$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    if nodes:
        node_criteria = {'node': {'$in': list(nodes)}}
        if include_children:
            node_criteria = {'$or': [node_criteria, {'parent': {'$in': list(nodes)}}]}
        match = merge_dicts(criteria, node_criteria)
    else:
        match = criteria

    lookup = {'from': coll,
              'startWith': '$node',
              'connectFromField': 'node',
              'connectToField': 'parent',
              'depthField': 'level',
              'restrictSearchWithMatch': criteria,
              'maxDepth': levels - 1,
              'as': 'descendants'}
    project = {'node': 1,
               'parent': 1,
               'label': 1,
               '_id': 0,
               'descendants': {'$map': {'input': '$descendants',
                                        'as': 'descendant',
                                        'in': {'node': '$$descendant.node',
                                               'parent': '$$descendant.parent',
                                               'level': '$$descendant.level'}}}}
    pipeline = [{'$match': match},
                {'$graphLookup': lookup},
                {'$project': project},
                ]

    return _desc_yielder(hier_collection.aggregate(pipeline), levels)


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_descendants_versioned(as_of,
                                nodes=None,
                                levels=1,
                                include_hidden=False,
                                include_children=False,
                                drilldown=True,
                                db=None,
                                period=None,
                                boundary_dates=None,
                                leads=False,
                                ):
    """
    fetch descendants of many hierarchy nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        nodes {list} -- nodes to find descendants for                           ['0050000FLN2C9I2', ]

    Keyword Arguments:
        nodes {list} -- nodes to find ancestors for, if None grabs all
                    (default: {None})
        levels {int} -- how many levels of tree to traverse down
                        (default: {1})
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        include_children {bool} -- if True, fetch children of nodes in nodes
                                   (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of ({node: node, parent: parent, descendants: ({children}, {grandchildren}, ... ,)})
    """
    if leads: coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    else: coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if boundary_dates:
        from_date, to_date = boundary_dates
    else:
        if period:
            as_of = get_period_as_of(period)
            from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
            _, to_date = get_period_begin_end(period)
        else:
            from_date = to_date = as_of


    criteria = {'$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    if nodes:
        node_criteria = {'node': {'$in': list(nodes)}}
        if include_children:
            node_criteria = {'$or': [node_criteria, {'parent': {'$in': list(nodes)}}]}
        match = merge_dicts(criteria, node_criteria)
    else:
        match = criteria

    lookup = {'from': coll,
              'startWith': '$node',
              'connectFromField': 'node',
              'connectToField': 'parent',
              'depthField': 'level',
              'restrictSearchWithMatch': criteria,
              'maxDepth': levels - 1,
              'as': 'descendants'}
    project = {'node': 1,
               'parent': 1,
               'label': 1,
               '_id': 0,
               'descendants': {'$map': {'input': '$descendants',
                                        'as': 'descendant',
                                        'in': {'node': '$$descendant.node',
                                               'parent': '$$descendant.parent',
                                               'level': '$$descendant.level'}}}}
    pipeline = [{'$match': match},
                {'$graphLookup': lookup},
                {'$project': project},
                ]

    return _desc_yielder(hier_collection.aggregate(pipeline), levels)


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_users_nodes_and_root_nodes(user,
                                     as_of,
                                     include_hidden=False,
                                     drilldown=True,
                                     db=None,
                                     period=None,
                                     service=None
                                     ):
    """
    fetch the top level nodes that a user has access to, and the hierarchy roots of those nodes

    Arguments:
        user {dict} -- user object from sec_context                             ??
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        tuple -- ([users top level node dicts], [users root dicts])
    """

    if service=='leads':
        user_access = get_user_permissions(user, 'leads_results')
    else:
        user_access = get_user_permissions(user, 'results')
    logger.info("User Access is data for nodes:" + str(user_access))

    # admin access, users nodes are the admin level roots, usually invisible in app
    if '!' in user_access:
        root_nodes = [merge_dicts(node, {'root': node['node']})
                      for node in fetch_root_nodes(as_of, include_hidden, drilldown, true_roots=True, db=db, service=service)]
        return root_nodes, root_nodes

    root_nodes = [merge_dicts(node, {'root': node['node']})
                  for node in fetch_root_nodes(as_of, include_hidden, drilldown, true_roots=False, db=db, service=service)]
    # global access, users nodes are the hierarchies roots
    if '*' in user_access:
        if not is_lead_service(service):
            return root_nodes, root_nodes
        user_access.remove('*')

    user_nodes, nodes, user_roots = [], set(), set()
    for node in fetch_ancestors(as_of, user_access, include_hidden, drilldown=drilldown, limit_to_visible=True, db=db, service=service):
        try:
            node['root'] = node['ancestors'][0]
        except IndexError:
            node['root'] = node['node']
        node['level'] = len(node['ancestors'])

        user_nodes.append(node)
        nodes.add(node['node'])
        user_roots.add(node['root'])

    user_nodes = sorted((node for node in user_nodes if not any(ancestor in nodes for ancestor in node['ancestors'])),
                        key=lambda x: x['level'])

    return user_nodes, [x for x in root_nodes if x['node'] in user_roots]


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_users_nodes_and_root_nodes_versioned(user,
                                               as_of,
                                               include_hidden=False,
                                               drilldown=True,
                                               db=None,
                                               period=None,
                                               service=None
                                               ):
    """
    fetch the top level nodes that a user has access to, and the hierarchy roots of those nodes

    Arguments:
        user {dict} -- user object from sec_context                             ??
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        tuple -- ([users top level node dicts], [users root dicts])
    """
    if service == 'leads':
        user_access = get_user_permissions(user, 'leads_results')
    else:
        user_access = get_user_permissions(user, 'results')
    logger.info("User Access is data for nodes:" + str(user_access))

    # admin access, users nodes are the admin level roots, usually invisible in app
    if '!' in user_access:
        root_nodes = [merge_dicts(node, {'root': node['node']})
                      for node in
                      fetch_root_nodes(as_of, include_hidden, drilldown, true_roots=True, db=db, period=period,
                                       service=service)]
        return root_nodes, root_nodes

    root_nodes = [merge_dicts(node, {'root': node['node']})
                  for node in fetch_root_nodes(as_of, include_hidden, drilldown, true_roots=False, db=db, period=period,
                                               service=service)]
    # global access, users nodes are the hierarchies roots
    if '*' in user_access:
        if not is_lead_service(service):
            return root_nodes, root_nodes
        user_access.remove('*')

    user_nodes, nodes, user_roots = [], set(), set()
    for node in fetch_ancestors(as_of, user_access, include_hidden, drilldown=drilldown, limit_to_visible=True, db=db,
                                period=period, service=service):
        try:
            node['root'] = node['ancestors'][0]
        except IndexError:
            node['root'] = node['node']
        node['level'] = len(node['ancestors'])

        user_nodes.append(node)
        nodes.add(node['node'])
        user_roots.add(node['root'])

    user_nodes = sorted((node for node in user_nodes if not any(ancestor in nodes for ancestor in node['ancestors'])),
                        key=lambda x: x['level'])

    return user_nodes, [x for x in root_nodes if x['node'] in user_roots]


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_node_to_parent_mapping_and_labels(as_of,
                                            include_hidden=False,
                                            drilldown=True,
                                            db=None,
                                            action=None,
                                            period=None,
                                            boundary_dates=None,
                                            service=None
                                            ):
    """
    fetch map of node : parent for all nodes in hierarchy

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        tuple -- ({node: parent}, {node: label})
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'$and': [
        {'$or': [{'from': None},
                 {'from': {'$lte': as_of}}]},
        {'$or': [{'to': None},
                 {'to': {'$gte': as_of}}]}
    ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]}, # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    hier_records = hier_collection.find(criteria, {'node': 1, 'parent': 1, 'label': 1, '_id': 0, 'is_team':1})

    node_to_parent, labels = {}, {}
    if action:
        for hier_record in hier_records:
            node_to_parent[hier_record['node']] = {"parent": hier_record['parent'], "is_team": hier_record.get("is_team", False)}
            labels[hier_record['node']] = hier_record['label']
    else:
        for hier_record in hier_records:
            node_to_parent[hier_record['node']] = hier_record['parent']
            labels[hier_record['node']] = hier_record['label']

    return node_to_parent, labels


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_node_to_parent_mapping_and_labels_versioned(as_of,
                                                      include_hidden=False,
                                                      drilldown=True,
                                                      db=None,
                                                      action=None,
                                                      period=None,
                                                      boundary_dates=None,
                                                      service=None
                                                      ):
    """
    fetch map of node : parent for all nodes in hierarchy

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        tuple -- ({node: parent}, {node: label})
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if boundary_dates:
        from_date, to_date = boundary_dates
    else:
        if period:
            as_of = get_period_as_of(period)
            from_date, _ = fetch_boundry(as_of, drilldown=drilldown, service=service)
            _, to_date = get_period_begin_end(period)
        else:
            from_date = to_date = as_of

    criteria = {'$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$nor':[{'how': {'$regex':'_hide'}},
                                {'how': {'$regex':'^hide','$options':'m'}}]}
        criteria = {'$and': [criteria, hide_criteria]}

    hier_records = hier_collection.find(criteria, {'node': 1, 'parent': 1, 'label': 1, '_id': 0, 'is_team':1})

    node_to_parent, labels = {}, {}
    if action:
        for hier_record in hier_records:
            node_to_parent[hier_record['node']] = {"parent": hier_record['parent'], "is_team": hier_record.get("is_team", False)}
            labels[hier_record['node']] = hier_record['label']
    else:
        for hier_record in hier_records:
            node_to_parent[hier_record['node']] = hier_record['parent']
            labels[hier_record['node']] = hier_record['label']

    return node_to_parent, labels


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def search_nodes(as_of,
                 search_terms,
                 include_hidden=False,
                 drilldown=True,
                 db=None,
                 period=None):
    """
    search for nodes fuzzy matching search terms

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        search_terms {list} -- list of terms to search for, OR not AND          ['alex', 'megan']

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of dicts of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}
    # TODO: this match could be looser, not super kind to typos
    criteria['$or'] = [{field: {'$regex': '|'.join(search_terms), '$options': 'i'}} for field in ['label', 'node']]

    return hier_collection.find(criteria, {'node': 1, 'label': 1, '_id': 0})


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def search_nodes_versioned(as_of,
                        search_terms,
                        include_hidden=False,
                        drilldown=True,
                        db=None,
                        period=None):
    """
    search for nodes fuzzy matching search terms

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        search_terms {list} -- list of terms to search for, OR not AND          ['alex', 'megan']

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- generator of dicts of node records
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if period:
        as_of = get_period_as_of(period)
        from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
        _, to_date = get_period_begin_end(period)
    else:
        from_date = to_date = as_of

    criteria = {'$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    # TODO: this match could be looser, not super kind to typos
    criteria['$or'] = [{field: {'$regex': '|'.join(search_terms), '$options': 'i'}} for field in ['label', 'node']]

    return hier_collection.find(criteria, {'node': 1, 'label': 1, '_id': 0})


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_descendant_ids(as_of,
                         node,
                         levels=None,
                         include_hidden=False,
                         drilldown=True,
                         db=None,
                         period=None,
                         service=None):
    """
    fetch unordered list of node ids of descendants of node

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'

    Keyword Arguments:
        levels {int} -- how many levels of tree to traverse down
                        if None, traverses entire tree (default: {None})
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        list -- [node ids]
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    if node:
        match = merge_dicts(criteria, {'node': node})
    else:
        roots = [x['node'] for x in fetch_root_nodes(as_of, drilldown=drilldown, service=service)]
        match = merge_dicts(criteria, {'node': {'$in': roots}})

    lookup = {'from': coll,
              'startWith': '$node',
              'connectFromField': 'node',
              'connectToField': 'parent',
              'depthField': 'level',
              'restrictSearchWithMatch': criteria,
              'as': 'descendants'}
    if levels:
        lookup['maxDepth'] = levels - 1

    project = {'_id': 0,
               'descendants': '$descendants.node'}
    pipeline = [{'$match': match},
                {'$graphLookup': lookup},
                {'$project': project},
                ]

    try:
        return hier_collection.aggregate(pipeline).next()['descendants']
    except StopIteration:
        return []


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_descendant_ids_versioned(as_of,
                                   node,
                                   levels=None,
                                   include_hidden=False,
                                   drilldown=True,
                                   db=None,
                                   period=None, service=None):
    """
    fetch unordered list of node ids of descendants of node

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {str} -- node                                                      '0050000FLN2C9I2'

    Keyword Arguments:
        levels {int} -- how many levels of tree to traverse down
                        if None, traverses entire tree (default: {None})
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        list -- [node ids]
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if period:
        as_of = get_period_as_of(period)
        from_date, _ = fetch_boundry(as_of, drilldown=drilldown, service=service)
        _, to_date = get_period_begin_end(period)
    else:
        from_date = to_date = as_of

    criteria = {'$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    if node:
        match = merge_dicts(criteria, {'node': node})
    else:
        roots = [x['node'] for x in fetch_root_nodes(as_of, drilldown=drilldown, period=period, service=service)]
        match = merge_dicts(criteria, {'node': {'$in': roots}})

    lookup = {'from': coll,
              'startWith': '$node',
              'connectFromField': 'node',
              'connectToField': 'parent',
              'depthField': 'level',
              'restrictSearchWithMatch': criteria,
              'as': 'descendants'}
    if levels:
        lookup['maxDepth'] = levels - 1

    project = {'_id': 0,
               'descendants': '$descendants.node'}
    pipeline = [{'$match': match},
                {'$graphLookup': lookup},
                {'$project': project},
                ]

    try:
        return hier_collection.aggregate(pipeline).next()['descendants']
    except StopIteration:
        return []


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_ancestor_ids(as_of,
                       nodes=None,
                       include_hidden=False,
                       hidden_only=False,
                       drilldown=True,
                       signature='',
                       db=None,
                       period=None,
                       service=None):
    """
    fetch set of node ids of ancestors of nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        nodes {list} -- nodes                                                   ['0050000FLN2C9I2']
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        hidden_only {bool} -- if True, include only hidden nodes and their ancestors
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        signature {str} -- if provided, fetch only hidden nodes with how
                           matching signature, (default: {''})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        set -- {node ids}
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    if nodes:
        node_criteria = {'node': {'$in': list(nodes)}}
        match = merge_dicts(criteria, node_criteria)
    else:
        match = {k: v for k, v in criteria.items()}

    if signature:
        match['how'] = signature

    if hidden_only:
        hidden_only_criteria = {'$or': [
                {'$and': [{'hidden_from': 0},
                          {"$or": [{"hidden_to": {"$exists": False}}, {"hidden_to": None}]}
                          ]
                 }]
            }
        match = {'$and': [criteria, hidden_only_criteria]}

    lookup = {'from': coll,
              'startWith': '$parent',
              'connectFromField': 'parent',
              'connectToField': 'node',
              'depthField': 'level',
              'restrictSearchWithMatch': criteria,
              'as': 'ancestors'}
    project = {'_id': 0,
               'ancestors': '$ancestors.node',
               'node': 1}

    pipeline = [{'$match': match},
                {'$graphLookup': lookup},
                {'$project': project}]

    ids = set()
    for rec in hier_collection.aggregate(pipeline):
        ids.add(rec['node'])
        ids |= set(rec['ancestors'])
    return ids


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_ancestor_ids_versioned(as_of,
                                nodes=None,
                                include_hidden=False,
                                hidden_only=False,
                                drilldown=True,
                                signature='',
                                db=None,
                                period=None,
                                service=None):
    """
    fetch set of node ids of ancestors of nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910

    Keyword Arguments:
        nodes {list} -- nodes                                                   ['0050000FLN2C9I2']
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        hidden_only {bool} -- if True, include only hidden nodes and their ancestors
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        signature {str} -- if provided, fetch only hidden nodes with how
                           matching signature, (default: {''})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        set -- {node ids}
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if period:
        as_of = get_period_as_of(period)
        from_date, _ = fetch_boundry(as_of, drilldown=drilldown, service=service)
        _, to_date = get_period_begin_end(period)
    else:
        from_date = to_date = as_of

    criteria = {'$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    if nodes:
        node_criteria = {'node': {'$in': list(nodes)}}
        match = merge_dicts(criteria, node_criteria)
    else:
        match = {k: v for k, v in criteria.items()}

    if signature:
        match['how'] = signature

    if hidden_only:
        hidden_only_criteria = {'$or': [
                {'$and': [{'hidden_from': {'$lte': from_date}},
                          {"$or": [{"hidden_to": {"$exists": False}}, {"hidden_to": None},
                                   {'hidden_to': {'$gte': to_date}}
                                   ]},
                          ]
                 }]
            }
        match = {'$and': [criteria, hidden_only_criteria]}

    lookup = {'from': coll,
              'startWith': '$parent',
              'connectFromField': 'parent',
              'connectToField': 'node',
              'depthField': 'level',
              'restrictSearchWithMatch': criteria,
              'as': 'ancestors'}
    project = {'_id': 0,
               'ancestors': '$ancestors.node',
               'node': 1}

    pipeline = [{'$match': match},
                {'$graphLookup': lookup},
                {'$project': project}]

    ids = set()
    for rec in hier_collection.aggregate(pipeline):
        ids.add(rec['node'])
        ids |= set(rec['ancestors'])
    return ids


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_labels(as_of,
                 nodes,
                 include_hidden=False,
                 drilldown=True,
                 db=None,
                 period=None,
                 boundary_dates=None,
                 service=None
                 ):
    """
    fetch labels of nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {list} -- list of nodes to find names for                          ['0050000FLN2C9I2', ]

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        dict -- mapping from node to label
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    as_of = 0

    criteria = {'node': {'$in': nodes},
                '$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
    ]}

    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return {node['node']: node['label'] for node in hier_collection.find(criteria, {'node': 1, 'label': 1, '_id': 0})}


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_labels_versioned(as_of,
                           nodes,
                           include_hidden=False,
                           drilldown=True,
                           db=None,
                           period=None,
                           boundary_dates=None,
                           service=None
                           ):
    """
    fetch labels of nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        node {list} -- list of nodes to find names for                          ['0050000FLN2C9I2', ]

    Keyword Arguments:
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        dict -- mapping from node to label
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if boundary_dates:
        from_date, to_date = boundary_dates
    else:
        if period:
            as_of = get_period_as_of(period)
            from_date, _ = fetch_boundry(as_of, drilldown=drilldown, service=service)
            _, to_date = get_period_begin_end(period)
        else:
            from_date = to_date = as_of

    criteria = {'node': {'$in': nodes},
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}

    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return {node['node']: node['label'] for node in hier_collection.aggregate([{'$match':criteria}, {'$project':{'node': 1, 'label': 1, '_id': 0}}])}


def find_closest_node(node,
                      period,
                      drilldown=True,
                      db=None
                      ):
    """
    find the node nearest to the given node that a user has access to

    Args:
        node {str} -- node                                                      '0050000FLN2C9I2'
        period {str} -- period mnemonic                                         '2020Q2'

    Keyword Arguments:
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        str: hierarchy node
    """
    user_nodes = get_user_permissions(sec_context.get_effective_user(), 'results')

    # global access, can see all nodes
    if '*' in user_nodes or node in user_nodes:
        return node

    as_of = get_period_as_of(period)
    ancestors = fetch_ancestors(as_of, [node], drilldown=drilldown, db=db, period=period).next()['ancestors']

    # user has access to an ancestor of the node, can see given node
    if any(user_node in ancestors for user_node in user_nodes):
        return node

    for nodes in fetch_descendants(as_of, [node], levels=10, drilldown=drilldown, db=db, period=period).next()['descendants']:
        # breadth first traverse down subtree of given node
        # return descendant of given node if user has access to it
        try:
            return next(desc_node for desc_node, user_node in product(nodes, user_nodes) if desc_node == user_node)
        except StopIteration:
            pass


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def node_is_valid(node,
                  period,
                  drilldown=True,
                  db=None,
                  ):
    """
    check if a node is a real live node

    Arguments:
        node {str} -- node                                                      '0050000FLN2C9I2'
        period {str} -- period mnemonic                                         '2020Q2'

    Keyword Arguments:
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if valid
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'node': node,
                '$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}

    hide_criteria = {'$or': [
            {"$and": [
                {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
            ]},  # Checking for the node which is not marked as hidden earlier
            {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
            {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
        }
    criteria = {'$and': [criteria, hide_criteria]}

    try:
        hier_collection.find(criteria, {'_id': 0}).next()
        return True
    except StopIteration:
        return False


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def node_is_valid_versioned(node,
                            period,
                            drilldown=True,
                            db=None,
                            ):
    """
    check if a node is a real live node

    Arguments:
        node {str} -- node                                                      '0050000FLN2C9I2'
        period {str} -- period mnemonic                                         '2020Q2'

    Keyword Arguments:
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if valid
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = get_period_as_of(period)
    from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
    _, to_date = get_period_begin_end(period)

    criteria = {'node': node,
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    hide_criteria = {'$or': [
            {"$and": [
                {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
            ]
            },  # Checking for the node which is not marked as hidden earlier
            {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
        ]
        }
    criteria = {'$and': [criteria, hide_criteria]}

    try:
        hier_collection.find(criteria, {'_id': 0}).next()
        return True
    except StopIteration:
        return False


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def node_exists(node,
                period,
                drilldown=True,
                db=None,
                ):
    """
    check if a node exists in the hierarchy, dead or alive

    Arguments:
        node {str} -- node                                                      '0050000FLN2C9I2'
        period {str} -- period mnemonic                                         '2020Q2'

    Keyword Arguments:
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if exists
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    as_of = get_period_as_of(period)

    criteria = {'node': node,
                '$and': [
                    {'$or': [{'from': None},
                             {'from': {'$lte': as_of}}]},
                    {'$or': [{'to': None},
                             {'to': {'$gte': as_of}}]}
                ]}
    try:
        hier_collection.find(criteria, {'_id': 0}).next()
        return True
    except StopIteration:
        return False


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def node_exists_versioned(node,
                        period,
                        drilldown=True,
                        db=None,
                        ):
    """
    check if a node exists in the hierarchy, dead or alive

    Arguments:
        node {str} -- node                                                      '0050000FLN2C9I2'
        period {str} -- period mnemonic                                         '2020Q2'

    Keyword Arguments:
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        bool -- True if exists
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    as_of = get_period_as_of(period)
    from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
    _, to_date = get_period_begin_end(period)

    criteria = {'node': node,
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    try:
        hier_collection.find(criteria, {'_id': 0}).next()
        return True
    except StopIteration:
        return False


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_top_level_nodes(as_of,
                          levels=1,
                          include_hidden=False,
                          include_children=False,
                          drilldown=True,
                          db=None,
                          period=None
                          ):
    """
    fetch descendants of many hierarchy nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        nodes {list} -- nodes to find descendants for                           ['0050000FLN2C9I2', ]

    Keyword Arguments:
        levels {int} -- how many levels of tree to traverse down
                        (default: {1})
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        include_children {bool} -- if True, fetch children of nodes in nodes
                                   (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- (nodes)
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    criteria = {'$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    match = merge_dicts(criteria, {'parent': None})

    lookup = {'from': coll,
              'startWith': '$node',
              'connectFromField': 'node',
              'connectToField': 'parent',
              'restrictSearchWithMatch': criteria,
              'maxDepth': levels - 1,
              'as': 'descendants'}
    project = {'node': 1,
               '_id': 0,
               'descendants': '$descendants.node'}
    pipeline = [{'$match': match},
                {'$graphLookup': lookup},
                {'$project': project},
                ]
    for node_rec in hier_collection.aggregate(pipeline):
        yield node_rec['node']
        for node in node_rec['descendants']:
            yield node

# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_top_level_nodes_versioned(as_of,
                                    levels=1,
                                    include_hidden=False,
                                    include_children=False,
                                    drilldown=True,
                                    db=None,
                                    period=None
                                    ):
    """
    fetch descendants of many hierarchy nodes

    Arguments:
        as_of {int} -- epoch timestamp to fetch data as of                      1556074024910
        nodes {list} -- nodes to find descendants for                           ['0050000FLN2C9I2', ]

    Keyword Arguments:
        levels {int} -- how many levels of tree to traverse down
                        (default: {1})
        include_hidden {bool} -- if True, include hidden nodes
                                 (default: {False})
        include_children {bool} -- if True, fetch children of nodes in nodes
                                   (default: {False})
        drilldown {bool} -- if True, fetch drilldown node instead of hierarchy
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Returns:
        generator -- (nodes)
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if period:
        as_of = get_period_as_of(period)
        from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
        _, to_date = get_period_begin_end(period)
    else:
        from_date = to_date = as_of

    criteria = {'$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}

    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    match = merge_dicts(criteria, {'parent': None})

    lookup = {'from': coll,
              'startWith': '$node',
              'connectFromField': 'node',
              'connectToField': 'parent',
              'restrictSearchWithMatch': criteria,
              'maxDepth': levels - 1,
              'as': 'descendants'}
    project = {'node': 1,
               '_id': 0,
               'descendants': '$descendants.node'}
    pipeline = [{'$match': match},
                {'$graphLookup': lookup},
                {'$project': project},
                ]
    for node_rec in hier_collection.aggregate(pipeline):
        yield node_rec['node']
        for node in node_rec['descendants']:
            yield node


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_owner_id_nodes(as_of,
                         include_hidden=False,
                         drilldown=True,
                         db=None,
                         period=None):
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    # TODO: only safe for salesforce right now...
    pattern = re.compile("^005") if not drilldown else re.compile("(?<=#)005", re.MULTILINE)
    criteria = {'node': {'$regex': pattern},
                '$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
    ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return hier_collection.find(criteria, {'node': 1, '_id': 0, 'label': 1})


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_owner_id_nodes_versioned(as_of,
                                include_hidden=False,
                                drilldown=True,
                                db=None,
                                period=None):
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if period:
        as_of = get_period_as_of(period)
        from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
        _, to_date = get_period_begin_end(period)
    else:
        from_date = to_date = as_of


    # TODO: only safe for salesforce right now...
    pattern = re.compile("^005") if not drilldown else re.compile("(?<=#)005", re.MULTILINE)
    criteria = {'node': {'$regex': pattern},
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return hier_collection.find(criteria, {'node': 1, '_id': 0, 'label': 1})


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def fetch_non_owner_id_nodes(as_of,
                             include_hidden=False,
                             drilldown=True,
                             db=None,
                             period=None):
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    as_of = 0

    # TODO: only safe for salesforce right now...
    pattern = re.compile("^(?!005).*", re.MULTILINE) if not drilldown else re.compile("#(?!005)(?:.(?!#))+$", re.MULTILINE)
    criteria = {'node': {'$regex': pattern},
                '$and': [
                {'$or': [{'from': None},
                         {'from': {'$lte': as_of}}]},
                {'$or': [{'to': None},
                         {'to': {'$gte': as_of}}]}
    ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]},  # Checking for the node which is not marked as hidden earlier
                {"$and": [{'hidden_from': 0}, {"hidden_to": 0}]},  # Already Node has been unblocked from hidden state
                {'hidden_to': 0}]  # Already Node has been unblocked from hidden state
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return hier_collection.find(criteria, {'node': 1, '_id': 0, 'label': 1})


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def fetch_non_owner_id_nodes_versioned(as_of,
                                    include_hidden=False,
                                    drilldown=True,
                                    db=None,
                                    period=None):
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    if period:
        as_of = get_period_as_of(period)
        from_date, _ = fetch_boundry(as_of, drilldown=drilldown)
        _, to_date = get_period_begin_end(period)
    else:
        from_date = to_date = as_of

    # TODO: only safe for salesforce right now...
    pattern = re.compile("^(?!005).*", re.MULTILINE) if not drilldown else re.compile("#(?!005)(?:.(?!#))+$", re.MULTILINE)
    criteria = {'node': {'$regex': pattern},
                '$and': [
                    {"$or": [
                        {"$and": [
                            {'from': {'$lte': from_date}},
                            {'$or': [
                                {'to': None},
                                {'to': {'$gte': to_date}}
                                ]
                            }]
                        },
                        {"$and": [
                            {'from': {'$gt': from_date}},
                            {'to': {'$lte': to_date}}
                            ]
                        }
                    ]}
                ]}
    if not include_hidden:
        hide_criteria = {'$or': [
                {"$and": [
                    {"$or": [{'hidden_from': {"$exists": False}}, {'hidden_from': None}]},
                    {"$or": [{'hidden_to': {"$exists": False}}, {'hidden_to': None}]}
                ]
                },  # Checking for the node which is not marked as hidden earlier
                {'hidden_to': {'$lte': to_date}}  # Already Node has been unblocked from hidden state
            ]
            }
        criteria = {'$and': [criteria, hide_criteria]}

    return hier_collection.find(criteria, {'node': 1, '_id': 0, 'label': 1})


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in versioned function as well if required
@versioned_func_switch
def validate_node_for_user(user,
                           as_of,
                           node=None,
                           drilldown=True,
                           db=None,
                           period=None
                           ):
    user_nodes = get_user_permissions(user, 'results')
    if not node:
        if '*' in user_nodes:
            return True
        raise AccessError()

    try:
        ancestors = fetch_ancestors(as_of, [node], drilldown=drilldown, db=db).next()['ancestors']
        if '*' in user_nodes or node in user_nodes:
            return True
        if any(user_node in ancestors for user_node in user_nodes):
            return True
    except StopIteration:
        # given a totally bogus node
        raise NodeDoesntExistError()

    raise AccessError()


# This function is for a non-versioned tenant
# TODO: Any changes to this function should be made in non-versioned function as well if required
def validate_node_for_user_versioned(user,
                                    as_of,
                                    node=None,
                                    drilldown=True,
                                    db=None,
                                    period=None
                                    ):
    user_nodes = get_user_permissions(user, 'results')
    if not node:
        if '*' in user_nodes:
            return True
        raise AccessError()

    try:
        ancestors = fetch_ancestors(as_of, [node], drilldown=drilldown, db=db, period=period).next()['ancestors']
        if '*' in user_nodes or node in user_nodes:
            return True
        if any(user_node in ancestors for user_node in user_nodes):
            return True
    except StopIteration:
        # given a totally bogus node
        raise NodeDoesntExistError()

    raise AccessError()


def fetch_closest_boundaries(as_of,
                            drilldown=False,
                            db=None):
    coll = DRILLDOWN_COLL if drilldown else HIER_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    match_criteria_for_prev_from_fld = {'from': {'$lte': as_of}}
    sort_criteria_for_prev_from_fld = {'from': -1}

    match_criteria_for_next_from_fld = {'from': {'$gte': as_of}}
    sort_criteria_for_next_from_fld = {'from': 1}

    limit = 1

    pipeline_for_prev_from_fld = [{'$match': match_criteria_for_prev_from_fld},
                                  {'$sort': sort_criteria_for_prev_from_fld},
                                  {'$limit': limit}]

    pipeline_for_next_from_fld = [{'$match': match_criteria_for_next_from_fld},
                                  {'$sort': sort_criteria_for_next_from_fld},
                                  {'$limit': limit}]

    try:
        results_for_prev_from_fld = hier_collection.aggregate(pipeline_for_prev_from_fld,
                                                         allowDiskUse=True)

        results_for_next_from_fld = hier_collection.aggregate(pipeline_for_next_from_fld,
                                                       allowDiskUse=True)

        closest_prev_from_timestamp = None
        closest_prev_to_timestamp = None
        closest_next_from_timestamp = None
        closest_next_to_timestamp = None

        prev_timestamp = None
        for prev_details in results_for_prev_from_fld:
            prev_timestamp = prev_details
            break

        if prev_timestamp:
            closest_prev_from_timestamp = prev_timestamp['from']
            closest_prev_to_timestamp = prev_timestamp['to']

        next_timestamp = None
        for next_details in results_for_next_from_fld:
            next_timestamp = next_details
            break

        if next_timestamp:
            closest_next_from_timestamp = next_timestamp['from']
            closest_next_to_timestamp = next_timestamp['to']

        return closest_prev_from_timestamp, closest_prev_to_timestamp, closest_next_from_timestamp, \
            closest_next_to_timestamp

    except Exception as e:
        logger.error(f"Failed to fetch from and to boundaries of hierarchy for the given as_of: {e}")

    return None, None, None, None


def fetch_boundry(as_of,
                  drilldown=False,
                  db=None,
                  service=None):
    coll = DRILLDOWN_COLL if drilldown else HIER_COLL
    if is_lead_service(service):
        coll = DRILLDOWN_LEADS_COLL if drilldown else HIER_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    match_criteria_for_prev_from_fld = {'from': {'$lte': as_of}}
    sort_criteria_for_prev_from_fld = {'from': -1}

    limit = 1

    pipeline_for_prev_from_fld = [{'$match': match_criteria_for_prev_from_fld},
                                  {'$sort': sort_criteria_for_prev_from_fld},
                                  {'$limit': limit}]

    try:
        results_for_prev_from_fld = hier_collection.aggregate(pipeline_for_prev_from_fld,
                                                         allowDiskUse=True)

        closest_prev_from_timestamp = None
        closest_prev_to_timestamp = None

        prev_timestamp = None
        for prev_details in results_for_prev_from_fld:
            prev_timestamp = prev_details
            break

        if prev_timestamp:
            closest_prev_from_timestamp = prev_timestamp['from']
            closest_prev_to_timestamp = prev_timestamp['to']

        return closest_prev_from_timestamp, closest_prev_to_timestamp

    except Exception as e:
        logger.error(f"Failed to fetch the boundry for given as_of: {e}")

    return as_of, None


def fetch_prev_boundaries(as_of,
                          drilldown=False,
                          db=None, service=None):
    coll = DRILLDOWN_COLL if drilldown else HIER_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]

    match_criteria_for_prev_from_fld = {'from': {'$lt': as_of}}
    sort_criteria_for_prev_from_fld = {'from': -1}

    limit = 1

    pipeline_for_prev_from_fld = [{'$match': match_criteria_for_prev_from_fld},
                                  {'$sort': sort_criteria_for_prev_from_fld},
                                  {'$limit': limit}]

    return hier_collection.aggregate(pipeline_for_prev_from_fld,
                                                         allowDiskUse=True)


def _make_desc(descendants, num_levels):
    desc = [{} for _ in range(num_levels)]
    for x in descendants:
        desc[x['level']][x['node']] = x['parent']
    return desc


def _desc_yielder(nodes, levels):
    for node in nodes:
        node['descendants'] = tuple(_make_desc(node['descendants'], levels))
        yield node


def _anc_yielder(nodes, exclude_admin_root=False):
    if not nodes.alive:
        yield {}
    for node in nodes:
        if not exclude_admin_root:
            node['ancestors'] = [x['node'] for x in sorted(node['ancestors'], key=lambda x: x.get('level'), reverse=True)]
        else:
            node['ancestors'] = [x['node'] for x in sorted(node['ancestors'], key=lambda x: x.get('level'), reverse=True)][1:]
        yield node


def _anc_yielder_versioned(nodes, exclude_admin_root=False):
    if not nodes.alive:
        yield {}
    for node in nodes:
        unique_ancestor_nodes = []
        for ancestor in node['ancestors']:
            if ancestor not in unique_ancestor_nodes:
                unique_ancestor_nodes.append(ancestor)
        node['ancestors'] = unique_ancestor_nodes
        if not exclude_admin_root:
            node['ancestors'] = [x['node'] for x in sorted(node['ancestors'], key=lambda x: x.get('level'), reverse=True)]
        else:
            node['ancestors'] = [x['node'] for x in sorted(node['ancestors'], key=lambda x: x.get('level'), reverse=True)][1:]
        yield node


class AccessError(Exception):
    pass


class NodeDoesntExistError(Exception):
    pass


# TODO: change implementation by giving level as 20
def fetch_leaves_of_node(node, period=None):
    """
    Note: Not efficient. Just using till a better implementaion comes along
    """
    parents = {node}
    period = period if period else get_current_period()
    as_of = get_period_as_of(period)
    leaves = set()
    while parents:
        all_descendants = list(fetch_descendants(as_of, list(parents), 1, period=period))
        node_parents = set()
        children = set()
        for node_descendants in all_descendants:

            curr_node_descendants = node_descendants['descendants'][0]
            # print(curr_node_descendants)
            children = children.union(set(curr_node_descendants.keys()))
            node_parents = node_parents.union(
                set(curr_node_descendants.values()))
        leaves = leaves.union(parents - node_parents)
        parents = children
    return list(leaves)


def get_results_using_query(query, return_fields={}, drilldown=False, db=None):
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    if return_fields:
        return hier_collection.find(query, return_fields)
    return hier_collection.find(query)


def get_distinct_using_query(query, distinct_field, drilldown=False, db=None):
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = db[coll] if db else sec_context.tenant_db[coll]
    records = hier_collection.distinct(distinct_field, query)
    return records

def find_all_subtree_height(as_of, root_node, leaf_level):
    descendants = fetch_descendants(as_of,  [root_node], leaf_level, include_children=False)
    descendants = [(rec['node'], rec['descendants']) for rec in descendants]
    levels_order_tree = descendants[0][1]

    if isinstance(levels_order_tree, dict):
        levels_order_tree = [levels_order_tree]
    elif isinstance(levels_order_tree, tuple):
        levels_order_tree = list(levels_order_tree)

    levels_order_tree = [d for d in levels_order_tree if d]
    height_sub_tree = {descendants[0][0]:0}

    for tree_level in reversed(levels_order_tree):
        for node_id, parent_id in tree_level.items():
            height = height_sub_tree.get(node_id)
            if height is None :
                height_sub_tree[node_id] =0
            height_sub_tree[parent_id] = max(height_sub_tree.get(parent_id, 0), 1 + height_sub_tree[node_id])

    return height_sub_tree


def find_levels_in_tree_from_top_to_down(as_of, root_node, leaf_level = 15):
    descendants = fetch_descendants(as_of,  [root_node], leaf_level, include_children=False)
    descendants = [(rec['node'], rec['descendants']) for rec in descendants]
    levels_order_tree = descendants[0][1]

    if isinstance(levels_order_tree, dict):
        levels_order_tree = [levels_order_tree]
    elif isinstance(levels_order_tree, tuple):
        levels_order_tree = list(levels_order_tree)

    levels_order_tree = [d for d in levels_order_tree if d]
    node_level_map = {descendants[0][0]:0}

    for tree_level in levels_order_tree:
        for node_id, parent_id in tree_level.items():
            parent_height = node_level_map.get(parent_id, 0)
            node_level_map[node_id] = parent_height+1

    return node_level_map


def find_map_in_nodes_and_lth_grand_children(as_of, root_node, nodes, lth_grand_children, leaf_level = 15):
    descendants = fetch_descendants(as_of,  [root_node], leaf_level, include_children=False)
    descendants = [(rec['node'], rec['descendants']) for rec in descendants]
    levels_order_tree = descendants[0][1]

    if isinstance(levels_order_tree, dict):
        levels_order_tree = [levels_order_tree]
    elif isinstance(levels_order_tree, tuple):
        levels_order_tree = list(levels_order_tree)

    levels_order_tree = [d for d in levels_order_tree if d]
    node_parent_map = {descendants[0][0]:descendants[0][0]}

    for tree_level in levels_order_tree:
        for node_id, parent_id in tree_level.items():
            if node_id in nodes:
                node_parent_map[node_id] = node_id
            else:
                node_parent_map[node_id] = node_parent_map[parent_id]

    map_data = {}
    for lth_grand_child in lth_grand_children:
        parent = node_parent_map.get(lth_grand_child)
        map_data.setdefault(parent, []).append(lth_grand_child)

    return map_data


def fetch_first_line_managers(as_of, root_node, leaf_level):
    return fetch_hier_level_line_managers(as_of, root_node, leaf_level, hier_level=1)

def fetch_second_line_managers(as_of, root_node, leaf_level):
    return fetch_hier_level_line_managers(as_of, root_node, leaf_level, hier_level=2)


def fetch_hier_level_line_managers(as_of, root_node, leaf_level, hier_level=1):
    first_line_managers = []
    height_sub_tree = find_all_subtree_height(as_of, root_node, leaf_level)
    for node_id, height in height_sub_tree.items():
        if height == hier_level:
            first_line_managers.append(node_id)
    return first_line_managers

def _prev_periods_fallback(config, prev_periods):
    if config.config.get('update_new_collection'):
        prev_periods = prev_periods or prev_periods_allowed_in_deals()
    return prev_periods


def is_second_level_manager_and_above(as_of, root_node, leaf_level, node):
    first_line_managers = []
    height_sub_tree = find_all_subtree_height(as_of, root_node, leaf_level)
    for node_id, height in height_sub_tree.items():
        if height >= 2:
            first_line_managers.append(node_id)
    return node in first_line_managers

def _fetch_favorites(period, user, db=None):
    favs_collection = db[FAVS_COLL] if db else sec_context.tenant_db[FAVS_COLL]

    criteria = {'user': user, 'fav': True, 'period': period}

    return {x['opp_id'] for x in favs_collection.find(criteria, {'opp_id': 1, '_id': 0})}

def fetch_deal_totals(period_and_close_periods,
                      node,
                      fields_and_operations,
                      config,
                      user=None,
                      filter_criteria=None,
                      filter_name=None,
                      favorites=None,
                      timestamp=None,
                      db=None,
                      cache=None,
                      return_seg_info=False,
                      dlf_node=None,
                      search_criteria=None,
                      criteria=None,
                      sort_fields=[],
                      custom_limit=None,
                      prev_periods=[],
                      sfdc_view=False,
                      actual_view=False,
                      nodes=None
                      ):
    """
    fetch total and count of deals for period and node

    Arguments:
        period_and_close_periods {tuple} -- as of mnemonic, [close mnemonics]   ('2020Q2', ['2020Q2'])
        node {str} -- hierarchy node                                            '0050000FLN2C9I2'
        fields_and_operations {list} -- tuples of label, deal field,            [('Amount', 'Amount', '$sum')]
                                        mongo op to total
        config {DealsConfig} -- instance of DealsConfig                          ...?

    Keyword Arguments:
        user {str} -- user name, if None, will use sec_context                  'gnana'
        filter_criteria {dict} -- mongo db filter criteria (default: {None})    {'Amount': {'$gte': 100000}}
        filter_name {str} -- label to identify filter in cache
        favorites {set}  -- set of opp_ids that have been favorited by user
                            if None, queries favs collectionf to find them
                            (default: {None})
        timestamp {float} -- epoch timestamp for when deal records expire
                             if None, checks flags to find it
                             (default: {None})
        db {pymongo.database.Database} -- instance of tenant_db
                                          (default: {None})
                                          if None, will create one
        cache {dict} -- dict to hold records fetched by func (default: {None})
                        (used to memoize fetching many recs)

    Returns:
        dict -- 'count' key and total for each total field
    """

    # Below line created a problem in threading, this fall back funtions calculation should be passed from outside
    quarter_collection = db[QUARTER_COLL] if db else sec_context.tenant_db[QUARTER_COLL]
    prev_periods = _prev_periods_fallback(config, prev_periods)

    user = user or sec_context.login_user_name
    hint_field = 'period_-1_close_period_-1_drilldown_list_-1_is_deleted_-1' if '#' in node \
        else 'period_-1_close_period_-1_hierarchy_list_-1_update_date_-1'
    if not isinstance(period_and_close_periods, list):
        period_and_close_periods = [period_and_close_periods]

    period_and_close_periods_map = {"old_deals_coll": [], "new_deals_coll": []}
    deals_collection_map = {}
    periods = []
    for period_and_close_period_for_db in period_and_close_periods:
        period, _ = period_and_close_period_for_db
        periods.append(period)
        if ((period in prev_periods and config.config.get('update_new_collection')) or
                (isinstance(period, list) and not set(period).isdisjoint(prev_periods) and
                 config.config.get('update_new_collection')) or
                period is None):
            period_and_close_periods_map['new_deals_coll'].append(period_and_close_period_for_db)
            deals_collection = db[NEW_DEALS_COLL] if db else \
                sec_context.tenant_db[NEW_DEALS_COLL]
            deals_collection_map['new_deals_coll'] = deals_collection
            collection_name = NEW_DEALS_COLL
        else:
            period_and_close_periods_map['old_deals_coll'].append(period_and_close_period_for_db)
            deals_collection = db[DEALS_COLL] if db else sec_context.tenant_db[DEALS_COLL]
            deals_collection_map['old_deals_coll'] = deals_collection
            collection_name = DEALS_COLL

    moved_deals_in_actual_view = False
    if actual_view:
        objectids = []
        moveddeals = list(quarter_collection.find({'period': {"$in" : periods}}))
        if len(moveddeals) > 0: objectids = list(map(ObjectId, moveddeals[0]['ObjectId']))
        if len(objectids) > 0: moved_deals_in_actual_view = True

    deals_collection_criteria = {}
    for period_map in period_and_close_periods_map.keys():
        match_list = []
        if (period_map == 'old_deals_coll' and period_and_close_periods_map['old_deals_coll']) or \
                (period_map == 'new_deals_coll' and period_and_close_periods_map['new_deals_coll']):
            pass
        else:
            continue

        period_and_close_periods = period_and_close_periods_map[period_map]

        for period_and_close_period in period_and_close_periods:
            period, close_periods = period_and_close_period
            if isinstance(favorites, dict):
                favs = favorites.get(period, None)
                if not favs:
                    favs = _fetch_favorites(period, user, db)
            else:
                favs = favorites or _fetch_favorites(period, user, db)
            if isinstance(timestamp, dict):
                timestamp = timestamp.get(period, None)
                if not timestamp:
                    timestamp = _get_timestamp(period)
            else:
                timestamp = timestamp or _get_timestamp(period)  # BUG: cant get timestamp when threaded from fm svc

            hier_field = 'drilldown_list' if '#' in node else 'hierarchy_list'

            if not isinstance(period, list):
                if not moved_deals_in_actual_view:
                    match = {'period': period, hier_field: {'$in': [node]}}
                else:
                    match = {hier_field: {'$in': [node]}}
                if criteria:
                    match.update(criteria)
                if timestamp:
                    match['update_date'] = {'$gte': timestamp}
                else:
                    match['is_deleted'] = False

                if close_periods:
                    period_key = get_key_for_close_periods(close_periods)
                    hint_field = modify_hint_field(close_periods, hint_field)
                    if not moved_deals_in_actual_view:
                        match[period_key] = {'$in': close_periods}
                    else:
                        closeperiods_value = close_periods
            else:
                match = {'$or': []}
                for temp_num in range(len(period)):
                    if not moved_deals_in_actual_view:
                        match1 = {'period': period[temp_num], hier_field: {'$in': [node]}}
                    else:
                        match1 = {hier_field: {'$in': [node]}}
                    if criteria:
                        match1.update(criteria)
                    if timestamp:
                        match1['update_date'] = {'$gte': timestamp}
                    else:
                        match1['is_deleted'] = False

                    if close_periods:
                        period_key = get_key_for_close_periods(close_periods)
                        if not moved_deals_in_actual_view:
                            match1[period_key] = {'$in': close_periods[temp_num]}
                        else:
                            closeperiods_value = close_periods
                    match['$or'] += [match1]

            if filter_criteria:
                match = {
                    '$and': [match, contextualize_filter(filter_criteria, dlf_node if dlf_node else node, favs, period)]}

            if search_criteria:
                search_terms, search_fields = search_criteria
                match['$or'] = [{field: {'$regex': '|'.join(search_terms), '$options': 'i'}} for field in search_fields]

            match_list.append(match)

        if not period_and_close_periods:
            period = {}
            favs = {}
            hier_field = 'drilldown_list' if '#' in node else 'hierarchy_list'
            match = {hier_field: {'$in': [node]}}
            if criteria:
                match.update(criteria)
            match['is_deleted'] = False
            if filter_criteria:
                match = {
                    '$and': [match, contextualize_filter(filter_criteria, dlf_node if dlf_node else node, favs, period)]}
            if search_criteria:
                search_terms, search_fields = search_criteria
                match['$or'] = [{field: {'$regex': '|'.join(search_terms), '$options': 'i'}} for field in search_fields]
            match_list.append(match)
            logger.info("Match List %s", match)

        match = {'$or': []}
        if len(match_list) > 1:
            for m in match_list:
                match['$or'].append(m)
        else:
            match = match_list[0]

        if actual_view:
            match['is_moved'] = {'$exists': False}

        if moved_deals_in_actual_view:
            actual_view_criteria = [{period_key: {'$in': closeperiods_value}, 'period': period},
                                    {'_id': {'$in': objectids}}]
            if '$or' in match.keys():
                existing_or = match.pop('$or')
                match['$and'] = [{'$or': existing_or}, {'$or': actual_view_criteria}]
            elif '$and' in match.keys():
                match['$and'].append({'$or': actual_view_criteria})
            else:
                match['$or'] = actual_view_criteria
        deals_collection_criteria[period_map] = match

    project = {contextualize_field(fld, config, dlf_node if dlf_node else node): 1 for _, fld, _ in
               fields_and_operations}
    project['update_date'] = 1

    group = {label: {op: '$' + contextualize_field(fld, config, dlf_node if dlf_node else node)} for label, fld, op in
             fields_and_operations}

    group.update({'_id': None,
                  'count': {'$sum': 1},
                  'timestamp': {'$last': '$update_date'}
                  })
    if period_and_close_periods_map['old_deals_coll'] and period_and_close_periods_map['new_deals_coll']:
        collection_name = NEW_DEALS_COLL + " & " + DEALS_COLL
        deals_collection = deals_collection_map['new_deals_coll']
        match1 = deals_collection_criteria['new_deals_coll']
        match2 = deals_collection_criteria['old_deals_coll']
        pipeline = [{'$match': match1},
                    {'$unionWith': {'coll': 'deals',
                                    'pipeline': [{'$match': match2}]}},
                    {'$project': project},
                    {'$group': group}]
    else:
        pipeline = [{'$match': match},
                    {'$project': project},
                    {'$group': group}]

    if custom_limit:
        if sort_fields:
            if period_and_close_periods_map['old_deals_coll'] and period_and_close_periods_map['new_deals_coll']:
                deals_collection = deals_collection_map['new_deals_coll']
                match1 = deals_collection_criteria['new_deals_coll']
                match2 = deals_collection_criteria['old_deals_coll']
                pipeline = [{'$match': match1},
                            {'$unionWith': {'coll': 'deals',
                                            'pipeline': [{'$match': match2}]}},
                            {'$project': project},
                            {'$sort': OrderedDict(
                                [(contextualize_field(field, config, node), direction) for
                                 field, direction in sort_fields])},
                            {'$limit': custom_limit},
                            {'$group': group}]
            else:
                pipeline = [{'$match': match},
                            {'$project': project},
                            {'$sort': OrderedDict(
                                [(contextualize_field(field, config, node), direction) for
                                 field, direction in sort_fields])},
                            {'$limit': custom_limit},
                            {'$group': group}]
        else:
            if period_and_close_periods_map['old_deals_coll'] and period_and_close_periods_map['new_deals_coll']:
                deals_collection = deals_collection_map['new_deals_coll']
                match1 = deals_collection_criteria['new_deals_coll']
                match2 = deals_collection_criteria['old_deals_coll']
                pipeline = [{'$match': match1},
                            {'$unionWith': {'coll': 'deals',
                                            'pipeline': [{'$match': match2}]}},
                            {'$project': project},
                            {'$limit': custom_limit},
                            {'$group': group}]
            else:
                pipeline = [{'$match': match},
                            {'$project': project},
                            {'$limit': custom_limit},
                            {'$group': group}]

    if return_seg_info:
        if config.segment_field:
            seg_field = config.segment_field
            group['_id'] = '$' + str(seg_field)
            project.update({seg_field: 1})
        else:
            if config.debug:
                logger.error("segment_field config not there in deal_svc config")

    if sfdc_view:
        sort = {'update_time': 1}
        project['opp_id'] = 1
        pregroup = {label: {"$last": '$' + contextualize_field(fld, config, dlf_node if dlf_node else node)} for
                    label, fld, _ in fields_and_operations}
        pregroup.update({'_id': '$opp_id',
                         'timestamp': {'$last': '$update_date'}
                         })
        group = {label: {op: '$' + label} for label, fld, op in fields_and_operations}
        group.update({'_id': None,
                      'count': {'$sum': 1},
                      'timestamp': {'$last': '$timestamp'}
                      })
        if period_and_close_periods_map['old_deals_coll'] and period_and_close_periods_map['new_deals_coll']:
            deals_collection = deals_collection_map['new_deals_coll']
            match1 = deals_collection_criteria['new_deals_coll']
            match2 = deals_collection_criteria['old_deals_coll']
            pipeline = [{'$match': match1},
                        {'$unionWith': {'coll': 'deals',
                                        'pipeline': [{'$match': match2}]}},
                        {'$project': project},
                        {'$sort': sort},
                        {'$group': pregroup},
                        {'$group': group}]
        else:
            pipeline = [{'$match': match},
                        {'$project': project},
                        {'$sort': sort},
                        {'$group': pregroup},
                        {'$group': group}]

    # AV-14059 Commenting below as part of the log fix
    # Reason: Cannot process this log everytime as ingestion is high and utility is low
    # logger.info('fetch_deal_totals collection_name %s,  filter_name: %s, pipeline: %s', collection_name,
    #                 filter_name, pipeline)

    aggs = list(deals_collection.aggregate(pipeline=pipeline, allowDiskUse=True, hint=hint_field))

    aggregated_val = {}
    if return_seg_info:
        for res in aggs:
            val = {}
            for k, v in res.items():
                if k != '_id':
                    val[k] = v
                    if k not in aggregated_val:
                        aggregated_val[k] = v
                    else:
                        aggregated_val[k] += v
            if config.segment_field:
                cache[(filter_name, res['_id'], node)] = val
        cache[(filter_name, 'all_deals', node)] = aggregated_val
        return cache
    else:
        try:
            val = {k: v for k, v in aggs[0].items() if k != '_id'}
        except IndexError:
            val = {label: 0 for label, _, _ in fields_and_operations}
            val['count'] = 0
            val['timestamp'] = None

        if cache is not None:
            cache[(filter_name, node)] = val

        return val

def fetch_crr_deal_rollup(period_and_close_periods,
                          nodes,
                          fields_and_operations,
                          config,
                          user=None,
                          filter_criteria=None,
                          filter_name=None,
                          favorites=None,
                          timestamp=None,
                          db=None,
                          cache=None,
                          return_seg_info=False,
                          filters=[],
                          root_node=None,
                          use_dlf_fcst_coll=False
                          ):
    deals_collection = db[GBM_CRR_COLL] if db else sec_context.tenant_db[GBM_CRR_COLL]
    user = user or sec_context.login_user_name

    if not isinstance(period_and_close_periods, list):
        period_and_close_periods = [period_and_close_periods]
    hier_aware = False
    match_list = []
    match_list2 = []
    for _, fld, _ in fields_and_operations:
        if fld in config.hier_aware_fields and fld != 'forecast':
            hier_aware = True
    for period_and_close_period in period_and_close_periods:
        period, close_periods = period_and_close_period
        if isinstance(favorites, dict):
            favs = favorites.get(period, None)
            if not favs:
                favs = _fetch_favorites(period, user, db)
        else:
            favs = favorites or _fetch_favorites(period, user, db)

        if isinstance(timestamp, dict):
            timestamp = timestamp.get(period, None)
            if not timestamp:
                timestamp = _get_timestamp(period)
        else:
            timestamp = timestamp or _get_timestamp(period)  # BUG: cant get timestamp when threaded from fm svc

        hier_field = '__segs'
        hint_field = 'monthly_period_-1___segs_-1_last_modified_-1'

        match = {'period': period,
                 hier_field: {'$in': nodes}}
        if timestamp:
            match['last_modified'] = {'$gte': timestamp}

        if close_periods:
            match['monthly_period'] = {'$in': close_periods}
        if filter_criteria:
            if "%(node)s" in str(filter_criteria):
                matches = None
                for node in nodes:
                    if matches is None:
                        matches = [contextualize_filter(filter_criteria, node, favs, period)]
                    else:
                        matches.append(contextualize_filter(filter_criteria, node, favs, period))
                match = {'$and': [match, {'$or': matches}]}
            else:
                match = {'$and': [match, contextualize_filter(filter_criteria, nodes[0], favs, period)]}
        match_list.append(match)
        if not hier_aware:
            match2 = {hier_field: {'$in': nodes}}
            match_list2.append(match2)

    match = {'$or': []}
    if len(match_list) > 1:
        for m in match_list:
            match['$or'].append(m)
    else:
        match = match_list[0]

    if not hier_aware:
        match2 = {'$or': []}
        if len(match_list2) > 1:
            for m in match_list2:
                match2['$or'].append(m)
        else:
            match2 = match_list2[0]


    group = {}
    regroup = {}
    if hier_aware:
        project = {contextualize_field(fld, config, node): 1 for node in nodes for _, fld, _ in fields_and_operations}
        group['_id'] = None
        group.update(
            {label + node.replace('.', '$'): {op: '$' + contextualize_field(fld, config, node)} for node in nodes for
             label, fld, op in fields_and_operations})
    else:
        if len(nodes) == 1 or root_node is None:
            root_node = nodes[0]
        project = {contextualize_field(fld, config, root_node, hier_aware=False if 'forecast' in fld else True): 1 for
                   _, fld, _ in fields_and_operations}

        group['_id'] = {
            label: '$' + contextualize_field(fld, config, root_node, hier_aware=False if 'forecast' in fld else True)
            for label, fld, op in fields_and_operations}
        group['_id'][hier_field] = '$' + hier_field
        group.update({hier_field: {'$addToSet': '$' + hier_field}})
        regroup = {label: {op: '$_id.' + label} for label, fld, op in fields_and_operations}
        regroup['_id'] = {hier_field: '$_id.' + hier_field}
        regroup.update({'count': {'$sum': 1}})

    if hier_aware:
        group.update({'count': {'$sum': 1}})
        pipeline = [{'$match': match},
                    {'$project': project},
                    {'$group': group}]
    else:
        project.update({'RPM_ID': 1, hier_field: 1})
        group['_id']['RPM_ID'] = '$RPM_ID'
        pipeline = [{'$match': match},
                    {'$project': project},
                    {'$unwind': "$" + hier_field},
                    {'$match': match2},
                    {'$group': group},
                    {'$unwind': "$" + hier_field},
                    {'$group': regroup}]

    if config.debug:
        logger.info('fetch_deal_rollup filter_name: %s, pipeline: %s', filter_name, pipeline)

    aggs = list(deals_collection.aggregate(pipeline, allowDiskUse=True))

    for node in nodes:
        aggregated_val = {}
        for res in aggs:
            val = {}
            segment = None
            drilldown = None
            if res['_id']:
                segment = res['_id'].get("segment", None)
                drilldown = res['_id'].get(hier_field, None)
            if not hier_aware:
                if node != drilldown:
                    continue
            for k, v in res.items():
                if k != '_id':
                    if hier_aware:
                        if k.endswith(node.replace('.', '$')):
                            k = k.split(node.replace('.', '$'))[0]
                        else:
                            continue
                    val[k] = v
                    if k not in aggregated_val:
                        aggregated_val[k] = v
                    else:
                        aggregated_val[k] += v
                if segment and return_seg_info:
                    cache[(filter_name, segment, node)] = val
                else:
                    cache[(filter_name, node)] = val
        if aggregated_val:
            if return_seg_info:
                cache[(filter_name, 'all_deals', node)] = aggregated_val
            else:
                cache[(filter_name, node)] = aggregated_val
        else:
            val = {label: 0 for label, _, _ in fields_and_operations}
            val['count'] = 0
            cache[(filter_name, node)] = val
    return cache

def fetch_deal_rollup_dlf_using_df(period_and_close_periods,
                                   nodes,
                                   fields_and_operations,
                                   config,
                                   user=None,
                                   filter_criteria=None,
                                   filter_name=None,
                                   favorites=None,
                                   timestamp=None,
                                   db=None,
                                   cache=None,
                                   return_seg_info=False,
                                   filters=[],
                                   root_node=None,
                                   use_dlf_fcst_coll=False,
                                   is_pivot_special=False,
                                   deals_coll_name=None,
                                   cumulative_fields=[],
                                   cumulative_close_periods=[],
                                   prev_periods=[],
                                   actual_view=False
                                   ):
    if is_pivot_special:
        return fetch_crr_deal_rollup(period_and_close_periods,
                                     nodes,
                                     fields_and_operations,
                                     config,
                                     user=user,
                                     filter_criteria=filter_criteria,
                                     filter_name=filter_name,
                                     favorites=favorites,
                                     timestamp=timestamp,
                                     db=db,
                                     cache=cache,
                                     return_seg_info=return_seg_info,
                                     filters=filters,
                                     root_node=root_node,
                                     use_dlf_fcst_coll=use_dlf_fcst_coll)

    quarter_collection = db[QUARTER_COLL] if db else sec_context.tenant_db[QUARTER_COLL]
    prev_periods = _prev_periods_fallback(config, prev_periods)
    if not isinstance(period_and_close_periods, list):
        period_and_close_periods = [period_and_close_periods]
    period_and_close_periods_map = {"old_deals_coll": [], "new_deals_coll": []}
    periods = []
    deals_collection_map = {}
    if deals_coll_name:
        deals_collection = db[deals_coll_name] if db else sec_context.tenant_db[deals_coll_name]
        deals_collection_map['new_deals_coll'] = deals_collection
    else:
        for period_and_close_period_for_db in period_and_close_periods:
            period, _ = period_and_close_period_for_db
            periods.append(period)
            if ((period in prev_periods and config.config.get('update_new_collection')) or
                    (isinstance(period, list) and not set(period).isdisjoint(prev_periods) and
                     config.config.get('update_new_collection')) or
                    period is None):
                period_and_close_periods_map['new_deals_coll'].append(period_and_close_period_for_db)
                deals_collection = db[NEW_DEALS_COLL] if db else \
                    sec_context.tenant_db[NEW_DEALS_COLL]
                deals_collection_map['new_deals_coll'] = deals_collection
                collection_name = NEW_DEALS_COLL
            else:
                period_and_close_periods_map['old_deals_coll'].append(period_and_close_period_for_db)
                deals_collection = db[DEALS_COLL] if db else sec_context.tenant_db[DEALS_COLL]
                deals_collection_map['old_deals_coll'] = deals_collection
                collection_name = DEALS_COLL

    moved_deals_in_actual_view = False
    if actual_view:
        objectids = []
        #moveddeals = list(quarter_collection.find({'period': periods}))
        moveddeals = list(quarter_collection.find({'period': {"$in" : periods}}))
        if len(moveddeals) > 0: objectids = list(map(ObjectId, moveddeals[0]['ObjectId']))
        if len(objectids) > 0: moved_deals_in_actual_view = True

    drilldown_nodes = []
    hierarchy_nodes = []
    hier_aware = False
    for _, fld, _ in fields_and_operations:
        if fld in config.hier_aware_fields:
            hier_aware = True
    for node in nodes:
        if '#' in node:
            drilldown_nodes.append(node)
        else:
            hierarchy_nodes.append(node)

    user = user or sec_context.login_user_name

    for hier_field in ['drilldown_list', 'hierarchy_list']:
        if timestamp:
            if hier_field == 'drilldown_list':
                nodes = drilldown_nodes
                hint_field = 'period_-1_close_period_-1_drilldown_list_-1_update_date_-1'
            else:
                nodes = hierarchy_nodes
                hint_field = 'period_-1_close_period_-1_hierarchy_list_-1_update_date_-1'
        else:
            if hier_field == 'drilldown_list':
                nodes = drilldown_nodes
                hint_field = 'period_-1_close_period_-1_drilldown_list_-1_is_deleted_-1'
            else:
                nodes = hierarchy_nodes
                hint_field = 'period_-1_close_period_-1_hierarchy_list_-1_is_deleted_-1'
        if not nodes:
            continue
        deals_collection_criteria = {}
        for period_map in period_and_close_periods_map.keys():
            if (period_map == 'old_deals_coll' and period_and_close_periods_map['old_deals_coll']) or \
                    (period_map == 'new_deals_coll' and period_and_close_periods_map['new_deals_coll']):
                pass
            else:
                continue

            period_and_close_periods = period_and_close_periods_map[period_map]
            match_list = []
            for period_and_close_period in period_and_close_periods:
                period, close_periods = period_and_close_period
                if isinstance(favorites, dict):
                    favs = favorites.get(period, None)
                    if not favs:
                        favs = _fetch_favorites(period, user, db)
                else:
                    favs = favorites or _fetch_favorites(period, user, db)

                if isinstance(timestamp, dict):
                    timestamp = timestamp.get(period, None)
                    if not timestamp:
                        timestamp = _get_timestamp(period)
                else:
                    timestamp = timestamp or _get_timestamp(period)  # BUG: cant get timestamp when threaded from fm svc

                if not moved_deals_in_actual_view:
                    match = {'period': period,
                             hier_field: {'$in': nodes}}
                else:
                    match = {hier_field: {'$in': nodes}}

                if timestamp:
                    match['update_date'] = {'$gte': timestamp}
                else:
                    match['is_deleted'] = False

                if close_periods:
                    period_key = get_key_for_close_periods(close_periods)
                    hint_field = modify_hint_field(close_periods, hint_field)
                    if not moved_deals_in_actual_view:
                        match[period_key] = {'$in': close_periods}
                    else:
                        closeperiods_value = close_periods
                matches = {}
                if filters:
                    for filt in filters:
                        op = filt['op']
                        if op != 'dlf':
                            matches.update(parse_filters(filt, config))
                    if matches:
                        match = {'$and': [match, matches]}
                match_list.append(match)
            match = {'$or': []}
            if len(match_list) > 1:
                for m in match_list:
                    match['$or'].append(m)
            else:
                match = match_list[0]

            if actual_view:
                match['is_moved'] = {'$exists': False}

            if moved_deals_in_actual_view:
                actual_view_criteria = [{period_key: {'$in': closeperiods_value}, 'period': period},
                                        {'_id': {'$in': objectids}}]
                if '$or' in match.keys():
                    existing_or = match.pop('$or')
                    match['$and'] = [{'$or': existing_or}, {'$or': actual_view_criteria}]
                elif '$and' in match.keys():
                    match['$and'].append({'$or': actual_view_criteria})
                else:
                    match['$or'] = actual_view_criteria
            deals_collection_criteria[period_map] = match

        amount_field_hierarchy = []  # Capture the hierarchy of amount field to find in the mongo result
        filter_field_project_hierarchy = []  # Capture the hierarchy of filter to use from the mongo result
        filter_field_match_hierarchy = []
        filter_field_values = []  # Capture the hierarchy of filter values to check for
        projected_seg_field = None

        group = {}
        project = {'dlf': 1}
        hier_aware = False
        for label, fld, op in fields_and_operations:
            if "%(node)s" in str(filter_criteria):
                amount_field_hierarchy.extend(fld.split('.'))  # Convert the '.' notation to a hierarchy of elements
                for node in nodes:
                    if not fld.startswith('dlf'):
                        project.update({contextualize_field(fld, config, node): 1})
                    group.update({label + node.replace('.', '$'): {op: '$' + contextualize_field(fld, config, node)}})
                hier_aware = True
            else:
                amount_field_hierarchy.extend(
                    fld.split('.'))  # Since this is not hier based, add it directly to the list
                group.update({label: {op: '$' + contextualize_field(fld, config, nodes[0])}})
                project.update({contextualize_field(fld, config, nodes[0]): 1})
        if filters:
            for filt in filters:
                op = filt['op']
                if op == 'dlf':
                    project['dlf'] = 1  # Always get the entire dlf field
                    value = filt['val']
                    filter_field_project_hierarchy.extend(['dlf', value])  # Build the hierarchy for filtering
                    # project['in_fcst_array'] = {'$objectToArray': '$dlf.' + value}

        matches = []
        for filt in filters:
            op = filt['op']
            negate = filt.get('negate', False)
            match_vals = filt.get('match_vals')
            match_vals = match_vals if match_vals else True
            mongo_op = '$in' if not negate else '$nin'
            if op == 'dlf':
                filter_field_match_hierarchy.extend(
                    ['%(node)s', 'state'])  # Update the filter hierarchy to the last depth
                if isinstance(match_vals, list):
                    filter_field_values.extend(match_vals)  # Build the filter values for  checking
                    # matches.append({"in_fcst_array.v.state": {mongo_op: match_vals}})
                else:
                    if not negate:
                        filter_field_values.append(True)  # Build the filter values for  checking
                        # matches.append({"in_fcst_array.v.state": True})
                    else:
                        filter_field_values.append(False)  # Build the filter values for  checking
                        # matches.append({"in_fcst_array.v.state": False})
            else:
                continue
        # matches.append({"in_fcst_array.k" : {"$in": nodes}})

        # match2 =  {'$and': matches}

        # group['_id'] =  {hier_field: "$in_fcst_array.k"}
        if return_seg_info:
            if config.segment_field:
                seg_field = config.segment_field
                # if group['_id']:
                #     group['_id']['segment'] = '$' + str(seg_field)
                # else:
                #     group['_id'] = '$' + str(seg_field)
                project.update({seg_field: 1})
                projected_seg_field = seg_field
            else:
                if config.debug:
                    logger.error("segment_field config not there in deal_svc config")

        project.update({'_id': 0})
        # group.update({'count': {'$sum': 1}})
        pipeline = [{'$match': match},
                    {'$project': project}]
        if period_and_close_periods_map['old_deals_coll'] and period_and_close_periods_map['new_deals_coll']:
            collection_name = NEW_DEALS_COLL + " & " + DEALS_COLL
            deals_collection = deals_collection_map['new_deals_coll']
            new_deals_match = deals_collection_criteria['new_deals_coll']
            old_deals_match = deals_collection_criteria['old_deals_coll']
            pipeline = [{'$match': new_deals_match},
                        {'$unionWith': {'coll': 'deals',
                                        'pipeline': [{'$match': old_deals_match}]}},
                        {'$project': project}]

        if config.debug:
            logger.info('fetch_deal_rollup_dlf_using_df filter_name: %s, pipeline: %s', filter_name, pipeline)

        aggs = deals_collection.aggregate(pipeline, allowDiskUse=True, hint=hint_field)

        # aggs = deals_collection.aggregate(pipeline, allowDiskUse=True)

        ## Create a data frame to unwind the data ##

        frames = []
        try:
            hier_aware_match_filter = True if filter_field_match_hierarchy.index('%(node)s') >= 0 else False
        except:
            hier_aware_match_filter = False
        for record in aggs:
            try:
                for dlf_node in get_nested(record, filter_field_project_hierarchy):
                    filter_val = None

                    if hier_aware_match_filter:
                        filter_val = get_nested_with_placeholder(record,
                                                                 filter_field_project_hierarchy + filter_field_match_hierarchy,
                                                                 {'node': dlf_node})
                    else:
                        filter_val = get_nested(record, filter_field_project_hierarchy + filter_field_match_hierarchy)

                    if filter_val in filter_field_values:
                        amount_data = get_nested_with_placeholder(record, amount_field_hierarchy, {'node': dlf_node})

                        node_frame_by_state = {
                            'amount': amount_data.get(dlf_node, 0) if isinstance(amount_data, dict) else amount_data,
                            'node': dlf_node,
                            'count': 1
                        }

                        if projected_seg_field is not None:
                            node_frame_by_state['segment'] = record[projected_seg_field]

                        frames.append(node_frame_by_state)
            except KeyError:
                pass

        # If the dataframe is empty then there is no need to calculate the totals, its always 0

        df = pd.DataFrame(frames,
                          columns=['node', 'amount', 'count'])

        # Now group the nodes by node and segment(if available)
        if projected_seg_field is not None:
            df_grouped = df.groupby(['node', 'segment']).sum().reset_index()
        else:
            df_grouped = df.groupby(['node']).sum().reset_index()

        amount_field_label = amount_field_hierarchy[0] if amount_field_hierarchy[0] != 'dlf' else \
            amount_field_hierarchy[1]

        for node in nodes:
            aggregated_val = {}
            df_by_node = df_grouped.loc[df_grouped['node'] == node]
            for index, res in df_by_node.iterrows():
                val = {}
                segment = None
                if projected_seg_field is not None:
                    segment = res['segment']

                val[amount_field_label] = res['amount']
                val['count'] = res['count']

                if amount_field_label not in aggregated_val:
                    aggregated_val[amount_field_label] = res['amount']
                    aggregated_val['count'] = res['count']
                else:
                    aggregated_val[amount_field_label] += res['amount']
                    aggregated_val['count'] += res['count']

                if segment and return_seg_info:
                    cache[(filter_name, segment, node)] = val
                else:
                    cache[(filter_name, node)] = val
            if aggregated_val:
                if return_seg_info:
                    cache[(filter_name, 'all_deals', node)] = aggregated_val
                else:
                    cache[(filter_name, node)] = aggregated_val
            else:
                val = {label: 0 for label, _, _ in fields_and_operations}
                val['count'] = 0
                cache[(filter_name, node)] = val
    return cache

def fetch_crr_deal_rollup_dlf(period_and_close_periods,
                              nodes,
                              fields_and_operations,
                              config,
                              user=None,
                              filter_criteria=None,
                              filter_name=None,
                              favorites=None,
                              timestamp=None,
                              db=None,
                              cache=None,
                              return_seg_info=False,
                              filters=[],
                              root_node=None,
                              use_dlf_fcst_coll=False
                              ):
    deals_collection = db[GBM_CRR_COLL] if db else sec_context.tenant_db[GBM_CRR_COLL]
    user = user or sec_context.login_user_name
    if not isinstance(period_and_close_periods, list):
        period_and_close_periods = [period_and_close_periods]
    hier_aware = False
    match_list = []
    match_list2 = []
    for _, fld, _ in fields_and_operations:
        if fld in config.hier_aware_fields and fld != 'forecast':
            hier_aware = True
    for period_and_close_period in period_and_close_periods:
        period, close_periods = period_and_close_period
        if isinstance(favorites, dict):
            favs = favorites.get(period, None)
            if not favs:
                favs = _fetch_favorites(period, user, db)
        else:
            favs = favorites or _fetch_favorites(period, user, db)

        if isinstance(timestamp, dict):
            timestamp = timestamp.get(period, None)
            if not timestamp:
                timestamp = _get_timestamp(period)
        else:
            timestamp = timestamp or _get_timestamp(period)  # BUG: cant get timestamp when threaded from fm svc

        hier_field = '__segs'
        hint_field = 'monthly_period_-1___segs_-1_last_modified_-1'

        match = {'period': period}
        if timestamp:
            match['last_modified'] = {'$gte': timestamp}

        if close_periods:
            match['monthly_period'] = {'$in': close_periods}

        if filter_criteria:
            if "%(node)s" in str(filter_criteria):
                matches = None
                for node in nodes:
                    if matches is None:
                        matches = [contextualize_filter(filter_criteria, node, favs, period)]
                    else:
                        matches.append(contextualize_filter(filter_criteria, node, favs, period))
                match = {'$and': [match, {'$or': matches}]}
            else:
                match = {'$and': [match, contextualize_filter(filter_criteria, nodes[0], favs, period)]}

        matches = {}
        if filters:
            for filt in filters:
                op = filt['op']
                if op != 'dlf':
                    matches.update(parse_filters(filt, config, is_pivot_special=True))
            if matches:
                match = {'$and': [match, matches]}
        match_list.append(match)
        matches = []
        for filt in filters:
            op = filt['op']
            negate = filt.get('negate', False)
            match_vals = filt.get('match_vals')
            match_vals = match_vals if match_vals else True
            mongo_op = '$in' if not negate else '$nin'
            if op == 'dlf':
                if isinstance(match_vals, list):
                    matches.append({"crr_in_fcst_array.v.state": {mongo_op: match_vals}})
                else:
                    if not negate:
                        matches.append({"crr_in_fcst_array.v.state": True})
                    else:
                        matches.append({"crr_in_fcst_array.v.state": False})
            else:
                continue
        matches.append({"crr_in_fcst_array.k": {"$in": nodes}})

        match2 = {'$and': matches}
        match_list2.append(match2)

    group = {}
    project = {}
    hier_aware = False
    con_fields_list = []
    for label, fld, op in fields_and_operations:
        if "%(node)s" in str(filter_criteria):
            for node in nodes:
                con_field = contextualize_field(fld, config, node)
                con_fields_list.append(con_field)
                project.update({con_field: 1})
                group.update({label + node.replace('.', '$'): {op: '$' + contextualize_field(fld, config, node)}})
            hier_aware = True
        else:
            group.update({label: {op: '$' + contextualize_field(fld, config, nodes[0])}})
            con_field = contextualize_field(fld, config, nodes[0])
            con_fields_list.append(con_field)
            project.update({con_field: 1})
    if filters:
        for filt in filters:
            op = filt['op']
            if op == 'dlf':
                value = filt['val']
                project['crr_in_fcst_array'] = {'$objectToArray': '$dlf.' + value}


    if not filters and con_fields_list and "crr_in_fcst_array" not in project:
        for con_fied in con_fields_list:
            con_fied_lst = con_fied.split(".")
            if con_fied_lst[0] == "dlf":
                dlf_val = con_fied_lst[1]
                project['crr_in_fcst_array'] = {'$objectToArray': '$dlf.' + dlf_val}
                break

    group['_id'] = {hier_field: "$crr_in_fcst_array.k"}

    group.update({'count': {'$sum': 1}})
    pipeline = [{'$match': match},
                {'$project': project},
                {'$unwind': "$crr_in_fcst_array"},
                {'$match': match2},
                {'$group': group}]

    if config.debug:
        logger.info('fetch_deal_rollup_dlf filter_name: %s, pipeline: %s', filter_name, pipeline)

    aggs = list(deals_collection.aggregate(pipeline, allowDiskUse=True))

    for node in nodes:
        aggregated_val = {}
        for res in aggs:
            val = {}
            segment = None
            drilldown = None
            if res['_id']:
                segment = res['_id'].get("segment", None)
                drilldown = res['_id'].get(hier_field, None)
            if node != drilldown:
                continue
            for k, v in res.items():
                if k != '_id':
                    if hier_aware:
                        if k.endswith(node.replace('.', '$')):
                            k = k.split(node.replace('.', '$'))[0]
                        else:
                            continue
                    val[k] = v
                    if k not in aggregated_val:
                        aggregated_val[k] = v
                    else:
                        aggregated_val[k] += v
                if segment and return_seg_info:
                    cache[(filter_name, segment, node)] = val
                else:
                    cache[(filter_name, node)] = val
        if aggregated_val:
            if return_seg_info:
                cache[(filter_name, 'all_deals', node)] = aggregated_val
            else:
                cache[(filter_name, node)] = aggregated_val
        else:
            val = {label: 0 for label, _, _ in fields_and_operations}
            val['count'] = 0
            cache[(filter_name, node)] = val
    return cache

def fetch_deal_rollup_dlf(period_and_close_periods,
                          nodes,
                          fields_and_operations,
                          config,
                          user=None,
                          filter_criteria=None,
                          filter_name=None,
                          favorites=None,
                          timestamp=None,
                          db=None,
                          cache=None,
                          return_seg_info=False,
                          filters=[],
                          root_node=None,
                          use_dlf_fcst_coll=False,
                          is_pivot_special=False,
                          deals_coll_name=None,
                          cumulative_fields=[],
                          cumulative_close_periods=[],
                          prev_periods=[],
                          actual_view=False
                          ):
    if is_pivot_special:
        return fetch_crr_deal_rollup_dlf(period_and_close_periods,
                                         nodes,
                                         fields_and_operations,
                                         config,
                                         user=user,
                                         filter_criteria=filter_criteria,
                                         filter_name=filter_name,
                                         favorites=favorites,
                                         timestamp=timestamp,
                                         db=db,
                                         cache=cache,
                                         return_seg_info=return_seg_info,
                                         filters=filters,
                                         root_node=root_node,
                                         use_dlf_fcst_coll=use_dlf_fcst_coll)
    quarter_collection = db[QUARTER_COLL] if db else sec_context.tenant_db[QUARTER_COLL]
    prev_periods = _prev_periods_fallback(config, prev_periods)
    if not isinstance(period_and_close_periods, list):
        period_and_close_periods = [period_and_close_periods]
    period_and_close_periods_map = {"old_deals_coll": [], "new_deals_coll": []}
    periods = []
    deals_collection_map = {}

    for period_and_close_period_for_db in period_and_close_periods:
        period, _ = period_and_close_period_for_db
        periods.append(period)
        if ((period in prev_periods and config.config.get('update_new_collection')) or
                (isinstance(period, list) and not set(period).isdisjoint(prev_periods) and
                 config.config.get('update_new_collection')) or
                period is None):
            period_and_close_periods_map['new_deals_coll'].append(period_and_close_period_for_db)
            deals_collection = db[NEW_DEALS_COLL] if db else \
                sec_context.tenant_db[NEW_DEALS_COLL]
            deals_collection_map['new_deals_coll'] = deals_collection
            collection_name = NEW_DEALS_COLL
        else:
            period_and_close_periods_map['old_deals_coll'].append(period_and_close_period_for_db)
            deals_collection = db[DEALS_COLL] if db else sec_context.tenant_db[DEALS_COLL]
            deals_collection_map['old_deals_coll'] = deals_collection
            collection_name = DEALS_COLL

    moved_deals_in_actual_view = False
    if actual_view:
        objectids = []
        #moveddeals = list(quarter_collection.find({'period': periods}))
        moveddeals = list(quarter_collection.find({'period': {"$in" : periods}}))
        if len(moveddeals) > 0: objectids = list(map(ObjectId, moveddeals[0]['ObjectId']))
        if len(objectids) > 0: moved_deals_in_actual_view = True

    drilldown_nodes = []
    hierarchy_nodes = []
    hier_aware = False
    for _, fld, _ in fields_and_operations:
        if fld in config.hier_aware_fields:
            hier_aware = True
    for node in nodes:
        if '#' in node:
            drilldown_nodes.append(node)
        else:
            hierarchy_nodes.append(node)

    user = user or sec_context.login_user_name

    for hier_field in ['drilldown_list', 'hierarchy_list']:
        if hier_field == 'drilldown_list':
            nodes = drilldown_nodes
            hint_field = 'period_-1_close_period_-1_drilldown_list_-1_update_date_-1'
        else:
            nodes = hierarchy_nodes
            hint_field = 'period_-1_close_period_-1_hierarchy_list_-1_update_date_-1'
        if not nodes:
            continue
        deals_collection_criteria = {}
        match_list2 = []
        for period_map in period_and_close_periods_map.keys():
            if (period_map == 'old_deals_coll' and period_and_close_periods_map['old_deals_coll']) or \
                    (period_map == 'new_deals_coll' and period_and_close_periods_map['new_deals_coll']):
                pass
            else:
                continue

            period_and_close_periods = period_and_close_periods_map[period_map]
            match_list = []
            for period_and_close_period in period_and_close_periods:
                period, close_periods = period_and_close_period
                if isinstance(favorites, dict):
                    favs = favorites.get(period, None)
                    if not favs:
                        favs = _fetch_favorites(period, user, db)
                else:
                    favs = favorites or _fetch_favorites(period, user, db)

                if isinstance(timestamp, dict):
                    timestamp = timestamp.get(period, None)
                    if not timestamp:
                        timestamp = _get_timestamp(period)
                else:
                    timestamp = timestamp or _get_timestamp(period)  # BUG: cant get timestamp when threaded from fm svc
                if not moved_deals_in_actual_view:
                    match = {'period': period}
                else:
                    match = {}

                if timestamp:
                    match['update_date'] = {'$gte': timestamp}
                else:
                    match['is_deleted'] = False
                    hint_field = hint_field.replace('update_date', 'is_deleted')

                if close_periods:
                    period_key = get_key_for_close_periods(close_periods)
                    hint_field = modify_hint_field(close_periods, hint_field)
                    if not moved_deals_in_actual_view:
                        match[period_key] = {'$in': close_periods}
                    else:
                        closeperiods_value = close_periods

                if filter_criteria:
                    if "%(node)s" in str(filter_criteria):
                        matches = None
                        for node in nodes:
                            if matches is None:
                                matches = [contextualize_filter(filter_criteria, node, favs, period)]
                            else:
                                matches.append(contextualize_filter(filter_criteria, node, favs, period))
                        match = {'$and': [match, {'$or': matches}]}
                    else:
                        match = {'$and': [match, contextualize_filter(filter_criteria, nodes[0], favs, period)]}

                matches = {}
                if filters:
                    for filt in filters:
                        op = filt['op']
                        if op != 'dlf':
                            matches.update(parse_filters(filt, config))
                    if matches:
                        match = {'$and': [match, matches]}
                match_list.append(match)
                matches = []
                for filt in filters:
                    op = filt['op']
                    negate = filt.get('negate', False)
                    match_vals = filt.get('match_vals')
                    match_vals = match_vals if match_vals else True
                    mongo_op = '$in' if not negate else '$nin'
                    if op == 'dlf':
                        if isinstance(match_vals, list):
                            matches.append({"in_fcst_array.v.state": {mongo_op: match_vals}})
                        else:
                            if not negate:
                                matches.append({"in_fcst_array.v.state": True})
                            else:
                                matches.append({"in_fcst_array.v.state": False})
                    else:
                        continue
                if not filters and con_fields_list and "in_fcst_array" not in project:
                    for con_fied in con_fields_list:
                        con_fied_lst = con_fied.split(".")
                        if con_fied_lst[0] == "dlf":
                            dlf_val = con_fied_lst[1]
                            project['in_fcst_array'] = {'$objectToArray': '$dlf.' + dlf_val}
                            break
                matches.append({"in_fcst_array.k": {"$in": nodes}})

                match2 = {'$and': matches}
                match_list2.append(match2)
            match = {'$or': []}
            if len(match_list) > 1:
                for m in match_list:
                    match['$or'].append(m)
            else:
                match = match_list[0]

            if actual_view:
                match['is_moved'] = {'$exists': False}

            if moved_deals_in_actual_view:
                actual_view_criteria = [{period_key: {'$in': closeperiods_value}, 'period': period},
                                        {'_id': {'$in': objectids}}]
                if '$or' in match.keys():
                    existing_or = match.pop('$or')
                    match['$and'] = [{'$or': existing_or}, {'$or': actual_view_criteria}]
                elif '$and' in match.keys():
                    match['$and'].append({'$or': actual_view_criteria})
                else:
                    match['$or'] = actual_view_criteria
            deals_collection_criteria[period_map] = match

        match2 = {'$or': []}
        if len(match_list2) > 1:
            for m in match_list2:
                match2['$or'].append(m)
        else:
            if match_list2:
                match2 = match_list2[0]

        group = {}
        project = {}
        hier_aware = False
        con_fields_list = []
        for label, fld, op in fields_and_operations:
            if "%(node)s" in str(filter_criteria):
                for node in nodes:
                    con_field = contextualize_field(fld, config, node)
                    con_fields_list.append(con_field)
                    project.update({con_field: 1})
                    group.update({label + node.replace('.', '$'): {op: '$' + contextualize_field(fld, config, node)}})
                hier_aware = True
            else:
                group.update({label: {op: '$' + contextualize_field(fld, config, nodes[0])}})
                con_field = contextualize_field(fld, config, nodes[0])
                con_fields_list.append(con_field)
                project.update({con_field: 1})
        if filters:
            for filt in filters:
                op = filt['op']
                if op == 'dlf':
                    value = filt['val']
                    project['in_fcst_array'] = {'$objectToArray': '$dlf.' + value}

        group['_id'] = {hier_field: "$in_fcst_array.k"}
        if return_seg_info:
            if config.segment_field:
                seg_field = config.segment_field
                if group['_id']:
                    group['_id']['segment'] = '$' + str(seg_field)
                else:
                    group['_id'] = '$' + str(seg_field)
                project.update({seg_field: 1})
            else:
                if config.debug:
                    logger.error("segment_field config not there in deal_svc config")

        group.update({'count': {'$sum': 1}})
        pipeline = [{'$match': match},
                    {'$project': project},
                    {'$unwind': "$in_fcst_array"},
                    {'$match': match2},
                    {'$group': group}]

        if period_and_close_periods_map['old_deals_coll'] and period_and_close_periods_map['new_deals_coll']:
            collection_name = NEW_DEALS_COLL + " & " + DEALS_COLL
            deals_collection = deals_collection_map['new_deals_coll']
            new_deals_match = deals_collection_criteria['new_deals_coll']
            old_deals_match = deals_collection_criteria['old_deals_coll']
            pipeline = [{'$match': new_deals_match},
                        {'$unionWith': {'coll': 'deals',
                                        'pipeline': [{'$match': old_deals_match}]}},
                        {'$project': project},
                        {'$unwind': "$in_fcst_array"},
                        {'$match': match2},
                        {'$group': group}]


        if config.debug:
            logger.info('fetch_deal_rollup_dlf filter_name: %s, pipeline: %s', filter_name, pipeline)

        aggs = list(deals_collection.aggregate(pipeline, allowDiskUse=True, hint=hint_field))

        for node in nodes:
            aggregated_val = {}
            for res in aggs:
                val = {}
                segment = None
                drilldown = None
                if res['_id']:
                    segment = res['_id'].get("segment", None)
                    drilldown = res['_id'].get(hier_field, None)
                if node != drilldown:
                    continue
                for k, v in res.items():
                    if k != '_id':
                        if hier_aware:
                            if k.endswith(node.replace('.', '$')):
                                k = k.split(node.replace('.', '$'))[0]
                            else:
                                continue
                        val[k] = v
                        if k not in aggregated_val:
                            aggregated_val[k] = v
                        else:
                            aggregated_val[k] += v
                    if segment and return_seg_info:
                        cache[(filter_name, segment, node)] = val
                    else:
                        cache[(filter_name, node)] = val
            if aggregated_val:
                if return_seg_info:
                    cache[(filter_name, 'all_deals', node)] = aggregated_val
                else:
                    cache[(filter_name, node)] = aggregated_val
            else:
                val = {label: 0 for label, _, _ in fields_and_operations}
                val['count'] = 0
                cache[(filter_name, node)] = val
    return cache

def fetch_deal_rollup(period_and_close_periods,
                      nodes,
                      fields_and_operations,
                      config,
                      user=None,
                      filter_criteria=None,
                      filter_name=None,
                      favorites=None,
                      timestamp=None,
                      db=None,
                      cache=None,
                      return_seg_info=False,
                      filters=[],
                      root_node=None,
                      use_dlf_fcst_coll=False,
                      is_pivot_special=False,
                      deals_coll_name=None,
                      cumulative_fields=[],
                      cumulative_close_periods=[],
                      prev_periods=[],
                      actual_view=False
                      ):
    if is_pivot_special:
        return fetch_crr_deal_rollup(period_and_close_periods,
                                     nodes,
                                     fields_and_operations,
                                     config,
                                     user=user,
                                     filter_criteria=filter_criteria,
                                     filter_name=filter_name,
                                     favorites=favorites,
                                     timestamp=timestamp,
                                     db=db,
                                     cache=cache,
                                     return_seg_info=return_seg_info,
                                     filters=filters,
                                     root_node=root_node,
                                     use_dlf_fcst_coll=use_dlf_fcst_coll)
    quarter_collection = db[QUARTER_COLL] if db else sec_context.tenant_db[QUARTER_COLL]
    prev_periods = _prev_periods_fallback(config, prev_periods)
    if not isinstance(period_and_close_periods, list):
        period_and_close_periods = [period_and_close_periods]
    period_and_close_periods_map = {"old_deals_coll": [], "new_deals_coll": []}
    periods = []
    deals_collection_map = {}

    for period_and_close_period_for_db in period_and_close_periods:
        period, _ = period_and_close_period_for_db
        periods.append(period)
        if ((period in prev_periods and config.config.get('update_new_collection')) or
                (isinstance(period, list) and not set(period).isdisjoint(prev_periods) and
                 config.config.get('update_new_collection')) or
                period is None):
            period_and_close_periods_map['new_deals_coll'].append(period_and_close_period_for_db)
            deals_collection = db[NEW_DEALS_COLL] if db else \
                sec_context.tenant_db[NEW_DEALS_COLL]
            deals_collection_map['new_deals_coll'] = deals_collection
            collection_name = NEW_DEALS_COLL
        else:
            period_and_close_periods_map['old_deals_coll'].append(period_and_close_period_for_db)
            deals_collection = db[DEALS_COLL] if db else sec_context.tenant_db[DEALS_COLL]
            deals_collection_map['old_deals_coll'] = deals_collection
            collection_name = DEALS_COLL

    moved_deals_in_actual_view = False
    if actual_view:
        objectids = []
        moveddeals = list(quarter_collection.find({'period': {"$in" : periods}}))
        if len(moveddeals) > 0: objectids = list(map(ObjectId, moveddeals[0]['ObjectId']))
        if len(objectids) > 0: moved_deals_in_actual_view = True

    drilldown_nodes = []
    hierarchy_nodes = []
    hier_aware = False
    for _, fld, _ in fields_and_operations:
        if fld in config.hier_aware_fields:
            hier_aware = True
    for node in nodes:
        if '#' in node:
            drilldown_nodes.append(node)
        else:
            hierarchy_nodes.append(node)

    user = user or sec_context.login_user_name

    for hier_field in ['drilldown_list', 'hierarchy_list']:
        if hier_field == 'drilldown_list':
            nodes = drilldown_nodes
            hint_field = 'period_-1_close_period_-1_drilldown_list_-1_update_date_-1'
        else:
            nodes = hierarchy_nodes
            hint_field = 'period_-1_close_period_-1_hierarchy_list_-1_update_date_-1'
        if not nodes:
            continue
        deals_collection_criteria = {}
        match_list2 = []
        for period_map in period_and_close_periods_map.keys():
            match_list = []
            if (period_map == 'old_deals_coll' and period_and_close_periods_map['old_deals_coll']) or \
                    (period_map == 'new_deals_coll' and period_and_close_periods_map['new_deals_coll']):
                pass
            else:
                continue

            period_and_close_periods = period_and_close_periods_map[period_map]
            match_list = []
            for period_and_close_period in period_and_close_periods:
                period, close_periods = period_and_close_period
                if isinstance(favorites, dict):
                    favs = favorites.get(period, None)
                    if not favs:
                        favs = _fetch_favorites(period, user, db)
                else:
                    favs = favorites or _fetch_favorites(period, user, db)

                if isinstance(timestamp, dict):
                    timestamp = timestamp.get(period, None)
                    if not timestamp:
                        timestamp = _get_timestamp(period)
                else:
                    timestamp = timestamp or _get_timestamp(period)  # BUG: cant get timestamp when threaded from fm svc

                if not moved_deals_in_actual_view:
                    match = {'period': period,
                             hier_field: {'$in': nodes}}
                else:
                    match = {hier_field: {'$in': nodes}}
                if timestamp:
                    match['update_date'] = {'$gte': timestamp}
                else:
                    match['is_deleted'] = False
                if close_periods:
                    if filter_name in cumulative_fields:
                        close_periods = cumulative_close_periods
                    period_key = get_key_for_close_periods(close_periods)
                    hint_field = modify_hint_field(close_periods, hint_field)
                    if not moved_deals_in_actual_view:
                        match[period_key] = {'$in': close_periods}
                    else:
                        closeperiods_value = close_periods

                if filter_criteria:
                    if "%(node)s" in str(filter_criteria):
                        matches = None
                        for node in nodes:
                            if matches is None:
                                matches = [contextualize_filter(filter_criteria, node, favs, period)]
                            else:
                                matches.append(contextualize_filter(filter_criteria, node, favs, period))
                        match = {'$and': [match, {'$or': matches}]}
                    else:
                        match = {'$and': [match, contextualize_filter(filter_criteria, nodes[0], favs, period)]}

                match_list.append(match)
                if not hier_aware:
                    match2 = {hier_field: {'$in': nodes}}
                    match_list2.append(match2)
            match = {'$or': []}
            if len(match_list) > 1:
                for m in match_list:
                    match['$or'].append(m)
            else:
                match = match_list[0]

            if actual_view:
                match['is_moved'] = {'$exists': False}

            if moved_deals_in_actual_view:
                actual_view_criteria = [{period_key: {'$in': closeperiods_value}, 'period': period},
                                        {'_id': {'$in': objectids}}]
                if '$or' in match.keys():
                    existing_or = match.pop('$or')
                    match['$and'] = [{'$or': existing_or}, {'$or': actual_view_criteria}]
                elif '$and' in match.keys():
                    match['$and'].append({'$or': actual_view_criteria})
                else:
                    match['$or'] = actual_view_criteria
            deals_collection_criteria[period_map] = match

        if not hier_aware:
            match2 = {'$or': []}
            if len(match_list2) > 1:
                for m in match_list2:
                    match2['$or'].append(m)
            else:
                match2 = match_list2[0]
        group = {}
        regroup = {}
        if hier_aware:
            project = {contextualize_field(fld, config, node): 1 for node in nodes for _, fld, _ in
                       fields_and_operations}
            group['_id'] = None
            group.update(
                {label + node.replace('.', '$'): {op: '$' + contextualize_field(fld, config, node)} for node in nodes
                 for label, fld, op in fields_and_operations})
        else:
            if len(nodes) == 1 or root_node is None:
                root_node = nodes[0]
            project = {contextualize_field(fld, config, root_node): 1 for _, fld, _ in fields_and_operations}

            group['_id'] = {label: '$' + contextualize_field(fld, config, root_node)
                            for label, fld, op in fields_and_operations}
            group['_id'][hier_field] = '$' + hier_field
            group.update({hier_field: {'$addToSet': '$' + hier_field}})
            regroup = {label: {op: '$_id.' + label} for label, fld, op in fields_and_operations}
            regroup['_id'] = {hier_field: '$_id.' + hier_field}
            regroup.update({'count': {'$sum': 1}})

        if return_seg_info:
            if config.segment_field:
                seg_field = config.segment_field
                if group['_id']:
                    group['_id']['segment'] = '$' + str(seg_field)
                else:
                    group['_id'] = {'segment': '$' + str(seg_field)}
                if regroup.get('_id', None):
                    regroup['_id']['segment'] = '$_id.segment'
                project.update({seg_field: 1})
            else:
                if config.debug:
                    logger.error("segment_field config not there in deal_svc config")

        if hier_aware:
            group.update({'count': {'$sum': 1}})
            pipeline = [{'$match': match},
                        {'$project': project},
                        {'$group': group}]
        else:
            project.update({'opp_id': 1, hier_field: 1})
            group['_id']['opp_id'] = '$opp_id'
            pipeline = [{'$match': match},
                        {'$project': project},
                        {'$unwind': "$" + hier_field},
                        {'$match': match2},
                        {'$group': group},
                        {'$unwind': "$" + hier_field},
                        {'$group': regroup}]
        if period_and_close_periods_map['old_deals_coll'] and period_and_close_periods_map['new_deals_coll']:
            collection_name = NEW_DEALS_COLL + " & " + DEALS_COLL
            deals_collection = deals_collection_map['new_deals_coll']
            new_deals_match = deals_collection_criteria['new_deals_coll']
            old_deals_match = deals_collection_criteria['old_deals_coll']
            if hier_aware:
                group.update({'count': {'$sum': 1}})
                pipeline = [{'$match': new_deals_match},
                            {'$unionWith': {'coll': 'deals',
                                            'pipeline': [{'$match': old_deals_match}]}},
                            {'$project': project},
                            {'$group': group}]
            else:
                project.update({'opp_id': 1, hier_field: 1})
                group['_id']['opp_id'] = '$opp_id'
                pipeline = [{'$match': new_deals_match},
                            {'$unionWith': {'coll': 'deals',
                                            'pipeline': [{'$match': old_deals_match}]}},
                            {'$project': project},
                            {'$unwind': "$" + hier_field},
                            {'$match': match2},
                            {'$group': group},
                            {'$unwind': "$" + hier_field},
                            {'$group': regroup}]

        if config.debug:
            logger.info('fetch_deal_rollup filter_name: %s, pipeline: %s', filter_name, pipeline)

        aggs = list(deals_collection.aggregate(pipeline, allowDiskUse=True, hint=hint_field))

        for node in nodes:
            aggregated_val = {}
            for res in aggs:
                val = {}
                segment = None
                drilldown = None
                if res['_id']:
                    segment = res['_id'].get("segment", None)
                    drilldown = res['_id'].get(hier_field, None)
                if not hier_aware:
                    if node != drilldown:
                        continue
                for k, v in res.items():
                    if k != '_id':
                        if hier_aware:
                            if k.endswith(node.replace('.', '$')):
                                k = k.split(node.replace('.', '$'))[0]
                            else:
                                continue
                        val[k] = v
                        if k not in aggregated_val:
                            aggregated_val[k] = v
                        else:
                            aggregated_val[k] += v
                    if segment and return_seg_info:
                        cache[(filter_name, segment, node)] = val
                    else:
                        cache[(filter_name, node)] = val
            if aggregated_val:
                if return_seg_info:
                    cache[(filter_name, 'all_deals', node)] = aggregated_val
                else:
                    cache[(filter_name, node)] = aggregated_val
            else:
                val = {label: 0 for label, _, _ in fields_and_operations}
                val['count'] = 0
                cache[(filter_name, node)] = val
    return cache

def fetch_many_prnt_DR_deal_totals(period_and_close_periods,
                                   nodes,
                                   fields_and_operations,
                                   config,
                                   prnt_node,
                                   user=None,
                                   filter_criteria=None,
                                   filter_name=None,
                                   favorites=None,
                                   timestamp=None,
                                   db=None,
                                   cache=None,
                                   return_seg_info=False,
                                   deals_coll_name=None,
                                   prev_periods=[]
                                   ):
    """
        fetch total amount of deals for period and prnt_node for the deals belonging to the
        node. It fetches the relevant deal as per the filter.

        Arguments:
            period_and_close_periods {tuple} -- as of mnemonic, [close mnemonics]   ('2020Q2', ['2020Q2'])
            node {str} -- hierarchy node                                            '0050000FLN2C9I2'
            fields_and_operations {list} -- tuples of label, deal field,            [('Amount', 'Amount', '$sum')]
                                            mongo op to total
            config {DealsConfig} -- instance of DealsConfig                          ...?
            prnt_node -- parent of the node. same as node if it is already the parent

        Keyword Arguments:
            user {str} -- user name, if None, will use sec_context                  'gnana'
            filter_criteria {dict} -- mongo db filter criteria (default: {None})    {'Amount': {'$gte': 100000}}
            filter_name {str} -- label to identify filter in cache
            favorites {set}  -- set of opp_ids that have been favorited by user
                                if None, queries favs collectionf to find them
                                (default: {None})
            timestamp {float} -- epoch timestamp for when deal records expire
                                 if None, checks flags to find it
                                 (default: {None})
            db {pymongo.database.Database} -- instance of tenant_db
                                              (default: {None})
                                              if None, will create one
            cache {dict} -- dict to hold records fetched by func (default: {None})
                            (used to memoize fetching many recs)

        Returns:
            dict -- 'total amount of deals
        """
    prev_periods = _prev_periods_fallback(config, prev_periods)
    deals_coll_name = deals_coll_name if deals_coll_name is not None else DEALS_COLL
    deals_collection = db[deals_coll_name] if db else sec_context.tenant_db[deals_coll_name]
    if isinstance(period_and_close_periods, list):
        for period_and_close_period in period_and_close_periods:
            period, close_periods = period_and_close_period
    else:
        period, close_periods = period_and_close_periods
    if deals_coll_name is not None and ((period in prev_periods and config.config.get('update_new_collection')) or
                                        (isinstance(period, list) and not set(period).isdisjoint(prev_periods) and
                                         config.config.get('update_new_collection')) or
                                        period is None):
        deals_collection = db[NEW_DEALS_COLL] if db else sec_context.tenant_db[NEW_DEALS_COLL]
    user = user or sec_context.login_user_name
    hint_field = 'period_-1_close_period_-1_drilldown_list_-1_update_date_-1' if '#' in nodes[0] \
        else 'period_-1_close_period_-1_hierarchy_list_-1_update_date_-1'
    match_list = []
    if isinstance(favorites, dict):
        favs = favorites.get(period, None)
        if not favs:
            favs = _fetch_favorites(period, user, db)
    else:
        favs = favorites or _fetch_favorites(period, user, db)

    if isinstance(timestamp, dict):
        timestamp = timestamp.get(period, None)
        if not timestamp:
            timestamp = _get_timestamp(period)
    else:
        timestamp = timestamp or _get_timestamp(period)  # BUG: cant get timestamp when threaded from fm svc

    hier_field = 'drilldown_list' if '#' in nodes[0] else 'hierarchy_list'

    if not isinstance(period, list):
        match = {'period': period,
                 hier_field: {'$in': nodes}}
        if timestamp:
            match['update_date'] = {'$gte': timestamp}
        else:
            match['is_deleted'] = False

        if close_periods:
            match['close_period'] = {'$in': close_periods}
    else:
        match = {'$or': []}
        for temp_num in range(len(period)):
            match1 = {'period': period[temp_num],
                      hier_field: {'$in': nodes}}
            if timestamp:
                match1['update_date'] = {'$gte': timestamp}
            else:
                match1['is_deleted'] = False

            if close_periods:
                period_key = get_key_for_close_periods(close_periods)
                hint_field = modify_hint_field(close_periods, hint_field)
                match1['close_period'] = {'$in': close_periods[temp_num]}
            match['$or'] += [match1]
    # Using the prnt_node in the filter criteria to fetch relevant deal
    if filter_criteria:
        match = {
            '$and': [match, contextualize_filter(filter_criteria, prnt_node if prnt_node else nodes[0], favs, period)]}
    match_list.append(match)

    match = {'$or': []}
    if len(match_list) > 1:
        for m in match_list:
            match['$or'].append(m)
    else:
        match = match_list[0]
    # Using the prnt_node in the for the projection
    project = {contextualize_field(fld, config, prnt_node if prnt_node else nodes[0]): 1 for _, fld, _ in
               fields_and_operations}
    # Using the prnt_node for grouping
    group = {label: {op: '$' + contextualize_field(fld, config, prnt_node if prnt_node else nodes[0])} for
             label, fld, op in
             fields_and_operations}

    group.update({'_id': {hier_field: "$" + hier_field},
                  'count': {'$sum': 1},
                  })
    project[hier_field] = 1

    if return_seg_info:
        if config.segment_field:
            seg_field = config.segment_field
            group['_id']['segment'] = '$' + str(seg_field)
            project.update({seg_field: 1})
        else:
            if config.debug:
                logger.error("segment_field config not there in deal_svc config")

    pipeline = [{'$match': match},
                {'$project': project},
                {"$unwind": "$" + hier_field},
                {'$match': {hier_field: {'$in': nodes}}},
                {'$group': group},
                {'$unwind': "$_id." + hier_field}]

    # AV-14059 Commenting below as part of the log fix,
    # config.debug is not working need to think of better approach
    # if config.debug:
    #     logger.info('fetch_many_prnt_DR_deal_totals filter_name: %s, pipeline: %s', filter_name, pipeline)

    aggs = list(deals_collection.aggregate(pipeline, allowDiskUse=True, hint=hint_field))

    aggregated_val = {}
    if return_seg_info:
        for res in aggs:
            val = {}
            for k, v in res.items():
                if k != '_id':
                    val[k] = v
                    if k not in aggregated_val:
                        aggregated_val[k] = v
                    else:
                        aggregated_val[k] += v
            node = res['_id'][hier_field]
            seg = res['_id']['segment']
            if config.segment_field:
                cache[(filter_name, seg, node)] = val
        cache[(filter_name, 'all_deals', node)] = aggregated_val
    else:
        for res in aggs:
            try:
                val = {}
                for k, v in res.items():
                    if k != '_id':
                        val[k] = v
                node = res['_id'][hier_field]
            except IndexError:
                val = {label: 0 for label, _, _ in fields_and_operations}
                val['count'] = 0
            cache[(filter_name, node)] = val
    return cache

def fetch_many_dr_from_deals(period_and_close_periods,
                             nodes,
                             filter_criteria_and_fields_and_operations,
                             config,
                             user=None,
                             db=None,
                             return_seg_info=False,
                             timestamp=None,
                             metrics=NOOPMetricSet(),
                             is_pivot_special=False,
                             prev_periods=[],
                             rollup_task=False,
                             parents_wise_nodes={},
                             deals_coll_name=None,
                             cumulative_fields=[],
                             cumulative_close_periods=[],
                             actual_view=False
                             ):
    """
    fetch a whole bunch of deal totals for many nodes and many different filters

    Arguments:
        period_and_close_periods {tuple} -- as of mnemonic, [close mnemonics]   ('2020Q2', ['2020Q2'])
        nodes {list} -- list of hierarchy nodes to get totals for               ['0050000FLN2C9I2',]
        filter_criteria_and_fields_and_operations {list} -- list of tuples of   [('big', [{'Amount': {'$gte': 100000}}],
               (filt name, [filt criteria], [(label, field, opp),])              [('Amount', 'Amount', '$sum')])]
        config {DealsConfig} -- instance of DealsConfig                          ...?

    Keyword Arguments:
        user {str} -- user name, if None, will use sec_context                  'gnana'
        db {pymongo.database.Database} -- instance of tenant_db
                                          (default: {None})
                                          if None, will create one
    Returns:
        dict -- {(filter name, node): {'count': 1, 'amount': 10}}
    """
    prev_periods = _prev_periods_fallback(config, prev_periods)
    cache = {}
    db = db or sec_context.tenant_db
    user = user or sec_context.login_user_name
    if not isinstance(period_and_close_periods, list):
        period_and_close_periods = [period_and_close_periods]

    fm_rollup_dlf_func = fetch_deal_rollup_dlf_using_df if use_df_for_dlf_rollup() else fetch_deal_rollup_dlf

    # doing some work upfront before the threading begins
    favs = {}
    timestamp_new = {}
    for period_and_close_period in period_and_close_periods:
        period, _ = period_and_close_period
        favs[period] = _fetch_favorites(period, user, db)
        timestamp_new[period] = timestamp or _get_timestamp(period)
    timestamp = timestamp_new

    from infra.read import get_period_as_of
    as_of = get_period_as_of(period)
    from infra.read import fetch_root_nodes
    root_nodes = fetch_root_nodes(as_of)
    root_nodes = [root_node['node'] for root_node in root_nodes]
    root_node = root_nodes[0]
    if is_pivot_special:
        for node in root_nodes:
            if node.split('#')[0] in config.not_deals_tenant.get('special_pivot', []):
                root_node = node
    if config.debug:
        if not rollup_task:
            for (filter_name,
                 filter_criteria,
                 fields_and_operations,
                 filters,
                 dlf_field) in filter_criteria_and_fields_and_operations:
                if dlf_field:
                    fm_rollup_dlf_func(period_and_close_periods,
                                       nodes,
                                       fields_and_operations,
                                       config,
                                       user,
                                       filter_criteria,
                                       filter_name,
                                       favs,
                                       timestamp,
                                       db,
                                       cache=cache,
                                       return_seg_info=return_seg_info,
                                       filters=filters,
                                       root_node=root_node,
                                       use_dlf_fcst_coll=use_dlf_fcst_coll_for_rollups(),
                                       is_pivot_special=is_pivot_special,
                                       deals_coll_name=deals_coll_name,
                                       cumulative_fields=cumulative_fields,
                                       cumulative_close_periods=cumulative_close_periods,
                                       prev_periods=prev_periods,
                                       actual_view=actual_view
                                       )
                else:
                    fetch_deal_rollup(period_and_close_periods,
                                      nodes,
                                      fields_and_operations,
                                      config,
                                      user,
                                      filter_criteria,
                                      filter_name,
                                      favs,
                                      timestamp,
                                      db,
                                      cache=cache,
                                      return_seg_info=return_seg_info,
                                      filters=filters,
                                      use_dlf_fcst_coll=use_dlf_fcst_coll_for_rollups(),
                                      root_node=root_node,
                                      is_pivot_special=is_pivot_special,
                                      deals_coll_name=deals_coll_name,
                                      cumulative_fields=cumulative_fields,
                                      cumulative_close_periods=cumulative_close_periods,
                                      prev_periods=prev_periods,
                                      actual_view=actual_view
                                      )
        else:
            for (filter_name,
                 filter_criteria,
                 fields_and_operations,
                 filters,
                 dlf_field,
                 dr_type) in filter_criteria_and_fields_and_operations:
                if dr_type == 'prnt_dr':
                    for prnt_node in nodes:
                        fetch_many_prnt_DR_deal_totals(period_and_close_periods,
                                                       parents_wise_nodes[prnt_node],
                                                       fields_and_operations,
                                                       config,
                                                       prnt_node,
                                                       user,
                                                       filter_criteria,
                                                       filter_name,
                                                       favs,
                                                       timestamp,
                                                       db,
                                                       cache=cache,
                                                       return_seg_info=return_seg_info,
                                                       deals_coll_name=deals_coll_name,
                                                       prev_periods=prev_periods
                                                       )
                elif dlf_field:
                    fm_rollup_dlf_func(period_and_close_periods,
                                       nodes,
                                       fields_and_operations,
                                       config,
                                       user,
                                       filter_criteria,
                                       filter_name,
                                       favs,
                                       timestamp,
                                       db,
                                       cache=cache,
                                       return_seg_info=return_seg_info,
                                       filters=filters,
                                       root_node=root_node,
                                       use_dlf_fcst_coll=use_dlf_fcst_coll_for_rollups(),
                                       is_pivot_special=is_pivot_special,
                                       prev_periods=prev_periods,
                                       deals_coll_name=deals_coll_name
                                       )
                else:
                    fetch_deal_rollup(period_and_close_periods,
                                      nodes,
                                      fields_and_operations,
                                      config,
                                      user,
                                      filter_criteria,
                                      filter_name,
                                      favs,
                                      timestamp,
                                      db,
                                      cache=cache,
                                      return_seg_info=return_seg_info,
                                      filters=filters,
                                      root_node=root_node,
                                      use_dlf_fcst_coll=use_dlf_fcst_coll_for_rollups(),
                                      is_pivot_special=is_pivot_special,
                                      deals_coll_name=deals_coll_name,
                                      cumulative_fields=cumulative_fields,
                                      cumulative_close_periods=cumulative_close_periods,
                                      prev_periods=prev_periods
                                      )
    else:
        for chunk in iter_chunks(list(filter_criteria_and_fields_and_operations), batch_size=100):
            metrics.inc_counter('fmdr_batches')
            max_threads = 0
            threads = []
            if not rollup_task:
                for (filter_name,
                     filter_criteria,
                     fields_and_operations,
                     filters,
                     dlf_field,
                     ) in chunk:
                    t = threading.Thread(target=fm_rollup_dlf_func if dlf_field else fetch_deal_rollup,
                                         args=(period_and_close_periods,
                                               nodes,
                                               fields_and_operations,
                                               config,
                                               user,
                                               filter_criteria,
                                               filter_name,
                                               favs,
                                               timestamp,
                                               db,
                                               cache,
                                               return_seg_info,
                                               filters,
                                               root_node,
                                               use_dlf_fcst_coll_for_rollups(),
                                               is_pivot_special,
                                               deals_coll_name,
                                               cumulative_fields,
                                               cumulative_close_periods,
                                               prev_periods,
                                               actual_view
                                               ))
                    threads.append(t)
                    t.start()
                    metrics.inc_counter('fmdr_threadcnt', max_threads)
            else:
                for (filter_name,
                     filter_criteria,
                     fields_and_operations,
                     filters,
                     dlf_field,
                     dr_type) in chunk:
                    if dr_type == 'prnt_dr':
                        for prnt_node in nodes:
                            t = threading.Thread(target=fetch_many_prnt_DR_deal_totals,
                                                 args=(period_and_close_periods,
                                                       parents_wise_nodes[prnt_node],
                                                       fields_and_operations,
                                                       config,
                                                       prnt_node,
                                                       user,
                                                       filter_criteria,
                                                       filter_name,
                                                       favs,
                                                       timestamp,
                                                       db,
                                                       cache,
                                                       return_seg_info,
                                                       deals_coll_name,
                                                       prev_periods
                                                       ))
                            threads.append(t)
                            t.start()
                            metrics.inc_counter('fmdr_threadcnt', max_threads)
                    else:
                        t = threading.Thread(target=fm_rollup_dlf_func if dlf_field else fetch_deal_rollup,
                                             args=(period_and_close_periods,
                                                   nodes,
                                                   fields_and_operations,
                                                   config,
                                                   user,
                                                   filter_criteria,
                                                   filter_name,
                                                   favs,
                                                   timestamp,
                                                   db,
                                                   cache,
                                                   return_seg_info,
                                                   filters,
                                                   root_node,
                                                   use_dlf_fcst_coll_for_rollups(),
                                                   is_pivot_special,
                                                   deals_coll_name,
                                                   cumulative_fields,
                                                   cumulative_close_periods,
                                                   prev_periods
                                                   ))

                        threads.append(t)
                        t.start()
                        metrics.inc_counter('fmdr_threadcnt', max_threads)
            for t in threads:
                t.join()
            if len(threads) > max_threads:
                max_threads = len(threads)
                metrics.set_counter('fmdr_maxthreads', max_threads)

    return cache

def fetch_prnt_DR_deal_totals(period_and_close_periods,
                              node,
                              fields_and_operations,
                              config,
                              prnt_node,
                              user=None,
                              filter_criteria=None,
                              filter_name=None,
                              favorites=None,
                              timestamp=None,
                              db=None,
                              cache=None,
                              return_seg_info=False,
                              dlf_node=None,
                              prev_periods=[]
                              ):
    """
        fetch total amount of deals for period and prnt_node for the deals belonging to the
        node. It fetches the relevant deal as per the filter.

        Arguments:
            period_and_close_periods {tuple} -- as of mnemonic, [close mnemonics]   ('2020Q2', ['2020Q2'])
            node {str} -- hierarchy node                                            '0050000FLN2C9I2'
            fields_and_operations {list} -- tuples of label, deal field,            [('Amount', 'Amount', '$sum')]
                                            mongo op to total
            config {DealsConfig} -- instance of DealsConfig                          ...?
            prnt_node -- parent of the node. same as node if it is already the parent

        Keyword Arguments:
            user {str} -- user name, if None, will use sec_context                  'gnana'
            filter_criteria {dict} -- mongo db filter criteria (default: {None})    {'Amount': {'$gte': 100000}}
            filter_name {str} -- label to identify filter in cache
            favorites {set}  -- set of opp_ids that have been favorited by user
                                if None, queries favs collectionf to find them
                                (default: {None})
            timestamp {float} -- epoch timestamp for when deal records expire
                                 if None, checks flags to find it
                                 (default: {None})
            db {pymongo.database.Database} -- instance of tenant_db
                                              (default: {None})
                                              if None, will create one
            cache {dict} -- dict to hold records fetched by func (default: {None})
                            (used to memoize fetching many recs)

        Returns:
            dict -- 'total amount of deals
        """
    prev_periods = _prev_periods_fallback(config, prev_periods)
    if not isinstance(period_and_close_periods, list):
        period_and_close_periods = [period_and_close_periods]
    for period_and_close_period_for_db in period_and_close_periods:
        period, _ = period_and_close_period_for_db
        if (period in prev_periods and config.config.get('update_new_collection')) or (
                isinstance(period, list) and not set(period).isdisjoint(prev_periods) and config.config.get(
            'update_new_collection')) or period is None:
            deals_collection = db[NEW_DEALS_COLL] if db else sec_context.tenant_db[NEW_DEALS_COLL]
        else:
            deals_collection = db[DEALS_COLL] if db else sec_context.tenant_db[DEALS_COLL]
            break
    user = user or sec_context.login_user_name
    hint_field = 'period_-1_close_period_-1_drilldown_list_-1_update_date_-1' if '#' in node \
        else 'period_-1_close_period_-1_hierarchy_list_-1_update_date_-1'

    match_list = []
    for period_and_close_period in period_and_close_periods:
        period, close_periods = period_and_close_period
        if isinstance(favorites, dict):
            favs = favorites.get(period, None)
            if not favs:
                favs = _fetch_favorites(period, user, db)
        else:
            favs = favorites or _fetch_favorites(period, user, db)

        if isinstance(timestamp, dict):
            timestamp = timestamp.get(period, None)
            if not timestamp:
                timestamp = _get_timestamp(period)
        else:
            timestamp = timestamp or _get_timestamp(period)  # BUG: cant get timestamp when threaded from fm svc

        hier_field = 'drilldown_list' if '#' in node else 'hierarchy_list'

        if not isinstance(period, list):
            match = {'period': period,
                     hier_field: {'$in': [node]}}
            if timestamp:
                match['update_date'] = {'$gte': timestamp}
            else:
                match['is_deleted'] = False

            if close_periods:
                period_key = get_key_for_close_periods(close_periods)
                hint_field = modify_hint_field(close_periods, hint_field)
                match[period_key] = {'$in': close_periods}
        else:
            match = {'$or': []}
            for temp_num in range(len(period)):
                match1 = {'period': period[temp_num],
                          hier_field: {'$in': [node]}}
                if timestamp:
                    match1['update_date'] = {'$gte': timestamp}
                else:
                    match1['is_deleted'] = False

                if close_periods:
                    period_key = get_key_for_close_periods(close_periods)
                    match1[period_key] = {'$in': close_periods[temp_num]}
                match['$or'] += [match1]
        # Using the prnt_node in the filter criteria to fetch relevant deal
        if filter_criteria:
            match = {
                '$and': [match, contextualize_filter(filter_criteria, prnt_node if prnt_node else node, favs, period)]}
        match_list.append(match)

    match = {'$or': []}
    if len(match_list) > 1:
        for m in match_list:
            match['$or'].append(m)
    else:
        match = match_list[0]
    # Using the prnt_node in the for the projection
    project = {contextualize_field(fld, config, prnt_node if prnt_node else node): 1 for _, fld, _ in
               fields_and_operations}
    # Using the prnt_node for grouping
    group = {label: {op: '$' + contextualize_field(fld, config, prnt_node if prnt_node else node)} for label, fld, op in
             fields_and_operations}

    group.update({'_id': None,
                  'count': {'$sum': 1},
                  })

    pipeline = [{'$match': match},
                {'$project': project},
                {'$group': group}]

    if return_seg_info:
        if config.segment_field:
            seg_field = config.segment_field
            group['_id'] = '$' + str(seg_field)
            project.update({seg_field: 1})
        else:
            if config.debug:
                logger.error("segment_field config not there in deal_svc config")

    if config.debug:
        logger.info('fetch_prnt_DR_deal_totals filter_name: %s, pipeline: %s', filter_name, pipeline)

    aggs = list(deals_collection.aggregate(pipeline, allowDiskUse=True, hint=hint_field))

    aggregated_val = {}
    if return_seg_info:
        for res in aggs:
            val = {}
            for k, v in res.items():
                if k != '_id':
                    val[k] = v
                    if k not in aggregated_val:
                        aggregated_val[k] = v
                    else:
                        aggregated_val[k] += v
            if config.segment_field:
                cache[(filter_name, res['_id'], node)] = val
        cache[(filter_name, 'all_deals', node)] = aggregated_val
        return cache
    else:
        try:
            val = {k: v for k, v in aggs[0].items() if k != '_id'}
        except IndexError:
            val = {label: 0 for label, _, _ in fields_and_operations}
            val['count'] = 0

        if cache is not None:
            cache[(filter_name, node)] = val

        return val

def fetch_crr_deal_totals(period_and_close_periods,
                          node,
                          fields_and_operations,
                          config,
                          user=None,
                          filter_criteria=None,
                          filter_name=None,
                          favorites=None,
                          timestamp=None,
                          db=None,
                          cache=None,
                          return_seg_info=False,
                          dlf_node=None,
                          search_criteria=None,
                          criteria=None,
                          sort_fields=[],
                          custom_limit=None
                          ):
    """
    fetch total and count of deals for period and node

    Arguments:
        period_and_close_periods {tuple} -- as of mnemonic, [close mnemonics]   ('2020Q2', ['2020Q2'])
        node {str} -- hierarchy node                                            '0050000FLN2C9I2'
        fields_and_operations {list} -- tuples of label, deal field,            [('Amount', 'Amount', '$sum')]
                                        mongo op to total
        config {DealsConfig} -- instance of DealsConfig                          ...?

    Keyword Arguments:
        user {str} -- user name, if None, will use sec_context                  'gnana'
        filter_criteria {dict} -- mongo db filter criteria (default: {None})    {'Amount': {'$gte': 100000}}
        filter_name {str} -- label to identify filter in cache
        favorites {set}  -- set of opp_ids that have been favorited by user
                            if None, queries favs collectionf to find them
                            (default: {None})
        timestamp {float} -- epoch timestamp for when deal records expire
                             if None, checks flags to find it
                             (default: {None})
        db {pymongo.database.Database} -- instance of tenant_db
                                          (default: {None})
                                          if None, will create one
        cache {dict} -- dict to hold records fetched by func (default: {None})
                        (used to memoize fetching many recs)

    Returns:
        dict -- 'count' key and total for each total field
    """
    deals_collection = db[GBM_CRR_COLL] if db else sec_context.tenant_db[GBM_CRR_COLL]
    user = user or sec_context.login_user_name
    match_list = []
    if period_and_close_periods:
        if not isinstance(period_and_close_periods, list):
            period_and_close_periods = [period_and_close_periods]
        for period_and_close_period in period_and_close_periods:
            period, close_periods = period_and_close_period
            if isinstance(favorites, dict):
                favs = favorites.get(period, None)
                if not favs:
                    favs = _fetch_favorites(period, user, db)
            else:
                favs = favorites or _fetch_favorites(period, user, db)

            hier_field = '__segs'
            if not isinstance(period, list):
                match = {hier_field: {'$in': [node]}}
                if criteria:
                    match.update(criteria)

                if close_periods:
                    match['monthly_period'] = {'$in': close_periods}
            else:
                match = {'$or': []}
                for temp_num in range(len(period)):
                    match1 = {hier_field: {'$in': [node]}}
                    if criteria:
                        match1.update(criteria)

                    if close_periods:
                        match1['monthly_period'] = {'$in': close_periods[temp_num]}
                    match['$or'] += [match1]

            if filter_criteria:
                match = {'$and': [match,
                                  contextualize_filter(filter_criteria, dlf_node if dlf_node else node, favs, period)]}

            if search_criteria:
                search_terms, search_fields = search_criteria
                match['$or'] = [{field: {'$regex': '|'.join(search_terms), '$options': 'i'}} for field in search_fields]

            match_list.append(match)
    else:
        period = {}
        favs = favorites
        hier_field = '__segs'
        match = {hier_field: {'$in': [node]}}
        if criteria:
            match.update(criteria)
        if filter_criteria:
            match = {
                '$and': [match, contextualize_filter(filter_criteria, dlf_node if dlf_node else node, favs, period)]}
        if search_criteria:
            search_terms, search_fields = search_criteria
            match['$or'] = [{field: {'$regex': '|'.join(search_terms), '$options': 'i'}} for field in search_fields]
        match_list.append(match)

    match = {'$or': []}
    if len(match_list) > 1:
        for m in match_list:
            match['$or'].append(m)
    else:
        match = match_list[0]

    _node = dlf_node if dlf_node else node
    project = {contextualize_field(fld, config, _node,  hier_aware='forecast' not in fld): 1
               for _, fld, _ in fields_and_operations}
    project['update_date'] = 1

    group = {label: {op: '$' + contextualize_field(fld, config, _node, hier_aware='forecast' not in fld)}
             for label, fld, op in fields_and_operations}

    group.update({'_id': None,
                  'count': {'$sum': 1},
                  'timestamp': {'$last': '$update_date'}
                  })

    pipeline = [{'$match': match},
                {'$project': project},
                {'$group': group}]
    if custom_limit:
        if sort_fields:
            pipeline = [{'$match': match},
                        {'$project': project},
                        {'$sort': OrderedDict(
                            [(contextualize_field(field, config, node), direction) for
                             field, direction in sort_fields])},
                        {'$limit': custom_limit},
                        {'$group': group}]
        else:
            pipeline = [{'$match': match},
                        {'$project': project},
                        {'$limit': custom_limit},
                        {'$group': group}]
    if return_seg_info:
        if config.segment_field:
            seg_field = config.segment_field
            group['_id'] = '$' + str(seg_field)
            project.update({seg_field: 1})
        else:
            if config.debug:
                logger.error("segment_field config not there in deal_svc config")

    aggregated_results = list(deals_collection.aggregate(pipeline, allowDiskUse=True))

    total_aggregated_values = {}
    if return_seg_info:
        for result in aggregated_results:
            segment_values = {key: value for key, value in result.items() if key != '_id'}
            for key, value in segment_values.items():
                total_aggregated_values[key] = total_aggregated_values.get(key, 0) + value
            if config.segment_field:
                cache[(filter_name, result['_id'], node)] = value
        cache[(filter_name, 'all_deals', node)] = total_aggregated_values
        return cache
    else:
        if aggregated_results:
            aggregated_values = {key: value for key, value in aggregated_results[0].items() if key != '_id'}
        else:
            aggregated_values = {label: 0 for label, _, _ in fields_and_operations}
            aggregated_values['count'] = 0
            aggregated_values['timestamp'] = None

        if cache is not None:
            cache[(filter_name, node)] = aggregated_values

        return aggregated_values

def get_waterfall_week_total(period, node, begin, end, close_date_field, sum_field, crit):
    config = DealConfig()
    if config.config.get('update_new_collection'):
        deals_collection = sec_context.tenant_db[NEW_DEALS_COLL]
    else:
        deals_collection = sec_context.tenant_db[DEALS_COLL]
    criteria = {
                "period": period,
                "drilldown_list": node,
                close_date_field: {
                    "$gte": begin,
                    "$lte": end
                },
                'is_deleted': False
            }
    criteria = {'$and': [crit, criteria]}
    pipeline = [
        {
            "$match": criteria
        },
        {
            "$group": {
                "_id": None,
                "val": {
                    "$sum": '$'+ sum_field
                }
            }
        }
    ]
    result = list(deals_collection.aggregate(pipeline))
    try:
        return result[0]["val"]
    except:
        return 0

def get_waterfall_weekly_totals(period, node, weeks, close_date_field, sum_field, crit, multiplier=1000):
    weekly_totals = {}
    for week in weeks:
        begin = week["begin"]
        end = week["end"]
        week_label = week["label"]
        total = get_waterfall_week_total(period, node, begin, end, close_date_field, sum_field, crit)
        weekly_totals[week_label] = total / multiplier
    return weekly_totals
