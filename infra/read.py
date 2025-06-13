import copy
import logging
from collections import namedtuple
from functools import wraps

import numpy as np
from aviso.framework.metric_logger import NOOPMetricSet
from aviso.settings import sec_context
from infra.constants import DEALS_COLL, NEW_DEALS_COLL
from tasks.hierarchy.hierarchy_utils import get_user_permissions
from utils.date_utils import (current_period, epoch, get_bom, get_boq, get_bow,
                              get_eod,
                              get_nextq_mnem_safe, get_prevq_mnem_safe,
                              get_week_ago, get_yest, monthly_periods,
                              next_period, period_details,
                              period_details_range, period_rng_from_mnem,
                              prev_period, prev_periods_allowed_in_deals)
from utils.misc_utils import (flatten_to_list,
                              is_lead_service, merge_dicts)

from infra.constants import DRILLDOWN_COLL, DRILLDOWN_LEADS_COLL, HIER_COLL, HIER_LEADS_COLL

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
    from config import PeriodsConfig
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
    from config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()
    if period[-2] != 'Q' or not config.monthly_fm:
        return [period]
    return [mnemonic for (mnemonic, _beg_dt, _end_dt) in monthly_periods(period, period_type)]


def get_now(config=None):
    """
    get time of now in app

    Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        epoch -- epoch of apps current time
    """

    from config import PeriodsConfig
    config = config if config is not None else PeriodsConfig()

    if config.use_sys_time:
        return epoch()
    else:
        try:
            context_details = sec_context.details
            return epoch(context_details.get_flag('prd_svc', 'now_date'))
        except:
            return epoch()

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

def get_quarter_period(period):
    from config import PeriodsConfig
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
    from config import PeriodsConfig
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
    from config import PeriodsConfig
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
    from config import PeriodsConfig
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
    from config import PeriodsConfig
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
    from config import PeriodsConfig
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
    from config import PeriodsConfig
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
    from config import PeriodsConfig
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
    from config import PeriodsConfig
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


def get_current_period(config=None):
    """
    get mnemonic of current app period

    Keyword Arguments:
        config {PeriodsConfig} -- instance of PeriodsConfig                      ...?

    Returns:
        str -- period mnemonic
    """
    # totes cheating to get across services
    from config import PeriodsConfig
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
