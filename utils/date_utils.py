import collections
import logging
import re
from datetime import datetime, timedelta
from itertools import chain

import pytz
from aviso.settings import sec_context, CNAME

from domainmodel.csv_data import CSVDataClass
from .cache_utils import memcached, memcacheable
from .relativedelta import relativedelta

xl2datetime = lambda xl_dt: EpochClass.from_xldate(xl_dt).as_datetime()
xl2datetime_ttz = lambda xl_dt: EpochClass.from_xldate(xl_dt, False).as_datetime()
datetime2epoch = lambda dt: EpochClass.from_datetime(dt).as_epoch()
excelBase = datetime(1899, 12, 30, 0, 0, 0)
epoch2datetime = lambda ep: EpochClass.from_epoch(ep).as_datetime()
base_epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
epoch2xl = lambda ep: EpochClass.from_epoch(ep).as_xldate()
datestr2xldate = lambda closedDate: EpochClass.from_string(cleanmmddyy(closedDate),
                                                           '%m/%d/%Y',
                                                           timezone='tenant').as_xldate()
BOOKINGS_CACHE_TYPE, BOOKINGS_CACHE_SUFFIX = 'fcst_cache', 'bookings'

period = collections.namedtuple("period", ["mnemonic", "begin", "end"])
onems = timedelta(milliseconds=1)

ALL_PERIODS_CACHE = {}
PRD_STR_CACHE = {}
ALL_PERIOD_RANGES_CACHE = {}
logger = logging.getLogger('aviso-core.%s' % __name__)
now = lambda: EpochClass().as_datetime()

class EpochClass:
    """ epoch function returns an object of this type which can be used as long for all
    computations. However printing the values will result in displaying the value
    as a date time in the timezone preference based on the shell.
    """

    def __init__(self, ts=None):
        self.timezone = get_tenant_timezone()
        if ts is None:
            import time
            ts = time.time()
        self.ts = ts

    def __str__(self):
        return "%s" % datetime.fromtimestamp(self.ts, self.timezone)

    __repr__ = __str__

    def as_datetime(self):
        return datetime.fromtimestamp(self.ts, self.timezone)

    def as_xldate(self):
        d = self.as_datetime()
        d = d.astimezone(pytz.utc)
        e = datetime(
            d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)
        delta = e - excelBase
        return delta.days + (float(delta.seconds) / (3600 * 24))

    def as_tenant_xl_int(self):
        """
        Useful for passing to the holidays which are defined as tenant excel date integers
        """
        d = self.as_datetime()
        e = datetime(
            d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)
        delta = e - excelBase
        return delta.days

    def as_pytime(self):
        return self.ts

    def as_epoch(self):
        return int(self.ts * 1000)

    def __eq__(self, other):
        if isinstance(other, EpochClass):
            return self.ts == other.ts
        elif isinstance(other, int):
            return self.as_epoch() == other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception(
                    "Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() == other
        else:
            return False

    def __ne__(self, other):
        return not (self.__eq__(other))

    def __lt__(self, other):
        if isinstance(other, EpochClass):
            return self.ts < other.ts
        elif isinstance(other, int):
            return self.as_epoch() < other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception(
                    "Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() < other
        else:
            return False

    def __le__(self, other):
        if isinstance(other, EpochClass):
            return self.ts <= other.ts
        elif isinstance(other, int):
            return self.as_epoch() <= other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception(
                    "Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() <= other
        else:
            return False

    def __gt__(self, other):
        if isinstance(other, EpochClass):
            return self.ts > other.ts
        elif isinstance(other, int):
            return self.as_epoch() > other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception(
                    "Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() > other
        else:
            return False

    def __ge__(self, other):
        if isinstance(other, EpochClass):
            return self.ts >= other.ts
        elif isinstance(other, int):
            return self.as_epoch() >= other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception(
                    "Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() >= other
        else:
            return False

    def __add__(self, other):
        if isinstance(other, int):
            return EpochClass(self.ts + other / 1000.0)
        elif isinstance(other, timedelta):
            return EpochClass(self.ts + other.total_seconds())
        elif isinstance(other, relativedelta):
            as_dt = self.as_datetime()
            new_datetime = as_dt + other
            tdelta = new_datetime - as_dt
            whole_days = tdelta.days * 86400
            seconds = tdelta.seconds
            microseconds = tdelta.microseconds
            result_1 = EpochClass(self.ts + whole_days)
            # check to see if we have to add or subtract hours due to daylight
            # saving
            hour_diff = (result_1.as_datetime().hour - as_dt.hour) % 24
            if hour_diff > 12:
                hour_diff = hour_diff - 24
            return EpochClass(self.ts + (whole_days + seconds + microseconds / 1000000.0 - hour_diff * 3600))
        else:
            raise Exception(
                "Unspported type %s.  EpochClass supports integers, timedelta and relativedelta" % type(other).__name__)

    def __sub__(self, other):
        if isinstance(other, int):
            return EpochClass(self.ts - other / 1000.0)
        elif isinstance(other, timedelta):
            return EpochClass(self.ts - other.total_seconds())
        elif isinstance(other, EpochClass):
            return timedelta(seconds=self.ts - other.ts)
        elif isinstance(other, relativedelta):
            as_dt = self.as_datetime()
            new_datetime = as_dt - other
            tdelta = new_datetime - as_dt
            whole_days = tdelta.days * 86400
            seconds = tdelta.seconds
            microseconds = tdelta.microseconds
            result_1 = EpochClass(self.ts + whole_days)
            # check to see if we have to add or subtract hours due to daylight
            # saving
            hour_diff = (result_1.as_datetime().hour - as_dt.hour) % 24
            if hour_diff > 12:
                hour_diff = hour_diff - 24
            return EpochClass(self.ts + (whole_days + seconds + microseconds / 1000000.0 - hour_diff * 3600))
        else:
            raise Exception("Unspported type %s.  EpochClass supports integers, EpochClass and timedelta" %
                            type(other).__name__)

    def __hash__(self):
        return hash(self.ts)

    @classmethod
    def from_pytime(cls, f):
        return cls(f)

    @classmethod
    def from_datetime(cls, dt):
        if not dt.tzinfo:
            raise Exception(
                "Using naive datetime is very confusing with epoch")
        else:
            utc_time = dt.astimezone(pytz.utc)
            delta = utc_time - base_epoch
            return cls(delta.total_seconds())

    @classmethod
    def from_xldate(cls, xlfloat, utc=True):
        # days = int(xlfloat)
        # secondsF = (xlfloat - days) * 3600 * 24
        # seconds = int(secondsF)
        # microseconds = int(secondsF * 1000 - seconds * 1000)
        # naive_dt = excelBase + timedelta(days, seconds, microseconds)
        naive_dt = excelBase + \
            timedelta(
                days=int(xlfloat), seconds=(xlfloat - int(xlfloat)) * 3600 * 24)
        if utc:
            dt = pytz.utc.localize(naive_dt)
        else:
            tz_info = get_tenant_timezone()
            dt = tz_info.localize(naive_dt, tz_info.dst(naive_dt))
        return cls.from_datetime(dt)

    @classmethod
    def from_epoch(cls, ep):
        return cls(float(ep) / 1000)

    @classmethod
    def from_string(cls, s, fmt, timezone='utc'):
        if timezone == 'utc':
            tzinfo = pytz.utc
        elif timezone == 'tenant':
            tzinfo = get_tenant_timezone()
        else:
            tzinfo = pytz.timezone(timezone)
        fuzzy_dt = datetime.strptime(s, fmt)
        return cls.from_datetime(tzinfo.localize(fuzzy_dt, is_dst=tzinfo.dst(fuzzy_dt)))


def get_tenant_timezone():

    if sec_context.name and sec_context.name != 'N/A':
        return sec_context.tenant_time_zone
    else:
        return pytz.timezone("US/Pacific")

def epoch(desc="NOW", month=None, day=None, hour=0, mins=0, second=0, utc=True):

    if desc is None:
        raise Exception("Ambiguous value to epoch")
    elif isinstance(desc, str):
        if desc == "NOW":
            return EpochClass()
        elif re.match('\d{14}', desc):
            return EpochClass.from_datetime(
                get_tenant_timezone().localize(datetime.strptime(str(desc), '%Y%m%d%H%M%S')))
        else:
            raise Exception(
                'ERROR: epoch() accepts only NOW or tenant date in format YYYYMMDDhhmmss ')
    elif isinstance(desc, datetime):
        return EpochClass.from_datetime(desc)
    elif isinstance(desc, int) and month is None:
        if desc < 100000:
            return EpochClass.from_xldate(desc, utc=utc)
        else:
            return EpochClass.from_epoch(desc)
    elif isinstance(desc, float):
        if desc < 100000:
            return EpochClass.from_xldate(desc, utc=utc)
        else:
            return EpochClass.from_pytime(desc)
    else:
        return EpochClass.from_datetime(
            get_tenant_timezone().localize(datetime(desc, month, day, hour, mins, second)))

def current_period(a_datetime=None, period_type='Q'):
    return period_details(a_datetime, period_type, 0)

def next_period(a_datetime=None, period_type='Q', skip=1, count=1):
    return period_details(a_datetime, period_type, delta=skip, count=count)

def period_details_by_mnemonic(mnemonic, period_type='Q', delta=0, count=1):
    all_periods = get_all_periods(period_type)
    for idx, time_span in enumerate(all_periods):
        if time_span[0] == mnemonic:
            if count == 1 or count == 0:
                return all_periods[idx + delta]
            elif delta > 0:
                return list(all_periods[idx + it + delta] for it in range(count))
            else:
                return list(all_periods[idx - it + delta] for it in range(count))
    else:
        raise Exception(
            "Not match for mnemonic=%s (period_type=%s)" % (mnemonic, period_type))

def monthly_periods(q_prd, period_type='Q'):
    """ Returns all monthly periods for a given quarterly period.
    Takes either a period namedtuple OR a string corresponding to a qtr mnem.
    NOTE: This is the UI / App layer format.
    """
    if isinstance(q_prd, str):
        q_prd = period_details_by_mnemonic(q_prd, period_type)
    boq_epoch = epoch(q_prd.begin)

    if period_type in ['Y', 'y']:
        months = ([current_period(boq_epoch.as_datetime(), period_type='M')] +
                  next_period(boq_epoch.as_datetime(), period_type='M', count=11))
    else:
        months = ([current_period(boq_epoch.as_datetime(), period_type='M')] +
              next_period(boq_epoch.as_datetime(), period_type='M', count=2))

    for p, month in enumerate(months):
        bom, eom = epoch(month.begin), epoch(month.end)
        bom_dt, eom_dt = bom.as_datetime(), eom.as_datetime()
        mom_dt = bom_dt - (bom_dt - eom_dt) / 2
        mnth_mnm = str(mom_dt.year) + format(mom_dt.month, '02')

        if period_type in ['Y', 'y']:
            if bom >= q_prd.begin and eom <= q_prd.end:
                yield period(mnth_mnm, bom_dt, eom_dt)
        else:
            yield period(mnth_mnm, bom_dt, eom_dt)

def period_range(a_datetime_begin=None, a_datetime_end=None, period_type='W'):
    return period_details_range(a_datetime_begin, a_datetime_end, period_type)

def weekly_periods(q_prd, period_type='Q'):
    """ Returns all weekly periods for a given quarterly period.
    The format for the weekly periods is YYYYMM(W)WW
    Week is defined as the following:
        1. First Monday of quarter is week2, if first day of quarter is not a Monday
        2. Days after last sunday of quarter is last week
        3. Weeks reset every quarter
    Takes either a period namedtuple OR a string corresponding to a qtr mnem.
    NOTE: This is the UI / App layer format.
    """
    if isinstance(q_prd, str):
        q_prd = period_details_by_mnemonic(q_prd, period_type)
    return period_range(q_prd.begin, q_prd.end, period_type='W')

def get_date_time_for_extended_month(year, month, tzinfo, day=1):
    if month < 0:
        year = year - 1
        month += 13
    elif month > 12:
        year += 1
        month -= 12
    dt_tuple = (year, month, day)
    try:
        dt = datetime(*dt_tuple)
        dt = tzinfo.localize(dt, is_dst=tzinfo.dst(dt))
    except pytz.exceptions.NonExistentTimeError:
        dt = datetime(*(dt_tuple + (1,)))
        dt = tzinfo.localize(dt, is_dst=tzinfo.dst(dt))
    return dt

def periods_for_year(y, quarters, period_type, tzinfo,
                     quarter_adjustments={},
                     month_adjustments={},
                     starting_quarter ={}, week_start = 7,
                     year_adjustments = {}
                     ):
    if period_type[0] in ('Y', 'y'):
        if not quarters:
            return []
        adjust_begin, adjust_end = year_adjustments.get(str(y), (0, 0))
        period_start = get_date_time_for_extended_month(y, quarters[0], tzinfo) + timedelta(adjust_begin)
        period_end = get_date_time_for_extended_month(
            y, quarters[-1], tzinfo) - onems + timedelta(adjust_end)
        return [period(str(y), period_start, period_end,)]
    elif period_type[0] in ('Q', 'q'):
        periods = []
        quarter_mnemonic_shift = 0
        if str(y) in starting_quarter:
            quarter_mnemonic_shift = starting_quarter[str(y)] - 1
        elif int(y) in starting_quarter:
            quarter_mnemonic_shift = starting_quarter[int(y)] - 1
        for i, start_month in enumerate(quarters[:-1]):
            end_month = quarters[i + 1]
            mnemonic = "%04dQ%d" % (int(y), int(i + 1 + quarter_mnemonic_shift))
            adjust_begin, adjust_end = quarter_adjustments.get(
                mnemonic, (0, 0))
            periods.append(period(mnemonic,
                                  get_date_time_for_extended_month(
                                      y, start_month, tzinfo) + timedelta(adjust_begin),
                                  get_date_time_for_extended_month(y, end_month, tzinfo) + timedelta(adjust_end) - onems)
                           )
        return periods
    elif period_type[0] in ('M', 'm'):
        if not quarters:
            return []
        periods = []
        start_month = quarters[0]
        end_month = quarters[-1]
        # there is no month 0:
        month_range = filter(lambda x: x != 0, range(start_month, end_month))
        for i, month in enumerate(month_range):
            # be careful as there is no month 0
            mnemonic = "%04dM%02d" % (y, i + 1)
            adjust_begin, adjust_end = month_adjustments.get(mnemonic, (0, 0))
#             print '***************************************'
#             print '****month adjust***'
#             print adjust_begin, adjust_end
            periods.append(period(mnemonic,
                                  get_date_time_for_extended_month(
                                      y, month, tzinfo) + timedelta(adjust_begin),
                                  get_date_time_for_extended_month(y, month + 1 if month + 1 else 1, tzinfo) + timedelta(adjust_end) - onems)
                           )
        return periods
    elif period_type[0] in ('W', 'w'):
        periods = []
        quarter_mnemonic_shift = 0
        if str(y) in starting_quarter:
            quarter_mnemonic_shift = starting_quarter[str(y)] - 1
        elif int(y) in starting_quarter:
            quarter_mnemonic_shift = starting_quarter[int(y)] - 1
        for i, start_month in enumerate(quarters[:-1]):
            end_month = quarters[i + 1]
            mnemonic = "%04dQ%d" % (int(y), int(i + 1 + quarter_mnemonic_shift))
            adjust_begin, adjust_end = quarter_adjustments.get(
                mnemonic, (0, 0))
            boq = get_date_time_for_extended_month(y, start_month, tzinfo) + timedelta(adjust_begin)
            eoq = get_date_time_for_extended_month(y, end_month, tzinfo) + timedelta(adjust_end) - onems

            bow = boq
            counter = 1
            while bow < eoq:
                month_range = filter(lambda x: x != 0, range(start_month, end_month))
                for j, m_month in enumerate(month_range):
                    mnemonic = "%04dM%02d" % (y, i*3 + j + 1)
                    adjust_begin, adjust_end = month_adjustments.get(mnemonic, (0, 0))
                    bom = get_date_time_for_extended_month(y, m_month, tzinfo) + timedelta(adjust_begin)
                    eom = get_date_time_for_extended_month(y, m_month + 1 if m_month + 1 else 1,
                                                                           tzinfo) + timedelta(adjust_end) - onems
                    eom = min(eom, eoq)

                    week = 1
                    while bow < eom:
                        eow = bow + timedelta(days=(week_start - bow.weekday() + 7) % 7 or 7)
                        eow = get_date_time_for_extended_month(eow.year, eow.month, tzinfo, day=eow.day)
                        eow = min(eow-onems, eoq)

                        lower_month_bound = get_date_time_for_extended_month(y, m_month, tzinfo)
                        lower_quarter_bound = get_date_time_for_extended_month(y, start_month, tzinfo)
                        lower_bound = max(lower_quarter_bound, lower_month_bound)
                        upper_month_bound = get_date_time_for_extended_month(y, m_month + 1 if m_month + 1 else 1,
                                                                           tzinfo) - onems
                        upper_quarter_bound = get_date_time_for_extended_month(y, end_month, tzinfo)
                        upper_bound = min(upper_month_bound, upper_quarter_bound)

                        month = min(max(bow, lower_bound), upper_bound).month
                        year = min(max(bow, lower_bound), upper_bound).year

                        mnemonic = "%04d%02dW%02d" % (year, month, week)
                        periods.append(period(mnemonic, bow, eow))

                        week += 1
                        bow = eow + onems

                        counter += 1
                        if counter % 100 == 0:
                            logger.info('counter has reached %s with bow %s, eoq %s, eom %s', counter, bow, eoq, eom)
                            logger.info('PERFORMANCE_DEBUG counter executes more than 100')
                            break

                counter += 1
                if counter % 100 == 0:
                    logger.info('counter has reached %s with bow %s, eoq %s', counter, bow, eoq)
                    logger.info('PERFORMANCE_DEBUG counter executes more than 100')
                    break
        return periods
    elif period_type == 'CM':
        # Calendar Month for App / UI.
        # Assumes 3 month quarters with where month length equals length of calendar months.
        # Probably doesn't play nice with other offset features.
        qtrs = periods_for_year(y, quarters, 'Q', tzinfo, quarter_adjustments, month_adjustments)
        return list(chain(*[monthly_periods(q_prd) for q_prd in qtrs]))
    else:
        raise Exception('Unkwon period type: ' + period_type)
        return []

def get_prev_eod(ep):
    """
    get previous day from epoch, returns an epoch with end timestamp
    """
    yest = ep.as_datetime() - timedelta(days=1)
    return epoch(yest.replace(hour=23, minute=59, second=59, microsecond=999000))

def get_all_periods(period_type, filt_out_qs=None):
    tenant_name = sec_context.name
    filtered_qs = ';'.join(
        sorted(filt_out_qs)) if filt_out_qs is not None else None
    try:
        return ALL_PERIODS_CACHE[tenant_name, period_type, filtered_qs]
    except KeyError:
        ret_val = _get_all_periods(period_type, filt_out_qs)
        ALL_PERIODS_CACHE[tenant_name, period_type, filtered_qs] = ret_val
        return ret_val

def _get_all_periods(period_type, filt_out_qs=None):
    # Read the configuration
    t = sec_context.details
    period_config = t.get_config('forecast', 'quarter_begins')
    year_adjustments = t.get_config('forecast', 'year_adjustments')
    if not year_adjustments:
        year_adjustments = {}
    if not period_config:
        raise Exception(
            'No configuration found for the quarter definition. (quarter_begins)')
    quarter_adjustments = t.get_config('forecast', 'quarter_adjustments')
    if not quarter_adjustments:
        quarter_adjustments = {}
    month_adjustments = t.get_config('forecast', 'month_adjustments')
    if not month_adjustments:
        month_adjustments = {}
    starting_quarter = t.get_config('forecast', 'starting_quarter')
    if not starting_quarter:
        starting_quarter = {}
    # week start
    # Use for Tuesday = 1, Wed = 2 ....... Monday = 7
    week_start = t.get_config('forecast', 'week_start', 7)
    tzinfo = get_tenant_timezone()

    if isinstance(period_config, list):
        period_config = {'1990-': period_config}

    # Create a list of all periods
    all_periods = []
    for x in period_config:
        # Find the proper range to use
        if x.endswith('-'):
            start, end = x[0:-1], 2050
        elif x.find('-') == -1:
            start, end = x, x
        else:
            start, end = x.split('-')

        # Convert the range to integers
        start, end = int(start), int(end)
        for y in range(start, end + 1):
            all_periods.extend(
                periods_for_year(y, period_config[x], period_type, tzinfo, quarter_adjustments, month_adjustments,
                                 starting_quarter, week_start, year_adjustments))
    all_periods = sorted(all_periods)
    # Validate that there are no duplicates
    if len(all_periods) > len(set(map(lambda x1: x1[0], all_periods))):
        raise Exception('Duplicate financial years found')

    if filt_out_qs:
        return [[qmn, beg, end] for qmn, beg, end in all_periods
                if qmn not in filt_out_qs]
    else:
        return all_periods

def period_details(a_datetime=None, period_type='Q', delta=0, count=1):
    if a_datetime is None:
        a_datetime = datetime.now(tz=pytz.utc)
    all_periods = get_all_periods(period_type)
    for idx, time_span in enumerate(all_periods):
        if time_span[1] <= a_datetime <= time_span[2]:
            if count == 1 or count == 0:
                return all_periods[idx + delta]
            elif delta > 0:
                return list(all_periods[idx + it + delta] for it in range(count))
            else:
                return list(all_periods[idx - it + delta] for it in range(count))
    else:
        raise Exception("Given time doesn't fall into any known time span")

def next_period_by_epoch(epoch_time, period_type='Q', skip=1, count=1):
    return period_details(epoch2datetime(epoch_time), period_type, delta=skip, count=count)


def prev_period_by_epoch(epoch_time, period_type='Q', skip=1, count=1):
    return period_details(epoch2datetime(epoch_time), period_type, delta=-skip, count=count)


def is_same_day(first, second):
    """
    Accepts two dates in epoch and tells whether they fall on the same day.
    """
    if first is None or second is None:
        return False
    first_dt = first.as_datetime()
    second_dt = second.as_datetime()
    if (first_dt.year, first_dt.month, first_dt.day) == (second_dt.year, second_dt.month, second_dt.day):
        return True
    else:
        return False

def get_eom(ep):
    """
    get end of month
    """
    epdt = ep.as_datetime()
    future = epdt + relativedelta(months=1)
    eom = future.replace(day=1, hour=23, minute=59, second=59, microsecond=999000) + relativedelta(days=-1)
    #Applying Daylight savings
    tzinfo = eom.tzinfo
    eom = eom.replace(tzinfo=None)
    eom = tzinfo.localize(eom, is_dst=tzinfo.dst(eom))
    return epoch(eom)

def get_eod(ep):
    """
    get end of day
    """
    epdt = ep.as_datetime()
    eod = epdt.replace(hour=23, minute=59, second=59, microsecond=0)
    tzinfo = eod.tzinfo
    eod = eod.replace(tzinfo=None)
    eod = tzinfo.localize(eod, is_dst=tzinfo.dst(eod))
    return epoch(eod)


def get_eoq_for_period(period, period_info, return_type='str'):
    """
    get end of period based on period and period_info
    """
    if len(period) == 4:
        if return_type == 'datetime':
            return epoch(current_period(period_info[0].begin, 'Y').end).as_datetime()
        return str(epoch(current_period(period_info[0].begin,'Y').end))
    if 'Q' not in period:
        return None
    for p in period_info:
        if p.mnemonic == period:
            if return_type == 'datetime':
                return epoch(p.end).as_datetime()
            return str(epoch(p.end))
    return None

def prev_period(a_datetime=None, period_type='Q', skip=1, count=1):
    return period_details(a_datetime, period_type, delta=-skip, count=count)

def prev_periods_allowed_in_deals():
    prev_periods = []
    for i in range(0, 1):
        # For getting the qtrs for 1 year
        prev_periods.append(prev_period(period_type='Y', skip=i).mnemonic)
        for j in range(0, 4):
            if j not in prev_periods:
                prev_periods.append(prev_period(period_type='Q', skip=j + (4 * i)).mnemonic)
    return prev_periods

def get_nested_with_placeholder(input_dict, nesting, placeholder_dict, default=None):
    """
    get value (or dict) from nested dict by specifying the levels to go through
    """
    if not nesting:
        return input_dict
    try:

        for level in nesting[:-1]:
            input_dict = input_dict.get(level % placeholder_dict, {})

        return input_dict.get(nesting[-1], default)
    except AttributeError:
        # Not a completely nested dictionary
        return default

@memcached
def get_all_periods__m(period_type, filt_out_qs=None):
    return get_all_periods(period_type, filt_out_qs)

def period_rng_from_mnem(mnemonic):
    # using tuples instead of namedtuples in here because this is performance critical
    tenant_name = sec_context.name
    month_adjustments = sec_context.details.get_config(category='forecast', config_name='month_adjustments')
    try:
        return ALL_PERIOD_RANGES_CACHE[tenant_name, mnemonic]
    except KeyError:
        all_periods = get_all_periods('Y') if len(mnemonic) == 4 else (get_all_periods('W') if 'W' in mnemonic else get_all_periods('Q'))
        for period in all_periods:
            if period.mnemonic == mnemonic:
                mnem_range = (datetime2epoch(period.begin), datetime2epoch(period.end))
                ALL_PERIOD_RANGES_CACHE[tenant_name, mnemonic] = mnem_range
                return mnem_range
            elif 'Q' not in mnemonic and 'W' not in mnemonic and (-1 <= int(mnemonic[:4]) - int(period.mnemonic[:4]) <= 1):
                boq_epoch = epoch(period.begin)
                if not month_adjustments:
                    for m in range(0, 3):
                        bom_ep = boq_epoch + relativedelta(months=m)
                        bom_dt = bom_ep.as_datetime()
                        month_mnem = str(bom_dt.year) + format(bom_dt.month, '02')
                        if mnemonic == month_mnem:
                            mnem_range = (bom_ep.as_epoch(), get_eom(bom_ep).as_epoch())
                            ALL_PERIOD_RANGES_CACHE[tenant_name, mnemonic] = mnem_range
                            return mnem_range
                else:
                    months = [current_period(boq_epoch.as_datetime(), 'M')]
                    for m in range(0, 2):
                        months.append(current_period(months[m].end+relativedelta(days=1), 'M'))
                    #year = current_period(boq_epoch.as_datetime(), 'Q').mnemonic[:4]
                    for m in range(0, 3):
                        adjust_begin, adjust_end = month_adjustments.get(months[m].mnemonic, (0, 0))
                        bom_dt = epoch(months[m].begin).as_datetime()
                        bom_dt_adj = epoch(months[m].begin).as_datetime() - timedelta(days=adjust_begin)
                        bom_dt = max(bom_dt, bom_dt_adj)
                        month_mnem = str(bom_dt.year) + format(bom_dt.month, '02')
                        if mnemonic == month_mnem:
                            mnem_range = (datetime2epoch(months[m].begin), datetime2epoch(months[m].end))
                            ALL_PERIOD_RANGES_CACHE[tenant_name, mnemonic] = mnem_range
                            return mnem_range
        else:
            raise Exception("No match for mnemonic: %s", mnemonic)

def get_bow2(ep):
    """
    Compute the weekly boundary (get_bow) timestamp for STLY/STLQ calculations.

    For Sunday, Monday, Tuesday, return the previous Saturday's EOD.
    For Wednesday, Thursday, Friday, Saturday, return the upcoming Saturday's EOD.

    Parameters:
        ep: An epoch instance that provides an `as_datetime()` method.

    Returns:
        An epoch instance representing the boundary of the week, set to Saturday 23:59:59.999.
    """
    # Convert epoch to datetime
    epdt = ep.as_datetime()
    # Python's weekday: Monday=0, Tuesday=1, ... Saturday=5, Sunday=6
    w = epdt.weekday()

    if w in [6, 0, 1]:
        # For Sunday (6), Monday (0), Tuesday (1), return previous Saturday's date.
        # Calculate days to subtract: (w - 5) mod 7 gives: for Sunday: 1, Monday: 2, Tuesday: 3.
        delta = (w - 5) % 7
        boundary_dt = epdt - timedelta(days=delta)
    else:
        # For Wednesday (2), Thursday (3), Friday (4), Saturday (5), return upcoming Saturday.
        # Calculate days to add: (5 - w) mod 7 gives: for Wednesday: 3, Thursday: 2, Friday: 1, Saturday: 0.
        delta = (5 - w) % 7
        boundary_dt = epdt + timedelta(days=delta)

    # Set the time to 23:59:59.999 (using 999000 microseconds)
    boundary_dt = boundary_dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    return epoch(boundary_dt)

def get_week_ago(ep):
    """
    get date a week ago from epoch, returns an epoch
    """
    day = ep.as_datetime()
    day = day.replace(minute=0, second=0, microsecond=0)
    week_ago = day - timedelta(days=6)
    return epoch(week_ago.replace(hour=0,minute=0, second=0, microsecond=0))


def get_bom(ep):
    """
    get beginning of month from epoch, returns an epoch
    """
    bom = ep.as_datetime().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    #Applying Daylight savings
    tzinfo = bom.tzinfo
    bom = bom.replace(tzinfo=None)
    bom = tzinfo.localize(bom, is_dst=tzinfo.dst(bom))
    return epoch(bom.replace(hour=0, minute=0, second=0, microsecond=0))


def get_boq(ep):
    """
    get beginning of period from epoch, returns an epoch
    """
    period_info = period_details(ep.as_datetime())
    return epoch(period_info.begin)


def lazy_get_boq(ep):
    """
    get inexact beginning of quarter by going back 90 days, returns an epoch
    """
    day = ep.as_datetime()
    return epoch(day.replace(minute=0, second=0) - timedelta(days=90))


def get_eoq(ep):
    """
    get end of period from epoch, returns an epoch
    """
    period_info = period_details(ep.as_datetime())
    return epoch(period_info.end)


def get_future_bom(ep, delta=None, months=0):
    """
    get beginning of a future month that months away from epoch, returns an epoch
    """
    epdt = ep.as_datetime()
    future = epdt + delta + relativedelta(months=months)
    return epoch(future.replace(day=1, hour=0, minute=0, second=0))


def get_nextq_mnem(mnem):
    """
    get next quarter mnemonic from mnemonic
    """
    try:
        year, q = mnem.split('Q')
        q = str(int(q) % 4 + 1)
        year = year if q != '1' else str(int(year) + 1)
        return 'Q'.join([year, q])
    except ValueError:
        year, m = mnem[:4], mnem[4:]
        m = str(int(m) % 12 + 1)
        year = year if m != '1' else str(int(year) + 1)
        return ''.join([year, m.zfill(2)])


def get_nextq_mnem_safe(mnem, skip=1):
    """
    get next quarter mnemonic from current quarter mnemonic
    """
    if len(mnem) == 4:
        return str(int(mnem)+1)
    all_prds = get_all_periods('Q')
    for (curr_q, _curr_beg, _curr_end), (next_q, _next_beg, _next_end) in zip(all_prds, all_prds[skip:]):
        if curr_q == mnem:
            return next_q
    else:
        return ''

def get_bow(ep):
    """
    get beginning of week from epoch, returns an epoch
    """
    day = ep.as_datetime()
    bow = day - timedelta(days=day.weekday())
    return epoch(bow.replace(hour=0, minute=0, second=0))

def get_prevq_mnem_safe(mnem, skip=1):
    """
    get prev quarter mnemonic from current quarter mnemonic
    """
    all_prds = get_all_periods('Y') if len(mnem) == 4 else get_all_periods('Q')
    for (curr_q, _curr_beg, _curr_end), (prev_q, _prev_beg, _prev_end) in zip(all_prds[::-1], all_prds[::-1][skip:]):
        if curr_q == mnem:
            return prev_q
    else:
        return ''

def get_yest(ep):
    """
    get previous day from epoch, returns an epoch
    """
    yest = ep.as_datetime() - timedelta(days=1)
    return epoch(yest.replace(hour=0, minute=0, second=0))

def period_details_range(a_datetime_begin=None, a_datetime_end=None, period_type='W'):
    if a_datetime_begin is None or a_datetime_end is None:
        raise Exception("Given range is open ended")

    all_periods = get_all_periods(period_type)
    periods = []
    for idx, time_span in enumerate(all_periods):
        if a_datetime_begin <= time_span[1] and time_span[2] <= a_datetime_end:
            periods.append(all_periods[idx])
    if not periods:
        raise Exception("Given time doesn't fall into any known time span")
    return periods

def is_valid_mnemonic(mnemonic, period_type='Q'):
    """
    Accepts a mnemonic string and returns whether its a valid mnemonic or not.
    period_type = Q/M/CM/Y
    """
    if period_type == 'Q':
        regex = "^\d{4}Q[1-4]$"
    elif period_type == 'M':
        regex = "^\d{4}M[0,1]\d$"
    elif period_type == 'CM':
        regex = "^\d{4}[0,1]\d$"
    else:
        regex = "^\d{4}$"
    return True if re.match(regex, mnemonic) else False

def get_weekly_periods_info_in_quarter(q_mnemonic, today_epoch):
    """
    Computes weekly period information within a given quarter.
    The function returns a dictionary containing weekly period mnemonics as keys
    and their corresponding details as values. Each entry includes:
    - 'begin'
    - 'end'
    - 'has_forecast'
    - 'relative_period'
    Parameters:
        q_mnemonic (str): The quarter mnemonic.
        today_epoch (Epoch): The current date as an epoch timestamp.
    Returns:
        dict: A dictionary mapping weekly period mnemonics to their details.
    """

    from config.periods_config import PeriodsConfig
    from infra.read import render_period

    period_config = PeriodsConfig()
    quarter_week_range = weekly_periods(q_mnemonic)
    now_dt = today_epoch.as_datetime()

    weekly_period_info = {
        week_range.mnemonic: render_period(week_range, now_dt, period_config)
        for week_range in quarter_week_range
    }

    return weekly_period_info

def is_demo():
    tc = sec_context.details.get_config(category='forecast',
                                        config_name='tenant')
    return tc.get('is_demo', CNAME not in ['app', 'stage'])

def get_now(verbose=False,):
    """ Computes the EpochClass object referring to 'now' for the tenant. If in demo mode
    (meaning that the tenant is 'frozen' and not expected to be refreshed regularly, we treat last
    refresh time as 'now'.

    If verbose, returns a tuple (now, latest_refresh_date, avail_prds)
    If not, just returns now.
    """

    if not verbose and not is_demo():
        return epoch()
    tc = sec_context.details.get_config(category='forecast', config_name='tenant')
    BookingsCacheClass = CSVDataClass(BOOKINGS_CACHE_TYPE,
                                      BOOKINGS_CACHE_SUFFIX)

    top_lvl_recs = list(BookingsCacheClass.getAllByFieldValue(
            'V', u'.~::~summary'))
    is_chipotle = tc.get('fm_config', {}).get('rtfm_type', 'standard') == "chipotle"

    if not top_lvl_recs:
        return None

    avail_prds = {record.Q for record in top_lvl_recs}
    latest_refresh_date = None
    if is_chipotle:
        try:
            latest_refresh_date = int(sec_context.details.get_flag('molecule_status',
                                                                   'rtfm', {}).get('last_execution_time'))
        except (TypeError, ValueError):
            pass

    if not latest_refresh_date:
        try:
            latest_refresh_date = max(top_lvl_recs, key=lambda x: x.Q).dynamic_fields['as_of'][-1]
        except Exception as e:
            logger.exception(e.message)
    if is_demo() and latest_refresh_date:
        now = epoch(latest_refresh_date)
    else:
        now = epoch()

    if not verbose:
        return now
    else:
        curr_mnem =  current_period(epoch(latest_refresh_date).as_datetime()).mnemonic
        avail_prds |= set(period_info_from_mnem(curr_mnem, has_nextq=True))
        return now, latest_refresh_date, avail_prds

@memcacheable('period_info_from_mnem', ('mnem', 0), ('monthly', 1), ('has_nextq', 2), ('as_of', 3))
def period_info_from_mnem(mnem, monthly=False, has_nextq=False, as_of=None, use_safe_nextq=False):
    """
    return info about a period from the mnemonic
    """
    prd_infos = collections.defaultdict(lambda: collections.defaultdict(list))
    if as_of:
        as_of = epoch(as_of)

    def add_period(mnem, qtr, beg=None, end=None, is_comp=False, as_of=None):
        if not beg and not end:
            beg, end = period_rng_from_mnem(mnem)
        beg = epoch(beg)
        end = epoch(end)
        prd_infos[mnem].update({'qtr': qtr,
                                'begin': beg.as_xldate(),
                                'end': end.as_xldate(),
                                'is_comp': is_comp,
                                })
        if is_comp:
            prd_infos[qtr]['comp_list'].append(mnem)
        if as_of:
            prd_infos[mnem]['is_hist'] = end < as_of

    add_period(mnem, mnem, is_comp=not monthly)

    if has_nextq:
        if use_safe_nextq:
            nextq_mnem = get_nextq_mnem_safe(mnem)
        else:
            nextq_mnem = get_nextq_mnem(mnem)
        add_period(nextq_mnem, nextq_mnem, is_comp=not monthly)

    if monthly:
        mnem_beg = prd_infos[mnem]['begin']
        quarterly_months = ([current_period(epoch(mnem_beg).as_datetime(), period_type='M')] +
                            next_period(epoch(mnem_beg).as_datetime(), period_type='M', count=5))
        boqs = [(mnem, epoch(mnem_beg), quarterly_months[:3])]
        if has_nextq:
            boqs += [(nextq_mnem, epoch(prd_infos[nextq_mnem]['begin']), quarterly_months[3:])]
        for (mnem, boq, months) in boqs:
            for p, month in enumerate(months):
                bom = epoch(month.begin)
                eom = epoch(month.end)
                bom_dt = bom.as_datetime()
                eom_dt = eom.as_datetime()
                # This is really messed up. The mnemonic that the app uses isn't the same as the mnemonic in the periods.
                # Current period gives '2019M04' where 2019 is FY, app wants '201804' where 2018 is calendar year.
                # We use the middle date to figure out the month, since first and last might not work with weird months.
                mom_dt = bom_dt - (bom_dt - eom_dt) / 2
                period = str(mom_dt.year) + format(mom_dt.month, '02')
                add_period(period, mnem, bom_dt, eom_dt, is_comp=True, as_of=as_of)
    return dict(prd_infos)

def rng_from_prd_str(std_prd_str, fmt='epoch', period_type='Q', user_level=False):
    """Given a standard period string, like thisQ, thisM, nextW, etc
    return the boundaries of that period as a tuple of (begin, end)"""

    if user_level:
        from config.periods_config import PeriodsConfig
        periodconfig = PeriodsConfig()
        from infra.read import get_now as infra_get_now
        today = infra_get_now(periodconfig)
    else:
        today = get_now()
    try:
        return PRD_STR_CACHE[sec_context.name, std_prd_str, fmt]
    except KeyError:
        if is_valid_mnemonic(std_prd_str, period_type):
            prd_details = period_details_by_mnemonic(std_prd_str, period_type)
            from_epoch, til_epoch = epoch(prd_details.begin), epoch(prd_details.end)
        elif std_prd_str == 'thisq' or std_prd_str == 'THIS_Q':
            cp = current_period()
            from_epoch, til_epoch = epoch(cp.begin), epoch(cp.end)
        elif std_prd_str == 'SELECTED_Q':
            slctd_prd_begin = sec_context.details.get_flag('selected_period', 'slctd_prd_begin', {})
            slctd_prd_end = sec_context.details.get_flag('selected_period', 'slctd_prd_end', {})
            from_epoch, til_epoch = epoch(slctd_prd_begin), epoch(slctd_prd_end)
        elif std_prd_str == 'SELECTED_NEXT':
            slctd_prd_next_q_start = sec_context.details.get_flag('selected_period', 'slctd_prd_next_q_start', 0)
            from_epoch, til_epoch = epoch(slctd_prd_next_q_start), None
        elif std_prd_str == 'ALL':
            PRD_STR_CACHE[sec_context.name, std_prd_str, fmt] = (None, None)
            return None, None
        elif std_prd_str == 'NEXT_Q':
            nxt = next_period()
            from_epoch, til_epoch = epoch(nxt.begin), epoch(nxt.end)
        elif std_prd_str == 'THIS_M':
            from_epoch, til_epoch = get_bom(today), get_eom(today)
        elif std_prd_str == 'NEXT_M':
            diff = relativedelta(months=1)
            from_epoch, til_epoch = get_bom(today) + diff, get_eom(get_eom(today) + diff)

        elif std_prd_str == 'THIS_W':
            current_p = current_period().mnemonic
            weekly_periods_info = get_weekly_periods_info_in_quarter(current_p, today)

            for week_info in weekly_periods_info.values():
                if week_info['relative_period'] == 'c':
                    from_epoch, til_epoch = map(epoch, (week_info['begin'], week_info['end']))
                    break

        elif std_prd_str == 'NEXT_W':
            diff = relativedelta(weeks=1)
            current_p = current_period().mnemonic
            weekly_periods_info = get_weekly_periods_info_in_quarter(current_p, today)

            for week_info in weekly_periods_info.values():
                if week_info['relative_period'] == 'c':
                    from_epoch, til_epoch = epoch(week_info['begin']) + diff,   epoch(week_info['end']) + diff
                    break

        elif std_prd_str == 'next180':
            from_epoch, til_epoch = epoch(), epoch() + relativedelta(days=180)
        elif std_prd_str == 'last90':
            from_epoch, til_epoch = epoch() - relativedelta(days=90), epoch()
        elif std_prd_str == 'next90':
            from_epoch, til_epoch = epoch(), epoch() + relativedelta(days=90)
        elif std_prd_str == 'last30':
            from_epoch, til_epoch = epoch() - relativedelta(days=30), epoch()
        elif std_prd_str == 'next30':
            from_epoch, til_epoch = epoch(), epoch() + relativedelta(days=30)
        elif std_prd_str == 'last7':
            from_epoch, til_epoch = epoch() - relativedelta(days=7), epoch()
        elif std_prd_str == 'next7':
            from_epoch, til_epoch = epoch(), epoch() + relativedelta(days=7)
        elif std_prd_str == 'more_than_30':
            from_epoch, til_epoch = epoch() + relativedelta(days=30), None
        elif std_prd_str == 'more_than_last_90':
            from_epoch, til_epoch = None, epoch() - relativedelta(days=90)
        elif std_prd_str == 'from_today':
            from_epoch, til_epoch = epoch(), None
        elif std_prd_str == 'PAST':
            yest_now = epoch(today.as_datetime() - timedelta(days=1))
            yesterdays_value = get_eod(yest_now)
            if fmt == "xldate":
                yesterdays_value = yesterdays_value.as_xldate()
            elif fmt == 'epoch':
                yesterdays_value = yesterdays_value.as_epoch()
            elif fmt == 'string':
                yesterdays_value = yesterdays_value.as_datetime().strftime("%Y-%m-%d")
            past_tuple = (None, yesterdays_value)
            PRD_STR_CACHE[sec_context.name, std_prd_str, fmt] = past_tuple
            return past_tuple

        elif std_prd_str == 'DYNAMIC_WEEK':
            cur_week_days = ['Sunday', 'Monday', 'Tuesday']
            dt = today.as_datetime()
            day_of_week = dt.strftime("%A")
            target_week_status = ['f']
            if day_of_week in cur_week_days:
                target_week_status.append('c')

            from config.periods_config import PeriodsConfig
            from infra.read import render_period
            periodconfig = PeriodsConfig()
            current_p = current_period().mnemonic
            quarter_week_range =  weekly_periods(current_p)
            now_dt = today.as_datetime()
            begin_timestamps = []

            for week_range in quarter_week_range:
                week_info = render_period(week_range, now_dt, periodconfig)
                if week_info['relative_period'] in target_week_status:
                    begin_timestamps.append(week_info['begin'])

            from_epoch = epoch(min(begin_timestamps))
            til_epoch = None

        else:
            PRD_STR_CACHE[sec_context.name, std_prd_str, fmt] = (None, None)
            return None, None

        if fmt == 'epoch':
            ret_val = (from_epoch.as_epoch() if from_epoch else None,
                       til_epoch.as_epoch() if til_epoch else None)
        elif fmt == 'xldate':
            ret_val = (from_epoch.as_xldate() if from_epoch else None,
                       til_epoch.as_xldate() if til_epoch else None)
        elif fmt == 'string':
            ret_val = (from_epoch.as_datetime().strftime("%Y-%m-%d") if from_epoch else None,
                       til_epoch.as_datetime().strftime("%Y-%m-%d") if til_epoch else None)
        if std_prd_str in ['SELECTED_Q', 'SELECTED_NEXT']:
            return ret_val
        PRD_STR_CACHE[sec_context.name, std_prd_str, fmt] = ret_val
        return ret_val

def get_a_date_time_as_float_some_how(date_like_thing):
    if not date_like_thing:
        return None
    # Convert modified date from string to an excel float
    try:
        a_date = float(date_like_thing)
        if a_date > 100000:
            a_date = EpochClass.from_epoch(a_date).as_xldate()
    except TypeError:
        # TODO: Move the type error to the exception list below so that
        # we will try the fall back mechanism after we see why it is failing
        # first
        logger.error(
            "Unable to determine the date from %s" + str(date_like_thing))
        logger.error('Type of the argument is %s', str(type(date_like_thing)))
        return None
    except ValueError:
        try:
            a_date = datetime2xl(
                EpochClass.from_string(cleanmmddyy(date_like_thing), '%m/%d/%Y').as_datetime())
        except:
            logger.error(
                "Unable to determine the date from " + date_like_thing)
            return None

    return a_date

def cleanmmddyy(anstr):
    a = anstr.split('/')
    if len(a) != 3:
        return anstr
    year = int(a[2])
    if year < 70:
        year += 2000
    return str(int(a[0])) + '/' + str(int(a[1])) + '/' + str(year)

datetime2xl = lambda dt: EpochClass.from_datetime(dt).as_xldate()