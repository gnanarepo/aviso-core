import collections
import logging
import re
from datetime import datetime, timedelta
from itertools import chain

import pytz
from aviso.settings import sec_context, CNAME

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

def get_future_bom(ep, delta=None, months=0):
    """
    get beginning of a future month that months away from epoch, returns an epoch
    """
    epdt = ep.as_datetime()
    future = epdt + delta + relativedelta(months=months)
    return epoch(future.replace(day=1, hour=0, minute=0, second=0))

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