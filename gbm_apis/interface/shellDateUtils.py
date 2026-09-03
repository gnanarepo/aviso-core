import bisect
import calendar
import datetime
from . import relativedelta  # vendored; python-dateutil is not an SDK client dependency
import itertools
import logging
import os
import pytz
import six

if six.PY3:
    basestring = str

logger = logging.getLogger()

excelBase = datetime.datetime(1899, 12, 30, 0, 0, 0)
excelBaseDate = datetime.date(1899, 12, 30)
utc_tz = pytz.utc
base_epoch = datetime.datetime(1970, 1, 1, tzinfo=utc_tz)
onems = datetime.timedelta(milliseconds=1)


# lambdas for convenience
datetime2xl = lambda shell, dt: EpochClass.from_datetime(shell, dt).as_xldate()
datetime2epoch = lambda shell, dt: EpochClass.from_datetime(shell, dt).as_epoch()
datetime2pytime = lambda shell, dt: EpochClass.from_datetime(shell, dt).as_pytime()

date2xl = lambda shell, dt: EpochClass.from_date(shell, dt).as_xldate()

xl2datetime = lambda shell, xl_dt: EpochClass.from_xldate(shell, xl_dt).as_datetime()
xl2epoch = lambda shell, xl_dt: EpochClass.from_xldate(shell, xl_dt).as_epoch()
xl2pytime = lambda shell, xl_dt: EpochClass.from_xldate(shell, xl_dt).as_pytime()

# if you excel date is in tenant time zone, you should use below conversions
xl2datetime_ttz = lambda xl_dt: EpochClass.from_xldate(xl_dt, False).as_datetime()
xl2epoch_ttz = lambda xl_dt: EpochClass.from_xldate(xl_dt, False).as_epoch()
xl2pytime_ttz = lambda xl_dt: EpochClass.from_xldate(xl_dt, False).as_pytime()

epoch2datetime = lambda shell, ep: EpochClass.from_epoch(shell, ep).as_datetime()
epoch2pytime = lambda shell, ep: EpochClass.from_epoch(shell, ep).as_pytime()
epoch2xl = lambda shell, ep: EpochClass.from_epoch(shell, ep).as_xldate()

pytime2datetime = lambda shell, ep: EpochClass.from_pytime(shell, ep).as_datetime()
pytime2epoch = lambda shell, ep: EpochClass.from_pytime(shell, ep).as_epoch()
pytime2xl = lambda shell, ep: EpochClass.from_pytime(shell, ep).as_xldate()


def timezone(shell, tz_str, period_info=False):
    """ Change the shell preferred timezone.  In addition to the
    timezones defined in pytz.timezone, 'utc', 'local' and 'tenant'
    are accepted as special case
    """
    if tz_str == 'local':
        shell._tz_preference = 'local'
        return shell.set_local_timezone()
    elif tz_str == 'tenant':
        shell._tz_preference = 'tenant'
        if hasattr(shell, '_tz_name'):
            del shell._tz_name
        uinfo = shell.me()
        if uinfo and 'current_tenant' in uinfo:
            tenant_name = uinfo['current_tenant']
        else:
            tenant_name = shell._tenant
        if tenant_name is None:
            logger.debug("Until you login, we can't set tenant timezone.\
                             Calls to epoch functions will fail until you login")
            shell._tz_name = None
        else:
            if uinfo:
                shell._tz_name = uinfo.get('timezone', None)
                if shell._tz_name is None:
                    logger.info("No timezone configured for tenant. Default to 'America/Los_Angeles'")
                    shell._tz_name = 'America/Los_Angeles'
                else:
                    logger.debug("Current timezone : %s" % shell._tz_name)
                    pass
            else:
                pass
    elif tz_str in pytz.all_timezones:
        shell._tz_preference = 'specific'
        shell._tz_name = tz_str
        print (shell._tz_name)
    else:
        logger.info("Unknown timezone preference")
    if period_info:
        PeriodInfo(shell, True)


def set_local_timezone(shell):
    if 'TZ' in os.environ:
        shell._tz_name = os.environ['TZ']
        return
    try:
        shell._tz_name = '/'.join(os.readlink('/etc/localtime').split('/')[-2:])
    except:
        logger.info("Unable to autodetect timezone from local time, setting to UTC")
        shell._tz_name = 'utc'
    return shell._tz_name


def epoch(shell, desc="NOW", month=None, day=None, hour=0, mins=0, second=0, millisecond=0, utc=True):
    if isinstance(millisecond, bool):
        raise Exception("Millisecond cannot be a boolean value.")

    if shell._tz_name is None:
        raise Exception("Unknown timezone information")

    if desc is None:
        raise Exception("Ambiguous value to epoch")
    elif desc == "NOW":
        return EpochClass(shell._tz_name)
    elif isinstance(desc, datetime.datetime):
        return EpochClass.from_datetime(shell, desc)
    elif isinstance(desc, int) and month is None:
        if desc < 100000:
            return EpochClass.from_xldate(shell, desc, utc=utc)
        else:
            return EpochClass.from_epoch(shell, desc)
    elif isinstance(desc, float):
        if desc < 100000:
            return EpochClass.from_xldate(shell, desc, utc=utc)
        else:
            return EpochClass.from_pytime(shell, desc)
    else:
        if shell:
            tz_info = pytz.timezone(shell._tz_name)
        else:
            tz_info = pytz.utc
        naive_dt = datetime.datetime(desc, month, day, hour, mins, second, millisecond)
        dt = tz_info.localize(naive_dt, tz_info.dst(naive_dt))
        return EpochClass.from_datetime(shell, dt)

epoch._time_profile = False


class EpochClass(object):
    """ epoch function returns an object of this type which can be used as int for all
    computations. However printing the values will result in displaying the value
    as a date time in the timezone preference based on the shell.
    """

    def __init__(self, tz_name, ts=None):
        if tz_name is None:
            raise Exception('Unknown timezone information')

        timezone_found = True
        if isinstance(tz_name, datetime.tzinfo):
            self.timezone = tz_name
        elif isinstance(tz_name, str):
            self.timezone = pytz.timezone(tz_name)
        else:
            timezone_found = False
            try:
                # python-sdk support for basestring
                if isinstance(tz_name, basestring):
                    timezone_found = True
                    self.timezone = pytz.timezone(tz_name)
            except NameError:
                pass
            try:
                if not timezone_found:
                    self.timezone = pytz.timezone(tz_name._tz_name)
            except:
                logger.warn("Unknown object passed to EpochClass " + str(tz_name))
                raise Exception("Unknown object passed to EpochClass")

        if ts is None:
            import time
            ts = time.time()
        self.ts = ts

    def __str__(self):
        return "%s" % datetime.datetime.fromtimestamp(self.ts, self.timezone)

    __repr__ = __str__

    def as_datetime(self):
        try:
            return datetime.datetime.fromtimestamp(self.ts, self.timezone)
        except:
            return datetime.datetime.fromtimestamp(0, self.timezone) + datetime.timedelta(seconds=self.ts)

    def as_xldate(self, utc=True):
        d = self.as_datetime()
        if utc:
            d = d.astimezone(utc_tz)
        e = datetime.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)
        delta = e - excelBase
        return delta.days + (float(delta.seconds) / (3600 * 24))

    def as_tenant_xl_int(self):
        '''
        Useful for passing to the holidays which are defined as tenant excel date integers
        '''
        d = self.as_datetime()
        e = datetime.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)
        delta = e - excelBase
        return delta.days

    def as_pytime(self):
        return self.ts

    def as_epoch(self):
        ts = int(self.ts * 1000)
        return ts + 1 if (self.ts * 1000) - ts else ts

    def __eq__(self, other):
        if isinstance(other, EpochClass):
            return self.ts == other.ts
        elif isinstance(other, int):
            return self.as_epoch() == other
        elif isinstance(other, datetime.datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
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
        elif isinstance(other, datetime.datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() < other
        else:
            return False

    def __le__(self, other):
        if isinstance(other, EpochClass):
            return self.ts <= other.ts
        elif isinstance(other, int):
            return self.as_epoch() <= other
        elif isinstance(other, datetime.datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() <= other
        else:
            return False

    def __gt__(self, other):
        if isinstance(other, EpochClass):
            return self.ts > other.ts
        elif isinstance(other, int):
            return self.as_epoch() > other
        elif isinstance(other, datetime.datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() > other
        else:
            return False

    def __ge__(self, other):
        if isinstance(other, EpochClass):
            return self.ts >= other.ts
        elif isinstance(other, int):
            return self.as_epoch() >= other
        elif isinstance(other, datetime.datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() >= other
        else:
            return False

    def __add__(self, other):
        if isinstance(other, int):
            return EpochClass(self.timezone, self.ts + other / 1000.0)
        if isinstance(other, datetime.timedelta):
            return EpochClass(self.timezone, self.ts + other.total_seconds())
        if isinstance(other, relativedelta.relativedelta):
            as_dt = self.as_datetime()
            new_datetime = as_dt + other
            tdelta = new_datetime - as_dt
            whole_days = tdelta.days * 86400
            seconds = tdelta.seconds
            microseconds = tdelta.microseconds
            result_1 = EpochClass(self.timezone, self.ts + (whole_days))
            # check to see if we have to add or subtract hours due to daylight saving
            hour_diff = (result_1.as_datetime().hour - as_dt.hour) % 24
            if hour_diff > 12:
                hour_diff = hour_diff - 24
            return EpochClass(self.timezone, self.ts +
                              (whole_days + seconds + microseconds / 1000000.0 - hour_diff * 3600))
        else:
            raise Exception(
                "Unspported type %s.  EpochClass supports integers, timedelta, and relativedelta" % (
                    type(other).__name__))

    def __sub__(self, other):
        if isinstance(other, int):
            return EpochClass(self.timezone, self.ts - other / 1000.0)
        if isinstance(other, datetime.timedelta):
            return EpochClass(self.timezone, self.ts - other.total_seconds())
        if isinstance(other, EpochClass):
            return datetime.timedelta(seconds=self.ts - other.ts)
        if isinstance(other, relativedelta.relativedelta):
            as_dt = self.as_datetime()
            new_datetime = as_dt - other
            tdelta = new_datetime - as_dt
            whole_days = tdelta.days * 86400
            seconds = tdelta.seconds
            microseconds = tdelta.microseconds
            result_1 = EpochClass(self.timezone, self.ts + (whole_days))
            # check to see if we have to add or subtract hours due to daylight saving
            hour_diff = (result_1.as_datetime().hour - as_dt.hour) % 24
            if hour_diff > 12:
                hour_diff = hour_diff - 24
            return EpochClass(self.timezone, self.ts +
                              (whole_days + seconds + microseconds / 1000000.0 - hour_diff * 3600))
        else:
            raise Exception(
                "Unspported type %s.  EpochClass supports integers, EpochClass relativedelta and timedelta" % (
                    type(other).__name__))

    def __hash__(self):
        return hash(self.ts)

    @classmethod
    def from_pytime(cls, shell, f):
        return cls(shell, f)

    @classmethod
    def from_datetime(cls, shell, dt):
        if not dt.tzinfo:
            raise Exception("Using naive datetime is very confusing with epoch")
        else:
            utc_time = dt.astimezone(utc_tz)
            delta = utc_time - base_epoch
            return cls.from_pytime(shell, delta.total_seconds())

    @classmethod
    def from_date(cls, shell, dt):
        tz = pytz.timezone(shell._tz_name)
        return cls.from_datetime(shell, tz.localize(datetime.datetime(dt.year, dt.month, dt.day)))

    @classmethod
    def from_xldate(cls, shell, xlfloat, utc=True):
        days = int(xlfloat)
        secondsF = (xlfloat - days) * 3600 * 24
        seconds = int(secondsF)
        microseconds = int(secondsF * 1000 - seconds * 1000)
        naive_dt = excelBase + datetime.timedelta(days, seconds, microseconds)
        if utc:
            dt = utc_tz.localize(naive_dt)
        else:
            tz = pytz.timezone(shell._tz_name)
            dt = tz.localize(naive_dt, is_dst=tz.dst(naive_dt))
        return cls.from_datetime(shell, dt)

    @classmethod
    def from_epoch(cls, shell, ep):
        return cls(shell, float(ep) / 1000)


class PeriodInfo(object):

    class __PeriodInfo:

        def __init__(self, shell, *args, **kwargs):
            self.count = 0
            self.PERIODS_CACHE = {}

        def __call__(self, **kwargs):
            action = kwargs.get('action', None)
            shell = kwargs.get('shell', None)
            period_type = kwargs.get('period_type')
            skip = kwargs.get('skip', 1)
            count = kwargs.get('count', 1)
            epoch_time = kwargs.get('epoch_time') if kwargs.get('epoch_time') else epoch(shell, 'NOW')
            start_index = None
            if isinstance(epoch_time, EpochClass):
                epoch_time = epoch_time.as_epoch()
            if period_type not in self.PERIODS_CACHE:
                self.initialize(shell, period_type)
            try:
                # Since bisect consider indexes from one thats why -1
                start_index = bisect.bisect(self.PERIODS_CACHE['%s_lookup' % period_type], int(epoch_time)) - 1
                if action == 'next':
                    start_index += skip
                    end_index = start_index + count
                    if (epoch_time > self.PERIODS_CACHE[period_type][start_index]['end'] or
                        end_index > len(self.PERIODS_CACHE['%s_lookup' % period_type])):
                        raise Exception("Out of buffer...")
                    return self.PERIODS_CACHE[period_type][start_index:end_index]
                elif action == 'prev':
                    start_index -= skip
                    end_index = start_index - count
                    if end_index == -1:
                        end_index = 0
                        return self.PERIODS_CACHE[period_type][start_index:0:-1] + [self.PERIODS_CACHE[period_type][0]]
                    if start_index < 0 or end_index < 0:
                        raise Exception("Out of buffer...")
                    return self.PERIODS_CACHE[period_type][start_index:end_index:-1]
                elif action == 'current':
                    if (epoch_time > self.PERIODS_CACHE[period_type][start_index]['end'] or
                        epoch_time < self.PERIODS_CACHE[period_type][start_index]['begin']):
                        raise Exception("Out of buffer...")
                    self.PERIODS_CACHE[period_type][start_index]
                    return self.PERIODS_CACHE[period_type][start_index]
            except Exception as e:
                logger.exception(str(e))
                logger.warn("Query is seems to go beyond cached boundaries")
                logger.info("Redirecting to api call...")
                return self.redirect_to_api(action, epoch_time, period_type, skip, count)

        def initialize(self, shell, period_type):
            # Capturing boundries for 15 years atmost
            period_config = shell.tenant('get_config', category='forecast', config_name='quarter_begins')
            if period_config:
                logger.info("Creating period cache ...")

                c = self.current_period_(shell, period_type=period_type)

                Y_count = min(int(c['mnemonic'][:4]) - 1990, 2050 - int(c['mnemonic'][:4]), 15)
                M_count = min(Y_count * 12, 180)
                Q_count = min(Y_count * 4, 60)
                CM_count = min(Y_count * 12, 180)
                count = eval('%s_count' % period_type)

                p = self.prev_period_(shell, count=count, period_type=period_type)
                n = self.next_period_(shell, count=count, period_type=period_type)
                from operator import itemgetter
                self.PERIODS_CACHE[period_type] = sorted(p + [c] + n, key=itemgetter('mnemonic'))
                self.PERIODS_CACHE['%s_lookup' % period_type] = self.create_lookup(period_type)
            else:
                logger.warn('Caching of period is not done, due to unavailability of quarter begins in new tenant.')
                raise Exception("'No configuration found for the quarter definition. (quarter_begins)'")

        def redirect_to_api(self, shell, action, epoch_time=None, period_type='Y', skip=1, count=1):
            function_name = eval("self.%s_period_" % action)
            if action == 'current':
                return function_name(shell, epoch_time, period_type)
            return function_name(shell, epoch_time, period_type, skip, count)

        def create_lookup(self, period_type):
            lookup = []
            for i in self.PERIODS_CACHE[period_type]:
                lookup.append(i['begin'])
            return lookup

        def current_period_(self, shell, epoch_time=None, period_type='Q'):
            if epoch_time:
                ret = shell.api('/date/current?type=%s&as_of=%s' % (period_type, epoch_time), None)
            else:
                ret = shell.api('/date/current?type=%s' % period_type, None)

            if ret and '_error' not in ret:
                return ret[0]
            else:
                print(ret)
                return None

        def next_period_(self, shell, epoch_time=None, period_type='Q', skip=1, count=1):
            if epoch_time:
                ret = shell.api('/date/next?type=%s&as_of=%s&skip=%s&count=%s' % (period_type, epoch_time, skip, count
                                                                                  ), None)
            else:
                ret = shell.api('/date/next?type=%s&skip=%s&count=%s' % (period_type, skip, count), None)

            if isinstance(ret, dict) and '_error' in ret:
                print(ret)
            return ret

        def prev_period_(self, shell, epoch_time=None, period_type='Q', skip=1, count=1):
            if epoch_time:
                ret = shell.api('/date/prev?type=%s&as_of=%s&skip=%s&count=%s' %
                                (period_type, epoch_time, skip, count), None)
            else:
                ret = shell.api('/date/prev?type=%s&skip=%s&count=%s' % (period_type, skip, count), None)

            if isinstance(ret, dict) and '_error' in ret:
                print (ret)
            return ret

    instance = None

    def __new__(self, shell, reset=False, *args, **kwargs):
        if not PeriodInfo.instance:
            PeriodInfo.instance = PeriodInfo.__PeriodInfo(shell, *args, **kwargs)
        elif reset:
            PeriodInfo.instance = None
        return PeriodInfo.instance


def current_period(shell, epoch_time=None, period_type='Q'):
    '''
    Return a tuple that contains the mnemonic name, start and
    end dates for the period in which this date falls into by calling
    the data manager API
    '''
    c = PeriodInfo(shell=shell)
    return c(action='current', shell=shell, epoch_time=epoch_time, period_type=period_type)


def next_period(shell, epoch_time=None, period_type='Q', skip=1, count=1):
    '''
    Return a tuple that contains the memonic name, start and
    end dates for the next period based on the given time and skip
    Actual instantiation happens only with case 2.
    '''
    n = PeriodInfo(shell=shell)
    return n(action='next', shell=shell, epoch_time=epoch_time, period_type=period_type, skip=skip, count=count)


def prev_period(shell, epoch_time=None, period_type='Q', skip=1, count=1):
    '''
    Return a tuple that contains the memonic name, start and
    end dates for the next previous based on the given time and skip
    '''
    p = PeriodInfo(shell=shell)
    return p(action='prev', shell=shell, epoch_time=epoch_time, period_type=period_type, skip=skip, count=count)


def shift_date(shell, as_of=None, num_period=0, period_type='Q', anchor_period_type='Q', holidays='None'):
    '''shift_date(as_of, num_period, period_type='Q', anchor_period_type='Q', holidays ='None'
    Return a dict that contains the mnemonic name and other useful attributes for the shifted date '''
    if as_of:
        as_of = as_of.as_epoch()
    else:
        as_of = epoch(shell, ).as_epoch()
    ret = shell.api('/date/shift?d=%s&np=%s&pt=%s&apt=%s&h=%s' % (
        as_of, num_period, period_type, anchor_period_type, holidays), None)

    if isinstance(ret, dict) and '_error' in ret:
        print (ret)
    return ret


default_cache_pattern = "'%s%s_%s_%s' % (cache_key_prefix, b.as_datetime().strftime('%Y%m%d%H%M%S')" \
    + ", a.as_datetime().strftime('%Y%m%d%H%M%S'), h.as_datetime().strftime('%Y%m%d%H%M%S'))"


def default_cache_key(args):
    return eval(default_cache_pattern, None, args)


def get_as_of_tuples(shell,
                     period_basis='NOW',
                     begins=('BOQ',), horizon=('EOQ',), as_of=('TODAY',),
                     begin_shifts=[0],
                     horizon_shifts=[0],
                     cache_key_generator=default_cache_key,
                     time_offset=(3, 0),
                     min_offset=1,
                     cache_key_prefix='',
                     forward_run=False
                     ):
    aot = generate_as_of_tuples(
        shell,
        period_basis,
        begins, horizon, as_of, begin_shifts, horizon_shifts,
        cache_key_generator,
        time_offset, min_offset, cache_key_prefix)
    aot = sorted(list(set(list(aot))), key=lambda x: x[0])
    if forward_run:
        aot = filter(lambda x: x[2] <= x[1] <= x[3], aot)
    else:
        aot = filter(lambda x: x[1] <= x[2] <= x[3], aot)
    return aot


def generate_as_of_tuples(
    shell,
    period_basis='NOW',
    begins=('BOQ',),
    horizon=('EOQ',),
    as_of=('TODAY',),
    begin_shifts = [0],
    horizon_shifts = [0],
    cache_key_generator=default_cache_key,
    time_offset=(3, 0),
    min_offset=1,
    cache_key_prefix=''
):
    if isinstance(period_basis, EpochClass):
        today = period_basis
    else:
        today = epoch(shell, period_basis)

    today_dt = today.as_datetime()
    bom = epoch(shell, today_dt.year, today_dt.month, 1)
    bom_1 = bom - relativedelta.relativedelta(months=1)  # beginning of previous month
    eom_1 = bom - onems  # end of previous month
    eom_2 = bom_1 - onems  # end of two months ago
    bom__1 = bom + relativedelta.relativedelta(months=1)  # beginning of next month
    eom = bom__1 - onems  # end of month
    bom__2 = bom + relativedelta.relativedelta(months=2)  # beginning of month after next month
    eom__1 = bom__2 - onems  # end of next month

    orig_period = current_period(shell, today)
    orig_p_start = epoch(shell, orig_period['begin'])
    orig_p_end = epoch(shell, orig_period['end'])
    for begin_shift in begin_shifts:
        if begin_shift > 0:
            period = next_period(shell, today, skip=begin_shift)[0]
        elif begin_shift < 0:
            period = prev_period(shell, today, skip=-begin_shift)[0]
        else:
            period = orig_period
        p_start = epoch(shell, period['begin'])
        for horizon_shift in horizon_shifts:
            if horizon_shift > 0:
                period = next_period(shell, today, skip=horizon_shift)[0]
            elif horizon_shift < 0:
                period = prev_period(shell, today, skip=-horizon_shift)[0]
            else:
                period = orig_period
            p_end = epoch(shell, period['end'])
            for seq, (b, a, h) in enumerate(itertools.product(
                date_iterator(p_start, p_end, today, min_offset, begins, time_offset, True,
                              bom_1, bom, bom__1, bom__2, eom_2, eom_1, eom, eom__1),
                date_iterator(orig_p_start, orig_p_end, today, min_offset, as_of, time_offset, False,
                              bom_1, bom, bom__1, bom__2, eom_2, eom_1, eom, eom__1),
                date_iterator(p_start, p_end, today, min_offset, horizon, time_offset, False,
                              bom_1, bom, bom__1, bom__2, eom_2, eom_1, eom, eom__1),
            )):
                yield (default_cache_key(locals()), b.as_epoch(), a.as_epoch(), h.as_epoch())

days = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']


def date_iterator(
    start,
    end,
    today,
    min_offset,
    dates_to_include,
    offset,
    precise_time,
    bom_1,
    bom,
    bom__1,
    bom__2,
    eom_2,
    eom_1,
    eom,
    eom__1
):
    td = relativedelta.relativedelta(days=min_offset)
    oneday = relativedelta.relativedelta(days=1)
    precise_start = start
    precise_end = end
    start = date_with_offset(start, offset)
    end = date_with_offset(end, offset)
    today = date_with_offset(today, offset)

    precise_bom = bom
    precise_eom = eom
    precise_eom_1 = eom_1
    precise_eom_2 = eom_2
    precise_eom__1 = eom__1
    precise_bom_1 = bom_1
    precise_bom__1 = bom__1
    precise_bom__2 = bom__2
    bom = date_with_offset(bom, offset)
    bom_1 = date_with_offset(bom_1, offset)
    bom__1 = date_with_offset(bom__1, offset)
    bom__2 = date_with_offset(bom__2, offset)
    eom = date_with_offset(eom, offset)
    eom_1 = date_with_offset(eom_1, offset)
    eom_2 = date_with_offset(eom_2, offset)

    if 'BOM' in dates_to_include:
        yield precise_time and precise_bom or bom

    if 'EOM' in dates_to_include:
        yield precise_eom

    if 'EOM_1' in dates_to_include and precise_eom_1 > precise_start:
        yield precise_eom_1

    if 'EOM_2' in dates_to_include and precise_eom_2 > precise_start:
        yield precise_eom_2

    if 'EOM__1' in dates_to_include and precise_eom__1 <= precise_end:
        yield precise_eom__1

    if 'BOM_1' in dates_to_include and precise_bom_1 >= precise_start:
        yield precise_time and precise_bom_1 or bom_1

    if 'BOM__1' in dates_to_include and precise_bom__1 <= precise_end:
        yield precise_time and precise_bom__1 or bom__1

    if 'BOM__2' in dates_to_include and precise_bom__2 <= precise_end:
        yield precise_time and precise_bom__2 or bom__2

    if 'BOQ' in dates_to_include:
        yield precise_time and precise_start or start

    if 'FIRST_DAY' in dates_to_include:
        yield start
        next_date = start + td
    else:
        next_date = start

    if 'TODAY' in dates_to_include and start <= today < end:
        yield today
        black_hole = (today - td, today + td)
    else:
        black_hole = (start, start)

    if 'EOQ' in dates_to_include:
        yield precise_end

    if 'LAST_DAY' in dates_to_include:
        yield date_with_offset(end, offset)
        last_iterative_date = end - td
    else:
        last_iterative_date = end

    while next_date <= last_iterative_date:
        # Skip the black hole
        if black_hole[0] < next_date < black_hole[1]:
            next_date = black_hole[1]
            continue
        # Check if this date is any good and include it
        next_date_as_datetime = next_date.as_datetime()
        if next_date <= precise_end:
            if (
                days[next_date_as_datetime.weekday()] in dates_to_include or
                next_date_as_datetime.day in dates_to_include or
                (calendar.monthrange(next_date_as_datetime.year,
                                     next_date_as_datetime.month)[1] == next_date_as_datetime.day and
                 'LAST_DOM' in dates_to_include)
            ):
                yield next_date
                next_date += td
                continue
        # Increment the date
        next_date = next_date + oneday


def date_with_offset(n, t):
    return n + relativedelta.relativedelta(seconds=3600 * t[0] + 60 * t[1])


def get_a_date_time_as_float_some_how(date_like_thing):
    if not date_like_thing:
        return None
    # Convert modified date from string to an excel float
    try:
        a_date = float(date_like_thing)
        if (a_date > 100000):
            a_date = EpochClass.from_epoch(a_date).as_xldate()
    except TypeError:
        # TODO: Move the type error to the exception list below so that
        # we will try the fall back mechanism after we see why it is failing first
        logger.error("Unable to determine the date from %s" + str(date_like_thing))
        logger.error('Type of the argument is %s', str(type(date_like_thing)))
        return None
    except (ValueError):
        try:
            a_date = datetime2xl(EpochClass.from_string(cleanmmddyy(date_like_thing), '%m/%d/%Y').as_datetime())
        except:
            logger.error("Unable to determine the date from " + date_like_thing)
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
