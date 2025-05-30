
# Built in imports
from datetime import datetime, timedelta
import re
import pytz

# Third part library imports
# TODO: Replace this with new library for Infra repo
from aviso.settings import sec_context

# Local imports
from .relativedelta import relativedelta


excelBase = datetime(1899, 12, 30, 0, 0, 0)
base_epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)

class EpochClass(object):
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
        '''
        Useful for passing to the holidays which are defined as tenant excel date integers
        '''
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
            result_1 = EpochClass(self.ts + (whole_days))
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
            result_1 = EpochClass(self.ts + (whole_days))
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
