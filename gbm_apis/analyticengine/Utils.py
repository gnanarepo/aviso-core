
from aviso.utils.dateUtils import current_period, next_period, EpochClass
from aviso.utils.dateUtils import datetime2xl
from dateutil.relativedelta import relativedelta
import pytz
import numbers
import numpy

SHIFT_CACHE={}
long=int
def num_holidays_between(day1, day2, holidays):
    '''returns the number of holidays between the two integers day1 and day2.
    if day1<=day2 the answer is non-negative
    if day1>=day2 the answer is non-positive'''
    if holidays is None:
        return 0
    try:
        assert(isinstance(day1, numbers.Integral))
    except:
        raise Exception('Error: you passed in non-integer ' + str(day1) + ' to num_holidays_between! \
Passing non-integers to this function is very confusing!')
    try:
        assert(isinstance(day2, numbers.Integral))
    except:
        raise Exception('Error: you passed in non-integer ' + str(day2) + ' to num_holidays_between! \
Passing non-integers to this function is very confusing!')
    try:
        assert(holidays[0] < day1 < holidays[-1])
    except:
        raise Exception("Error: you used a day " + str(day1) + " that is outside of the range of your holiday array! Update your holiday array!")
    try:
        assert(holidays[0] < day2 < holidays[-1])
    except:
        raise Exception("Error: you used a day " + str(day2) + " that is outside of the range of your holiday array! Update your holiday array!")
    return holidays.searchsorted(day2, side='left') - holidays.searchsorted(day1, side='left')


def num_businessdays_between(day1, day2, holidays):
    '''returns the number of business between the two integers day1 and day2.
    if day1<=day2 the answer is non-negative
    if day1>=day2 the answer is non-positive'''
    return day2 - day1 - num_holidays_between(day1, day2, holidays)


def add_business_days(day, n, holidays):
    '''returns the number of holidays between the two integers day1 and day2.
    if day1<=day2 the answer is non-negative
    if day1>=day2 the answer is non-positive'''
    if holidays is None or n == 0:
        return day + n
    try:
        assert(isinstance(day, numbers.Integral))
    except:
        raise Exception("Error: you passed in a  non-integer day " + str(day) + " to add_business_days")
    try:
        assert(isinstance(n, numbers.Integral))
    except:
        raise Exception("Error: you passed in a  non-integer num days " + str(n) + " to add_business_days")
    candidate = day + n
    error = num_holidays_between(day, candidate, holidays)
    while(error != 0):
        prev_cadidate = candidate
        candidate = candidate + error
        error = num_holidays_between(prev_cadidate, candidate, holidays)
    while is_holiday(candidate,holidays):
        if n>0:
            candidate = candidate +1
        else:
            candidate = candidate -1
    return candidate


def is_holiday(day, holidays):
    '''returns the True if given day is a holiday, False otherwise. '''
    if holidays is None:
        return False
    try:
        assert(isinstance(day, numbers.Integral))
    except:
        raise Exception('Error: you passed in non-integer ' + str(day) + ' to is_holiday! \
Passing non-integers to this function is very confusing!')
    try:
        assert(holidays[0] < day < holidays[-1])
    except:
        raise Exception("Error: you used a day " + str(day) + " that is outside of the range of your holiday array! Update your holiday array!")
    return day in holidays


def is_businessday(day, holidays):
    return not is_holiday(day, holidays)


def shift_date(as_of_dt, num_period, period_type, anchor_period_type, holidays, legacy_mode=False, holiday_name=None):
    #caching for holiday shift stuff
    #hilday name is required to get benefits of caching
    try:
        return SHIFT_CACHE[as_of_dt, num_period, period_type, anchor_period_type, holiday_name, legacy_mode]
    except KeyError:
        shift_res = _shift_date(as_of_dt, num_period, period_type, anchor_period_type, holidays, legacy_mode)
        if holiday_name is not None:
            SHIFT_CACHE[as_of_dt, num_period, period_type, anchor_period_type, holiday_name, legacy_mode] = shift_res
        return shift_res


def _shift_date(as_of_dt, num_period, period_type, anchor_period_type, holidays, legacy_mode=False):
    if isinstance(as_of_dt, str):
        as_of_dt = EpochClass.from_epoch(long(as_of_dt)).as_datetime()
    if not as_of_dt.tzinfo:
        as_of_dt = pytz.utc.localize(as_of_dt)
        as_of_dt = EpochClass.from_datetime(as_of_dt).as_datetime()

    hours_in_day = as_of_dt.hour
    minutes_in_day = as_of_dt.minute
    seconds_in_day = as_of_dt.second
    cp = current_period(as_of_dt, period_type=period_type)
    cp_end = EpochClass.from_datetime(cp.end).as_epoch()
    as_of_xl = datetime2xl(as_of_dt)
    as_of_hour = as_of_dt.hour
    as_of_int = int(as_of_xl)

    cp_anchor = current_period(as_of_dt, anchor_period_type)
    cp_anchor_begin = EpochClass.from_datetime(cp_anchor.begin).as_epoch()
    cp_anchor_end = EpochClass.from_datetime(cp_anchor.end).as_epoch()
    end_anchor = EpochClass.from_epoch(EpochClass.from_datetime(cp_anchor.end).as_epoch() + 1)
    end_anchor_xl = end_anchor.as_xldate()
    end_anchor_int = int(end_anchor_xl)

    mid_cp_anchors_to_end = cp_end + 1 - (cp_anchor_begin + cp_anchor_end + 1) / 2

    num_bdays_to_end = num_businessdays_between(as_of_int, end_anchor_int, holidays)

    period = next_period(as_of_dt, skip=num_period, period_type=period_type)
    mnemonic = period.mnemonic
    per_end = EpochClass.from_datetime(period.end).as_epoch()
    period_end_precise = EpochClass.from_epoch(per_end)
    period_end_precise_xl = period_end_precise.as_xldate()
    per_begin = EpochClass.from_datetime(period.begin).as_epoch()
    period_begin = EpochClass.from_epoch(per_begin)
    period_begin_xl = period_begin.as_xldate()
    period_mid_anchor = EpochClass.from_epoch(per_end + 1 - mid_cp_anchors_to_end).as_datetime()

    period_anchor = current_period(period_mid_anchor, period_type=anchor_period_type)
    anchor_mnemonic = period_anchor.mnemonic
    pa_end = EpochClass.from_datetime(period_anchor.end).as_epoch()
    period_anchor_end = EpochClass.from_epoch(pa_end + 1)
    period_anchor_end_precise = EpochClass.from_epoch(pa_end)
    period_anchor_end_precise_xl = period_anchor_end_precise.as_xldate()
    pa_begin = EpochClass.from_datetime(period_anchor.begin).as_epoch()
    period_anchor_begin = EpochClass.from_epoch(pa_begin)
    period_anchor_begin_xl = period_anchor_begin.as_xldate()
    period_anchor_end_xl = period_anchor_end.as_xldate()
    period_anchor_end_int = int(period_anchor_end_xl)

    result_int = add_business_days(period_anchor_end_int, -num_bdays_to_end, holidays)
    num_days = result_int - period_anchor_end_int
    num_days = numpy.int(num_days)

    result_whole = period_anchor_end + relativedelta(days=num_days)

    if legacy_mode:
        result_hour = result_whole.as_datetime().hour
        daylight_diff = as_of_hour - result_hour
        if daylight_diff == 23:
            daylight_diff = -1
        if daylight_diff == -23:
            daylight_diff = 1
        if daylight_diff in [-1, 1]:
            result_whole = result_whole + relativedelta(hours=daylight_diff)


    result = result_whole + relativedelta(hours=hours_in_day) + relativedelta(minutes=minutes_in_day) + relativedelta(seconds=seconds_in_day)
    if result < period_anchor_begin:
        result = period_anchor_begin
    if result > period_anchor_end_precise:
        result = period_anchor_end_precise

    if not legacy_mode:
        result_hour = result.as_datetime().hour
        daylight_diff = as_of_hour - result_hour

        if daylight_diff == 23:
            daylight_diff = -1
        if daylight_diff == -23:
            daylight_diff = 1
        if daylight_diff in [-1, 1]:
            result = result + relativedelta(hours=daylight_diff)

    # this is done purely for historical reasons--Maybe Sam can comment
    # I don't know why we should do this
    # I will remove this at some point once the code is stable
    # -Hessam
    three_hr_delay = period_anchor_begin + relativedelta(hours=3)
    if result < three_hr_delay:
        result = three_hr_delay

    result_xl = result.as_xldate()

    result_dict = {'epoch_date': result.as_epoch(),
                   'xl_date':result_xl,
                   'period_begin': period_begin.as_epoch(),
                   'period_begin_xl':period_begin_xl,
                   'period_end': period_end_precise.as_epoch(),
                   'period_end_xl': period_end_precise_xl,
                   'anchor_begin': period_anchor_begin.as_epoch(),
                   'anchor_begin_xl':period_anchor_begin_xl,
                   'anchor_end': period_anchor_end_precise.as_epoch(),
                   'anchor_end_xl': period_anchor_end_precise_xl,
                   'period_mnemonic': mnemonic,
                   'anchor_mnemonic': anchor_mnemonic}

    return result_dict
