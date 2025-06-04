# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 15:37:47 2019

@author: python
"""
import datetime
import pandas as pd
import pytz
import bisect
from datetime import timedelta
from typing import Union
from typing import Any

MAX_MONTH_RANGE = 23
MAX_WEEK_RANGE = 5


def ensure_utc(dt: Any, tz="Asia/Shanghai", fmt="%Y-%m-%d") -> float:
    """
    支持 datetime / timestamp(float) / string → UTC timestamp(float)
    """
    local_tz = pytz.timezone(tz)
    utc_tz = pytz.utc

    # 1. datetime.datetime 对象
    if isinstance(dt, datetime.datetime):
        if dt.tzinfo is None:
            dt = local_tz.localize(dt)
        dt_utc = dt.astimezone(utc_tz)

    # 2. float/int timestamp（秒）
    elif isinstance(dt, (float, int)):
        dt = datetime.datetime.fromtimestamp(dt, tz=local_tz)
        dt_utc = dt.astimezone(utc_tz)

    # 3. 字符串
    elif isinstance(dt, str):
        dt = datetime.datetime.strptime(dt, fmt)
        dt = local_tz.localize(dt)
        dt_utc = dt.astimezone(utc_tz)

    else:
        raise ValueError(f"Unsupported datetime format: {type(dt)}")
    # return dt_utc.timestamp()
    return dt_utc 

def date2utc(date, tzinfo="Asia/Shanghai"):
    struct_dt = datetime.datetime.strptime(str(date), '%Y%m%d')
    struct_dt = struct_dt.astimezone(tz=pytz.timezone(tzinfo))
    struct_dt = struct_dt.replace(tzinfo=datetime.timezone.utc) 
    return struct_dt

def market_utc(date, tzinfo="Asia/Shanghai"):
    format_dt = date2utc(date, tzinfo=tzinfo)
    m_open = format_dt + datetime.timedelta(hours=9, minutes=30)
    m_close = format_dt + datetime.timedelta(hours=15, minutes=0)
    # trans to utc
    return m_open, m_close

def naive_to_utc(ts):
    """
    Converts a UTC tz-naive timestamp to a tz-aware timestamp.
    """
    # Drop the nanoseconds field. warn=False suppresses the warning
    # that we are losing the nanoseconds; however, this is intended.
    return pd.Timestamp(ts.to_pydatetime(warn=False), tz='UTC')


# def ensure_utc(time, tz='UTC'):
#     """
#     Normalize a time. If the time is tz-naive, assume it is UTC.
#     """
#     if not time.tzinfo:
#         time = time.replace(tzinfo=pytz.timezone(tz))
#     return time.replace(tzinfo=pytz.utc)


def get_trading_range(date: Union[str, int], freq="M", opens=("9:30", "13:00"), closes=("11:30", "15:00")):
    #      ("opens", ("9:30", "13:00")),
    #      ("closes", ("11:30", "15:00"))
    dt = datetime.datetime.strptime(str(date), "%Y-%m-%d")
    intervals = zip(opens, closes)
    ticks = []
    for interval in intervals:
        open = dt + pd.Timedelta(interval[0])
        close = dt + pd.Timedelta(interval[1])
        ranges = pd.date_range(open, close, freq=freq, inclusive="left")
        ticks.extend(list(ranges))
    return ticks

def locate_index(calendar, start_time, end_time):
        """Locate the start time index and end time index in a calendar under certain frequency.

        Parameters
        ----------
        start_time : pd.Timestamp
            start of the time range.
        end_time : pd.Timestamp
            end of the time range.
        Returns
        -------
        pd.Timestamp
            the real start time.
        pd.Timestamp
            the real end time.
        int
            the index of start time.
        int
            the index of end time.
        """
        # start_time = pd.Timestamp(start_time.trading_date)
        # end_time = pd.Timestamp(end_time.trading_date)
        trading_days = [x.trading_date for x in calendar]
        s = bisect.bisect_left(trading_days, start_time)
        e = bisect.bisect_right(trading_days, end_time) -1 
        # loc = np.searchsorted(data["date"], cur_time_int, side="right")
        return s, e

def calc_delta(tick, _format="%Y%m%d%H%M"):
    # %-m 不补0
    formate_date = datetime.datetime.strptime(str(tick), _format)
    delta = formate_date - datetime.datetime(year=formate_date.year, month=formate_date.month, day=formate_date.day, hours=9, minutes=30)
    return delta.seconds, formate_date

def str2date(date_str, _format):
    return datetime.datetime.strptime(str(date_str), _format)

def parse_date_str_series(format_str, tz, date_str_series):
    tz_str = str(tz)
    if tz_str == pytz.utc.zone:

        parsed = pd.to_datetime(
            date_str_series.values,
            format=format_str,
            utc=True,
            errors='coerce',
        )
    else:
        parsed = pd.to_datetime(
            date_str_series.values,
            format=format_str,
            errors='coerce',
        ).tz_localize(tz_str).tz_convert('UTC')
    return parsed


def _out_of_range_error(a, b=None, var='offset'):
    start = 0
    if b is None:
        end = a - 1
    else:
        start = a
        end = b - 1
    return ValueError(
        '{var} must be in between {start} and {end} inclusive'.format(
            var=var,
            start=start,
            end=end,
        )
    )


def _td_check(td):
    seconds = td.total_seconds()

    # 43200 seconds = 12 hours
    if 60 <= seconds <= 43200:
        return td
    else:
        raise ValueError('offset must be in between 1 minute and 12 hours, '
                         'inclusive.')


def _build_offset(offset, kwargs, default):
    """
    Builds the offset argument for event rules.
    """
    # Filter down to just kwargs that were actually passed.
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    if offset is None:
        if not kwargs:
            return default  # use the default.
        else:
            return _td_check(datetime.timedelta(**kwargs))
    elif kwargs:
        raise ValueError('Cannot pass kwargs and an offset')
    elif isinstance(offset, datetime.timedelta):
        return _td_check(offset)
    else:
        raise TypeError("Must pass 'hours' and/or 'minutes' as keywords")


def _build_date(date, kwargs):
    """
    Builds the date argument for event rules.
    """
    if date is None:
        if not kwargs:
            raise ValueError('Must pass a date or kwargs')
        else:
            return datetime.date(**kwargs)

    elif kwargs:
        raise ValueError('Cannot pass kwargs and a date')
    else:
        return date


def _build_time(time, kwargs):
    """
    Builds the time argument for event rules.
    """
    tz = kwargs.pop('tz', 'UTC')
    if time:
        if kwargs:
            raise ValueError('Cannot pass kwargs and a time')
        else:
            return ensure_utc(time, tz)
    elif not kwargs:
        raise ValueError('Must pass a time or kwargs')
    else:
        return datetime.time(**kwargs)


def _time_to_micros(time):
    """Convert a time into microseconds since midnight.
    Parameters
    ----------
    time : datetime.time
        The time to convert.
    Returns
    -------
    us : int
        The number of microseconds since midnight.
    Notes
    -----
    This does not account for leap seconds or daylight savings.
    """
    seconds = time.hour * 60 * 60 + time.minute * 60 + time.second
    return 1000000 * seconds + time.microsecond


def timedelta_to_integral_seconds(delta):
    """
    Convert a pd.Timedelta to a number of seconds as an int.
    """
    return int(delta.total_seconds())


def timedelta_to_integral_minutes(delta):
    """
    Convert a pd.Timedelta to a number of minutes as an int.
    """
    return timedelta_to_integral_seconds(delta) // 60


def normalize_quarters(years, quarters):
    return years * 4 + quarters - 1


def split_normalized_quarters(normalized_quarters):
    years = normalized_quarters // 4
    quarters = normalized_quarters % 4
    return years, quarters + 1


def date_gen(start,
             end,
             trading_calendar,
             delta=timedelta(minutes=1),
             repeats=None):
    """
    Utility to generate a stream of dates.
    """
    daily_delta = not (delta.total_seconds()
                       % timedelta(days=1).total_seconds())
    cur = start
    if daily_delta:
        # if we are producing daily timestamps, we
        # use midnight
        cur = cur.replace(hour=0, minute=0, second=0,
                          microsecond=0)

    def advance_current(cur):
        """
        Advances the current dt skipping non market days and minutes.
        """
        cur = cur + delta

        currently_executing = \
            (daily_delta and (cur in trading_calendar.all_sessions)) or \
            (trading_calendar.is_open_on_minute(cur))

        if currently_executing:
            return cur
        else:
            if daily_delta:
                return trading_calendar.minute_to_session_label(cur)
            else:
                return trading_calendar.open_and_close_for_session(
                    trading_calendar.minute_to_session_label(cur)
                )[0]

    # yield count trade events, all on trading days, and
    # during trading hours.
    while cur < end:
        if repeats:
            for j in range(repeats):
                yield cur
        else:
            yield cur

        cur = advance_current(cur)
