enum RangeBoundType
{
	open,
	closed
}

auto isOpen(RangeBoundType bound)
{
	return (bound==RangeBoundType.open);
}

auto isClosed(RangeBoundType bound)
{
	return (bound==RangeBoundType.closed);
}

auto andOpen(bool)(RangeBoundType left, RangeBoundType right)
{
	return (left.isOpen && right.isOpen);
}
auto andOpen(RangeBoundType)(RangeBoundType left, RangeBoundType right)
{
	return left.andOpen(right)?RangeBoundType.open:RangeBoundType.closed;
}
auto orOpen(bool)(RangeBoundType left, RangeBoundType right)
{
	return left.orOpen(right)?RangeBoundType.open:RangeBoundType.closed;
}
auto andOpen(RangeBoundType left, RangeBoundType right)
{
	return left.andOpen!RangeBoundType(right);
}
auto orOpen(RangeBoundType left, RangeBoundType right)
{
	return left.orOpen!RangeBoundType(right);
}

struct DateRangeNode(DateType)
{
	Nullable!DateType date;
	RangeBoundType bound;
}
struct DateRange(DateType)
{
	DateRangeNode!DateType start;
	DateRangeNode!DateType end;

    auto opCmp(SomeDateRange)(SomeDateRange right)
    {
    	if ((right.end.date==left.end.date) && (right.start.date==left.start.date))
    		return 0;
    	if (left.start.date<right.start.date)
    		return -1;
    	if (left.end.date>=right.end.date)
    		return 1;
    	throw new Exception("date Ranges "~left.to!string~" and "~right.to!string~" are not comparable");
    }
/+
    auto toHash()
    {
    	// start, end, step, interval;
    }
+/
}


alias DateRangeBoundsTuple=Tuple!(RangeBoundType, "left",RangeBoundType,"right");

auto boundsTuple(SomeDateRange)(SomeDateRange dateRange)
{
	return DateRangeBoundsTuple(dateRange.left.bound, dateRange.right.bound);
}

bool isUnbounded(DateType)(DateRange!DateType dateRange)
{
	return (dateRange.start.bound.isNull || dateRange.end.bound.isNull);
}


auto intersectionHelperStart(LeftDateRangeNode, RightDateRangeNode, ReturnDateRangeNodeType)
	(LeftDateRangeNode left, RightDateRangeNode right)
{
	ReturnDateRangeNodeType ret;
	if (left.date.isNull)
		ret.bound=right.bound;
	else if (right.date.isNull)
		ret.bound=left.bound;
	else if(left.date<right.date)
		ret.bound=other.bound;
	else if (left.date>right.date)
		ret.bound=left.bound;
	else ret.bound=left.bound.andOpen!RangeBoundType(right.bound);

	if (right.date.isNull)
		ret.date=left.date;
	else if (left.date.isNull)
		ret.date=other.date;
	else
		ret.date=max(left.date,right.date);

	return ret;
}

auto intersectionHelperEnd(LeftDateRangeNode, RightDateRangeNode, ReturnDateRangeNodeType)
	(LeftDateRangeNode left, RightDateRangeNode right)
{
	ReturnDateRangeNodeType ret;
	if (left.date.isNull)
		ret.bound=right.bound;
	else if (right.date.isNull)
		ret.bound=left.bound;
	else if(left.date>right.date)
		ret.bound=other.bound;
	else if (left.date<right.date)
		ret.bound=left.bound;
	else ret=left.bound.andOpen!RangeBoundType(right.bound);

	if (right.date.isNull)
		ret.date=left.date;
	else if (left.date.isNull)
		ret.date=other.date;
	else
		ret.date=min(left.date, other.date);
	return ret;
}

auto intersection(LeftDateRange, RightDateRange, ReturnDateRange=LeftDateRange)(LeftDateRange left, RightDateRange right)
{
	ReturnDateRange ret;
	ret.start=intersectionHelperStart(left.start, right.start);
	ret.end=intersectionHelperEnd(left.end,right.end);
	return ret;
}

string asMongoQueryString(RangeBoundType rangeBound)
{
	return rangeBound.isOpen?"t":"te";
}
auto asMongoQuery(SomeDateRangeNode)(SomeDateRangeNode node)
{
	// mongo can only handle DateTimes in queries
	if (!node.date.isNull)
		return tuple(arg.start.bound.asMongoQueryString,arg.date.asDateTime(node.bound).to!string);
	else
		return tuple(null,null);
}

DateTime asDateTime(SomeDateType)(SomeDateType date, RangeBoundType rangeBound)
{
	// fixme - should be careful about RangeBoundType logic
	return date.to!DateTime;
}
string[string] asMongoQuery(DateRange)(DateRange arg)
{
	string[string] query;
	if (!arg.start.date.isNull)
	{
		auto tup=arg.start.asMongoQuery;
		query["$g" ~ tup[0]] = tup[0];
	}

	if (!arg.end.date.isNull)
	{
		auto tup=arg.end.asMongoQuery;
		query["$l"~tup[0]= = tup[1];
	}
	return query;
}

string asQueryTerm(DateRangeType)(DateRangeType dateRange)
{
	auto ret=appender!string;
	if (!dateRange.start.date.isNull)
	{
		ret.put(dateRange.start.bound.isOpen?">=":">");
		ret.put(dateRange.start.date.asDateTime(dateRange.start.bound).to!string);
	}
	if (!dateRange.end.date.isNull)
	{
		ret.put(dateRange.end.bound.isOpen?"<=":"<");
		ret.put(dateRange.end.date.asDateTime(dateRange.end.bound).to!string);
	}
	return ret;
}

bool contains(SomeDateRange, SomeDate)(SomeDateRange dateRange, SomeDate date)
{
	alias BoundsT=DateRangeBoundsTuple;

	with(RangeBoundType)
	{
		return dateRange.boundsTuple.predSwitch(
			BoundsT(closed,closed),
				((dateRange.start.date.isNull || date >= dateRange.start.date) &&
				(dateRange.end.date.isNull || date <= dateRange.end.date)),
			BoundsT(closed,open),
				((dateRange.start.date.isNull || date>=dateRange.start.date) &&
				(dateRange.end.date.isNull || date < dateRange.end.date)),
			BoundsT(open,closed),
				((dateRange.start.date.isNull || date > dateRange.start.date) &&
				(dateRange.end.date.isNull || date <= dateRange.end.date.get)),
			BoundsT(open,open),
				((dateRange.start.date.isNull || date >dateRange.start.date.get) &&
				(dateRange.end.date.isNull || date <dateRange.end.date.get))
			);
	}
	assert(0);

}

string toString(RangeBoundType boundType)
{
	return boundType.isOpen?"O":"C";
}

string toString(DateType)(Nullable!DateType date)
{
	return (date.isNull)?"empty":date.get.to!string;
}

string toString(SomeDateRange)(SomeDateRange dateRange)
{
	return format("date range(%s[%s], %s[%s])",dateRange.start.date,dateRange.start.bound,dateRange.end.date,dateRange.end.bound);
}

from enum import Enum


class Intervals(Enum):
    (OPEN_CLOSED, CLOSED_OPEN, OPEN_OPEN, CLOSED_CLOSED) = range(1101, 1105)
(OPEN_CLOSED, CLOSED_OPEN, OPEN_OPEN, CLOSED_CLOSED) = INTERVALS = Intervals.__members__.values()


class GeneralSlice(object):
    """General slice object, supporting open/closed ranges:

    =====  ====  ============================  ===============================
    start  end  interval                      Meaning
    -----  ----  ----------------------------  -------------------------------
    None   None                                any item
    a      None  CLOSED_CLOSED or CLOSED_OPEN  item >= a
    a      None  OPEN_CLOSED or OPEN_OPEN      item > a
    None   b     CLOSED_CLOSED or OPEN_CLOSED  item <= b
    None   b     CLOSED_OPEN or OPEN_OPEN      item < b
    a      b     CLOSED_CLOSED                 item >= a and item <= b
    a      b     OPEN_CLOSED                   item > a and item <= b
    a      b     CLOSED_OPEN                   item >= a and item < b
    a      b     OPEN_OPEN                     item > a and item < b
    =====  ====  ============================  ===============================
    """

    def __init__(self, start, end, step=None, interval=CLOSED_CLOSED):
        self.start = start
        self.end = end
        self.step = step
        self.interval = interval

    @property
    def startopen(self):
        """True if the start of the range is open (item > start),
        False if the start of the range is closed (item >= start)."""
        return self.interval in (OPEN_CLOSED, OPEN_OPEN)

    @property
    def endopen(self):
        """True if the end of the range is open (item < end),
        False if the end of the range is closed (item <= end)."""
        return self.interval in (CLOSED_OPEN, OPEN_OPEN)

import bisect
import os
import dateutil
import tzlocal

DEFAULT_TIME_ZONE_NAME = tzlocal.get_localzone().zone  # 'Europe/London'
TIME_ZONE_DATA_SOURCE = '/usr/share/zoneinfo/'


class TimezoneError(Exception):
    pass


class tzfile(dateutil.tz.tzfile):

    def _find_ttinfo(self, dtm, laststd=0):
        """Faster version of parent class's _find_ttinfo() as this uses bisect rather than a linear search."""
        if dtm is None:
            # This will happen, for example, when a datetime.time object gets utcoffset() called.
            raise ValueError('tzinfo object can not calculate offset for date %s' % dtm)
        ts = ((dtm.toordinal() - dateutil.tz.EPOCHORDINAL) * 86400
                     + dtm.hour * 3600
                     + dtm.minute * 60
                     + dtm.second)
        idx = bisect.bisect_right(self._trans_list, ts)
        if len(self._trans_list) == 0 or idx == len(self._trans_list):
            return self._ttinfo_std
        if idx == 0:
            return self._ttinfo_before
        if laststd:
            while idx > 0:
                tti = self._trans_idx[idx - 1]
                if not tti.isdst:
                    return tti
                idx -= 1
            else:
                return self._ttinfo_std
        else:
            return self._trans_idx[idx - 1]


def mktz(zone=None):
    """
    Return a new timezone based on the zone using the python-dateutil
    package.  This convenience method is useful for resolving the timezone
    names as dateutil.tz.tzfile requires the full path.

    The concise name 'mktz' is for convenient when using it on the
    console.

    Parameters
    ----------
    zone : `String`
           The zone for the timezone. This defaults to 'local'.

    Returns
    -------
    An instance of a timezone which implements the tzinfo interface.

    Raises
    - - - - - -
    TimezoneError : Raised if a user inputs a bad timezone name.
    """

    if zone is None:
        zone = DEFAULT_TIME_ZONE_NAME
    _path = os.path.join(TIME_ZONE_DATA_SOURCE, zone)
    try:
        tz = tzfile(_path)
    except (ValueError, IOError) as err:
        raise TimezoneError('Timezone "%s" can not be read, error: "%s"' % (zone, err))
    # Stash the zone name as an attribute (as pytz does)
    tz.zone = zone if not zone.startswith(TIME_ZONE_DATA_SOURCE) else zone[len(TIME_ZONE_DATA_SOURCE):]
    return tz
from dateutil.parser import parse as _parse


def parse(string, agnostic=False, **kwargs):
    parsed = _parse(string, **kwargs)
    if agnostic or (parsed == _parse(string, yearfirst=True, **kwargs)
                           == _parse(string, dayfirst=True, **kwargs)):
        return parsed
    else:
        raise ValueError("The date was ambiguous: %s" % string)
import calendar
import datetime
from datetime import timedelta

from ._daterange import DateRange
from ._generalslice import OPEN_OPEN, CLOSED_CLOSED, OPEN_CLOSED, CLOSED_OPEN
from ._parse import parse
from ._mktz import mktz
import sys
if sys.version_info > (3,):
    long = int

    
# Support standard brackets syntax for open/closed ranges.
Ranges = {'()': OPEN_OPEN,
          '(]': OPEN_CLOSED,
          '[)': CLOSED_OPEN,
          '[]': CLOSED_CLOSED}


def string_to_daterange(str_range, delimiter='-', as_dates=False, interval=CLOSED_CLOSED):
    """
    Convert a string to a DateRange type. If you put only one date, it generates the
    relevant range for just that date or datetime till 24 hours later. You can optionally
    use mixtures of []/() around the DateRange for OPEN/CLOSED interval behaviour.

    Parameters
    ----------
    str_range : `String`
        The range as a string of dates separated by one delimiter.

    delimiter : `String`
        The separator between the dates, using '-' as default.

    as_dates : `Boolean`
        True if you want the date-range to use datetime.date rather than datetime.datetime.

    interval : `int`
               CLOSED_CLOSED, OPEN_CLOSED, CLOSED_OPEN or OPEN_OPEN.
               **Default is CLOSED_CLOSED**.

    Returns
    -------
        `arctic.date.DateRange` : the DateRange parsed from the string.

    Examples
    --------
    >>> from arctic.date import string_to_daterange
    >>> string_to_daterange('20111020', as_dates=True)
    DateRange(start=datetime.date(2011, 10, 20), end=datetime.date(2011, 10, 21))

    >>> string_to_daterange('201110201030')
    DateRange(start=datetime.datetime(2011, 10, 20, 10, 30), end=datetime.datetime(2011, 10, 21, 10, 30))

    >>> string_to_daterange('20111020-20120120', as_dates=True)
    DateRange(start=datetime.date(2011, 10, 20), end=datetime.date(2012, 1, 20))

    >>> string_to_daterange('[20111020-20120120)', as_dates=True)
    DateRange(start=datetime.date(2011, 10, 20), end=datetime.date(2012, 1, 20))
    """
    num_dates = str_range.count(delimiter) + 1
    if num_dates > 2:
        raise ValueError('Too many dates in input string [%s] with delimiter (%s)' % (str_range, delimiter))

    # Allow the user to use the [date-date), etc. range syntax to specify the interval.
    range_mode = Ranges.get(str_range[0] + str_range[-1], None)
    if range_mode:
        return string_to_daterange(str_range[1:-1], delimiter, as_dates, interval=range_mode)

    if as_dates:
        parse_dt = lambda s: parse(s).date() if s else None
    else:
        parse_dt = lambda s: parse(s) if s else None
    if num_dates == 2:
        d = [parse_dt(x) for x in str_range.split(delimiter)]
        oc = interval
    else:
        start = parse_dt(str_range)
        d = [start, start + datetime.timedelta(1)]
        oc = CLOSED_OPEN  # Always use closed-open for a single date/datetime.
    return DateRange(d[0], d[1], oc)


def to_dt(date, default_tz=None):
    """
    Returns a non-naive datetime.datetime.
    
    Interprets numbers as ms-since-epoch.

    Parameters
    ----------
    date : `int` or `datetime.datetime`
        The datetime to convert

    default_tz : tzinfo
        The TimeZone to use if none is found.  If not supplied, and the
        datetime doesn't have a timezone, then we raise ValueError

    Returns
    -------
    Non-naive datetime
    """
    if isinstance(date, (int, long)):
        return ms_to_datetime(date, default_tz)
    elif date.tzinfo is None:
        if default_tz is None:
            raise ValueError("Must specify a TimeZone on incoming data")
        return date.replace(tzinfo=default_tz)
    return date


def to_pandas_closed_closed(date_range, add_tz=True):
    """
    Pandas DateRange slicing is CLOSED-CLOSED inclusive at both ends.

    Parameters
    ----------
    date_range : `DateRange` object 
        converted to CLOSED_CLOSED form for Pandas slicing

    add_tz : `bool`
        Adds a TimeZone to the daterange start and end if it doesn't
        have one.

    Returns
    -------
    Returns a date_range with start-end suitable for slicing in pandas.
    """
    if not date_range:
        return None

    start = date_range.start
    end = date_range.end
    if start:
        start = to_dt(start, mktz()) if add_tz else start
        if date_range.startopen:
            start += timedelta(milliseconds=1)

    if end:
        end = to_dt(end, mktz()) if add_tz else end
        if date_range.endopen:
            end -= timedelta(milliseconds=1)
    return DateRange(start, end)


def ms_to_datetime(ms, tzinfo=None):
    """Convert a millisecond time value to an offset-aware Python datetime object."""
    if not isinstance(ms, (int, long)):
        raise TypeError('expected integer, not %s' % type(ms))

    if tzinfo is None:
        tzinfo = mktz()

    return datetime.datetime.fromtimestamp(ms * 1e-3, tzinfo)


def _add_tzone(dtm):
    if dtm.tzinfo is None:
        dtm = dtm.replace(tzinfo=mktz())
    return dtm


def datetime_to_ms(d):
    """Convert a Python datetime object to a millisecond epoch (UTC) time value."""
    try:
        return long((calendar.timegm(_add_tzone(d).utctimetuple()) + d.microsecond / 1000000.0) * 1e3)
    except AttributeError:
        raise TypeError('expect Python datetime object, not %s' % type(d))
