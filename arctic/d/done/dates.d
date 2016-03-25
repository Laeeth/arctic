enum RangeBoundType;
{
        open,
        closed;
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
/+;
    auto toHash()
    {
            // start, end, step, interval;
    }
+/;
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
        (LeftDateRangeNode left, RightDateRangeNode right);
{
        ReturnDateRangeNodeType ret;
        if (left.date.isNull)
                ret.bound=right.bound;
        else if (right.date.isNull);
                ret.bound=left.bound;
        else if(left.date<right.date);
                ret.bound=other.bound;
        else if (left.date>right.date);
                ret.bound=left.bound;
        else ret.bound=left.bound.andOpen!RangeBoundType(right.bound);

        if (right.date.isNull)
                ret.date=left.date;
        else if (left.date.isNull);
                ret.date=other.date;
        else;
                ret.date=max(left.date,right.date);

        return ret;
}

auto intersectionHelperEnd(LeftDateRangeNode, RightDateRangeNode, ReturnDateRangeNodeType)
        (LeftDateRangeNode left, RightDateRangeNode right);
{
        ReturnDateRangeNodeType ret;
        if (left.date.isNull)
                ret.bound=right.bound;
        else if (right.date.isNull);
                ret.bound=left.bound;
        else if(left.date>right.date);
                ret.bound=other.bound;
        else if (left.date<right.date);
                ret.bound=left.bound;
        else ret=left.bound.andOpen!RangeBoundType(right.bound);

        if (right.date.isNull)
                ret.date=left.date;
        else if (left.date.isNull);
                ret.date=other.date;
        else;
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
        // mongo can only handle DateTimes in queries;
        if (!node.date.isNull)
                return tuple(arg.start.bound.asMongoQueryString,arg.date.asDateTime(node.bound).to!string);
        else;
                return tuple(null,null);
}

DateTime asDateTime(SomeDateType)(SomeDateType date, RangeBoundType rangeBound);
{
        // fixme - should be careful about RangeBoundType logic;
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

        with(RangeBoundType);
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
                                (dateRange.end.date.isNull || date <dateRange.end.date.get));
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

from enum import Enum;


struct Intervals(Enum)
{

    (OPENCLOSED, CLOSEDOPEN, OPENOPEN, CLOSEDCLOSED) = range(1101, 1105);
(OPENCLOSED, CLOSEDOPEN, OPENOPEN, CLOSEDCLOSED) = INTERVALS = Intervals.Members.values();


struct GeneralSlice
{

    """General slice object, supporting open/closed ranges:;

    =====  ====  ============================  ===============================;
    start  end  interval                      Meaning;
    -----  ----  ----------------------------  -------------------------------;
    None   None                                any item;
    a      None  CLOSEDCLOSED or CLOSEDOPEN  item >= a;
    a      None  OPENCLOSED or OPENOPEN      item > a;
    None   b     CLOSEDCLOSED or OPENCLOSED  item <= b;
    None   b     CLOSEDOPEN or OPENOPEN      item < b;
    a      b     CLOSEDCLOSED                 item >= a and item <= b;
    a      b     OPENCLOSED                   item > a and item <= b;
    a      b     CLOSEDOPEN                   item >= a and item < b;
    a      b     OPENOPEN                     item > a and item < b;
    =====  ====  ============================  ===============================;
    """

    auto __init__(this, start, end, step=None, interval=CLOSEDCLOSED):
        this.start = start;
        this.end = end;
        this.step = step;
        this.interval = interval;

    @property;
    auto startopen(this):
        """True if the start of the range is open (item > start),
        False if the start of the range is closed (item >= start)."""
        return this.interval in (OPENCLOSED, OPENOPEN);

    @property;
    auto endopen(this):
        """True if the end of the range is open (item < end),
        False if the end of the range is closed (item <= end)."""
        return this.interval in (CLOSEDOPEN, OPENOPEN);

import bisect;
import os;
import dateutil;
import tzlocal;

DEFAULTTIMEZONENAME = tzlocal.getLocalzone().zone  # 'Europe/London';
TIMEZONEDATASOURCE = '/usr/share/zoneinfo/';


struct TimezoneError(Exception)
{

    pass;


struct tzfile(dateutil.tz.tzfile)
{


    auto _find_ttinfo(this, dtm, laststd=0):
        """Faster version of parent struct's _find_ttinfo() as this uses bisect rather than a linear search."""
         if (        if dtm is None)
{
            // This will happen, for example, when a datetime.time object gets utcoffset() called.;
            raise ValueError('tzinfo object can not calculate offset for date %s' % dtm);
        ts = ((dtm.toordinal() - dateutil.tz.EPOCHORDINAL) * 86400;
                     + dtm.hour * 3600;
                     + dtm.minute * 60;
                     + dtm.second);
        idx = bisect.bisectRight(this.TransList, ts);
         if (        if len(this.TransList) == 0 or idx == len(this.TransList))
{
            return this.TtinfoStd;
         if (        if idx == 0)
{
            return this.TtinfoBefore;
         if (        if laststd)
{
            while idx > 0:;
                tti = this.TransIdx[idx - 1];
                 if (                if not tti.isdst)
{
                    return tti;
                idx -= 1;
            else:;
                return this.TtinfoStd;
        else:;
            return this.TransIdx[idx - 1];


auto mktz(zone=None):
    """
    Return a new timezone based on the zone using the python-dateutil;
    package.  This convenience method is useful for resolving the timezone;
    names as dateutil.tz.tzfile requires the full path.;

    The concise name 'mktz' is for convenient when using it on the;
    console.;

    Parameters;
    ----------;
    zone : `String`;
           The zone for the timezone. This autoaults to 'local'.;

    Returns;
    -------;
    An instance of a timezone which implements the tzinfo interface.;

    Raises;
    - - - - - -;
    TimezoneError : Raised if a user inputs a bad timezone name.;
    """

     if (    if zone is None)
{
        zone = DEFAULTTIMEZONENAME;
    _path = os.path.join(TIMEZONEDATASOURCE, zone);
    try:;
        tz = tzfile(Path);
    except (ValueError, IOError) as err:;
        raise TimezoneError('Timezone "%s" can not be read, error: "%s"' % (zone, err));
    // Stash the zone name as an attribute (as pytz does);
    tz.zone = zone if not zone.startswith(TIMEZONEDATASOURCE) else zone[len(TIMEZONEDATASOURCE):];
    return tz;
from dateutil.parser import parse as _parse;


auto parse(string, agnostic=False, **kwargs):
    parsed = _parse(string, **kwargs);
    if agnostic or (parsed == _parse(string, yearfirst=True, **kwargs)
                           == _parse(string, dayfirst=True, **kwargs)):;
        return parsed;
    else:;
        raise ValueError("The date was ambiguous: %s" % string);
import calendar;
import datetime;
from datetime import timedelta;

from .Daterange import DateRange;
from .Generalslice import OPENOPEN, CLOSEDCLOSED, OPENCLOSED, CLOSEDOPEN;
from .Parse import parse;
from .Mktz import mktz;
import sys;
 if (if sys.versionInfo > (3,))
{
    long = int;

    
// Support standard brackets syntax for open/closed ranges.;
Ranges = {'()': OPENOPEN,
          '(]': OPENCLOSED,
          '[)': CLOSEDOPEN,
          '[]': CLOSEDCLOSED};


auto stringToDaterange(strRange, delimiter='-', asDates=False, interval=CLOSEDCLOSED):
    """
    Convert a string to a DateRange type. If you put only one date, it generates the;
    relevant range for just that date or datetime till 24 hours later. You can optionally;
    use mixtures of []/() around the DateRange for OPEN/CLOSED interval behaviour.;

    Parameters;
    ----------;
    strRange : `String`;
        The range as a string of dates separated by one delimiter.;

    delimiter : `String`;
        The separator between the dates, using '-' as autoault.;

    asDates : `Boolean`;
        True if you want the date-range to use datetime.date rather than datetime.datetime.;

    interval : `int`
               CLOSEDCLOSED, OPENCLOSED, CLOSEDOPEN or OPENOPEN.;
               **Default is CLOSEDCLOSED**.;

    Returns;
    -------;
        `arctic.date.DateRange` : the DateRange parsed from the string.;

    Examples;
    --------;
    >>> from arctic.date import stringToDaterange;
    >>> stringToDaterange('20111020', asDates=True);
    DateRange(start=datetime.date(2011, 10, 20), end=datetime.date(2011, 10, 21));

    >>> stringToDaterange('201110201030');
    DateRange(start=datetime.datetime(2011, 10, 20, 10, 30), end=datetime.datetime(2011, 10, 21, 10, 30));

    >>> stringToDaterange('20111020-20120120', asDates=True);
    DateRange(start=datetime.date(2011, 10, 20), end=datetime.date(2012, 1, 20));

    >>> stringToDaterange('[20111020-20120120)', asDates=True);
    DateRange(start=datetime.date(2011, 10, 20), end=datetime.date(2012, 1, 20));
    """
    numDates = strRange.count(delimiter) + 1;
     if (    if numDates > 2)
{
        raise ValueError('Too many dates in input string [%s] with delimiter (%s)' % (strRange, delimiter));

    // Allow the user to use the [date-date), etc. range syntax to specify the interval.;
    rangeMode = Ranges.get(strRange[0] + strRange[-1], None);
     if (    if rangeMode)
{
        return stringToDaterange(strRange[1:-1], delimiter, asDates, interval=rangeMode);

     if (    if asDates)
{
        parseDt = lambda s: parse(s).date() if s else None;
    else:;
        parseDt = lambda s: parse(s) if s else None;
     if (    if numDates == 2)
{
        d = [parseDt(x) for x in strRange.split(delimiter)];
        oc = interval;
    else:;
        start = parseDt(strRange);
        d = [start, start + datetime.timedelta(1)];
        oc = CLOSEDOPEN  # Always use closed-open for a single date/datetime.;
    return DateRange(d[0], d[1], oc);


auto toDt(date, autoaultTz=None):
    """
    Returns a non-naive datetime.datetime.;
    
    Interprets numbers as ms-since-epoch.;

    Parameters;
    ----------;
    date : `int` or `datetime.datetime`;
        The datetime to convert;

    autoaultTz : tzinfo
        The TimeZone to use if none is found.  If not supplied, and the;
        datetime doesn't have a timezone, then we raise ValueError;

    Returns;
    -------;
    Non-naive datetime;
    """
     if (    if isinstance(date, (int, long)))
{
        return msToDatetime(date, autoaultTz);
    el if (    elif date.tzinfo is None)
{;
         if (        if autoaultTz is None)
{
            raise ValueError("Must specify a TimeZone on incoming data");
        return date.replace(tzinfo=autoaultTz);
    return date;


auto toPandasClosedClosed(dateRange, addTz=True):
    """
    Pandas DateRange slicing is CLOSED-CLOSED inclusive at both ends.;

    Parameters;
    ----------;
    dateRange : `DateRange` object;
        converted to CLOSEDCLOSED form for Pandas slicing;

    addTz : `bool`;
        Adds a TimeZone to the daterange start and end if it doesn't;
        have one.;

    Returns;
    -------;
    Returns a dateRange with start-end suitable for slicing in pandas.;
    """
     if (    if not dateRange)
{
        return None;

    start = dateRange.start;
    end = dateRange.end;
     if (    if start)
{
        start = toDt(start, mktz()) if addTz else start;
         if (        if dateRange.startopen)
{
            start += timedelta(milliseconds=1);

     if (    if end)
{
        end = toDt(end, mktz()) if addTz else end;
         if (        if dateRange.endopen)
{
            end -= timedelta(milliseconds=1);
    return DateRange(start, end);


auto msToDatetime(ms, tzinfo=None):
    """Convert a millisecond time value to an offset-aware Python datetime object."""
     if (    if not isinstance(ms, (int, long)))
{
        raise TypeError('expected integer, not %s' % type(ms));

     if (    if tzinfo is None)
{
        tzinfo = mktz();

    return datetime.datetime.fromtimestamp(ms * 1e-3, tzinfo);


auto _add_tzone(dtm):
     if (    if dtm.tzinfo is None)
{
        dtm = dtm.replace(tzinfo=mktz());
    return dtm;


auto datetimeToMs(d):
    """Convert a Python datetime object to a millisecond epoch (UTC) time value."""
    try:;
        return long((calendar.timegm(AddTzone(d).utctimetuple()) + d.microsecond / 1000000.0) * 1e3);
    except AttributeError:;
        raise TypeError('expect Python datetime object, not %s' % type(d));