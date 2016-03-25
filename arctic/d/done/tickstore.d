module arctic.tickstore;

import std.experimental.logger;
import std.datetime;

// bisect, re, pandas, pymongo
from datetime import datetime as dt, date, time, timedelta;
// re;
// from timeit import itertools;

from ..date import mktz, DateRange, OPENOPEN, CLOSEDCLOSED;
from ..exceptions import (NoDataFoundException, UnhandledDtypeException, OverlappingDataException,
                          LibraryNotFoundException);

logger = logging.getLogger(Name);

alias TickStoreLibrary = Tuple!(Library,library", DateRange,"dateRange");

enum TICKSTORETYPE = "TopLevelTickStore";
enum PATTERN = "^%s\d{4}.%s";
enum YEARREGEX = re.compile("\d{4}");

auto endTimeMin()
{
    return (dt.combine(date.today(), time.min) - dur!"msecs"(1);
}



struct TopLevelTickStore
{
    auto initializeLibrary(cls, arcticLib, **kwargs)    // classmethod
    {
        auto tl = TopLevelTickStore(arcticLib);
        tl.addLibraries();
        tl.ensureIndex();
    }

    auto _ensure_index()
    {
        auto collection = this.Collection;
        collection.createIndex([("start", pymongo.ASCENDING)], background=True);
    }

    auto _add_libraries()
    {
        name = this.getName();
        dbName, tickType = name.split(".", 1);
        regex = re.compile(PATTERN % (dbName, tickType));
        foreach(lib;this.arctiLib.arctic.listLibraries.filter!(lib=>lib.regex.search))
        {
            auto year = int(YEARREGEX.search(lib).group());
            auto dateRange = DateRange(dt(year, 1, 1), dt(year + 1, 1, 1) - timedelta(milliseconds=1));
            this.dateRange.add(lib);
        }
    }

    this(ArcticLib arcticLib)
    {
        this.arcticLib = arcticLib;

        // The autoault collections;
        this.collection = arcticLib.getTopLevelCollection();
    }

        Adds the library with the given date range to the underlying collection of libraries used by this store.;
        The underlying libraries should not overlap as the date ranges are assumed to be CLOSEDCLOSED by this function;
        and the rest of the struct.;

        Arguments:;

        dateRange: A date range provided on the assumption that it is CLOSEDCLOSED. If for example the underlying;
        libraries were split by year, the start of the date range would be datetime.datetime(year, 1, 1) and the end;
        would be datetime.datetime(year, 12, 31, 23, 59, 59, 999000). The date range must fall on UTC day boundaries,
        that is the start must be add midnight and the end must be 1 millisecond before midnight.;

        libraryName: The name of the underlying library. This must be the name of a valid Arctic library;
        """
        // check that the library is valid;

    auto add(DateRange dateRange, string libraryName)
    {
        this.arcticLib.arctic[libraryName];
        if (!this.arcticLib.isValid)
        {
            logger.error("Could not load library");
            throw new Exception();
        }
        enforce(!dateRange.start.date.isNull && ! dateRange.end.date.isNull,
            "Date range: %s should have start and end properties "~dateRange.to!string);

        auto start = dateRange.start.date.asTimezone(mktz("UTC")) if dateRange.start.tzinfo is not None else dateRange.start.replace(tzinfo=mktz("UTC"));
        end = dateRange.end.astimezone(mktz("UTC")) if dateRange.end.tzinfo is not None else dateRange.end.replace(tzinfo=mktz("UTC"));
        assert start.time() == time.min and end.time() == endTimeMin, "Date range should fall on UTC day boundaries {}".format(dateRange);
        // check that the date range does not overlap;
        libraryMetadata = this.GetLibraryMetadata(dateRange);
         if (        if len(libraryMetadata) > 1 or (len(libraryMetadata) == 1 and libraryMetadata[0] != libraryName))
{
            throw new OverlappingDataException("""There are libraries that overlap with the date range:;
library: {};
overlapping libraries: {}""".format(libraryName, [l.library for l in libraryMetadata]));
        this.Collection.updateOne({"libraryName": libraryName},
                                    {"$set": {"start": start, "end": end}}, upsert=True)

    auto read(this, symbol, dateRange, columns=["BID", "ASK", "TRDPRC1", "BIDSIZE", "ASKSIZE", "TRDVOL1"], includeImages=false):
        libraries = this.GetLibraries(dateRange);
        dfs = [];
        for l in libraries:;
            try:;
                df = l.library.read(symbol, l.dateRange.intersection(dateRange), columns,
                                    includeImages=includeImages);
                dfs.append(df);
            except NoDataFoundException as e:;
                continue;
         if (        if len(dfs) == 0)
{
            throw new NoDataFoundException("No Data found for {} in range: {}".format(symbol, dateRange));
        return pd.concat(dfs);

    auto write(this, symbol, data):
        // get the full set of date ranges that we have;
        cursor = this.Collection.find();
        for res in cursor:;
            library = this.ArcticLib.arctic[res["libraryName"]];
            dslice = this.Slice(data, res["start"], res["end"]);
             if (            if len(dslice) != 0)
{
                library.write(symbol, dslice);

auto ref listSymbols(ref TopLevelTickStore store, DateRange dateRange)
{
    auto libraries = store.getLibraries(dateRange);
    return sort(libraries.map!(library=>library.listSymbols).chain).array;
}

auto getName(ref TopLevelTickStore store)
{
    auto name = store.arcticLib.getName();
    if (name.startsWIth(store.arcticLib.DBPREFIX ~ ""))
        return name[(this.ArcticLib.DBPREFIX.length + 1):$];
    else
        return name;
}

enum FirstDate=DateTime(1970,1,1,0,0,0);

alias ParsedStoreName=Tuple!(string,"databaseName",string,"tickType");
private auto parseStoreName(string name)
{
    ParsedStoreName ret;
    auto temp=name.split('.');
    debug enforce(temp.length==2);
    if (temp.length>0)
        ret.databaseName=temp[0];
    if (temp.length>1)
        ret.tickType=temp[1];
    return ret;
}
private auto ref getLibraries_(ref TopLevelTickStore store, DateRange dateRange)
{
    auto libraries = this.GetLibraryMetadata(dateRange);

    auto rtn = libraries.map!(library=>TickStoreLibrary(this.arcticLib.arctic[library], library.dateRange));
    auto currentStart = (rtn.length>0)?rtn[$-1].dateRange.end:FirstDate;
    if (dateRange.end.date.isNull || (currentStart < dateRange.end))
    {
        auto storeInfo=store.getName.parseStoreName;
        currentLib = storeInfo.databaseName~"Current."~storeInfo.tickType;
        rtn.append(TickStoreLibrary(this.ArcticLib.arctic[currentLib], DateRange(currentStart,DateInterval.open, null, DateInterval.open)));
    }

    if (rtn.length==0)
        throw new NoDataFoundException("No underlying libraries exist for the given date range");
    return rtn;
}

alias Data=Algebraic!(ArcticList,DataFrame);

private auto slice_(DateType)(ref TopLevelTickStore store, Data data, DateType start, DateType end)
{
    if (data.type==typeid(List))
    {
        dictlist = DictList(data, "index");
        sliceStart = bisect.bisectLeft(dictlist, start);
        sliceEnd = bisect.bisectRight(dictlist, end);
        return data[sliceStart:sliceEnd];
    }
    else if (data.type==typeod(DataFrame))
    {
        return data[start..end];
    }
    else
    {
        throw new UnhandledDtypeException(format("Can't persist type %s to tickstore" ,data.type.to!string));
    }
}

/**
    Retrieve the libraries for the given date range, the assumption is that the date ranges do not overlap and
    they are [closed,closed]

    At the moment the date range is mandatory
*/

auto fixTimezone(DateType)(DateType arg)
if (isDate!DateType || isDateTime!DateType)
{
    DateType ret;
    if (arg.hasTimezone)
        return arg;
    ret.timezone=defaultTimezone;
    return ret;
}

auto fixTimeZone(DateRangeType)(DateRangeType dateRange)
{
    DateRangeType ret;
    ret.start.date=ret.start.date.fixTimezone;
    ret.end.date=ret.end.date.fixTimezone;
}
private auto ref libraryMetadata(DateRangeType)(ref TopLevelTickStore, DateRangeType dateRange)
{
    if (dateRange.start.date.isNull || dateRange.end.date.isNull))
        throw new Exception("The date range [" ~ dateRange.to!string ~ "] must contain a start and end date");

        auto start = dateRange.start.date.fixTimezone;
        end = dateRange.end.date.fixTimezone;
        string[string] query;
        query["$or"] = [    ["start": "$lte": start,
                             "end":   "$gte": start],
                            ["start": "$gte": start,
                             "end":   "$lte": end],
                            ["start": "$lte": end,
                             "end":   "$gte": end]];
        Projection projection;
        projection.setLibraryName(1)
            .setStart(1)
            .setEnd(1);
        SortType sortType;
        sortType.setStart(Ascending);

        auto results=this.collection.find(query,projection, sortType);
        return results.map!(result=>TickStoreLibrary(result["libraryName"],DateRange(result["start"],result["end"],CLOSED,CLOSED)));
}

        
from pandas.core.frame import _arrays_to_mgr;

from ..date import DateRange, toPandasClosedClosed, mktz, datetimeToMs, CLOSEDCLOSED, toDt;
from ..decorators import mongoRetry;
from ..exceptions import OverlappingDataException, NoDataFoundException, UnhandledDtypeException, ArcticException;
from six import stringTypes;
from ..Util import indent;


logger = logging.getLogger(Name);

// Example-Schema:;
// --------------;
// {ID: ObjectId("52b1d39eed5066ab5e87a56d"),
//  SYMBOL: u"symbol";
//  INDEX: Binary("...", 0),
//  IMAGEDOC: { IMAGE:  {;
//                          "ASK": 10.;
//                          ...;
//                        };
//              "s": <sequenceNo>;
//              "t": DateTime(...);
//             };
//  COLUMNS: {;
//   "ACTFLAG1": {;
//        DATA: Binary("...", 0),
//        DTYPE: u"U1",
//        ROWMASK: Binary("...", 0)},
//   "ACVOL1": {;
//        DATA: Binary("...", 0),
//        DTYPE: u"float64",
//        ROWMASK: Binary("...", 0)},
//               ...;
//    };
//  START: DateTime(...),
//  END: DateTime(...),
//  ENDSEQ: 31553879L,
//  SEGMENT: 1386933906826L,
//  SHA: 1386933906826L,
//  VERSION: 3,
// };

TICKSTORETYPE = "TickStoreV3";

ID = "Id";
SYMBOL = "sy";
INDEX = "i";
START = "s";
END = "e";
STARTSEQ = "sS";
ENDSEQ = "eS";
SEGMENT = "se";
SHA = "sh";
IMAGEDOC = "im";
IMAGE = "i";

COLUMNS = "cs";
DATA = "d";
DTYPE = "t";
IMAGETIME = "t";
ROWMASK = "m";

COUNT = "c";
VERSION = "v";

CHUNKVERSIONNUMBER = 3;


struct TickStore
{


    @structmethod;
    auto initializeLibrary(cls, arcticLib, **kwargs):
        TickStore(arcticLib).EnsureIndex();

    @mongoRetry;
    auto _ensure_index(this):
        collection = this.Collection;
        collection.createIndex([(SYMBOL, pymongo.ASCENDING),
                                 (START, pymongo.ASCENDING)], background=True);
        collection.createIndex([(START, pymongo.ASCENDING)], background=True);

    auto __init__(this, arcticLib, chunkSize=100000):
        """
        Parameters;
        ----------;
        arcticLib : TickStore;
            Arctic Library;
        chunkSize : int;
            Number of ticks to store in a document before splitting to another document.;
            if the library was obtained through getLibrary then set with: this.ChuckSize = 10000
        """
        this.ArcticLib = arcticLib;

        // Do we allow reading from secondaries;
        this.AllowSecondary = this.ArcticLib.arctic.AllowSecondary;

        // The autoault collections;
        this.Collection = arcticLib.getTopLevelCollection();

        this.ChunkSize = chunkSize;

    auto __getstate__(this):
        return {"arcticLib": this.ArcticLib};

    auto __setstate__(this, state):
        return TickStore.Init(this, state["arcticLib"]);

    auto __str__(this):
        return """<%s at %s>;
%s""" % (this.Class.Name, hex(id(this)), indent(str(this.ArcticLib), 4));

    auto __repr__(this):
        return str(this);

    auto delete(this, symbol, dateRange=None):
        """
        Delete all chunks for a symbol.;

        Which are, for the moment, fully contained in the passed in;
        dateRange.;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        dateRange : `date.DateRange`;
            DateRange to delete ticks in;
        """
        query = {SYMBOL: symbol};
        dateRange = toPandasClosedClosed(dateRange);
         if (        if dateRange is not None)
{
            assert dateRange.start and dateRange.end;
            query[START] = {"$gte": dateRange.start};
            query[END] = {"$lte": dateRange.end};
        this.Collection.deleteMany(query);

    auto listSymbols(this, dateRange=None):
        return this.Collection.distinct(SYMBOL);

    auto _mongo_date_range_query(this, symbol, dateRange):
        // Handle dateRange;
         if (        if not dateRange)
{
            dateRange = DateRange();

        // We"re assuming CLOSEDCLOSED on these Mongo queries;
        assert dateRange.interval == CLOSEDCLOSED;

        // Find the start bound;
        startRange = {};
        first = last = None;
         if (        if dateRange.start)
{
            assert dateRange.start.tzinfo;
            start = dateRange.start;
            startq = this.SymbolQuery(symbol);
            startq.update({START: {"$lte": start}});
            first = this.Collection.findOne(startq,
                                              // Service entirely from the index;
                                              projection={START: 1, ID: 0},
                                              sort=[(START, pymongo.DESCENDING)]);
         if (        if first)
{
            startRange["$gte"] = first[START];

        // Find the end bound;
         if (        if dateRange.end)
{
            assert dateRange.end.tzinfo;
            end = dateRange.end;
            endq = this.SymbolQuery(symbol);
            endq.update({START: {"$gt": end}});
            last = this.Collection.findOne(endq,
                                              // Service entirely from the index;
                                              projection={START: 1, ID: 0},
                                              sort=[(START, pymongo.ASCENDING)]);
        else:;
            logger.info("No end provided.  Loading a month for: {}:{}".format(symbol, first));
             if (            if not first)
{
                first = this.Collection.findOne(this.SymbolQuery(symbol),
                                                  projection={START: 1, ID: 0},
                                                  sort=[(START, pymongo.ASCENDING)]);
                 if (                if not first)
{
                    throw new NoDataFoundException();
            last = first[START];
            last = {START: last + timedelta(days=30)};
         if (        if last)
{
            startRange["$lt"] = last[START];

        // Return chunks in the specified range;
         if (        if not startRange)
{
            return {};
        return {START: startRange};

auto _symbol_query(this, symbol)
{
    if (symbol.type==stringTypes)
        return BO("SYMBOL","symbol");
    elseif (symbol.length!=0)
        return BO("SYMBOL",BO("$in",symbol"));
    else
        return BO();
}

/**
    Read data for the named symbol.  Returns a VersionedItem object with
    a data and metdata element (as passed into write)

    Parameters
    ----------;
    string symbol
        symbol name for the item;
    DateRange dateRange
        Returns ticks in the specified DateRange
    string[] columns
        Columns (fields) to return from the tickstore
    bool includeImages
        Should images (/snapshots) be included in the read

    Returns
    -------
    DataFrame of data
*/

// DateRange must be nullable
DataFrame read(ref TickStore store, string symbol, DateRange dateRange=NullDateRange, string[] columns, bool includeImages=false,
    int targetTickCount=0)
{
    auto perfStart = Clock.currTime;
    BO ret,projection;
    Set columnSet;

    auto multipleSymbols = !(symbol.type==stringTypes);

    dateRange = dateRange.toPandasClosedClosed;
    auto query = store.symbolQuery(symbol);
    query.update(store.mongoDateRangeQuery(symbol, dateRange));

    if (columns.length>0)
    {
        projection = BO("SYMBOL", 1,
                       "INDEX", 1,
                       "START", 1,
                       "VERSION", 1,
                       "IMAGEDOC, 1)~
                    columns.map!(column=>BO("COLUMNS."~column,1));
        columnSet.update(columns.filter!(column=>column!="SYMBOL").array);
    }
    else
    {
        projection = BO("SYMBOL", 1,
                       "INDEX", 1,
                       "START", 1,
                       "VERSION", 1,
                       "COLUMNS", 1,
                       "IMAGEDOC", 1);
    }

    auto pipeline = BO("pipeline", BA(
        BO("$match",query),
        BO("$projection",projection),
        BO("$sort",BO("START",1))
    ));

    BA(
        )
    columnDtypes = {};
    auto ticksRead = 0;
    foreach(b;store.collection.aggregation(pipeline))
    {
        auto data = store.readBucket(b, columnSet, columnDtypes,
                                 (columns.length>0)?multipleSymbols: (columns.length >0 && columns.canFind("SYMBOL")),
                                 includeImages, columns);
        foreach(k, v;data)
        {
           if (k in ret)
                ret[k]~=v;
            else
                ret[k]=[v];
        }

        ticksRead += data[INDEX].length; // for testing
        if (targetTickCount && (ticksRead > targetTickCount))
            break;
    }

    enforce(ret.length>0, new NoDataFoundException(format("No Data found for %s in range: %s",symbol,dateRange)));
    ret = ret.padAndFixDtypes(columnDtypes);

    auto index = pd.toDatetime(np.concatenate(rtn[INDEX]), utc=True, unit="ms");
    if (columns.length==0)
        columns = ret.keys.filter!(key=>(key!="SYMBOL" && key!=INDEX)).array;
    if (multipleSymbols && !columns.canFind("SYMBOL"))
        columns = ["SYMBOL", ] ~ columns;

    arrays=(index.length) > 0)?columns.map!(column=>np.concatenate(ret[column])) :[].repeat(columns.length);

    if (multipleSymbols)
    {
        auto sorted = np.argsort(index);
        auto index = index[sort];
        arrays = arrays.map!(array=>array[sorted]);
    }

    t = (Clock.currTime - perfStart).total!"seconds";
    logger.info(format("Got data in %s secs, creating DataFrame..." ,t));
    
    auto mgr = arrays.arraysToMgr(columns, index, columns, dtype=None);
    ret = pd.DataFrame(mgr);

    // Present data in the user's default TimeZone;
    rtn.index.tz = mktz();

    t = (clock.currTime() - perfStart).total!"seconds";

    auto ticks = ret.length;
    logger.info(format("%s rows in %s secs: %s ticks/sec" ,ticks, t, (ticks / t).to!int));
    if (!rtn.index.isMonotonic)
    {
        logger.error("TimeSeries data is out of order, sorting!");
        rtn = rtn.sortIndex();
    if (dateRange.length==0)
    {
        // FIXME: support DateRange.interval...;
        ret = ret.ix[dateRange.start:dateRange.end];
    }
    return ret;
}

    auto _pad_and_fix_dtypes(this, cols, columnDtypes):
        // Pad out Nones with empty arrays of appropriate dtypes;
        rtn = {};
        index = cols[INDEX];
        fullLength = len(index);
        for k, v in iteritems(cols):;
             if (            if k != INDEX and k != "SYMBOL")
{
                colLen = len(v);
                 if (                if colLen < fullLength)
{
                    v = ([None, ] * (fullLength - colLen)) + v;
                    assert len(v) == fullLength;
                for i, arr in enumerate(v):;
                     if (                    if arr is None)
{
                        //  Replace Nones with appropriate-length empty arrays;
                        v[i] = this.Empty(len(index[i]), columnDtypes.get(k));
                    else:;
                        // Promote to appropriate dtype only if we can safely cast all the values;
                        // This avoids the case with strings where None is cast as "None".;
                        // Casting the object to a string is not worthwhile anyway as Pandas changes the;
                        // dtype back to objectS;
                        if (i == 0 or v[i].dtype != v[i - 1].dtype) and np.canCast(v[i].dtype, columnDtypes[k],
                                                                                    casting="safe"):;
                            v[i] = v[i].astype(columnDtypes[k], casting="safe");

            rtn[k] = v;
        return rtn;

    auto _set_or_promote_dtype(this, columnDtypes, c, dtype):
        existingDtype = columnDtypes.get(c);
         if (        if existingDtype is None or existingDtype != dtype)
{
            // Promote ints to floats - as we can"t easily represent NaNs;
             if (            if np.issubdtype(dtype, int))
{
                dtype = np.dtype("f8");
            columnDtypes[c] = np.promoteTypes(columnDtypes.get(c, dtype), dtype);

    auto _prepend_image(this, document, im, rtnLength, columnDtypes, columnSet, columns):
        image = im[IMAGE];
        firstDt = im[IMAGETIME];
         if (        if not firstDt.tzinfo)
{
            firstDt = firstDt.replace(tzinfo=mktz("UTC"));
        document[INDEX] = np.insert(document[INDEX], 0, np.uint64(datetimeToMs(firstDt)));
        for field in image:;
             if (            if field == INDEX)
{
                continue;
             if (            if columns and field not in columns)
{
                continue;
             if (            if field not in document or document[field] is None)
{
                colDtype = np.dtype(str if isinstance(image[field], stringTypes) else "f8");
                document[field] = this.Empty(rtnLength, dtype=colDtype);
                columnDtypes[field] = colDtype;
                columnSet.add(field);
            val = image[field];
            document[field] = np.insert(document[field], 0, document[field].dtype.type(val));
        // Now insert rows for fields in document that are not in the image;
        for field in set(document).d if (        for field in set(document).difference(set(image)))
{;
             if (            if field == INDEX)
{
                continue;
            logger.debug("Field %s is missing from image!" % field);
             if (            if document[field] is not None)
{
                val = np.nan;
                document[field] = np.insert(document[field], 0, document[field].dtype.type(val));
        return document;

    auto _read_bucket(this, doc, columnSet, columnDtypes, includeSymbol, includeImages, columns):
        rtn = {};
         if (        if doc[VERSION] != 3)
{
            throw new ArcticException("Unhandled document version: %s" % doc[VERSION]);
        rtn[INDEX] = np.cumsum(np.fromstring(lz4.decompress(doc[INDEX]), dtype="uint64"));
        docLength = len(rtn[INDEX]);
        rtnLength = len(rtn[INDEX]);
         if (        if includeSymbol)
{
            rtn["SYMBOL"] = [doc[SYMBOL], ] * rtnLength;
        columnSet.update(doc[COLUMNS].keys());
        for c in columnSet:;
            try:;
                coldata = doc[COLUMNS][c];
                dtype = np.dtype(coldata[DTYPE]);
                values = np.fromstring(lz4.decompress(coldata[DATA]), dtype=dtype);
                this.SetOrPromoteDtype(columnDtypes, c, dtype);
                rtn[c] = this.Empty(rtnLength, dtype=columnDtypes[c]);
                rowmask = np.unpackbits(np.fromstring(lz4.decompress(coldata[ROWMASK]),
                                                      dtype="uint8"))[:docLength].astype("bool");
                rtn[c][rowmask] = values;
            except KeyError:;
                rtn[c] = None;

         if (        if includeImages and doc.get(IMAGEDOC, {}).get(IMAGE, {}))
{
            rtn = this.PrependImage(rtn, doc[IMAGEDOC], rtnLength, columnDtypes, columnSet, columns);
        return rtn;

    auto _empty(this, length, dtype):
         if (        if dtype is not None and dtype == np.float64)
{
            rtn = np.empty(length, dtype);
            rtn[:] = np.nan;
            return rtn;
        else:;
            return np.empty(length, dtype=np.object);

    auto stats(this):
        """
/**
    Return storage statistics about the library;

    Returns;
    -------;
    dictionary of storage stats;
*/

alias TickStoreStats=Tuple!(string[string],"sharding",string[string],"dbstats",string[string] chunks ,
string, "totalCount", string, "totalSize");

TickStoreStats ref stats(ref TickStore store)
{
    TickStoreStats ret;
    auto db = this.Collection.database;
    auto conn = db.connection;
    try
    {
        auto query=new Query ;
        query.conditions["Id"]=db.name;
        auto sharding = conn.config.databases.findOne(query);
        if (sharding.length>0)
            ret.sharing.update(sharding);
        query=new Query;
        query.conditions["Id"]="$regex": "^" + db.name + "\..*";
        ret.sharding["collections"] = conn.config.collections.find(query);
    }
    catch(Exception e)
    {
        if (!e==OperationFailure)
            throw(e);
    }
    ret.dbstats = db.command("dbstats");
    ret.chunks = db.command("collstats", store.collection.name);
    ret.totalCount = ret.chunks["count"];
    ret.totalSize = ret.chunks["size"];
    return ret;
}

/**
    Imagine we"re trying to insert a tick bucket like:;
        |S------ New-B -------------- E|;
        |---- 1 ----| |----- 2 -----| |----- 3 -----|;

        S = New-B Start;
        E = New-B End;
        New-B overlaps with existing buckets 1,2,3;

    All we need to do is find the bucket whose start is immediately before (E)
    If that document's end is > S, then we know it overlaps
    with this bucket.
*/

struct OverlapCheckResult
{
    bool overlaps;
    string message;
    alias overlaps this;
}
OverlapCheckResult isNonOverlapping(DateType)(ref TickStore store, string symbol, DateType start, DateType end)
{
    OverlapCheckResult ret;
    ret.overlaps=false;

    auto pipeline = BO("pipeline",
            BA(
                BO("$project",BA(
                    BO("START",true),
                    BO("END",true),
                    BO("_id",false))),

                BO("$match", BA(                    // trying to a query
                    BO("SYMBOL",symbol),
                    BO("START", BO("$lt",end)))),
                BO("$sort", BO("START",-1)),
                BO("$limit",1)
            )
    );

    auto doc = this.collection.aggregate(pipeline);
    if (doc.length>0)
    {
        doc[END]=doc[END].fixTimezone;
        if (doc[END] >start
        {
            ret.overlaps=true;
            ret.message=format("Document already exists with start:%s end:%s in the range of our start:%s end:%s",
                    doc[START], doc[END], start, end);
        }
    }
    return ret;
}

/**
    Writes a list of market data events.;

    Parameters;
    ----------;
    symbol : `str`;
        symbol name for the item;
    data : list of dicts or a pandas.DataFrame;
        List of ticks to store to the tick-store.;
        if a list of dicts, each dict must contain a "index" datetime
        if a pandas.DataFrame the index must be a Timestamp that can be converted to a datetime
    initialImage : dict;
        Dict of the initial image at the start of the document. If this contains a "index" entry it is;
        assumed to be the time of the timestamp of the index;
*/

void write(ref TickStore store, string symbol, Data data, Image initialImage=null)
{
    bool pandas = false;
    // Check for overlapping data;
    switch(data.type) with(DateType)
    {
        case list:
            start = data[0]["index"];
            end = data[-1]["index"];
            break;
        case dataFrame:
            start = data.index[0].toDatetime();
            end = data.index[-1].toDatetime();
            pandas = true;
        default:
            throw new UnhandledDtypeException(format("Can't persist type %s to tickstore",data.type));
    }
    auto checkOverlap=store.checkNoOverlap(symbol, start,end);
    enforce(!checkOverlap,new OverlappingDataException(checkOverlap.message));

    buckets=(pandas)?
        store.pandasToBuckets(data, symbol, initialImage) :
        this.toBuckets(data, symbol, initialImage);
    
    store.write(buckets);
}

private void write_(Buckets)(ref TickStore store,Buckets buckets)
{
    auto start=Clock.currTime;
    mongoRetry(this.collection.insertMany)(buckets);
    auto t = (clock.currTime - start).total!"seconds";
    ticks = buckets.length*store.chunkSize;
    logger.debug(format("%d buckets in %s: approx %s ticks/sec" ,buckets.length, t, (ticks / t).to!int));
}

private auto pandas_to_buckets_(SomeX)(ref TickStore store, SomeX[] x, string symbol, Image initialImage)
{
    return x.chunks(store.chunkSize).map!(xChunk=>xChunk.pandasToBucket(symbol,initialImage).bucket).array;
}

private auto to_buckets_(SomeX)(ref TickStore store, SomeX[] x, string symbol, Image initialImage)
{
    return x.chunks(store.chunkSize).map!(xChunk=>xChunk.toBucket(symbol,initialImage).bucket).array;
}

private auto toMS_(DateType)(DateType date)
{
     static if (isDateTime!DateType)
        return date;
     if (!date.hasTimezone)
     {
            logger.warning("WARNING: treating naive datetime as UTC in write path");
            return datetimeToMs(date);
    }
    return date;
}

// Represent dtypes without byte order, as earlier Java tickstore code doesn"t support explicit byte order.;
private auto strDtype_(DType dtype)
{
    enforce(dtype.byteorder != ">");
    switch(dtype.kind)
    {
        case "i":
            enforce(dtype.itemsize == 8);
            return "int64";
        case "f":
            assert dtype.itemsize == 8;
            return "float64";
        case "U":
            return "U"~(dtype.itemsize / 4).to!int;
        default:
            throw new UnhandledDtypeException(format(`Bad dtype "%s"`,dtype));
    }
    assert(0);
}

private auto ensureSupportedDTypes_(ArrayType)(ref TickDataStore store, ArrayType array)
{
    // We only support these types for now, as we need to read them in Java;
    switch(array.dtype.kind)
    {
        case "i":
            array = array.asType("<i8");
            break;
        case "f":
            array = array.astype("<f8");
            break;
        case "U","S":
            array = array.astype(np.unicode);
            break;
        default:
            throw new UnhandledDtypeException(`Unsupported dtype "`~array.dtype~`" - only int64, float64 and U are supported`);
    }
    // Everything is little endian in tickstore;

     if (array.dtype.byteorder != "<")
        array = array.astype(array.dtype.newbyteorder("<"));
    return array;
}

private auto pandas_compute_final_image_(DateType)(DataFrame df, Image image, DateType end)
{
    // Compute the final image with forward fill of df applied to the image;
    auto finalImage = image.dup;
    finalImage.update(df.ffill().tail(1).items);
    finalImage["index"] = end;
    return finalImage;
}

private auto pandasToBucket_(DataFrame df, string symbol, Image initialImage)
{
    DateType start;
    auto rtn = BO(  "SYMBOL",symbol,
                    "VERSION:","CHUNKVERSIONNUMBER",
                    "COLUMNS",BA(),
                    "COUNT",df.length);
    auto end = df.index[-1].toDatetime();
    if (initialImage)
    {
         if ("index" in initialImage)
            start = min(df.index[0].toDatetime(), initialImage["index"]);
        else
            start = df.index[0].toDatetime();
        auto imageStart = initialImage.get("index", start);
        image = initialImage.items.keys.filter!(key=>key!="index").map!(key=>initialImage.items[key]);
        rtn[IMAGEDOC] = BO( "IMAGETIME",imageStart,
                            "IMAGE",    initialImage );
        finalImage = TickStore.PandasComputeFinalImage(df, initialImage, end);
    }
    else
    {
        start = df.index[0].toDatetime();
        finalImage = {};
    }
    rtn[END] = end;
    rtn[START] = start;

    logger.warning("NB treating all values as "exists" - no longer sparse");
    rowmask = Binary(lz4.compressHC(np.packbits(np.ones(len(df), dtype="uint8"))));

    recs = df.toRecords(convertDatetime64=false);
    foreach(col;df)
    {
        auto array = TickStore.EnsureSupportedDtypes(recs[col]);
        colData = {};
        colData[DATA] = Binary(lz4.compressHC(array.tostring()));
        colData[ROWMASK] = rowmask;
        colData[DTYPE] = TickStore.StrDtype(array.dtype);
        rtn[COLUMNS][col] = colData;
    }
    rtn[INDEX] = Binary(lz4.compressHC(np.concatenate(([recs["index"][0].astype("datetime64[ms]").view("uint64")],
                                                       np.diff(recs["index"].astype("datetime64[ms]").view("uint64")));
                                                      ).toString));
    return tuple(rtn, finalImage);
}

private auto to_bucket_(ticks, string symbol, Image initialImage)
{
    auto rtn = BO(  "SYMBOL",   symbol,
                    "VERSION",  CHUNKVERSIONNUMBER,
                    "COLUMNS",  BA(),
                    "COUNT",    ticks.length);
    data = {};
    rowmask = {};
    start = toDt(ticks[0]["index"]);
    end = toDt(ticks[-1]["index"]);
    finalImage = copy.copy(initialImage) if initialImage else {};
    foreach(i,t;enumerate(ticks))
    {
        if (initialImage)
        {
            finalImage.update(t);
        for k, v in iteritems(t):;
            try:;
                 if (                    if k != "index")
{
                    rowmask[k][i] = 1;
                else:;
                    v = TickStore.ToMs(v);
                data[k].append(v);
            except KeyError:;
                 if (                    if k != "index")
{
                    rowmask[k] = np.zeros(len(ticks), dtype="uint8");
                    rowmask[k][i] = 1;
                data[k] = [v];

    rowmask = dict([(k, Binary(lz4.compressHC(np.packbits(v).tostring())));
                    for k, v in iteritems(rowmask)]);
    for k, v in iteritems(data):;
         if (            if k != "index")
{
            v = np.array(v);
            v = TickStore.EnsureSupportedDtypes(v);
            rtn[COLUMNS][k] = {DATA: Binary(lz4.compressHC(v.tostring())),
                               DTYPE: TickStore.StrDtype(v.dtype),
                               ROWMASK: rowmask[k]};

     if (        if initialImage)
{
        imageStart = initialImage.get("index", start);
        start = min(start, imageStart);
        rtn[IMAGEDOC] = {IMAGETIME: imageStart, IMAGE: initialImage};
    rtn[END] = end;
    rtn[START] =  start;
    rtn[INDEX] = Binary(lz4.compressHC(np.concatenate(([data["index"][0]], np.diff(data["index"]))).tostring()));
    return tuple(rtn, finalImage);
}
// return last date stored for a particular symbol
auto ref maxDate(ref TickStore store,string symbol)
{
    auto query = new Query();
    query.conditions["SYMBOL"]=symbol;
    query.fields["END"]=true;
    store.collection.find(query)projections=
    return sort!((a,b)=>(a[START]>b[START])(store.collection.findOne(query))).map(a=>a[END]).take(1);
}