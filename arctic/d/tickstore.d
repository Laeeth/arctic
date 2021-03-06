import logging
import bisect
from collections import namedtuple
from datetime import datetime as dt, date, time, timedelta
import re
from timeit import itertools

import pandas as pd
import pymongo

from ..date import mktz, DateRange, OPEN_OPEN, CLOSED_CLOSED
from ..exceptions import (NoDataFoundException, UnhandledDtypeException, OverlappingDataException,
                          LibraryNotFoundException)

logger = logging.getLogger(__name__)

TickStoreLibrary = namedtuple("TickStoreLibrary", ["library", "date_range"])

TICK_STORE_TYPE = 'TopLevelTickStore'

PATTERN = "^%s_\d{4}.%s"
YEAR_REGEX = re.compile("\d{4}")

end_time_min = (dt.combine(date.today(), time.min) - timedelta(milliseconds=1)).time()


class DictList(object):
    def __init__(self, lst, key):
        self.lst = lst
        self.key = key

    def __len__(self):
        return len(self.lst)

    def __getitem__(self, idx):
        return self.lst[idx][self.key]


class TopLevelTickStore(object):

    @classmethod
    def initialize_library(cls, arctic_lib, **kwargs):
        tl = TopLevelTickStore(arctic_lib)
        tl._add_libraries()
        tl._ensure_index()

    def _ensure_index(self):
        collection = self._collection
        collection.create_index([('start', pymongo.ASCENDING)], background=True)

    def _add_libraries(self):
        name = self.get_name()
        db_name, tick_type = name.split('.', 1)
        regex = re.compile(PATTERN % (db_name, tick_type))
        libraries = [lib for lib in self._arctic_lib.arctic.list_libraries() if regex.search(lib)]
        for lib in libraries:
            year = int(YEAR_REGEX.search(lib).group())
            date_range = DateRange(dt(year, 1, 1), dt(year + 1, 1, 1) - timedelta(milliseconds=1))
            self.add(date_range, lib)

    def __init__(self, arctic_lib):
        self._arctic_lib = arctic_lib

        # The default collections
        self._collection = arctic_lib.get_top_level_collection()

    def add(self, date_range, library_name):
        """
        Adds the library with the given date range to the underlying collection of libraries used by this store.
        The underlying libraries should not overlap as the date ranges are assumed to be CLOSED_CLOSED by this function
        and the rest of the class.

        Arguments:

        date_range: A date range provided on the assumption that it is CLOSED_CLOSED. If for example the underlying
        libraries were split by year, the start of the date range would be datetime.datetime(year, 1, 1) and the end
        would be datetime.datetime(year, 12, 31, 23, 59, 59, 999000). The date range must fall on UTC day boundaries,
        that is the start must be add midnight and the end must be 1 millisecond before midnight.

        library_name: The name of the underlying library. This must be the name of a valid Arctic library
        """
        # check that the library is valid
        try:
            self._arctic_lib.arctic[library_name]
        except Exception as e:
            logger.error("Could not load library")
            raise e
        assert date_range.start and date_range.end, "Date range should have start and end properties {}".format(date_range)
        start = date_range.start.astimezone(mktz('UTC')) if date_range.start.tzinfo is not None else date_range.start.replace(tzinfo=mktz('UTC'))
        end = date_range.end.astimezone(mktz('UTC')) if date_range.end.tzinfo is not None else date_range.end.replace(tzinfo=mktz('UTC'))
        assert start.time() == time.min and end.time() == end_time_min, "Date range should fall on UTC day boundaries {}".format(date_range)
        # check that the date range does not overlap
        library_metadata = self._get_library_metadata(date_range)
        if len(library_metadata) > 1 or (len(library_metadata) == 1 and library_metadata[0] != library_name):
            raise OverlappingDataException("""There are libraries that overlap with the date range:
library: {}
overlapping libraries: {}""".format(library_name, [l.library for l in library_metadata]))
        self._collection.update_one({'library_name': library_name},
                                    {'$set': {'start': start, 'end': end}}, upsert=True)

    def read(self, symbol, date_range, columns=['BID', 'ASK', 'TRDPRC_1', 'BIDSIZE', 'ASKSIZE', 'TRDVOL_1'], include_images=False):
        libraries = self._get_libraries(date_range)
        dfs = []
        for l in libraries:
            try:
                df = l.library.read(symbol, l.date_range.intersection(date_range), columns,
                                    include_images=include_images)
                dfs.append(df)
            except NoDataFoundException as e:
                continue
        if len(dfs) == 0:
            raise NoDataFoundException("No Data found for {} in range: {}".format(symbol, date_range))
        return pd.concat(dfs)

    def write(self, symbol, data):
        # get the full set of date ranges that we have
        cursor = self._collection.find()
        for res in cursor:
            library = self._arctic_lib.arctic[res['library_name']]
            dslice = self._slice(data, res['start'], res['end'])
            if len(dslice) != 0:
                library.write(symbol, dslice)

    def list_symbols(self, date_range):
        libraries = self._get_libraries(date_range)
        return sorted(list(set(itertools.chain(*[l.library.list_symbols() for l in libraries]))))

    def get_name(self):
        name = self._arctic_lib.get_name()
        if name.startswith(self._arctic_lib.DB_PREFIX + '_'):
            name = name[len(self._arctic_lib.DB_PREFIX) + 1:]
        return name

    def _get_libraries(self, date_range):
        libraries = self._get_library_metadata(date_range)

        rtn = [TickStoreLibrary(self._arctic_lib.arctic[library.library], library.date_range)
               for library in libraries]
        current_start = rtn[-1].date_range.end if rtn else dt(1970, 1, 1, 0, 0)  # epoch
        if date_range.end is None or current_start < date_range.end:
            name = self.get_name()
            db_name, tick_type = name.split('.', 1)
            current_lib = "{}_current.{}".format(db_name, tick_type)
            try:
                rtn.append(TickStoreLibrary(self._arctic_lib.arctic[current_lib],
                                            DateRange(current_start, None, OPEN_OPEN)))
            except LibraryNotFoundException:
                pass  # No '_current', move on.

        if not rtn:
            raise NoDataFoundException("No underlying libraries exist for the given date range")
        return rtn

    def _slice(self, data, start, end):
        if isinstance(data, list):
            dictlist = DictList(data, 'index')
            slice_start = bisect.bisect_left(dictlist, start)
            slice_end = bisect.bisect_right(dictlist, end)
            return data[slice_start:slice_end]
        elif isinstance(data, pd.DataFrame):
            return data[start:end]
        else:
            raise UnhandledDtypeException("Can't persist type %s to tickstore" % type(data))

    def _get_library_metadata(self, date_range):
        """
        Retrieve the libraries for the given date range, the assumption is that the date ranges do not overlap and
        they are CLOSED_CLOSED.

        At the moment the date range is mandatory
        """
        if date_range is None:
            raise Exception("A date range must be provided")
        if not (date_range.start and date_range.end):
            raise Exception("The date range {0} must contain a start and end date".format(date_range))

        start = date_range.start if date_range.start.tzinfo is not None else date_range.start.replace(tzinfo=mktz())
        end = date_range.end if date_range.end.tzinfo is not None else date_range.end.replace(tzinfo=mktz())
        query = {'$or': [{'start': {'$lte': start}, 'end': {'$gte': start}},
                         {'start': {'$gte': start}, 'end': {'$lte': end}},
                         {'start': {'$lte': end}, 'end': {'$gte': end}}]}
        return [TickStoreLibrary(res['library_name'], DateRange(res['start'], res['end'], CLOSED_CLOSED))
                for res in self._collection.find(query,
                                                 projection={'library_name': 1,
                                                             'start': 1, 'end': 1},
                                                 sort=[('start', pymongo.ASCENDING)])]

from __future__ import print_function
import logging

from six import iteritems
from bson.binary import Binary
from datetime import datetime as dt, timedelta
import lz4
import numpy as np
import pandas as pd
from pandas.core.frame import _arrays_to_mgr
import pymongo
from pymongo.errors import OperationFailure
import copy

from ..date import DateRange, to_pandas_closed_closed, mktz, datetime_to_ms, CLOSED_CLOSED, to_dt
from ..decorators import mongo_retry
from ..exceptions import OverlappingDataException, NoDataFoundException, UnhandledDtypeException, ArcticException
from six import string_types
from .._util import indent


logger = logging.getLogger(__name__)

# Example-Schema:
# --------------
# {ID: ObjectId('52b1d39eed5066ab5e87a56d'),
#  SYMBOL: u'symbol'
#  INDEX: Binary('...', 0),
#  IMAGE_DOC: { IMAGE:  {
#                          'ASK': 10.
#                          ...
#                        }
#              's': <sequence_no>
#              't': DateTime(...)
#             }
#  COLUMNS: {
#   'ACT_FLAG1': {
#        DATA: Binary('...', 0),
#        DTYPE: u'U1',
#        ROWMASK: Binary('...', 0)},
#   'ACVOL_1': {
#        DATA: Binary('...', 0),
#        DTYPE: u'float64',
#        ROWMASK: Binary('...', 0)},
#               ...
#    }
#  START: DateTime(...),
#  END: DateTime(...),
#  END_SEQ: 31553879L,
#  SEGMENT: 1386933906826L,
#  SHA: 1386933906826L,
#  VERSION: 3,
# }

TICK_STORE_TYPE = 'TickStoreV3'

ID = '_id'
SYMBOL = 'sy'
INDEX = 'i'
START = 's'
END = 'e'
START_SEQ = 'sS'
END_SEQ = 'eS'
SEGMENT = 'se'
SHA = 'sh'
IMAGE_DOC = 'im'
IMAGE = 'i'

COLUMNS = 'cs'
DATA = 'd'
DTYPE = 't'
IMAGE_TIME = 't'
ROWMASK = 'm'

COUNT = 'c'
VERSION = 'v'

CHUNK_VERSION_NUMBER = 3


class TickStore(object):

    @classmethod
    def initialize_library(cls, arctic_lib, **kwargs):
        TickStore(arctic_lib)._ensure_index()

    @mongo_retry
    def _ensure_index(self):
        collection = self._collection
        collection.create_index([(SYMBOL, pymongo.ASCENDING),
                                 (START, pymongo.ASCENDING)], background=True)
        collection.create_index([(START, pymongo.ASCENDING)], background=True)

    def __init__(self, arctic_lib, chunk_size=100000):
        """
        Parameters
        ----------
        arctic_lib : TickStore
            Arctic Library
        chunk_size : int
            Number of ticks to store in a document before splitting to another document.
            if the library was obtained through get_library then set with: self._chuck_size = 10000
        """
        self._arctic_lib = arctic_lib

        # Do we allow reading from secondaries
        self._allow_secondary = self._arctic_lib.arctic._allow_secondary

        # The default collections
        self._collection = arctic_lib.get_top_level_collection()

        self._chunk_size = chunk_size

    def __getstate__(self):
        return {'arctic_lib': self._arctic_lib}

    def __setstate__(self, state):
        return TickStore.__init__(self, state['arctic_lib'])

    def __str__(self):
        return """<%s at %s>
%s""" % (self.__class__.__name__, hex(id(self)), indent(str(self._arctic_lib), 4))

    def __repr__(self):
        return str(self)

    def delete(self, symbol, date_range=None):
        """
        Delete all chunks for a symbol.

        Which are, for the moment, fully contained in the passed in
        date_range.

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        date_range : `date.DateRange`
            DateRange to delete ticks in
        """
        query = {SYMBOL: symbol}
        date_range = to_pandas_closed_closed(date_range)
        if date_range is not None:
            assert date_range.start and date_range.end
            query[START] = {'$gte': date_range.start}
            query[END] = {'$lte': date_range.end}
        self._collection.delete_many(query)

    def list_symbols(self, date_range=None):
        return self._collection.distinct(SYMBOL)

    def _mongo_date_range_query(self, symbol, date_range):
        # Handle date_range
        if not date_range:
            date_range = DateRange()

        # We're assuming CLOSED_CLOSED on these Mongo queries
        assert date_range.interval == CLOSED_CLOSED

        # Find the start bound
        start_range = {}
        first = last = None
        if date_range.start:
            assert date_range.start.tzinfo
            start = date_range.start
            startq = self._symbol_query(symbol)
            startq.update({START: {'$lte': start}})
            first = self._collection.find_one(startq,
                                              # Service entirely from the index
                                              projection={START: 1, ID: 0},
                                              sort=[(START, pymongo.DESCENDING)])
        if first:
            start_range['$gte'] = first[START]

        # Find the end bound
        if date_range.end:
            assert date_range.end.tzinfo
            end = date_range.end
            endq = self._symbol_query(symbol)
            endq.update({START: {'$gt': end}})
            last = self._collection.find_one(endq,
                                              # Service entirely from the index
                                              projection={START: 1, ID: 0},
                                              sort=[(START, pymongo.ASCENDING)])
        else:
            logger.info("No end provided.  Loading a month for: {}:{}".format(symbol, first))
            if not first:
                first = self._collection.find_one(self._symbol_query(symbol),
                                                  projection={START: 1, ID: 0},
                                                  sort=[(START, pymongo.ASCENDING)])
                if not first:
                    raise NoDataFoundException()
            last = first[START]
            last = {START: last + timedelta(days=30)}
        if last:
            start_range['$lt'] = last[START]

        # Return chunks in the specified range
        if not start_range:
            return {}
        return {START: start_range}

    def _symbol_query(self, symbol):
        if isinstance(symbol, string_types):
            query = {SYMBOL: symbol}
        elif symbol is not None:
            query = {SYMBOL: {'$in': symbol}}
        else:
            query = {}
        return query

    def read(self, symbol, date_range=None, columns=None, include_images=False, _target_tick_count=0):
        """
        Read data for the named symbol.  Returns a VersionedItem object with
        a data and metdata element (as passed into write).

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        date_range : `date.DateRange`
            Returns ticks in the specified DateRange
        columns : `list` of `str`
            Columns (fields) to return from the tickstore
        include_images : `bool`
            Should images (/snapshots) be included in the read
        Returns
        -------
        pandas.DataFrame of data
        """
        perf_start = dt.now()
        rtn = {}
        column_set = set()

        multiple_symbols = not isinstance(symbol, string_types)

        date_range = to_pandas_closed_closed(date_range)
        query = self._symbol_query(symbol)
        query.update(self._mongo_date_range_query(symbol, date_range))

        if columns:
            projection = dict([(SYMBOL, 1),
                           (INDEX, 1),
                           (START, 1),
                           (VERSION, 1),
                           (IMAGE_DOC, 1)] +
                          [(COLUMNS + '.%s' % c, 1) for c in columns])
            column_set.update([c for c in columns if c != 'SYMBOL'])
        else:
            projection = dict([(SYMBOL, 1),
                           (INDEX, 1),
                           (START, 1),
                           (VERSION, 1),
                           (COLUMNS, 1),
                           (IMAGE_DOC, 1)])

        column_dtypes = {}
        ticks_read = 0
        for b in self._collection.find(query, projection=projection).sort([(START, pymongo.ASCENDING)],):
            data = self._read_bucket(b, column_set, column_dtypes,
                                     multiple_symbols or (columns is not None and 'SYMBOL' in columns),
                                     include_images, columns)
            for k, v in iteritems(data):
                try:
                    rtn[k].append(v)
                except KeyError:
                    rtn[k] = [v]
            # For testing
            ticks_read += len(data[INDEX])
            if _target_tick_count and ticks_read > _target_tick_count:
                break

        if not rtn:
            raise NoDataFoundException("No Data found for {} in range: {}".format(symbol, date_range))
        rtn = self._pad_and_fix_dtypes(rtn, column_dtypes)

        index = pd.to_datetime(np.concatenate(rtn[INDEX]), utc=True, unit='ms')
        if columns is None:
            columns = [x for x in rtn.keys() if x not in (INDEX, 'SYMBOL')]
        if multiple_symbols and 'SYMBOL' not in columns:
            columns = ['SYMBOL', ] + columns

        if len(index) > 0:
            arrays = [np.concatenate(rtn[k]) for k in columns]
        else:
            arrays = [[] for k in columns]

        if multiple_symbols:
            sort = np.argsort(index)
            index = index[sort]
            arrays = [a[sort] for a in arrays]

        t = (dt.now() - perf_start).total_seconds()
        logger.info("Got data in %s secs, creating DataFrame..." % t)
        mgr = _arrays_to_mgr(arrays, columns, index, columns, dtype=None)
        rtn = pd.DataFrame(mgr)
        # Present data in the user's default TimeZone
        rtn.index.tz = mktz()

        t = (dt.now() - perf_start).total_seconds()
        ticks = len(rtn)
        logger.info("%d rows in %s secs: %s ticks/sec" % (ticks, t, int(ticks / t)))
        if not rtn.index.is_monotonic:
            logger.error("TimeSeries data is out of order, sorting!")
            rtn = rtn.sort_index()
        if date_range:
            # FIXME: support DateRange.interval...
            rtn = rtn.ix[date_range.start:date_range.end]
        return rtn

    def _pad_and_fix_dtypes(self, cols, column_dtypes):
        # Pad out Nones with empty arrays of appropriate dtypes
        rtn = {}
        index = cols[INDEX]
        full_length = len(index)
        for k, v in iteritems(cols):
            if k != INDEX and k != 'SYMBOL':
                col_len = len(v)
                if col_len < full_length:
                    v = ([None, ] * (full_length - col_len)) + v
                    assert len(v) == full_length
                for i, arr in enumerate(v):
                    if arr is None:
                        #  Replace Nones with appropriate-length empty arrays
                        v[i] = self._empty(len(index[i]), column_dtypes.get(k))
                    else:
                        # Promote to appropriate dtype only if we can safely cast all the values
                        # This avoids the case with strings where None is cast as 'None'.
                        # Casting the object to a string is not worthwhile anyway as Pandas changes the
                        # dtype back to objectS
                        if (i == 0 or v[i].dtype != v[i - 1].dtype) and np.can_cast(v[i].dtype, column_dtypes[k],
                                                                                    casting='safe'):
                            v[i] = v[i].astype(column_dtypes[k], casting='safe')

            rtn[k] = v
        return rtn

    def _set_or_promote_dtype(self, column_dtypes, c, dtype):
        existing_dtype = column_dtypes.get(c)
        if existing_dtype is None or existing_dtype != dtype:
            # Promote ints to floats - as we can't easily represent NaNs
            if np.issubdtype(dtype, int):
                dtype = np.dtype('f8')
            column_dtypes[c] = np.promote_types(column_dtypes.get(c, dtype), dtype)

    def _prepend_image(self, document, im, rtn_length, column_dtypes, column_set, columns):
        image = im[IMAGE]
        first_dt = im[IMAGE_TIME]
        if not first_dt.tzinfo:
            first_dt = first_dt.replace(tzinfo=mktz('UTC'))
        document[INDEX] = np.insert(document[INDEX], 0, np.uint64(datetime_to_ms(first_dt)))
        for field in image:
            if field == INDEX:
                continue
            if columns and field not in columns:
                continue
            if field not in document or document[field] is None:
                col_dtype = np.dtype(str if isinstance(image[field], string_types) else 'f8')
                document[field] = self._empty(rtn_length, dtype=col_dtype)
                column_dtypes[field] = col_dtype
                column_set.add(field)
            val = image[field]
            document[field] = np.insert(document[field], 0, document[field].dtype.type(val))
        # Now insert rows for fields in document that are not in the image
        for field in set(document).difference(set(image)):
            if field == INDEX:
                continue
            logger.debug("Field %s is missing from image!" % field)
            if document[field] is not None:
                val = np.nan
                document[field] = np.insert(document[field], 0, document[field].dtype.type(val))
        return document

    def _read_bucket(self, doc, column_set, column_dtypes, include_symbol, include_images, columns):
        rtn = {}
        if doc[VERSION] != 3:
            raise ArcticException("Unhandled document version: %s" % doc[VERSION])
        rtn[INDEX] = np.cumsum(np.fromstring(lz4.decompress(doc[INDEX]), dtype='uint64'))
        doc_length = len(rtn[INDEX])
        rtn_length = len(rtn[INDEX])
        if include_symbol:
            rtn['SYMBOL'] = [doc[SYMBOL], ] * rtn_length
        column_set.update(doc[COLUMNS].keys())
        for c in column_set:
            try:
                coldata = doc[COLUMNS][c]
                dtype = np.dtype(coldata[DTYPE])
                values = np.fromstring(lz4.decompress(coldata[DATA]), dtype=dtype)
                self._set_or_promote_dtype(column_dtypes, c, dtype)
                rtn[c] = self._empty(rtn_length, dtype=column_dtypes[c])
                rowmask = np.unpackbits(np.fromstring(lz4.decompress(coldata[ROWMASK]),
                                                      dtype='uint8'))[:doc_length].astype('bool')
                rtn[c][rowmask] = values
            except KeyError:
                rtn[c] = None

        if include_images and doc.get(IMAGE_DOC, {}).get(IMAGE, {}):
            rtn = self._prepend_image(rtn, doc[IMAGE_DOC], rtn_length, column_dtypes, column_set, columns)
        return rtn

    def _empty(self, length, dtype):
        if dtype is not None and dtype == np.float64:
            rtn = np.empty(length, dtype)
            rtn[:] = np.nan
            return rtn
        else:
            return np.empty(length, dtype=np.object_)

    def stats(self):
        """
        Return storage statistics about the library

        Returns
        -------
        dictionary of storage stats
        """
        res = {}
        db = self._collection.database
        conn = db.connection
        res['sharding'] = {}
        try:
            sharding = conn.config.databases.find_one({'_id': db.name})
            if sharding:
                res['sharding'].update(sharding)
            res['sharding']['collections'] = list(conn.config.collections.find(
                                                  {'_id': {'$regex': '^' + db.name + "\..*"}}))
        except OperationFailure:
            # Access denied
            pass
        res['dbstats'] = db.command('dbstats')
        res['chunks'] = db.command('collstats', self._collection.name)
        res['totals'] = {'count': res['chunks']['count'],
                         'size': res['chunks']['size'],
                         }
        return res

    def _assert_nonoverlapping_data(self, symbol, start, end):
        #
        # Imagine we're trying to insert a tick bucket like:
        #      |S------ New-B -------------- E|
        #  |---- 1 ----| |----- 2 -----| |----- 3 -----|
        #
        # S = New-B Start
        # E = New-B End
        # New-B overlaps with existing buckets 1,2,3
        #
        # All we need to do is find the bucket who's start is immediately before (E)
        # If that document's end is > S, then we know it overlaps
        # with this bucket.
        doc = self._collection.find_one({SYMBOL: symbol,
                                         START: {'$lt': end}
                                         },
                                        projection={START: 1,
                                                END: 1,
                                                '_id': 0},
                                        sort=[(START, pymongo.DESCENDING)])
        if doc:
            if not doc[END].tzinfo:
                doc[END] = doc[END].replace(tzinfo=mktz('UTC'))
            if doc[END] > start:
                raise OverlappingDataException("Document already exists with start:{} end:{} in the range of our start:{} end:{}".format(
                                                            doc[START], doc[END], start, end))

    def write(self, symbol, data, initial_image=None):
        """
        Writes a list of market data events.

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        data : list of dicts or a pandas.DataFrame
            List of ticks to store to the tick-store.
            if a list of dicts, each dict must contain a 'index' datetime
            if a pandas.DataFrame the index must be a Timestamp that can be converted to a datetime
        initial_image : dict
            Dict of the initial image at the start of the document. If this contains a 'index' entry it is
            assumed to be the time of the timestamp of the index
        """
        pandas = False
        # Check for overlapping data
        if isinstance(data, list):
            start = data[0]['index']
            end = data[-1]['index']
        elif isinstance(data, pd.DataFrame):
            start = data.index[0].to_datetime()
            end = data.index[-1].to_datetime()
            pandas = True
        else:
            raise UnhandledDtypeException("Can't persist type %s to tickstore" % type(data))
        self._assert_nonoverlapping_data(symbol, to_dt(start), to_dt(end))

        if pandas:
            buckets = self._pandas_to_buckets(data, symbol, initial_image)
        else:
            buckets = self._to_buckets(data, symbol, initial_image)
        self._write(buckets)

    def _write(self, buckets):
        start = dt.now()
        mongo_retry(self._collection.insert_many)(buckets)
        t = (dt.now() - start).total_seconds()
        ticks = len(buckets) * self._chunk_size
        logger.debug("%d buckets in %s: approx %s ticks/sec" % (len(buckets), t, int(ticks / t)))

    def _pandas_to_buckets(self, x, symbol, initial_image):
        rtn = []
        for i in range(0, len(x), self._chunk_size):
            bucket, initial_image = TickStore._pandas_to_bucket(x[i:i + self._chunk_size], symbol, initial_image)
            rtn.append(bucket)
        return rtn

    def _to_buckets(self, x, symbol, initial_image):
        rtn = []
        for i in range(0, len(x), self._chunk_size):
            bucket, initial_image = TickStore._to_bucket(x[i:i + self._chunk_size], symbol, initial_image)
            rtn.append(bucket)
        return rtn

    @staticmethod
    def _to_ms(date):
        if isinstance(date, dt):
            if not date.tzinfo:
                logger.warning('WARNING: treating naive datetime as UTC in write path')
            return datetime_to_ms(date)
        return date

    @staticmethod
    def _str_dtype(dtype):
        """
        Represent dtypes without byte order, as earlier Java tickstore code doesn't support explicit byte order.
        """
        assert dtype.byteorder != '>'
        if (dtype.kind) == 'i':
            assert dtype.itemsize == 8
            return 'int64'
        elif (dtype.kind) == 'f':
            assert dtype.itemsize == 8
            return 'float64'
        elif (dtype.kind) == 'U':
            return 'U%d' % (dtype.itemsize / 4)
        else:
            raise UnhandledDtypeException("Bad dtype '%s'" % dtype)

    @staticmethod
    def _ensure_supported_dtypes(array):
        # We only support these types for now, as we need to read them in Java
        if (array.dtype.kind) == 'i':
            array = array.astype('<i8')
        elif (array.dtype.kind) == 'f':
            array = array.astype('<f8')
        elif (array.dtype.kind) in ('U', 'S'):
            array = array.astype(np.unicode_)
        else:
            raise UnhandledDtypeException("Unsupported dtype '%s' - only int64, float64 and U are supported" % array.dtype)
        # Everything is little endian in tickstore
        if array.dtype.byteorder != '<':
            array = array.astype(array.dtype.newbyteorder('<'))
        return array

    @staticmethod
    def _pandas_compute_final_image(df, image, end):
        # Compute the final image with forward fill of df applied to the image
        final_image = copy.copy(image)
        last_values = df.ffill().tail(1).to_dict()
        last_dict = {i: list(a.values())[0] for i, a in last_values.items()}
        final_image.update(last_dict)
        final_image['index'] = end
        return final_image

    @staticmethod
    def _pandas_to_bucket(df, symbol, initial_image):
        rtn = {SYMBOL: symbol, VERSION: CHUNK_VERSION_NUMBER, COLUMNS: {}, COUNT: len(df)}
        end = to_dt(df.index[-1].to_datetime())
        if initial_image :
            if 'index' in initial_image:
                start = min(to_dt(df.index[0].to_datetime()), initial_image['index'])
            else:
                start = to_dt(df.index[0].to_datetime())
            image_start = initial_image.get('index', start)
            image = {k: v for k, v in initial_image.items() if k != 'index'}
            rtn[IMAGE_DOC] = {IMAGE_TIME: image_start, IMAGE: initial_image}
            final_image = TickStore._pandas_compute_final_image(df, initial_image, end)
        else:
            start = to_dt(df.index[0].to_datetime())
            final_image = {}
        rtn[END] = end
        rtn[START] = start

        logger.warning("NB treating all values as 'exists' - no longer sparse")
        rowmask = Binary(lz4.compressHC(np.packbits(np.ones(len(df), dtype='uint8'))))

        recs = df.to_records(convert_datetime64=False)
        for col in df:
            array = TickStore._ensure_supported_dtypes(recs[col])
            col_data = {}
            col_data[DATA] = Binary(lz4.compressHC(array.tostring()))
            col_data[ROWMASK] = rowmask
            col_data[DTYPE] = TickStore._str_dtype(array.dtype)
            rtn[COLUMNS][col] = col_data
        rtn[INDEX] = Binary(lz4.compressHC(np.concatenate(([recs['index'][0].astype('datetime64[ms]').view('uint64')],
                                                           np.diff(recs['index'].astype('datetime64[ms]').view('uint64')))
                                                          ).tostring()))
        return rtn, final_image

    @staticmethod
    def _to_bucket(ticks, symbol, initial_image):
        rtn = {SYMBOL: symbol, VERSION: CHUNK_VERSION_NUMBER, COLUMNS: {}, COUNT: len(ticks)}
        data = {}
        rowmask = {}
        start = to_dt(ticks[0]['index'])
        end = to_dt(ticks[-1]['index'])
        final_image = copy.copy(initial_image) if initial_image else {}
        for i, t in enumerate(ticks):
            if initial_image:
                final_image.update(t)
            for k, v in iteritems(t):
                try:
                    if k != 'index':
                        rowmask[k][i] = 1
                    else:
                        v = TickStore._to_ms(v)
                    data[k].append(v)
                except KeyError:
                    if k != 'index':
                        rowmask[k] = np.zeros(len(ticks), dtype='uint8')
                        rowmask[k][i] = 1
                    data[k] = [v]

        rowmask = dict([(k, Binary(lz4.compressHC(np.packbits(v).tostring())))
                        for k, v in iteritems(rowmask)])
        for k, v in iteritems(data):
            if k != 'index':
                v = np.array(v)
                v = TickStore._ensure_supported_dtypes(v)
                rtn[COLUMNS][k] = {DATA: Binary(lz4.compressHC(v.tostring())),
                                   DTYPE: TickStore._str_dtype(v.dtype),
                                   ROWMASK: rowmask[k]}

        if initial_image:
            image_start = initial_image.get('index', start)
            start = min(start, image_start)
            rtn[IMAGE_DOC] = {IMAGE_TIME: image_start, IMAGE: initial_image}
        rtn[END] = end
        rtn[START] =  start
        rtn[INDEX] = Binary(lz4.compressHC(np.concatenate(([data['index'][0]], np.diff(data['index']))).tostring()))
        return rtn, final_image

    def max_date(self, symbol):
        """
        Return the maximum datetime stored for a particular symbol

        Parameters
        ----------
        symbol : `str`
            symbol name for the item
        """
        res = self._collection.find_one({SYMBOL: symbol}, projection={ID: 0, END: 1},
                                        sort=[(START, pymongo.DESCENDING)])
        return res[END]
