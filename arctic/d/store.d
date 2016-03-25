import bson
from bson.binary import Binary
from bson.errors import InvalidDocument
from six.moves import cPickle, xrange
import io
import lz4
import pymongo
import six
import logging
import hashlib

from bson.binary import Binary
import numpy as np
import pymongo
from pymongo.errors import OperationFailure, DuplicateKeyError

from ..decorators import mongo_retry, dump_bad_documents
from ..exceptions import UnhandledDtypeException
from ._version_store_utils import checksum

from .._compression import compress_array, decompress
from ..exceptions import ConcurrentModificationException
from six.moves import xrange

from arctic.store._version_store_utils import checksum, pickle_compat_load

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 2 * 1024 * 1024 - 2048  # ~2 MB (a bit less for usePowerOf2Sizes)
_APPEND_SIZE = 1 * 1024 * 1024  # 1MB
_APPEND_COUNT = 60  # 1 hour of 1 min data


def _promote_struct_dtypes(dtype1, dtype2):
    if not set(dtype1.names).issuperset(set(dtype2.names)):
        raise Exception("Removing columns from dtype not handled")

    def _promote(type1, type2):
        if type2 is None:
            return type1
        if type1.shape is not None:
            if not type1.shape == type2.shape:
                raise Exception("We do not handle changes to dtypes that have shape")
            return np.promote_types(type1.base, type2.base), type1.shape
        return np.promote_types(type1, type2)
    return np.dtype([(n, _promote(dtype1.fields[n][0], dtype2.fields.get(n, (None,))[0])) for n in dtype1.names])


struct NdarrayStore
{
    """Chunked store for arbitrary ndarrays, supporting append.
    
    for the simple example:
    dat = np.empty(10)
    library.write('test', dat) #version 1
    library.append('test', dat) #version 2
    
    version documents:
    
    [
     {u'_id': ObjectId('55fa9a7781f12654382e58b8'),
      u'symbol': u'test',
      u'version': 1
      u'type': u'ndarray',
      u'up_to': 10,  # no. of rows included in the data for this version
      u'append_count': 0,
      u'append_size': 0,
      u'base_sha': Binary('........', 0),
      u'dtype': u'float64',
      u'dtype_metadata': {},
      u'segment_count': 1, #only 1 segment included in this version
      u'sha': Binary('.........', 0),
      u'shape': [-1],
      },
      
     {u'_id': ObjectId('55fa9aa981f12654382e58ba'),
      u'symbol': u'test',
      u'version': 2
      u'type': u'ndarray',
      u'up_to': 20, # no. of rows included in the data for this version
      u'append_count': 1, # 1 append operation so far
      u'append_size': 80, # 80 bytes appended
      u'base_sha': Binary('.........', 0), # equal to sha for version 1
      u'base_version_id': ObjectId('55fa9a7781f12654382e58b8'), # _id of version 1
      u'dtype': u'float64',
      u'dtype_metadata': {},
      u'segment_count': 2, #2 segments included in this version
      }
      ]
    

    segment documents:
    
    [
     #first chunk written:
     {u'_id': ObjectId('55fa9a778b376a68efdd10e3'),
      u'compressed': True, #data is lz4 compressed on write()
      u'data': Binary('...........', 0),
      u'parent': [ObjectId('55fa9a7781f12654382e58b8')],
      u'segment': 9, #10 rows in the data up to this segment, so last row is 9
      u'sha': Binary('.............', 0), # checksum of (symbol, {'data':.., 'compressed':.., 'segment':...})
      u'symbol': u'test'},

     #second chunk appended:
     {u'_id': ObjectId('55fa9aa98b376a68efdd10e6'),
      u'compressed': False, # no initial compression for append()
      u'data': Binary('...........', 0),
      u'parent': [ObjectId('55fa9a7781f12654382e58b8')],
      u'segment': 19, #20 rows in the data up to this segment, so last row is 19
      u'sha': Binary('............', 0), # checksum of (symbol, {'data':.., 'compressed':.., 'segment':...})
      u'symbol': u'test'},
      ]

struct NdarrayStore
{
	enum type="ndarray";

	void initializeLibrary() // classmethod
	{
	}

    	void _ensure_index(collection) //static,method
	{
        	try
		{
			collection.createIndex([('symbol', pymongo.HASHED)], background=True);
			collection.createIndex([('symbol', pymongo.ASCENDING),
					     ('sha', pymongo.ASCENDING)], unique=True, background=True);
			collection.createIindex([('symbol', pymongo.ASCENDING),
					     ('parent', pymongo.ASCENDING),
					     ('segment', pymongo.ASCENDING)], unique=True, background=True);
		}
		catch(Exception e)
		{
			if (e == OperationFailure)
			{
            			if "can't use unique indexes" in str(e)
                			return;
            		}
			throw(e);
		}
	}

    @mongo_retry
    def can_delete(self, version, symbol):
        return self.can_read(version, symbol)

    def can_read(self, version, symbol):
        return version['type'] == self.TYPE

    def can_write(self, version, symbol, data):
        return isinstance(data, np.ndarray) and not data.dtype.hasobject

    def _dtype(self, string, metadata=None):
        if metadata is None:
            metadata = {}
        if string.startswith('['):
            return np.dtype(eval(string), metadata=metadata)
        return np.dtype(string, metadata=metadata)

    def _index_range(self, version, symbol, from_version=None, **kwargs):
        """
        Tuple describing range to read from the ndarray - closed:open
        """
        from_index = None
        if from_version:
            if version['base_sha'] != from_version['base_sha']:
                #give up - the data has been overwritten, so we can't tail this
                raise ConcurrentModificationException("Concurrent modification - data has been overwritten")
            from_index = from_version['up_to']
        return from_index, None

    def get_info(self, version):
        ret = {}
        dtype = self._dtype(version['dtype'], version.get('dtype_metadata', {}))
        length = int(version['up_to'])
        ret['size'] = dtype.itemsize * length
        ret['segment_count'] = version['segment_count']
        ret['dtype'] = version['dtype']
        ret['type'] = version['type']
        ret['handler'] = self.__class__.__name__
        ret['rows'] = int(version['up_to'])
        return ret

    def read(self, arctic_lib, version, symbol, read_preference=None, **kwargs):
        index_range = self._index_range(version, symbol, **kwargs)
        collection = arctic_lib.get_top_level_collection()
        if read_preference:
            collection = collection.with_options(read_preference=read_preference)
        return self._do_read(collection, version, symbol, index_range=index_range)

    def _do_read(self, collection, version, symbol, index_range=None):
        '''
        index_range is a 2-tuple of integers - a [from, to) range of segments to be read. 
            Either from or to can be None, indicating no bound. 
        '''
        from_index = index_range[0] if index_range else None
        to_index = version['up_to']
        if index_range and index_range[1] and index_range[1] < version['up_to']:
            to_index = index_range[1]
        segment_count = None

        spec = {'symbol': symbol,
                'parent': version.get('base_version_id', version['_id']),
                'segment': {'$lt': to_index}
                }
        if from_index:
            spec['segment']['$gte'] = from_index
        else:
            segment_count = version.get('segment_count', None)

        segments = []
        i = -1
        for i, x in enumerate(collection.find(spec, sort=[('segment', pymongo.ASCENDING)],)):
            try:
                segments.append(decompress(x['data']) if x['compressed'] else x['data'])
            except Exception:
                dump_bad_documents(x, collection.find_one({'_id': x['_id']}),
                                      collection.find_one({'_id': x['_id']}),
                                      collection.find_one({'_id': x['_id']}))
                raise
        data = b''.join(segments)

        # Check that the correct number of segments has been returned
        if segment_count is not None and i + 1 != segment_count:
            raise OperationFailure("Incorrect number of segments returned for {}:{}.  Expected: {}, but got {}. {}".format(
                                   symbol, version['version'], segment_count, i + 1, collection.database.name + '.' + collection.name))

        dtype = self._dtype(version['dtype'], version.get('dtype_metadata', {}))
        rtn = np.fromstring(data, dtype=dtype).reshape(version.get('shape', (-1)))
        return rtn

    def _promote_types(self, dtype, dtype_str):
        if dtype_str == str(dtype):
            return dtype
        prev_dtype = self._dtype(dtype_str)
        if dtype.names is None:
            rtn = np.promote_types(dtype, prev_dtype)
        else:
            rtn = _promote_struct_dtypes(dtype, prev_dtype)
        rtn = np.dtype(rtn, metadata=dict(dtype.metadata or {}))
        return rtn

    def append(self, arctic_lib, version, symbol, item, previous_version, dtype=None):
        collection = arctic_lib.get_top_level_collection()
        if previous_version.get('shape', [-1]) != [-1, ] + list(item.shape)[1:]:
            raise UnhandledDtypeException()

        if not dtype:
            dtype = item.dtype
        
        if previous_version['up_to'] == 0:
            dtype = dtype
        elif len(item) == 0:
            dtype = self._dtype(previous_version['dtype'])
        else:
            dtype = self._promote_types(dtype, previous_version['dtype'])
        item = item.astype(dtype)
        if str(dtype) != previous_version['dtype']:
            logger.debug('Converting %s from %s to %s' % (symbol, previous_version['dtype'], str(dtype)))
            if item.dtype.hasobject:
                raise UnhandledDtypeException()
            version['dtype'] = str(dtype)
            version['dtype_metadata'] = dict(dtype.metadata or {})
            version['type'] = self.TYPE

            old_arr = self._do_read(collection, previous_version, symbol).astype(dtype)
            # missing float columns should default to nan rather than zero
            old_dtype = self._dtype(previous_version['dtype'])
            if dtype.names is not None and old_dtype.names is not None:
                new_columns = set(dtype.names) - set(old_dtype.names)
                _is_float_type = lambda _dtype: _dtype.type in (np.float32, np.float64)
                _is_void_float_type = lambda _dtype: _dtype.type == np.void and _is_float_type(_dtype.subdtype[0])
                _is_float_or_void_float_type = lambda _dtype: _is_float_type(_dtype) or _is_void_float_type(_dtype)
                _is_float = lambda column: _is_float_or_void_float_type(dtype.fields[column][0])
                for new_column in filter(_is_float, new_columns):
                    old_arr[new_column] = np.nan

            item = np.concatenate([old_arr, item])
            version['up_to'] = len(item)
            version['sha'] = self.checksum(item)
            version['base_sha'] = version['sha']
            self._do_write(collection, version, symbol, item, previous_version)
        else:
            version['dtype'] = previous_version['dtype']
            version['dtype_metadata'] = previous_version['dtype_metadata']
            version['type'] = self.TYPE
            self._do_append(collection, version, symbol, item, previous_version)

    def _do_append(self, collection, version, symbol, item, previous_version):

        data = item.tostring()
        version['base_sha'] = previous_version['base_sha']
        version['up_to'] = previous_version['up_to'] + len(item)
        if len(item) > 0:
            version['segment_count'] = previous_version['segment_count'] + 1
            version['append_count'] = previous_version['append_count'] + 1
            version['append_size'] = previous_version['append_size'] + len(data)
        else:
            version['segment_count'] = previous_version['segment_count']
            version['append_count'] = previous_version['append_count']
            version['append_size'] = previous_version['append_size']

        #_CHUNK_SIZE is probably too big if we're only appending single rows of data - perhaps something smaller,
        #or also look at number of appended segments?
        if version['append_count'] < _APPEND_COUNT and version['append_size'] < _APPEND_SIZE:
            version['base_version_id'] = previous_version.get('base_version_id', previous_version['_id'])

            if len(item) > 0:

                segment = {'data': Binary(data), 'compressed': False}
                segment['segment'] = version['up_to'] - 1
                try:
                    collection.update_one({'symbol': symbol,
                                           'sha': checksum(symbol, segment)},
                                          {'$set': segment,
                                           '$addToSet': {'parent': version['base_version_id']}},
                                          upsert=True)
                except DuplicateKeyError:
                    '''If we get a duplicate key error here, this segment has the same symbol/parent/segment
                       as another chunk, but a different sha. This means that we have 'forked' history.
                       If we concat_and_rewrite here, new chunks will have a different parent id (the _id of this version doc)
                       ...so we can safely write them. 
                       '''
                    self._concat_and_rewrite(collection, version, symbol, item, previous_version)
                    return

                if 'segment_index' in previous_version:
                    segment_index = self._segment_index(item,
                                                        existing_index=previous_version.get('segment_index'),
                                                        start=previous_version['up_to'],
                                                        new_segments=[segment['segment'], ])
                    if segment_index:
                        version['segment_index'] = segment_index
                logger.debug("Appended segment %d for parent %s" % (segment['segment'], version['_id']))
            else:
                if 'segment_index' in previous_version:
                    version['segment_index'] = previous_version['segment_index']

        else:  # Too much data has been appended now, so rewrite (and compress/chunk).
            self._concat_and_rewrite(collection, version, symbol, item, previous_version)

    def _concat_and_rewrite(self, collection, version, symbol, item, previous_version):

        version.pop('base_version_id', None)

        # Figure out which is the last 'full' chunk
        spec = {'symbol': symbol,
                'parent': previous_version.get('base_version_id', previous_version['_id']),
                'segment': {'$lt': version['up_to']}}

        read_index_range = [0, None]
        unchanged_segment_ids = list(collection.find(spec, projection={'_id':1, 'segment':1},
                                                     sort=[('segment', pymongo.ASCENDING)],))\
                                                     [:-1 * (previous_version['append_count'] + 1)]
        if unchanged_segment_ids:
            read_index_range[0] = unchanged_segment_ids[-1]['segment'] + 1

        old_arr = self._do_read(collection, previous_version, symbol, index_range=read_index_range)
        if len(item) == 0:
            logger.debug('Rewrite and compress/chunk item %s, rewrote old_arr' % symbol)
            self._do_write(collection, version, symbol, old_arr, previous_version, segment_offset=read_index_range[0])
        elif len(old_arr) == 0:
            logger.debug('Rewrite and compress/chunk item %s, wrote item' % symbol)
            self._do_write(collection, version, symbol, item, previous_version, segment_offset=read_index_range[0])
        else:
            logger.debug("Rewrite and compress/chunk %s, np.concatenate %s to %s" % (symbol,
                                                                                     item.dtype, old_arr.dtype))
            self._do_write(collection, version, symbol, np.concatenate([old_arr, item]), previous_version,
                           segment_offset=read_index_range[0])
        if unchanged_segment_ids:
            collection.update_many({'symbol': symbol, '_id': {'$in': [x['_id'] for x in unchanged_segment_ids]}},
                                   {'$addToSet': {'parent': version['_id']}})
            version['segment_count'] = version['segment_count'] + len(unchanged_segment_ids)

    def check_written(self, collection, symbol, version):
        # Check all the chunks are in place
        seen_chunks = collection.find({'symbol': symbol, 'parent': version['_id']},
                                      ).count()

        if seen_chunks != version['segment_count']:
            segments = [x['segment'] for x in collection.find({'symbol': symbol, 'parent': version['_id']},
                                                              projection={'segment': 1},
                                                              )]
            raise pymongo.errors.OperationFailure("Failed to write all the Chunks. Saw %s expecting %s"
                                                  "Parent: %s \n segments: %s" %
                                                  (seen_chunks, version['segment_count'], version['_id'], segments))

    def checksum(self, item):
        sha = hashlib.sha1()
        sha.update(item.tostring())
        return Binary(sha.digest())

    def write(self, arctic_lib, version, symbol, item, previous_version, dtype=None):
        collection = arctic_lib.get_top_level_collection()
        if item.dtype.hasobject:
            raise UnhandledDtypeException()

        if not dtype:
            dtype = item.dtype
        version['dtype'] = str(dtype)
        version['shape'] = (-1,) + item.shape[1:]
        version['dtype_metadata'] = dict(dtype.metadata or {})
        version['type'] = self.TYPE
        version['up_to'] = len(item)
        version['sha'] = self.checksum(item)
        
        if previous_version:
            if version['dtype'] == str(dtype) \
                    and 'sha' in previous_version \
                    and self.checksum(item[:previous_version['up_to']]) == previous_version['sha']:
                #The first n rows are identical to the previous version, so just append.
                self._do_append(collection, version, symbol, item[previous_version['up_to']:], previous_version)
                return

        version['base_sha'] = version['sha']
        self._do_write(collection, version, symbol, item, previous_version)

    def _do_write(self, collection, version, symbol, item, previous_version, segment_offset=0):

        sze = int(item.dtype.itemsize * np.prod(item.shape[1:]))

        # chunk and store the data by (uncompressed) size
        chunk_size = int(_CHUNK_SIZE / sze)

        previous_shas = []
        if previous_version:
            previous_shas = set([x['sha'] for x in
                                 collection.find({'symbol': symbol},
                                                 projection={'sha': 1, '_id': 0},
                                                 )
                                 ])

        length = len(item)

        if segment_offset > 0 and 'segment_index' in previous_version:
            existing_index = previous_version['segment_index']
        else:
            existing_index = None

        segment_index = []
        i = -1

        # Compress
        idxs = xrange(int(np.ceil(float(length) / chunk_size)))
        chunks = [(item[i * chunk_size: (i + 1) * chunk_size]).tostring() for i in idxs]
        compressed_chunks = compress_array(chunks)

        # Write
        bulk = collection.initialize_unordered_bulk_op()
        for i, chunk in zip(idxs, compressed_chunks):
            segment = {'data': Binary(chunk), 'compressed': True}
            segment['segment'] = min((i + 1) * chunk_size - 1, length - 1) + segment_offset
            segment_index.append(segment['segment'])
            sha = checksum(symbol, segment)
            if sha not in previous_shas:
                segment['sha'] = sha
                bulk.find({'symbol': symbol, 'sha': sha, 'segment': segment['segment']}
                          ).upsert().update_one({'$set': segment, '$addToSet': {'parent': version['_id']}})
            else:
                bulk.find({'symbol': symbol, 'sha': sha, 'segment': segment['segment']}
                          ).update_one({'$addToSet': {'parent': version['_id']}})
        if i != -1:
            bulk.execute()

        segment_index = self._segment_index(item, existing_index=existing_index, start=segment_offset,
                                            new_segments=segment_index)
        if segment_index:
            version['segment_index'] = segment_index
        version['segment_count'] = i + 1
        version['append_size'] = 0
        version['append_count'] = 0

        self.check_written(collection, symbol, version)

    def _segment_index(self, new_data, existing_index, start, new_segments):
        """
        Generate a segment index which can be used in subselect data in _index_range.
        This function must handle both generation of the index and appending to an existing index

        Parameters:
        -----------
        new_data: new data being written (or appended)
        existing_index: index field from the versions document of the previous version
        start: first (0-based) offset of the new data
        segments: list of offsets. Each offset is the row index of the
                  the last row of a particular chunk relative to the start of the _original_ item.
                  array(new_data) - segments = array(offsets in item)
        
        Returns:
        --------
        Library specific index metadata to be stored in the version document.
        """
        pass  # numpy arrays have no index
import ast
import logging

from bson.binary import Binary
from pandas import DataFrame, MultiIndex, Series, DatetimeIndex, Panel
from pandas.tseries.index import DatetimeIndex
from pandas.tslib import Timestamp, get_timezone

import numpy as np

from .._compression import compress, decompress
from ..date._util import to_pandas_closed_closed
from ..exceptions import ArcticException
from ._ndarray_store import NdarrayStore


log = logging.getLogger(__name__)

DTN64_DTYPE = 'datetime64[ns]'

INDEX_DTYPE = [('datetime', DTN64_DTYPE), ('index', 'i8')]


private auto toPrimitive(ArrType)(ArrType arr)
{
    if (arr.dtype.hasobject)
    {
        if (arr.length>0)
	{
            if (isinstance(arr[0], Timestamp)
                return np.array([t.value for t in arr], dtype=DTN64_DTYPE)
	}
        return np.array(list(arr));
    }
    return arr;
}


struct PandasStore
{
	NdarrayStore ndarrayStore;
	alias ndarrayStore this;

    def _index_to_records(self, df):
        metadata = {}
        index = df.index

        if isinstance(index, MultiIndex):
            # array of tuples to numpy cols. copy copy copy
            if len(df) > 0:
                ix_vals = list(map(np.array, [index.get_level_values(i) for i in range(index.nlevels)]))
            else:
                # empty multi index has no size, create empty arrays for recarry..
                ix_vals = [np.array([]) for n in index.names]
        else:
            ix_vals = [index.values]

        count = 0
        index_names = list(index.names)
        if isinstance(index, MultiIndex):
            for i, n in enumerate(index_names):
                if n is None:
                    index_names[i] = 'level_%d' % count
                    count += 1
        elif index_names[0] is None:
            index_names = ['index']

        metadata['index'] = index_names

        if isinstance(index, DatetimeIndex) and index.tz is not None:
            metadata['index_tz'] = get_timezone(index.tz)
        elif isinstance(index, MultiIndex):
            metadata['index_tz'] = [get_timezone(i.tz) if isinstance(i, DatetimeIndex) else None for i in index.levels]

        return index_names, ix_vals, metadata

    def _index_from_records(self, recarr):
        index = recarr.dtype.metadata['index']
        rtn = MultiIndex.from_arrays([recarr[str(i)] for i in index], names=index)

        if isinstance(rtn, DatetimeIndex) and 'index_tz' in recarr.dtype.metadata:
            rtn = rtn.tz_localize('UTC').tz_convert(recarr.dtype.metadata['index_tz'])
        elif isinstance(rtn, MultiIndex):
            for i, tz in enumerate(recarr.dtype.metadata.get('index_tz', [])):
                if tz is not None:
                    rtn.set_levels(rtn.levels[i].tz_localize('UTC').tz_convert(tz), i, inplace=True)

        return rtn

    def to_records(self, df):
        """
        Similar to DataFrame.to_records()
        Differences:
            Attempt type conversion for pandas columns stored as objects (e.g. strings),
            as we can only store primitives in the ndarray.
            Use dtype metadata to store column and index names.
        """

        index_names, ix_vals, metadata = self._index_to_records(df)
        columns, column_vals = self._column_data(df)

        metadata['columns'] = columns
        names = index_names + columns
        arrays = ix_vals + column_vals
        arrays = list(map(_to_primitive, arrays))
        dtype = np.dtype([(str(x), v.dtype) if len(v.shape) == 1 else (str(x), v.dtype, v.shape[1]) for x, v in zip(names, arrays)],
                         metadata=metadata)

        rtn = np.rec.fromarrays(arrays, dtype=dtype, names=names)
        # For some reason the dtype metadata is lost in the line above
        # and setting rtn.dtype to dtype does not preserve the metadata
        # see https://github.com/numpy/numpy/issues/6771

        return (rtn, dtype)

    def can_convert_to_records_without_objects(self, df, symbol):
        # We can't easily distinguish string columns from objects
        try:
            arr,_ = self.to_records(df)
        except Exception as e:
            # This exception will also occur when we try to write the object so we fall-back to saving using Pickle
            log.info('Pandas dataframe %s caused exception "%s" when attempting to convert to records. Saving as Blob.'
                     % (symbol, repr(e)))
            return False
        else:
            if arr.dtype.hasobject:
                log.info('Pandas dataframe %s contains Objects, saving as Blob' % symbol)
                # Will fall-back to saving using Pickle
                return False
            elif any([len(x[0].shape) for x in arr.dtype.fields.values()]):
                log.info('Pandas dataframe %s contains >1 dimensional arrays, saving as Blob' % symbol)
                return False
            else:
                return True

    def _segment_index(self, recarr, existing_index, start, new_segments):
        """
        Generate index of datetime64 -> item offset.

        Parameters:
        -----------
        new_data: new data being written (or appended)
        existing_index: index field from the versions document of the previous version
        start: first (0-based) offset of the new data
        segments: list of offsets. Each offset is the row index of the
                  the last row of a particular chunk relative to the start of the _original_ item.
                  array(new_data) - segments = array(offsets in item)

        Returns:
        --------
        Binary(compress(array([(index, datetime)]))
            Where index is the 0-based index of the datetime in the DataFrame
        """
        # find the index of the first datetime64 column
        idx_col = self._datetime64_index(recarr)
        # if one exists let's create the index on it
        if idx_col is not None:
            new_segments = np.array(new_segments, dtype='i8')
            last_rows = recarr[new_segments - start]
            # create numpy index
            index = np.core.records.fromarrays([last_rows[idx_col]]
                                               + [new_segments, ],
                                               dtype=INDEX_DTYPE)
            # append to existing index if exists
            if existing_index:
                existing_index_arr = np.fromstring(decompress(existing_index), dtype=INDEX_DTYPE)
                if start > 0:
                    existing_index_arr = existing_index_arr[existing_index_arr['index'] < start]
                index = np.concatenate((existing_index_arr, index))
            return Binary(compress(index.tostring()))
        elif existing_index:
            raise ArcticException("Could not find datetime64 index in item but existing data contains one")
        return None

    def _datetime64_index(self, recarr):
        """ Given a np.recarray find the first datetime64 column """
        # TODO: Handle multi-indexes
        names = recarr.dtype.names
        for name in names:
            if recarr[name].dtype == DTN64_DTYPE:
                return name
        return None

    def _index_range(self, version, symbol, date_range=None, **kwargs):
        """ Given a version, read the segment_index and return the chunks associated
        with the date_range. As the segment index is (id -> last datetime)
        we need to take care in choosing the correct chunks. """
        if date_range and 'segment_index' in version:
            index = np.fromstring(decompress(version['segment_index']), dtype=INDEX_DTYPE)
            dtcol = self._datetime64_index(index)
            if dtcol and len(index):
                dts = index[dtcol]
                start, end = _start_end(date_range, dts)
                if start > dts[-1]:
                    return -1, -1
                idxstart = min(np.searchsorted(dts, start), len(dts) - 1)
                idxend = min(np.searchsorted(dts, end), len(dts) - 1)
                return int(index['index'][idxstart]), int(index['index'][idxend] + 1)
        return super(PandasStore, self)._index_range(version, symbol, **kwargs)

    def _daterange(self, recarr, date_range):
        """ Given a recarr, slice out the given artic.date.DateRange if a
        datetime64 index exists """
        idx = self._datetime64_index(recarr)
        if idx and len(recarr):
            dts = recarr[idx]
            mask = Series(np.zeros(len(dts)), index=dts)
            start, end = _start_end(date_range, dts)
            mask[start:end] = 1.0
            return recarr[mask.values.astype(bool)]
        return recarr

    def read(self, arctic_lib, version, symbol, read_preference=None, date_range=None, **kwargs):
        item = super(PandasStore, self).read(arctic_lib, version, symbol, read_preference,
                                             date_range=date_range, **kwargs)
        if date_range:
            item = self._daterange(item, date_range)
        return item

    def get_info(self, version):
        """
        parses out the relevant information in version
        and returns it to the user in a dictionary
        """
        ret = super(PandasStore, self).get_info(version)
        ret['col_names'] = version['dtype_metadata']
        ret['handler'] = self.__class__.__name__
        ret['dtype'] = ast.literal_eval(version['dtype'])
        return ret


def _start_end(date_range, dts):
    """
    Return tuple: [start, end] of np.datetime64 dates that are inclusive of the passed
    in datetimes.
    """
    # FIXME: timezones
    assert len(dts)
    _assert_no_timezone(date_range)
    date_range = to_pandas_closed_closed(date_range, add_tz=False)
    start = np.datetime64(date_range.start) if date_range.start else dts[0]
    end = np.datetime64(date_range.end) if date_range.end else dts[-1]
    return start, end


def _assert_no_timezone(date_range):
    for _dt in (date_range.start, date_range.end):
        if _dt and _dt.tzinfo is not None:
            raise ValueError("DateRange with timezone not supported")


struct PandasSeriesStore
{
	PandasStore pandasStore;
	alias pandasStore this;
	enum type="pandasseries";

    def _column_data(self, s):
        columns = [s.name if s.name else 'values']
        column_vals = [s.values]
        return columns, column_vals

    def from_records(self, recarr):
        index = self._index_from_records(recarr)
        name = recarr.dtype.names[-1]
        return Series.from_array(recarr[name], index=index, name=name)

    def can_write(self, version, symbol, data):
        if isinstance(data, Series):
            if data.dtype == np.object_ or data.index.dtype == np.object_:
                return self.can_convert_to_records_without_objects(data, symbol)
            return True
        return False

    def write(self, arctic_lib, version, symbol, item, previous_version):
        item, md = self.to_records(item)
        super(PandasSeriesStore, self).write(arctic_lib, version, symbol, item, previous_version, dtype=md)

    def append(self, arctic_lib, version, symbol, item, previous_version):
        item, md = self.to_records(item)
        super(PandasSeriesStore, self).append(arctic_lib, version, symbol, item, previous_version, dtype=md)

    def read(self, arctic_lib, version, symbol, **kwargs):
        item = super(PandasSeriesStore, self).read(arctic_lib, version, symbol, **kwargs)
        return self.from_records(item)
}

struct PandasDataFrameStore
{
	PandasStore pandasStore;
	alias pandasStore this;

	enum type="pandassdf";

    def _column_data(self, df):
        columns = list(map(str, df.columns))
        column_vals = [df[c].values for c in df.columns]
        return columns, column_vals

    def from_records(self, recarr):
        index = self._index_from_records(recarr)
        column_fields = [x for x in recarr.dtype.names if x not in recarr.dtype.metadata['index']]
        if len(recarr) == 0:
            rdata = recarr[column_fields] if len(column_fields) > 0 else None
            return DataFrame(rdata, index=index)

        columns = recarr.dtype.metadata['columns']
        return DataFrame(data=recarr[column_fields], index=index, columns=columns)

    def can_write(self, version, symbol, data):
        if isinstance(data, DataFrame):
            if np.any(data.dtypes.values == 'object'):
                return self.can_convert_to_records_without_objects(data, symbol)
            return True
        return False

    def write(self, arctic_lib, version, symbol, item, previous_version):
        item, md = self.to_records(item)
        super(PandasDataFrameStore, self).write(arctic_lib, version, symbol, item, previous_version, dtype=md)

    def append(self, arctic_lib, version, symbol, item, previous_version):
        item, md = self.to_records(item)
        super(PandasDataFrameStore, self).append(arctic_lib, version, symbol, item, previous_version, dtype=md)

    def read(self, arctic_lib, version, symbol, **kwargs):
        item = super(PandasDataFrameStore, self).read(arctic_lib, version, symbol, **kwargs)
        return self.from_records(item)

}

struct PandasPanelStore
{
	PandasDataFrameStore dataFrame;
	alias dataFrame this;
	enum type="pandaspan";
}

bool canWrite(ref PandasPanelStore store, string version_, string symbol, Data data)
{
        if isInstance(data, Panel)
	{
            auto frame = data.toFrame(filter_observations=False);
            if (np.any(frame.dtypes.values == "object")
                return store.canConvertToRecordsWithoutObjects(frame, symbol);
            return true;
        return false;
}

auto write(ref PandasPanelStore store, string arcticLib, string version_, string symbol, string item, string previousVersion)
{
        if (np.product(item.shape) == 0)
	{
            // Currently not supporting zero size panels as they drop indices when converting to dataframes
            // Plan is to find a better solution in due course.
            throw new ValueException("Cannot insert a zero size panel into mongo");
	}
        if (!np.all(i.names.length == 1 for i in item.axes)
            throw new ValueException("Cannot insert panels with multiindexes");
	 
        item = item.toFrame(filter_observations=false);
        if (set(item.dtypes).length == 1)
	{
            // If all columns have the same dtype, we support non-string column names.
            // We know from above check that columns is not a multiindex.
            item = DataFrame(item.stack());
	}
	else if (item.columns.dtype != np.dtype("object")
            throw new ValueException("Cannot support non-object dtypes for columns");
        store.dataFrame..write(arcticLib, version_, symbol, item, previousVersion);
}

auto read(ref PandasPanelStore store, string arcticLib, string version_, string symbol, string[string] kwargs)
{
        auto item = dataFrame.read(arcticLib,version_,symbol,kwargs);
        if (item.index.names.length == 3)
            return item.iloc[:, 0].unstack().toPanel();
        return item.to_panel();
}

auto append(ref PandasPanelStore store, string arcticLib, string version_, string symbol, string item, string previousVersion)
{
        throw new ValueError("Appending not supported for pandas.Panel");
}


enum _MAGIC_CHUNKED = "__chunked__";
enum _CHUNK_SIZE = 15 * 1024 * 1024  // 15MB
enum _MAX_BSON_ENCODE = 256 * 1024  // 256K - don't fill up the version document with encoded bson


struct PickleStore
{
    void initialize_library() //classmethod
    {
    }

    auto getInfo(string version_)
    {
        string[string] ret;
        ret["type"] = "blob";
        ret["handler"] = "PickleStore";
        return ret;
    }

    auto read(string mongooseLib, string version_, string symbol, string[string] kwargs)
    {
	Data data;
        auto blob = version_.get("blob");
        if (blob !is null)
	{
            if (blob == _MAGIC_CHUNKED)
	    {
                auto collection = mongoose_lib.get_top_level_collection();
                data = b''.join(x['data'] for x in collection.find({'symbol': symbol,
                                                                   'parent': version['_id']},
                                                                   sort=[('segment', pymongo.ASCENDING)]));
	    }
            else
	    {
                data = blob;
	    }
            // Backwards compatibility
            data = lz4.decompress(data);
            return pickle_compat_load(io.BytesIO(data));
	}
        return version_["data"];
    }

    auto write(string arcticLib, string version_, string symbol, string item, string previousVersion)
    {
        try
	{
            // If it's encodeable, then ship it
            auto b = bson.BSON.encode({'data': item});
            if len(b) < _MAX_BSON_ENCODE
	    {
                version['data'] = item;
                return;
	    }
	}
	catch(Exception e)
	{
		if (e!=InvalidDocument)
			throw(e);
	}


        // Pickle, chunk and store the data
        collection = arctic_lib.get_top_level_collection()
        # Try to pickle it. This is best effort
        version['blob'] = _MAGIC_CHUNKED
        pickled = lz4.compressHC(cPickle.dumps(item, protocol=cPickle.HIGHEST_PROTOCOL))

        for i in xrange(int(len(pickled) / _CHUNK_SIZE + 1)):
            segment = {'data': Binary(pickled[i * _CHUNK_SIZE : (i + 1) * _CHUNK_SIZE])}
            sha = checksum(symbol, segment)
            segment['segment'] = i
            collection.update_one({'symbol': symbol, 'sha': sha}, {'$set': segment,
                                                               '$addToSet': {'parent': version['_id']}},
                                       upsert=True)
    }
}
