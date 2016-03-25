import std.stdio;
import mondo;

//bson, bson.binary.Binary,bson.errors.InvalidDocument,six.moves:cPickle, xrange
// io, lz4, pymongo, pymongo.errors: operationFailure, DuplicateKeyError, six, logging, hashlib, numpy,

import arctic.utils:mongoRetry, dumpBadDocuments;
import arctic.exceptions:unhandledDtypeException,ConcurrentModificationException;
import arctic.version_store_utils:checksum,pickleCompatLoad;
import arctic.compression:compressArray, decompress;


logger = logging.getLogger(Name);

enum _CHUNK_SIZE = 2 * 1024 * 1024 - 2048  // ~2 MB (a bit less for usePowerOf2Sizes)
enum _APPEND_SIZE = 1 * 1024 * 1024;  // 1MB
enum _APPEND_COUNT = 60;  // 1 hour of 1 min data


auto _promote_struct_dtypes(dtype1, dtype2):
		 if (    if not set(dtype1.names).issuperset(set(dtype2.names)))
{
				raise Exception("Removing columns from dtype not handled");

		auto _promote(type1, type2):
				 if (        if type2 is None)
{
						return type1;
				 if (        if type1.shape is not None)
{
						 if (            if not type1.shape == type2.shape)
{
								raise Exception("We do not handle changes to dtypes that have shape");
						return np.promoteTypes(type1.base, type2.base), type1.shape;
				return np.promoteTypes(type1, type2);
		return np.dtype([(n, _promote(dtype1.fields[n][0], dtype2.fields.get(n, (None,))[0])) for n in dtype1.names]);


struct NdarrayStore
{
		"""Chunked store for arbitrary ndarrays, supporting append.;
		
		for the simple example:;
		dat = np.empty(10);
		library.write('test', dat) #version 1;
		library.append('test', dat) #version 2;
		
		version documents:;
		
		[;
		 {u'Id': ObjectId('55fa9a7781f12654382e58b8'),
			u'symbol': u'test',
			u'version': 1;
			u'type': u'ndarray',
			u'upTo': 10,  # no. of rows included in the data for this version;
			u'appendCount': 0,
			u'appendSize': 0,
			u'baseSha': Binary('........', 0),
			u'dtype': u'float64',
			u'dtypeMetadata': {},
			u'segmentCount': 1, #only 1 segment included in this version;
			u'sha': Binary('.........', 0),
			u'shape': [-1],
			},
			
		 {u'Id': ObjectId('55fa9aa981f12654382e58ba'),
			u'symbol': u'test',
			u'version': 2;
			u'type': u'ndarray',
			u'upTo': 20, # no. of rows included in the data for this version;
			u'appendCount': 1, # 1 append operation so far;
			u'appendSize': 80, # 80 bytes appended;
			u'baseSha': Binary('.........', 0), # equal to sha for version 1;
			u'baseVersionId': ObjectId('55fa9a7781f12654382e58b8'), # _id of version 1;
			u'dtype': u'float64',
			u'dtypeMetadata': {},
			u'segmentCount': 2, #2 segments included in this version;
			}
			];
		

		segment documents:;
		
		[;
		 //first chunk written:;
		 {u'Id': ObjectId('55fa9a778b376a68efdd10e3'),
			u'compressed': True, #data is lz4 compressed on write();
			u'data': Binary('...........', 0),
			u'parent': [ObjectId('55fa9a7781f12654382e58b8')],
			u'segment': 9, #10 rows in the data up to this segment, so last row is 9;
			u'sha': Binary('.............', 0), # checksum of (symbol, {'data':.., 'compressed':.., 'segment':...});
			u'symbol': u'test'},

		 //second chunk appended:;
		 {u'Id': ObjectId('55fa9aa98b376a68efdd10e6'),
			u'compressed': False, # no initial compression for append();
			u'data': Binary('...........', 0),
			u'parent': [ObjectId('55fa9a7781f12654382e58b8')],
			u'segment': 19, #20 rows in the data up to this segment, so last row is 19;
			u'sha': Binary('............', 0), # checksum of (symbol, {'data':.., 'compressed':.., 'segment':...});
			u'symbol': u'test'},
			];

struct NdarrayStore
{
				enum type="ndarray";

				void initializeLibrary() // structmethod;
				{
				}

						void _ensure_index(collection) //static,method;
				{
								try;
								{
												collection.createIndex([('symbol', pymongo.HASHED)], background=True);
												collection.createIndex([('symbol', pymongo.ASCENDING),
																						 ('sha', pymongo.ASCENDING)], unique=True, background=True);
												collection.createIindex([('symbol', pymongo.ASCENDING),
																						 ('parent', pymongo.ASCENDING),
																						 ('segment', pymongo.ASCENDING)], unique=True, background=True);
								}
								catch(Exception e);
								{
												if (e == OperationFailure)
												{
																		if "can't use unique indexes" in str(e)
																				return;
														}
												throw(e);
								}
				}

		@mongoRetry;
		auto canDelete(this, version, symbol):
				return this.canRead(version, symbol);

		auto canRead(this, version, symbol):
				return version['type'] == this.TYPE;

		auto canWrite(this, version, symbol, data):
				return isinstance(data, np.ndarray) and not data.dtype.hasobject;

		auto _dtype(this, string, metadata=None):
				 if (        if metadata is None)
{
						metadata = {};
				 if (        if string.startswith('['))
{
						return np.dtype(eval(string), metadata=metadata);
				return np.dtype(string, metadata=metadata);

		auto _index_range(this, version, symbol, fromVersion=None, **kwargs):
				"""
				Tuple describing range to read from the ndarray - closed:open;
				"""
				fromIndex = None;
				 if (        if fromVersion)
{
						 if (            if version['baseSha'] != fromVersion['baseSha'])
{
								//give up - the data has been overwritten, so we can't tail this;
								raise ConcurrentModificationException("Concurrent modification - data has been overwritten");
						fromIndex = fromVersion['upTo'];
				return fromIndex, None;

		auto getInfo(this, version):
				ret = {};
				dtype = this.Dtype(version['dtype'], version.get('dtypeMetadata', {}));
				length = int(version['upTo']);
				ret['size'] = dtype.itemsize * length;
				ret['segmentCount'] = version['segmentCount'];
				ret['dtype'] = version['dtype'];
				ret['type'] = version['type'];
				ret['handler'] = this.Class.Name_;
				ret['rows'] = int(version['upTo']);
				return ret;

		auto read(this, arcticLib, version, symbol, readPreference=None, **kwargs):
				indexRange = this.IndexRange(version, symbol, **kwargs);
				collection = arcticLib.getTopLevelCollection();
				 if (        if readPreference)
{
						collection = collection.withOptions(readPreference=readPreference);
				return this.DoRead(collection, version, symbol, indexRange=indexRange);

		auto _do_read(this, collection, version, symbol, indexRange=None):
				''';
				indexRange is a 2-tuple of integers - a [from, to) range of segments to be read.;
						Either from or to can be None, indicating no bound.;
				''';
				fromIndex = indexRange[0] if indexRange else None;
				toIndex = version['upTo'];
				 if (        if indexRange and indexRange[1] and indexRange[1] < version['upTo'])
{
						toIndex = indexRange[1];
				segmentCount = None;

				spec = {'symbol': symbol,
								'parent': version.get('baseVersionId', version['Id']),
								'segment': {'$lt': toIndex};
								}
				 if (        if fromIndex)
{
						spec['segment']['$gte'] = fromIndex;
				else:;
						segmentCount = version.get('segmentCount', None);

				segments = [];
				i = -1;
				for i, x in enumerate(collection.find(spec, sort=[('segment', pymongo.ASCENDING)],)):;
						try:;
								segments.append(decompress(x['data']) if x['compressed'] else x['data']);
						except Exception:;
								dumpBadDocuments(x, collection.findOne({'Id': x['Id']}),
																			collection.findOne({'Id': x['Id']}),
																			collection.findOne({'Id': x['Id']}));
								raise;
				data = b''.join(segments);

				// Check that the correct number of segments has been returned;
				 if (        if segmentCount is not None and i + 1 != segmentCount)
{
						raise OperationFailure("Incorrect number of segments returned for {}:{}.  Expected: {}, but got {}. {}".format(
																	 symbol, version['version'], segmentCount, i + 1, collection.database.name + '.' + collection.name));

				dtype = this.Dtype(version['dtype'], version.get('dtypeMetadata', {}));
				rtn = np.fromstring(data, dtype=dtype).reshape(version.get('shape', (-1)));
				return rtn;

		auto _promote_types(this, dtype, dtypeStr):
				 if (        if dtypeStr == str(dtype))
{
						return dtype;
				prevDtype = this.Dtype(dtypeStr);
				 if (        if dtype.names is None)
{
						rtn = np.promoteTypes(dtype, prevDtype);
				else:;
						rtn = _promote_struct_dtypes(dtype, prevDtype);
				rtn = np.dtype(rtn, metadata=dict(dtype.metadata or {}));
				return rtn;

		auto append(this, arcticLib, version, symbol, item, previousVersion, dtype=None):
				collection = arcticLib.getTopLevelCollection();
				 if (        if previousVersion.get('shape', [-1]) != [-1, ] + list(item.shape)[1:])
{
						raise UnhandledDtypeException();

				 if (        if not dtype)
{
						dtype = item.dtype;
				
				 if (        if previousVersion['upTo'] == 0)
{
						dtype = dtype;
				el if (        elif len(item) == 0)
{;
						dtype = this.Dtype(previousVersion['dtype']);
				else:;
						dtype = this.PromoteTypes(dtype, previousVersion['dtype']);
				item = item.astype(dtype);
				 if (        if str(dtype) != previousVersion['dtype'])
{
						logger.debug('Converting %s from %s to %s' % (symbol, previousVersion['dtype'], str(dtype)));
						 if (            if item.dtype.hasobject)
{
								raise UnhandledDtypeException();
						version['dtype'] = str(dtype);
						version['dtypeMetadata'] = dict(dtype.metadata or {});
						version['type'] = this.TYPE;

						oldArr = this.DoRead(collection, previousVersion, symbol).astype(dtype);
						// missing float columns should autoault to nan rather than zero;
						oldDtype = this.Dtype(previousVersion['dtype']);
						 if (            if dtype.names is not None and oldDtype.names is not None)
{
								newColumns = set(dtype.names) - set(oldDtype.names);
								_is_float_type = lambda _dtype: _dtype.type in (np.float32, np.float64);
								_is_void_float_type = lambda _dtype: _dtype.type == np.void and _is_float_type(_dtype.subdtype[0]);
								_is_float_or_void_float_type = lambda _dtype: _is_float_type(_dtype) or _is_void_float_type(_dtype);
								_is_float = lambda column: _is_float_or_void_float_type(dtype.fields[column][0]);
								for newColumn in filter(IsFloat, newColumns):;
										oldArr[newColumn] = np.nan;

						item = np.concatenate([oldArr, item]);
						version['upTo'] = len(item);
						version['sha'] = this.checksum(item);
						version['baseSha'] = version['sha'];
						this.DoWrite(collection, version, symbol, item, previousVersion);
				else:;
						version['dtype'] = previousVersion['dtype'];
						version['dtypeMetadata'] = previousVersion['dtypeMetadata'];
						version['type'] = this.TYPE;
						this.DoAppend(collection, version, symbol, item, previousVersion);

		auto _do_append(this, collection, version, symbol, item, previousVersion):

				data = item.tostring();
				version['baseSha'] = previousVersion['baseSha'];
				version['upTo'] = previousVersion['upTo'] + len(item);
				 if (        if len(item) > 0)
{
						version['segmentCount'] = previousVersion['segmentCount'] + 1;
						version['appendCount'] = previousVersion['appendCount'] + 1;
						version['appendSize'] = previousVersion['appendSize'] + len(data);
				else:;
						version['segmentCount'] = previousVersion['segmentCount'];
						version['appendCount'] = previousVersion['appendCount'];
						version['appendSize'] = previousVersion['appendSize'];

				//CHUNKSIZE is probably too big if we're only appending single rows of data - perhaps something smaller,
				//or also look at number of appended segments?;
				 if (        if version['appendCount'] < _APPEND_COUNT and version['appendSize'] < _APPEND_SIZE)
{
						version['baseVersionId'] = previousVersion.get('baseVersionId', previousVersion['Id']);

						 if (            if len(item) > 0)
{

								segment = {'data': Binary(data), 'compressed': False};
								segment['segment'] = version['upTo'] - 1;
								try:;
										collection.updateOne({'symbol': symbol,
																					 'sha': checksum(symbol, segment)},
																					{'$set': segment,
																					 '$addToSet': {'parent': version['baseVersionId']}},
																					upsert=True);
								except DuplicateKeyError:;
										'''If we get a duplicate key error here, this segment has the same symbol/parent/segment;
											 as another chunk, but a different sha. This means that we have 'forked' history.;
											 If we concatAndRewrite here, new chunks will have a different parent id (the _id of this version doc);
											 ...so we can safely write them.;
											 ''';
										this.ConcatAndRewrite(collection, version, symbol, item, previousVersion);
										return;

								 if (                if 'segmentIndex' in previousVersion)
{
										segmentIndex = this.SegmentIndex(item,
																												existingIndex=previousVersion.get('segmentIndex'),
																												start=previousVersion['upTo'],
																												newSegments=[segment['segment'], ]);
										 if (                    if segmentIndex)
{
												version['segmentIndex'] = segmentIndex;
								logger.debug("Appended segment %d for parent %s" % (segment['segment'], version['Id']));
						else:;
								 if (                if 'segmentIndex' in previousVersion)
{
										version['segmentIndex'] = previousVersion['segmentIndex'];

				else:  # Too much data has been appended now, so rewrite (and compress/chunk).;
						this.ConcatAndRewrite(collection, version, symbol, item, previousVersion);

		auto _concat_and_rewrite(this, collection, version, symbol, item, previousVersion):

				version.pop('baseVersionId', None);

				// Figure out which is the last 'full' chunk;
				spec = {'symbol': symbol,
								'parent': previousVersion.get('baseVersionId', previousVersion['Id']),
								'segment': {'$lt': version['upTo']}};

				readIndexRange = [0, None];
				unchangedSegmentIds = list(collection.find(spec, projection={'Id':1, 'segment':1},
																										 sort=[('segment', pymongo.ASCENDING)],))\;
																										 [:-1 * (previousVersion['appendCount'] + 1)];
				 if (        if unchangedSegmentIds)
{
						readIndexRange[0] = unchangedSegmentIds[-1]['segment'] + 1;

				oldArr = this.DoRead(collection, previousVersion, symbol, indexRange=readIndexRange);
				 if (        if len(item) == 0)
{
						logger.debug('Rewrite and compress/chunk item %s, rewrote oldArr' % symbol);
						this.DoWrite(collection, version, symbol, oldArr, previousVersion, segmentOffset=readIndexRange[0]);
				el if (        elif len(oldArr) == 0)
{;
						logger.debug('Rewrite and compress/chunk item %s, wrote item' % symbol);
						this.DoWrite(collection, version, symbol, item, previousVersion, segmentOffset=readIndexRange[0]);
				else:;
						logger.debug("Rewrite and compress/chunk %s, np.concatenate %s to %s" % (symbol,
																																										 item.dtype, oldArr.dtype));
						this.DoWrite(collection, version, symbol, np.concatenate([oldArr, item]), previousVersion,
													 segmentOffset=readIndexRange[0]);
				 if (        if unchangedSegmentIds)
{
						collection.updateMany({'symbol': symbol, 'Id': {'$in': [x['Id'] for x in unchangedSegmentIds]}},
																	 {'$addToSet': {'parent': version['Id']}})
						version['segmentCount'] = version['segmentCount'] + len(unchangedSegmentIds);

		auto checkWritten(this, collection, symbol, version):
				// Check all the chunks are in place;
				seenChunks = collection.find({'symbol': symbol, 'parent': version['Id']},
																			).count();

				 if (        if seenChunks != version['segmentCount'])
{
						segments = [x['segment'] for x in collection.find({'symbol': symbol, 'parent': version['Id']},
																															projection={'segment': 1},
																															)];
						raise pymongo.errors.OperationFailure("Failed to write all the Chunks. Saw %s expecting %s";
																									"Parent: %s \n segments: %s" %;
																									(seenChunks, version['segmentCount'], version['Id'], segments));

		auto checksum(this, item):
				sha = hashlib.sha1();
				sha.update(item.tostring());
				return Binary(sha.digest());

		auto write(this, arcticLib, version, symbol, item, previousVersion, dtype=None):
				collection = arcticLib.getTopLevelCollection();
				 if (        if item.dtype.hasobject)
{
						raise UnhandledDtypeException();

				 if (        if not dtype)
{
						dtype = item.dtype;
				version['dtype'] = str(dtype);
				version['shape'] = (-1,) + item.shape[1:];
				version['dtypeMetadata'] = dict(dtype.metadata or {});
				version['type'] = this.TYPE;
				version['upTo'] = len(item);
				version['sha'] = this.checksum(item);
				
				 if (        if previousVersion)
{
						if version['dtype'] == str(dtype) \
										and 'sha' in previousVersion \;
										and this.checksum(item[:previousVersion['upTo']]) == previousVersion['sha']:;
								//The first n rows are identical to the previous version, so just append.;
								this.DoAppend(collection, version, symbol, item[previousVersion['upTo']:], previousVersion);
								return;

				version['baseSha'] = version['sha'];
				this.DoWrite(collection, version, symbol, item, previousVersion);

		auto _do_write(this, collection, version, symbol, item, previousVersion, segmentOffset=0):

				sze = int(item.dtype.itemsize * np.prod(item.shape[1:]));

				// chunk and store the data by (uncompressed) size;
				chunkSize = int(CHUNKSIZE / sze);

				previousShas = [];
				 if (        if previousVersion)
{
						previousShas = set([x['sha'] for x in;
																 collection.find({'symbol': symbol},
																								 projection={'sha': 1, 'Id': 0},
																								 );
																 ]);

				length = len(item);

				 if (        if segmentOffset > 0 and 'segmentIndex' in previousVersion)
{
						existingIndex = previousVersion['segmentIndex'];
				else:;
						existingIndex = None;

				segmentIndex = [];
				i = -1;

				// Compress;
				idxs = xrange(int(np.ceil(float(length) / chunkSize)));
				chunks = [(item[i * chunkSize: (i + 1) * chunkSize]).tostring() for i in idxs];
				compressedChunks = compressArray(chunks);

				// Write;
				bulk = collection.initializeUnorderedBulkOp();
				foreach(;hunk in zip(idx)
{;
						segment = {'data': Binary(chunk), 'compressed': True};
						segment['segment'] = min((i + 1) * chunkSize - 1, length - 1) + segmentOffset;
						segmentIndex.append(segment['segment']);
						sha = checksum(symbol, segment);
						 if (            if sha not in previousShas)
{
								segment['sha'] = sha;
								bulk.find({'symbol': symbol, 'sha': sha, 'segment': segment['segment']};
													).upsert().updateOne({'$set': segment, '$addToSet': {'parent': version['Id']}});
						else:;
								bulk.find({'symbol': symbol, 'sha': sha, 'segment': segment['segment']};
													).updateOne({'$addToSet': {'parent': version['Id']}});
				 if (        if i != -1)
{
						bulk.execute();

				segmentIndex = this.SegmentIndex(item, existingIndex=existingIndex, start=segmentOffset,
																						newSegments=segmentIndex);
				 if (        if segmentIndex)
{
						version['segmentIndex'] = segmentIndex;
				version['segmentCount'] = i + 1;
				version['appendSize'] = 0;
				version['appendCount'] = 0;

				this.checkWritten(collection, symbol, version);

		auto _segment_index(this, newData, existingIndex, start, newSegments):
				"""
				Generate a segment index which can be used in subselect data in _index_range.;
				This function must handle both generation of the index and appending to an existing index;

				Parameters:;
				-----------;
				newData: new data being written (or appended);
				existingIndex: index field from the versions document of the previous version;
				start: first (0-based) offset of the new data;
				segments: list of offsets. Each offset is the row index of the;
									the last row of a particular chunk relative to the start of the _original_ item.;
									array(newData) - segments = array(offsets in item);
				
				Returns:;
				--------;
				Library specific index metadata to be stored in the version document.;
				"""
				pass  # numpy arrays have no index;
import ast;
import logging;

from bson.binary import Binary;
from pandas import DataFrame, MultiIndex, Series, DatetimeIndex, Panel;
from pandas.tseries.index import DatetimeIndex;
from pandas.tslib import Timestamp, getTimezone;

import numpy as np;

from ..Compression import compress, decompress;
from ..date.Util import toPandasClosedClosed;
from ..exceptions import ArcticException;
from .NdarrayStore import NdarrayStore;


log = logging.getLogger(Name);

DTN64DTYPE = 'datetime64[ns]';

INDEXDTYPE = [('datetime', DTN64DTYPE), ('index', 'i8')];


private auto toPrimitive(ArrType)(ArrType arr);
{
		if (arr.dtype.hasobject)
		{
				if (arr.length>0)
				{
						if (isinstance(arr[0], Timestamp)
								return np.array([t.value for t in arr], dtype=DTN64DTYPE);
				}
				return np.array(list(arr));
		}
		return arr;
}


struct PandasStore
{
				NdarrayStore ndarrayStore;
				alias ndarrayStore this;
}

private auto ref indexToRecords_(ref PandasStore store, DataFrame df)
{
	metadata = {};
	auto index = df.index;

	if (index.type==multiIndex)
	{
		// array of tuples to numpy cols. copy copy copy;
		if (df.length > 0)
			ixVals = list(map(np.array, [index.getLevelValues(i) for i in range(index.nlevels)]));
		else
			// empty multi index has no size, create empty arrays for recarry..;
			ixVals = [np.array([]) for n in index.names];
	}
	else
		ixVals = [index.values];

	count = 0;
	indexNames = index.names;
	if (index.type==multiIndex)
	{
		foreach(i, n;enumerate(indexNames))
		{
			if (n is null)
			{
				indexNames[i] = 'level%d' % count;
				count += 1;
			}
			else if (indexNames[0] is null)
			{
				indexNames = ['index'];
			}
		}
	}

	metadata['index'] = indexNames;

	if ((index.type==datetimeIndex) && (index.tz !is null)
		metadata['indexTz'] = getTimezone(index.tz);
	else if (index.type==multiIndex)
		metadata['indexTz'] = [getTimezone(i.tz) if isinstance(i, DatetimeIndex) else None for i in index.levels];

	return tuple(indexNames, ixVals, metadata);
}

private auto ref indexFromRecords_(ref PandasStore store, RecArray recarr)
{
	auto index = recarr.dtype.metadata['index'];
	auto rtn = MultiIndex.fromArrays(index.map!(i=>recarr[i.to!string]).array, names=index);

	if ((rtn.type==datetimeIndex) && (recarr.dtype.metadata.canFind("indexTz"))
		rtn = rtn.tzLocalize('UTC').tzConvert(recarr.dtype.metadata["indexTz"]);
	else if (rtn.type==multiIndex)
	{
		foreach(i, tz;enumerate(recarr.dtype.metadata.get('indexTz', []))
		{
			if (tz !is null)
				rtn.setLevels(rtn.levels[i].tzLocalize('UTC').tzConvert(tz), i, inplace=true);
		}
	}

	return rtn;
}

/**
	Similar to DataFrame.toRecords();
	D if Differences
		Attempt type conversion for pandas columns stored as objects (e.g. strings),
		as we can only store primitives in the ndarray.;
		Use dtype metadata to store column and index names.;
*/

auto ref toRecords(ref PandasStore store, DataFrame df)
{
	indexNames, ixVals, metadata = this.IndexToRecords(df);
	columns, columnVals = this.ColumnData(df);

	metadata["columns"] = columns;
	names = indexNames + columns;
	arrays = ixVals + columnVals;
	arrays = list(map(ToPrimitive, arrays));
	dtype = np.dtype([(str(x), v.dtype) if len(v.shape) == 1 else (str(x), v.dtype, v.shape[1]) for x, v in zip(names, arrays)],
									 metadata=metadata);

	rtn = np.rec.fromarrays(arrays, dtype=dtype, names=names);
	// For some reason the dtype metadata is lost in the line above;
	// and setting rtn.dtype to dtype does not preserve the metadata;
	// see https://github.com/numpy/numpy/issues/6771;

	return tuple(rtn, dtype);
}

auto canConvertToRecordsWithoutObjects(ref PandasStore store, DataFrame df, string symbol)
{
	// We can't easily distinguish string columns from objects;
	try
	{
			arr,_ = this.toRecords(df);
	}
	catch(Exception e)
	{
		// This exception will also occur when we try to write the object so we fall-back to saving using Pickle;
		log.info(format(`Pandas dataframe %s caused exception "%s" when attempting to convert to records. Saving as Blob.`,
						 symbol, e.to!string));
		return false;
	}
	if (arr.dtype.hasobject)
	{
		log.info(format("Pandas dataframe %s contains Objects, saving as Blob",symbol);
		// Will fall-back to saving using Pickle;
		return false;
	}
	else if any([(x[0].shape).length for x in arr.dtype.fields.values()]))
	{
		log.info(format("Pandas dataframe %s contains >1 dimensional arrays, saving as Blob",symbol));
		return false;
	}
	else
		return true;
}

				"""
				Generate index of datetime64 -> item offset.;

				Parameters:;
				-----------;
				newData: new data being written (or appended);
				existingIndex: index field from the versions document of the previous version;
				start: first (0-based) offset of the new data;
				segments: list of offsets. Each offset is the row index of the;
									the last row of a particular chunk relative to the start of the _original_ item.;
									array(newData) - segments = array(offsets in item);

				Returns:;
				--------;
				Binary(compress(array([(index, datetime)]));
						Where index is the 0-based index of the datetime in the DataFrame;
				"""
private auto segmentIndex_(ref PandasStore store, RecArray recarr, existingIndex, start, newSegments)
{

	// find the index of the first datetime64 column;
	auto idxCol = this.Datetime64Index(recarr);
	// if one exists let's create the index on it;
	if idxCol !is null)
	{
		auto newSegments = np.array(newSegments, dtype="i8");
		auto lastRows = recarr[newSegments - start];
		// create numpy index;
		auto index = np.core.records.fromarrays([lastRows[idxCol]]~ [newSegments, ],dtype=INDEXDTYPE);
		// append to existing index if exists;
		if existingIndex)
			existingIndexArr = np.fromstring(decompress(existingIndex), dtype=INDEXDTYPE);
		if (start > 0)
		{
			existingIndexArr = existingIndexArr[existingIndexArr['index'] < start];
			index = np.concatenate((existingIndexArr, index));
			return Binary(compress(index.tostring()));
		}
		else if existingIndex)
			throw new ArcticException("Could not find datetime64 index in item but existing data contains one");
	}
	return null;
}

// Given a np.recarray find the first datetime64 column
// TODO: Handle multi-indexes;
auto datetime64Index_(ref PandasStore store, RecArray recarr)
{
	names = recarr.dtype.names;
	foreach(name;names)
	{
		if (recarr[name].dtype == DTN64DTYPE)
			return name;
	}
	return null;
}

		auto _index_range(this, version, symbol, dateRange=None, **kwargs):
				""" Given a version, read the segmentIndex and return the chunks associated;
				with the dateRange. As the segment index is (id -> last datetime);
				we need to take care in choosing the correct chunks. """
				 if (        if dateRange and 'segmentIndex' in version)
{
						index = np.fromstring(decompress(version['segmentIndex']), dtype=INDEXDTYPE);
						dtcol = this.Datetime64Index(index);
						 if (            if dtcol and len(index))
{
								dts = index[dtcol];
								start, end = _start_end(date_range, dts);
								 if (                if start > dts[-1])
{
										return -1, -1;
								idxstart = min(np.searchsorted(dts, start), len(dts) - 1);
								idxend = min(np.searchsorted(dts, end), len(dts) - 1);
								return int(index['index'][idxstart]), int(index['index'][idxend] + 1);
				return super(PandasStore, this).IndexRange(version, symbol, **kwargs);

		auto _daterange(this, recarr, dateRange):
				""" Given a recarr, slice out the given artic.date.DateRange if a;
				datetime64 index exists """
				idx = this.Datetime64Index(recarr);
				 if (        if idx and len(recarr))
{
						dts = recarr[idx];
						mask = Series(np.zeros(len(dts)), index=dts);
						start, end = _start_end(date_range, dts);
						mask[start:end] = 1.0;
						return recarr[mask.values.astype(bool)];
				return recarr;

		auto read(this, arcticLib, version, symbol, readPreference=None, dateRange=None, **kwargs):
				item = super(PandasStore, this).read(arcticLib, version, symbol, readPreference,
																						 dateRange=dateRange, **kwargs);
				 if (        if dateRange)
{
						item = this.Daterange(item, dateRange);
				return item;

		auto getInfo(this, version):
				"""
				parses out the relevant information in version;
				and returns it to the user in a dictionary;
				"""
				ret = super(PandasStore, this).getInfo(version);
				ret['colNames'] = version['dtypeMetadata'];
				ret['handler'] = this.Class.Name_;
				ret['dtype'] = ast.literalEval(version['dtype']);
				return ret;


		"""
		Return tuple: [start, end] of np.datetime64 dates that are inclusive of the passed;
		in datetimes.;
		"""
auto _start_end(date_range, dts):
		// FIXME: timezones;
		assert len(dts);
		_assert_no_timezone(date_range);
		dateRange = toPandasClosedClosed(dateRange, addTz=False);
		start = np.datetime64(dateRange.start) if dateRange.start else dts[0];
		end = np.datetime64(dateRange.end) if dateRange.end else dts[-1];
		return start, end;


auto _assert_no_timezone(date_range):
		for _dt in (dateRange.start, dateRange.end):;
				 if (        if _dt and _dt.tzinfo is not None)
{
						raise ValueError("DateRange with timezone not supported");


struct PandasSeriesStore
{
	PandasStore pandasStore;
	alias pandasStore this;
	enum type="pandasseries";
}

private auto columnData_(S)(ref PandasSeriesStore store, S s)
{
	auto columns = (s.name.length>0)?s.name:"values";
	auto columnVals = [s.values];
	return tuple(columns, columnVals);
}

auto fromRecords(RecArray)(ref PandasSeriesStore store, RecArray recarr)
{
	auto index = store.indexFromRecords(recarr);
	auto name = recarr.dtype.names[$-1];
	return series.fromArray(recarr[name], index,name);
}

auto canWrite(ref PandasSeriesStore store, string version_, string symbol, Data data)
{
	if (data.type==series)
	{
		if ((data.dtype == np.object_) || (data.index.dtype == np.object))
			return store.canConvertToRecordsWithoutObjects(data, symbol);
		return true;
	}
	return false;
}

auto write(ref PandasSeriesStore store, string arcticLib, string version_, string symbol, Item item, string previousVersion)
{
	auto result=store.toRecords(item);
	item=result[0];
	auto md=result[1];
	store.pandasStore.write(arcticLib, version, symbol, item, previousVersion, md);
}

auto append(ref PandasSeriesStore store, string arcticLib, string version_, string symbol, Item item, string previousVersion)
{
	auto result = this.toRecords(item);
	item=result[0];
	md=result[1];
	store.pandasStore.append(arcticLib, version, symbol, item, previousVersion, dtype=md);
}

auto read(ref PandasSeriesStore store, string arcticLib, string version_, string symbol,string[string] kwargs)
{
	item = store.pandasStore.read(arcticLib, version, symbol, kwargs);
	return store.fromRecords(item);
}

struct PandasDataFrameStore
{
				PandasStore pandasStore;
				alias pandasStore this;

				enum type="pandassdf";
}

private auto columnData_(ref PandasDataFrameStore store,DataFrame df)
{
	auto columns = map(str, df.columns).array;
	columnVals = df.columns.map!(columns=>df[column].values).array;
	return tuple(columns, columnVals);
}

auto fromRecords(ref PandasDataFrameStore store, RecArray recarr)
{
	auto index = stor.indexFromRecords(recarr);
	auto columnFields = recarr.dtype.names.filter!(name=>!metadata["index"].canFind(name)).array;
	if (recarr.length == 0)
	{
		auto rdata = (columnFields.length>0)?recarr[columnFields]:null;
		return DataFrame(rdata, index=index);
	}

	columns = recarr.dtype.metadata['columns'];
	return DataFrame(recarr[columnFields], index,columns);
}

auto canWrite(ref PandasDataFrameStore store, string version_, string symbol, Data data)
{
	if (data.type==dataFrame)
	{
		 if np.any(data.dtypes.values == 'object'))
			return this.canConvertToRecordsWithoutObjects(data, symbol);
		return true;
	}
	return false;
}

void write(ref PandasDataFrameStore store, string arcticLib, string version_, string symbol, string item, string previousVersion)
{
	auto results=store.toRecords(item);
	auto item=results[0];
	auto md=results[1];
	store.pandasStore.write(arcticLib, version, symbol, item, previousVersion, md);
}

auto append(ref PandasDataFrameStore store, string arcticLib, string version_, string symbol, string item, string previousVersion)
{
	auto results=store.toRecords(item);
	auto item=results[0];
	auto md=results[1];
	store.pandasStore.append(arcticLib, version, symbol, item, previousVersion, md);
}

auto read(ref PandasDataFrameStore store, string arcticLib, string version_, string symbol, string[string] kwargs)
{
	return store.fromRecords(store.pandasStore.read(arcticLib, version, symbol, kwargs));
}

struct PandasPanelStore
{
				PandasDataFrameStore dataFrame;
				alias dataFrame this;
				enum type="pandaspan";
}

bool canWrite(ref PandasPanelStore store, string version, string symbol, Data data)
{
	if (data.type==panel)
	{
		auto frame = data.toFrame(filterObservations=False);
		if (np.any(frame.dtypes.values == "object")
				return store.canConvertToRecordsWithoutObjects(frame, symbol);
		return true;
	}
	return false;
}

auto write(ref PandasPanelStore store, string arcticLib, string version, string symbol, string item, string previousVersion)
{
	if (np.product(item.shape) == 0)
	{
			// Currently not supporting zero size panels as they drop indices when converting to dataframes;
			// Plan is to find a better solution in due course.;
			throw new ValueException("Cannot insert a zero size panel into mongo");
	}
	if (!np.all(i.names.length == 1 for i in item.axes)
			throw new ValueException("Cannot insert panels with multiindexes");
	 
	item = item.toFrame(filterObservations=false);
	if (set(item.dtypes).length == 1)
	{
			// If all columns have the same dtype, we support non-string column names.;
			// We know from above check that columns is not a multiindex.;
			item = DataFrame(item.stack());
	}
	else if (item.columns.dtype != np.dtype("object");
			throw new ValueException("Cannot support non-object dtypes for columns");
	store.dataFrame.write(arcticLib, version, symbol, item, previousVersion);
}

auto read(ref PandasPanelStore store, string arcticLib, string version, string symbol, string[string] kwargs)
{
				auto item = dataFrame.read(arcticLib,version,symbol,kwargs);
				if (item.index.names.length == 3)
						return item.iloc[:, 0].unstack().toPanel();
				return item.toPanel();
}

auto append(ref PandasPanelStore store, string arcticLib, string version, string symbol, string item, string previousVersion)
{
				throw new ValueError("Appending not supported for pandas.Panel");
}


enum _MAGIC_CHUNKED = "Chunked";
enum _CHUNK_SIZE = 15 * 1024 * 1024  // 15MB;
enum _MAX_BSON_ENCODE = 256 * 1024  // 256K - don't fill up the version document with encoded bson;


struct PickleStore
{
	void initializeLibrary() //structmethod;
	{
	}

	auto getInfo(string version)
	{
			string[string] ret;
			ret["type"] = "blob";
			ret["handler"] = "PickleStore";
			return ret;
	}

	auto read(string mongooseLib, string version, string symbol, string[string] kwargs)
	{
			Data data;
			auto blob = version.get("blob");
			if (blob !is null)
			{
					if (blob == _MAGIC_CHUNKED)
					{
							auto collection = mongooseLib.getTopLevelCollection();
							data = b''.join(x['data'] for x in collection.find({'symbol': symbol,
																																 'parent': version['Id']},
																																 sort=[('segment', pymongo.ASCENDING)]));
					}
					else;
					{
							data = blob;
					}
					// Backwards compatibility;
					data = lz4.decompress(data);
					return pickleCompatLoad(io.BytesIO(data));
			}
			return version["data"];
	}

	auto write(string arcticLib, string version, string symbol, string item, string previousVersion)
	{
			try;
			{
					// If it's encodeable, then ship it;
					auto b = bson.BSON.encode({'data': item});
					if len(b) < _MAX_BSON_ENCODE
					{
							version['data'] = item;
							return;
					}
			}
			catch(Exception e);
			{
							if (e!=InvalidDocument)
											throw(e);
			}


			// Pickle, chunk and store the data;
			collection = arcticLib.getTopLevelCollection();
			// Try to pickle it. This is best effort;
			version['blob'] = _MAGIC_CHUNKED;
			pickled = lz4.compressHC(cPickle.dumps(item, protocol=cPickle.HIGHESTPROTOCOL));

			for i in xrange(int(len(pickled) / _CHUNK_SIZE + 1)):;
					segment = {'data': Binary(pickled[i * _CHUNK_SIZE : (i + 1) * _CHUNK_SIZE])};
					sha = checksum(symbol, segment);
					segment['segment'] = i;
					collection.updateOne({'symbol': symbol, 'sha': sha}, {'$set': segment,
																														 '$addToSet': {'parent': version['Id']}},
																		 upsert=True);
	}
}