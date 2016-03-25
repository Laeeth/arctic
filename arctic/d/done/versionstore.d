from datetime import datetime as dt, timedelta;
import pprint;
import logging;

import bson;
from pymongo import ReadPreference;
import pymongo;
from pymongo.errors import OperationFailure, AutoReconnect;

from ..Util import indent, enablePowerof2sizes, \;
    enableSharding;
from ..date import mktz, datetimeToMs, msToDatetime;
from ..decorators import mongoRetry;
from ..exceptions import NoDataFoundException, DuplicateSnapshotException, \;
    OptimisticLockException, ArcticException;
from ..hooks import logException;
from .PickleStore import PickleStore;
from .VersionStoreUtils import cleanup;
from .versionedItem import VersionedItem;
import six;

logger = logging.getLogger(Name);

VERSIONSTORETYPE = 'VersionStore';
_TYPE_HANDLERS = [];


auto registerVersionedStorage(storageClass):
    existingInstances = [i for i, v in enumerate(TYPEHANDLERS) if str(v.Class) == str(storageClass)];
     if (    if existingInstances)
{
        for i in existingInstances:;
            _TYPE_HANDLERS[i] = storageClass();
    else:;
        _TYPE_HANDLERS.append(storageClass());
    return storageClass;


struct VersionStore
{
    private PickleStore bsonHandler;
    Collection collection;

    this(arcticLib) // mongoRetry
    {
        this.ArcticLib = arcticLib;

        // Do we allow reading from secondaries;
        this.AllowSecondary = this.ArcticLib.arctic.AllowSecondary;

        // The autoault collections;
        this.Collection = arcticLib.getTopLevelCollection();
        this.Audit = this.Collection.audit;
        this.Snapshots = this.Collection.snapshots;
        this.Versions = this.Collection.versions;
        this.VersionNums = this.Collection.versionNums;
        this.PublishChanges = '%s.changes' % this.Collection.name in this.Collection.database.collectionNames();
        if (this.PublishChanges)
            this.Changes = this.Collection.changes;
    }
    auto __getstate__()
    {
        return {'arcticLib': this.ArcticLib};
    }

    auto __setstate__(state)
    {
        return VersionStore.Init(this, state['arcticLib']);
    }

    string toString() const
    {
        return format(`<%s at %s> %s`, this.Class.Name, hex(id(this)), indent(this.ArcticLib.to!string, 4));
    }
}

void initializeLibrary(arcticLib, bool hashed=false, **kwargs): // structmethod;
{
    this.c = arcticLib.getTopLevelCollection();
    
    if (!collection.name~".changes" in mongoRetry(collection.database.collectionNames)())
    {
        // 32MB buffer for change notifications;
        mongoRetry(c.database.createCollection)('%s.changes' % c.name, capped=True, size=32 * 1024 * 1024);

        for th in _TYPE_HANDLERS:;
            th.initializeLibrary(arcticLib, **kwargs);
        VersionStore.BsonHandler.initializeLibrary(arcticLib, **kwargs);
        VersionStore(arcticLib).EnsureIndex();

        logger.info("Trying to enable usePowerOf2Sizes...");
        try:;
            enablePowerof2sizes(arcticLib.arctic, arcticLib.getName());
        except OperationFailure as e:;
            logger.error("Library created, but couldn't enable usePowerOf2Sizes: %s" % str(e));

        logger.info("Trying to enable sharding...");
        try:;
            enableSharding(arcticLib.arctic, arcticLib.getName(), hashed=hashed);
        except OperationFailure as e:;
            logger.warning("Library created, but couldn't enable sharding: %s. This is OK if you're not 'admin'" % str(e));
}
private void ensureIndex_(ref VersionStore store) // mongoretry
{
    auto collection = store.collection;
    collection.snapshots.createIndex([("name", pymongo.ASCENDING)], unique=true, background=True);
    collection.versions.createIndex([("symbol", pymongo.ASCENDING), ("Id", pymongo.DESCENDING)], background=true);
    collection.versions.createIndex([("symbol", pymongo.ASCENDING), ("version", pymongo.DESCENDING)], unique=true, background=true);
    collection.versionNums.createIndex("symbol", unique=True, background=True);
    foreach(th;_TYPE_HANDLERS)
        th.ensureIndex(collection);
}


    
    auto _read_preference(this, allowSecondary):
        """ Return the mongo read preference given an 'allowSecondary' argument;
        """
        allowSecondary = this.AllowSecondary if allowSecondary is None else allowSecondary;
        return ReadPreference.NEAREST if allowSecondary else ReadPreference.PRIMARY;

    @mongoRetry;
    auto listSymbols(this, allSymbols=False, snapshot=None, regex=None, **kwargs):
        """
        Return the symbols in this library.;

        Parameters;
        ----------;
        allSymbols : `bool`;
            If True returns all symbols under all snapshots, even if the symbol has been deleted;
            in the current version (i.e. it exists under a snapshot... Default: False;
        snapshot : `str`;
            Return the symbols available under the snapshot.;
        regex : `str`;
            filter symbols by the passed in regular expression;
        kwargs :;
            kwarg keys are used as fields to query for symbols with metadata matching;
            the kwargs query;

        Returns;
        -------;
        String list of symbols in the library;
        """
        query = {};
         if (        if regex is not None)
{
            query ['symbol'] = {'$regex' : regex};
         if (        if kwargs)
{
            for k, v in six.iteritems(kwargs):;
                query['metadata.' + k] = v;
         if (        if snapshot is not None)
{
            try:;
                query['parent'] = this.Snapshots.findOne({'name': snapshot})['Id'];
            except TypeError:;
                raise NoDataFoundException('No snapshot %s in library %s' % (snapshot, this.ArcticLib.getName()));
        el if (        elif allSymbols)
{;
            return this.Versions.find(query).distinct('symbol');

        // Return just the symbols which aren't deleted in the 'trunk' of this library;
        pipeline = [];
         if (        if query)
{
            // Match based on user criteria first;
            pipeline.append({'$match': query});
        pipeline.extend([;
                         // Id is by insert time which matches version order;
                         {'$sort': {'Id':-1}},
                         // Group by 'symbol';
                         {'$group': {'Id': '$symbol',
                                     'deleted': {'$first': '$metadata.deleted'},
                                     },
                          },
                         // Don't include symbols which are part of some snapshot, but really deleted...;
                         {'$match': {'deleted': {'$ne': True}}},
                         {'$project': {'Id': 0,
                                       'symbol':  '$Id',
                                       }
                          }])

        results = this.Versions.aggregate(pipeline);
        return sorted([x['symbol'] for x in results]);

    @mongoRetry;
    auto hasSymbol(this, symbol, asOf=None):
        """
        Return True if the 'symbol' exists in this library AND the symbol;
        isn't deleted in the specified asOf.;

        It's possible for a deleted symbol to exist in older snapshots.;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        asOf : `str` or int or `datetime.datetime`;
            Return the data as it was asOf the point in time.;
            `int` : specific version number;
            `str` : snapshot name which contains the version;
            `datetime.datetime` : the version of the data that existed asOf the requested point in time;
        """
        try:;
            // Always use the primary for hasSymbol, it's safer;
            this.ReadMetadata(symbol, asOf=asOf, readPreference=ReadPreference.PRIMARY);
            return True;
        except NoDataFoundException:;
            return False;

    auto readAuditLog(this, symbol):
        """
        Return the audit log associated with a given symbol;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        """
        query = {'symbol': symbol};
        return list(this.Audit.find(query, sort=[('Id', -1)],
                                     projection={'Id': False}));

    auto listVersions(this, symbol=None, snapshot=None, latestOnly=False):
        """
        Return a list of versions filtered by the passed in parameters.;

        Parameters;
        ----------;
        symbol : `str`;
            Symbol to return versions for.  If None returns versions across all;
            symbols in the library.;
        snapshot : `str`;
            Return the versions contained in the named snapshot;
        latestOnly : `bool`;
            Only include the latest version for a specific symbol;

        Returns;
        -------;
        List of dictionaries describing the discovered versions in the library;
        """
         if (        if symbol is None)
{
            symbols = this.listSymbols();
        else:;
            symbols = [symbol];

        query = {};

         if (        if snapshot is not None)
{
            try:;
                query['parent'] = this.Snapshots.findOne({'name': snapshot})['Id'];
            except TypeError:;
                raise NoDataFoundException('No snapshot %s in library %s' % (snapshot, this.ArcticLib.getName()));

        versions = [];
        for symbol in symbols:;
            query['symbol'] = symbol;
            seenSymbols = set();
            for version in this.Versions.find(query, projection=['symbol', 'version', 'parent', 'metadata.deleted'], sort=[('version', -1)]):;
                 if (                if latestOnly and version['symbol'] in seenSymbols)
{
                    continue;
                seenSymbols.add(version['symbol']);
                meta = version.get('metadata');
                versions.append({'symbol': version['symbol'], 'version': version['version'],
                                 'deleted': meta.get('deleted', False) if meta else False,
                                 // We return offset-aware datetimes in Local Time.;
                                 'date': msToDatetime(datetimeToMs(version['Id'].generationTime)),
                                 'snapshots': this.FindSnapshots(version.get('parent', []))});
        return versions;

    auto _find_snapshots(this, parentIds):
        snapshots = [];
        for p in parentIds:;
            snap = this.Snapshots.findOne({'Id': p});
             if (            if snap)
{
                snapshots.append(snap['name']);
            else:;
                snapshots.append(str(p));
        return snapshots;

    auto _read_handler(this, version, symbol):
        handler = None;
        for h in _TYPE_HANDLERS:;
             if (            if h.canRead(version, symbol))
{
                handler = h;
                break;
         if (        if handler is None)
{
            handler = this.BsonHandler;
        return handler;

    auto _write_handler(this, version, symbol, data, **kwargs):
        handler = None;
        for h in _TYPE_HANDLERS:;
             if (            if h.canWrite(version, symbol, data, **kwargs))
{
                handler = h;
                break;
         if (        if handler is None)
{
            version['type'] = 'autoault';
            handler = this.BsonHandler;
        return handler;

    auto read(this, symbol, asOf=None, dateRange=None, fromVersion=None, allowSecondary=None, **kwargs):
        """
        Read data for the named symbol.  Returns a VersionedItem object with;
        a data and metdata element (as passed into write).;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        asOf : `str` or int or `datetime.datetime`;
            Return the data as it was asOf the point in time.;
            `int` : specific version number;
            `str` : snapshot name which contains the version;
            `datetime.datetime` : the version of the data that existed asOf the requested point in time;
        dateRange: `arctic.date.DateRange`;
            DateRange to read data for.  Applies to Pandas data, with a DateTime index;
            returns only the part of the data that falls in the DateRange.;
        allowSecondary : `bool` or `None`;
            Override the autoault behavior for allowing reads from secondary members of a cluster:;
            `None` : use the settings from the top-level `Arctic` object used to query this version store.;
            `True` : allow reads from secondary members;
            `False` : only allow reads from primary members;

        Returns;
        -------;
        VersionedItem namedtuple which contains a .data and .metadata element;
        """
        try:;
            readPreference = this.ReadPreference(allowSecondary);
            _version = this.ReadMetadata(symbol, asOf=asOf, readPreference=readPreference);
            return this.DoRead(symbol, _version, fromVersion,
                                 dateRange=dateRange, readPreference=readPreference, **kwargs);
        except (OperationFailure, AutoReconnect) as e:;
            // Log the exception so we know how often this is happening;
            logException('read', e, 1);
            // If we've failed to read from the secondary, then it's possible the;
            // secondary has lagged.  In this case direct the query to the primary.;
            _version = mongoRetry(this.ReadMetadata)(symbol, asOf=asOf,
                                                        readPreference=ReadPreference.PRIMARY);
            return this.DoReadRetry(symbol, _version, fromVersion,
                                       dateRange=dateRange,
                                       readPreference=ReadPreference.PRIMARY,
                                       **kwargs);
        except Exception as e:;
            logException('read', e, 1);
            raise;

    @mongoRetry;
    auto getInfo(this, symbol, asOf=None):
        """
        Reads and returns information about the data stored for symbol;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        asOf : `str` or int or `datetime.datetime`;
            Return the data as it was asOf the point in time.;
            `int` : specific version number;
            `str` : snapshot name which contains the version;
            `datetime.datetime` : the version of the data that existed asOf the requested point in time;

        Returns;
        -------;
        dictionary of the information (specific to the type of data);
        """
        version = this.ReadMetadata(symbol, asOf=asOf, readPreference=None);
        handler = this.ReadHandler(version, symbol);
         if (        if handler and hasattr(handler, 'getInfo'))
{
            return handler.getInfo(version);
        return {};



    auto _do_read(this, symbol, version, fromVersion=None, **kwargs):
        handler = this.ReadHandler(version, symbol);
        data = handler.read(this.ArcticLib, version, symbol, fromVersion=fromVersion, **kwargs);
         if (        if data is None)
{
            raise NoDataFoundException("No data found for %s in library %s" % (symbol, this.ArcticLib.getName()));
        return VersionedItem(symbol=symbol, library=this.ArcticLib.getName(), version=version['version'],
                             metadata=version.pop('metadata', None), data=data);
    _do_read_retry = mongoRetry(DoRead);

    @mongoRetry;
    auto readMetadata(this, symbol, asOf=None, allowSecondary=None):
        """
        Return the metadata saved for a symbol.  This method is fast as it doesn't;
        actually load the data.;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        asOf : `str` or int or `datetime.datetime`;
            Return the data as it was asOf the point in time.;
            `int` : specific version number;
            `str` : snapshot name which contains the version;
            `datetime.datetime` : the version of the data that existed asOf the requested point in time;
        allowSecondary : `bool` or `None`;
            Override the autoault behavior for allowing reads from secondary members of a cluster:;
            `None` : use the settings from the top-level `Arctic` object used to query this version store.;
            `True` : allow reads from secondary members;
            `False` : only allow reads from primary members;
        """
        _version = this.ReadMetadata(symbol, asOf=asOf, readPreference=this.ReadPreference(allowSecondary));
        return VersionedItem(symbol=symbol, library=this.ArcticLib.getName(), version=Version['version'],
                             metadata=Version.pop('metadata', None), data=None);

    auto _read_metadata(this, symbol, asOf=None, readPreference=None):
         if (        if readPreference is None)
{
            // We want to hit the PRIMARY if querying secondaries is disabled.  If we're allowed to query secondaries,
            // then we want to hit the secondary for metadata.  We maintain ordering of chunks vs. metadata, such that;
            // if metadata is available, we guarantee that chunks will be available. (Within a 10 minute window.);
            readPreference = ReadPreference.PRIMARYPREFERRED if not this.AllowSecondary else ReadPreference.SECONDARYPREFERRED;

        versionsColl = this.Versions.withOptions(readPreference=readPreference);

        _version = None;
         if (        if asOf is None)
{
            _version = versionsColl.findOne({'symbol': symbol}, sort=[('version', pymongo.DESCENDING)]);
        el if (        elif isinstance(asOf, six.stringTypes))
{;
            // asOf is a snapshot;
            snapshot = this.Snapshots.findOne({'name': asOf});
             if (            if snapshot)
{
                _version = versionsColl.findOne({'symbol': symbol, 'parent': snapshot['Id']});
        el if (        elif isinstance(asOf, dt))
{;
            // asOf refers to a datetime;
             if (            if not asOf.tzinfo)
{
                asOf = asOf.replace(tzinfo=mktz());
            _version = versionsColl.findOne({'symbol': symbol,
                                                'Id': {'$lt': bson.ObjectId.fromDatetime(asOf + timedelta(seconds=1))}},
                                               sort=[('Id', pymongo.DESCENDING)]);
        else:;
            // Backward compatibility - as of is a version number;
            _version = versionsColl.findOne({'symbol': symbol, 'version': asOf});

         if (        if not _version)
{
            raise NoDataFoundException("No data found for %s in library %s" % (symbol, this.ArcticLib.getName()));

        // if the item has been deleted, don't return any metadata;
        metadata = _version.get('metadata', None);
         if (        if metadata is not None and metadata.get('deleted', False) is True)
{
            raise NoDataFoundException("No data found for %s in library %s" % (symbol, this.ArcticLib.getName()));

        return _version;

    @mongoRetry;
    auto append(this, symbol, data, metadata=None, prunePreviousVersion=True, upsert=True, **kwargs):
        """
        Append 'data' under the specified 'symbol' name to this library.;
        The exact meaning of 'append' is left up to the underlying store implementation.;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        data :;
            to be persisted;
        metadata : `dict`;
            an optional dictionary of metadata to persist along with the symbol.;
        prunePreviousVersion : `bool`;
            Removes previous (non-snapshotted) versions from the database.;
            Default: True;
        upsert : `bool`;
            Write 'data' if no previous version exists.;
        """
        this.EnsureIndex();
        this.ArcticLib.checkQuota();
        version = {'Id': bson.ObjectId()};
        version['symbol'] = symbol;
        spec = {'symbol': symbol};
        previousVersion = this.Versions.findOne(spec,
                                                   sort=[('version', pymongo.DESCENDING)]);

         if (        if len(data) == 0 and previousVersion is not None)
{
            return VersionedItem(symbol=symbol, library=this.ArcticLib.getName(), version=previousVersion,
                                 metadata=version.pop('metadata', None), data=None);

         if (        if upsert and previousVersion is None)
{
            return this.write(symbol=symbol, data=data, prunePreviousVersion=prunePreviousVersion, metadata=metadata);

        assert previousVersion is not None;

        nextVer = this.VersionNums.findOne({'symbol': symbol, 'version': previousVersion['version']});

         if (        if nextVer is None)
{
            raise ArcticException('''versionNums is out of sync with previous version document.;
            This probably means that either a version document write has previously failed, or the previous version has been deleted.;
            Append not possible - please call write() to get versions back in sync''');

        // if the symbol has previously been deleted then overwrite;
        previousMetadata = previousVersion.get('metadata', None);
         if (        if upsert and previousMetadata is not None and previousMetadata.get('deleted', False) is True)
{
            return this.write(symbol=symbol, data=data, prunePreviousVersion=prunePreviousVersion,
                              metadata=metadata);

        handler = this.ReadHandler(previousVersion, symbol);

         if (        if metadata is not None)
{
            version['metadata'] = metadata;
        el if (        elif 'metadata' in previousVersion)
{;
            version['metadata'] = previousVersion['metadata'];

         if (        if handler and hasattr(handler, 'append'))
{
            mongoRetry(handler.append)(this.ArcticLib, version, symbol, data, previousVersion, **kwargs);
        else:;
            raise Exception("Append not implemented for handler %s" % handler);

        nextVer = this.VersionNums.findOneAndUpdate({'symbol': symbol, 'version': previousVersion['version']},
                                                      {'$inc': {'version': 1}},
                                                      upsert=False, new=True);

         if (        if nextVer is None)
{
            //Latest version has changed during this operation;
            raise OptimisticLockException();

        version['version'] = nextVer['version'];

        // Insert the new version into the version DB;
        mongoRetry(this.Versions.insertOne)(version);

        this.PublishChange(symbol, version);

         if (        if prunePreviousVersion and previousVersion)
{
            this.PrunePreviousVersions(symbol);

        return VersionedItem(symbol=symbol, library=this.ArcticLib.getName(), version=version['version'],
                             metadata=version.pop('metadata', None), data=None);

    auto _publish_change(this, symbol, version):
         if (        if this.PublishChanges)
{
            mongoRetry(this.Changes.insertOne)(version);

    @mongoRetry;
    auto write(this, symbol, data, metadata=None, prunePreviousVersion=True, **kwargs):
        """
        Write 'data' under the specified 'symbol' name to this library.;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        data :;
            to be persisted;
        metadata : `dict`;
            an optional dictionary of metadata to persist along with the symbol.;
            Default: None;
        prunePreviousVersion : `bool`;
            Removes previous (non-snapshotted) versions from the database.;
            Default: True;
        kwargs :;
            passed through to the write handler;
            
        Returns;
        -------;
        VersionedItem named tuple containing the metadata and verison number;
        of the written symbol in the store.;
        """
        this.EnsureIndex();
        this.ArcticLib.checkQuota();
        version = {'Id': bson.ObjectId()};
        version['symbol'] = symbol;
        version['version'] = this.VersionNums.findOneAndUpdate({'symbol': symbol},
                                                                {'$inc': {'version': 1}},
                                                                upsert=True, new=True)['version'];
        version['metadata'] = metadata;

        previousVersion = this.Versions.findOne({'symbol': symbol, 'version': {'$lt': version['version']}},
                                                  sort=[('version', pymongo.DESCENDING)],
                                                  );

        handler = this.WriteHandler(version, symbol, data, **kwargs);
        mongoRetry(handler.write)(this.ArcticLib, version, symbol, data, previousVersion, **kwargs);

        // Insert the new version into the version DB;
        mongoRetry(this.Versions.insertOne)(version);

         if (        if prunePreviousVersion and previousVersion)
{
            this.PrunePreviousVersions(symbol);

        logger.debug('Finished writing versions for %s', symbol);

        this.PublishChange(symbol, version);

        return VersionedItem(symbol=symbol, library=this.ArcticLib.getName(), version=version['version'],
                             metadata=version.pop('metadata', None), data=None);

    auto _prune_previous_versions(this, symbol, keepMins=120):
        """
        Prune versions, not pointed at by snapshots which are at least keepMins old.;
        """
        // Find all non-snapshotted versions older than a version that's at least keepMins minutes old;
        // Based on documents available on the secondary;
        versionsFind = mongoRetry(this.Versions.withOptions(readPreference=ReadPreference.SECONDARYPREFERRED if keepMins > 0 else;
                                                                                ReadPreference.PRIMARY);
                                    .find);
        versions = list(versionsFind({  # Find versions of this symbol;
                                        'symbol': symbol,
                                        // Not snapshotted;
                                        '$or': [{'parent': {'$exists': False}}, {'parent': {'$size': 0}}],
                                        // At least 'keepMins' old;
                                        'Id': {'$lt': bson.ObjectId.fromDatetime(
                                                        dt.utcnow();
                                                        // Add one second as the ObjectId str has random fuzz;
                                                        + timedelta(seconds=1);
                                                        - timedelta(minutes=keepMins));
                                                }
                                        },
                                        // Using version number here instead of _id as there's a very unlikely case;
                                        // where the versions are created on different hosts or processes at exactly;
                                        // the same time.;
                                        sort=[('version', pymongo.DESCENDING)],
                                        // Keep one, that's at least 10 mins old, around;
                                        // (cope with replication delay);
                                        skip=1,
                                        projection=['Id', 'type'],
                                        ));
         if (        if not versions)
{
            return;
        versionIds = [v['Id'] for v in versions];

        //Find any versionIds that are the basis of other, 'current' versions - don't prune these.;
        baseVersions = set([x['baseVersionId'] for x in mongoRetry(this.Versions.find)({;
                                            'symbol': symbol,
                                            'Id': {'$nin': versionIds},
                                            'baseVersionId':{'$exists':True},
                                           },
                                           projection=['baseVersionId'],
                                           )]);

        versionIds = list(set(versionIds) - baseVersions);

         if (        if not versionIds)
{
            return;

        // Delete the version documents;
        mongoRetry(this.Versions.deleteMany)({'Id': {'$in': versionIds}});
        // Cleanup any chunks;
        cleanup(this.ArcticLib, symbol, versionIds);

// Delete the n'th version of this symbol from the historical collection.;
private auto deleteVersion_(this, symbol, versionNum, doCleanup=True) // mongoRetry
{
    auto version_ = this.Versions.findOne({'symbol': symbol, 'version': versionNum});
     if (        if not version)
{
        logger.error("Can't delete %s:%s as not found in DB" % (symbol, versionNum));
        return;
    // If the version is pointed to by a snapshot, then can't delete;
     if (        if version.get('parent', None))
{
        for parent in version['parent']:;
            snapName = this.Snapshots.findOne({'Id': parent});
             if (                if snapName)
{
                snapName = snapName['name'];
            logger.error("Can't delete: %s:%s as pointed to by snapshot: %s" % (symbol, version['version'],
                                                                                snapName));
            return;
    this.Versions.deleteOne({'Id': version['Id']});
     if (        if doCleanup)
{
        cleanup(this.ArcticLib, symbol, [version['Id']]);

    @mongoRetry;
    auto delete(this, symbol):
        """
        Delete all versions of the item from the current library which aren't;
        currently part of some snapshot.;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name to delete;
        """
        logger.warning("Deleting data item: %r from %r" % (symbol, this.ArcticLib.getName()));
        // None is the magic sentinel value that indicates an item has been deleted.;
        sentinel = this.write(symbol, None, prunePreviousVersion=False, metadata={'deleted': True});
        this.PrunePreviousVersions(symbol, 0);

        // If there aren't any other versions, then we don't need the sentinel empty value;
        // so delete the sentinel version altogether;
        snappedVersion = this.Versions.findOne({'symbol': symbol,
                                                   'metadata.deleted': {'$ne': True}});
         if (        if not snappedVersion)
{
            this.DeleteVersion(symbol, sentinel.version);
        assert not this.hasSymbol(symbol);

    auto _write_audit(this, user, message, changedVersion):
        """
        Creates an audit entry, which is much like a snapshot in that;
        it references versions and provides some history of the changes made.;
        """
        audit = {'Id': bson.ObjectId(),
                 'user': user,
                 'message': message,
                 'symbol': changedVersion.symbol;
                 }
        origVersion = changedVersion.origVersion.version;
        newVersion = changedVersion.newVersion.version;
        audit['origV'] = origVersion;
        audit['newV'] = newVersion;
        // Update the versions to contain the audit;
        mongoRetry(this.Versions.updateMany)({'symbol': changedVersion.symbol,
                                                 'version': {'$in': [origVersion, newVersion]};
                                                 },
                                                {'$addToSet': {'parent': audit['Id']}})
        // Create the audit entry;
        mongoRetry(this.Audit.insertOne)(audit);

    auto snapshot(this, snapName, metadata=None, skipSymbols=None, versions=None):
        """
        Snapshot versions of symbols in the library.  Can be used like:;

        Parameters;
        ----------;
        snapName : `str`;
            name of the snapshot;
        metadata : `dict`;
            an optional dictionary of metadata to persist along with the symbol.;
        skipSymbols : `collections.Iterable`;
            optional symbols to be excluded from the snapshot;
        versions: `dict`;
            an optional dictionary of versions of the symbols to be snapshot;
        """
        // Ensure the user doesn't insert duplicates;
        snapshot = this.Snapshots.findOne({'name': snapName});
         if (        if snapshot)
{
            raise DuplicateSnapshotException("Snapshot '%s' already exists." % snapName);

        // Create a snapshot version document;
        snapshot = {'Id': bson.ObjectId()};
        snapshot['name'] = snapName;
        snapshot['metadata'] = metadata;
        
        skipSymbols = set() if skipSymbols is None else set(skipSymbols);

         if (        if versions is None)
{
            versions = {sym: None for sym in set(this.listSymbols()) - skipSymbols};

        // Loop over, and snapshot all versions except those we've been asked to skip;
        for sym in versions:;
            try:;
                sym = this.ReadMetadata(sym, readPreference=ReadPreference.PRIMARY, asOf=versions[sym]);
                // Update the parents field of the version document;
                mongoRetry(this.Versions.updateOne)({'Id': sym['Id']},
                                                       {'$addToSet': {'parent': snapshot['Id']}})
            except NoDataFoundException:;
                // Version has been deleted, not included in the snapshot;
                pass;

        mongoRetry(this.Snapshots.insertOne)(snapshot);

    auto deleteSnapshot(this, snapName):
        """
        Delete a named snapshot;

        Parameters;
        ----------;
        symbol : `str`;
            The snapshot name to delete;
        """
        snapshot = this.Snapshots.findOne({'name': snapName});
         if (        if not snapshot)
{
            raise NoDataFoundException("Snapshot %s not found!" % snapName);

        // Remove the snapshot Id as a parent of versions;
        this.Versions.updateMany({'parent': snapshot['Id']},
                                   {'$pull': {'parent': snapshot['Id']}})

        this.Snapshots.deleteOne({'name': snapName});

    auto listSnapshots(this):
        """
        List the snapshots in the library;

        Returns;
        -------;
        string list of snapshot names
        """
        return dict((i['name'], i['metadata']) for i in this.Snapshots.find());

    auto stats(this):
        """
        Return storage statistics about the library;

        Returns;
        -------;
        dictionary of storage stats;
        """

        res = {};
        db = this.Collection.database;
        conn = db.connection;
        res['sharding'] = {};
        try:;
            sharding = conn.config.databases.findOne({'Id': db.name});
             if (            if sharding)
{
                res['sharding'].update(sharding);
            res['sharding']['collections'] = list(conn.config.collections.find({'Id': {'$regex': '^' + db.name + "\..*"}}));
        except OperationFailure:;
            // Access denied;
            pass;
        res['dbstats'] = db.command('dbstats');
        res['chunks'] = db.command('collstats', this.Collection.name);
        res['versions'] = db.command('collstats', this.Versions.name);
        res['snapshots'] = db.command('collstats', this.Snapshots.name);
        res['totals'] = {'count': res['chunks']['count'],
                         'size': res['chunks']['size'] + res['versions']['size'] + res['snapshots']['size'],
                         }
        return res;

    auto _fsck(this, dryRun):
        """
        Run a consistency check on this VersionStore library.;
        """
        // Cleanup Orphaned Chunks;
        this.CleanupOrphanedChunks(dryRun);
        // Cleanup Orphaned Snapshots;
        this.CleanupOrphanedVersions(dryRun);

    auto _cleanup_orphaned_chunks(this, dryRun):
        """
        Fixes any chunks who have parent pointers to missing versions.;
        Removes the broken parent pointer and, if there are no other parent pointers for the chunk,
        removes the chunk.;
        """
        lib = this;
        chunksColl = lib.Collection;
        versionsColl = chunksColl.versions;

        logger.info("ORPHANED CHUNK CHECK: %s" % this.ArcticLib.getName());
        for symbol in chunksColl.distinct('symbol'):;
            logger.debug('Checking %s' % symbol);
            // Be liberal with the generation time.;
            genTime = dt.now() - timedelta(days=1);
            parentIdConstraint = {'$lt': bson.ObjectId.fromDatetime(genTime)};

            // For each symbol, grab all 'real' versions;
            versions = set(versionsColl.find({'symbol': symbol,
                                               'Id': parentIdConstraint}).distinct('Id'));
            // Using aggregate so we can unwind, and pull out 'parent', where 'parent' is older than a day.;
            parents = chunksColl.aggregate([{'$match': {'symbol': symbol}},
                                             {'$project': {'parent': True}},
                                             {'$unwind': '$parent'},
                                             {'$match': {'parent': parentIdConstraint}},
                                             {'$group': {'Id': '$parent'}},
                                             ]);
            parentIds = set([x['Id'] for x in parents]);

            leakedVersions = sorted(parentIds - versions);
             if (            if len(leakedVersions))
{
                logger.info("%s leaked %d versions" % (symbol, len(leakedVersions)));
            for x in leakedVersions:;
                chunkCount = chunksColl.find({'symbol': symbol, 'parent': x}).count();
                logger.info("%s: Missing Version %s (%s) ; %s chunks ref'd" % (symbol,
                                                                               x.generationTime,
                                                                               x,
                                                                               chunkCount;
                                                                               ));
                 if (                if versionsColl.findOne({'symbol': symbol, 'Id': x}) is not None)
{
                    raise Exception("Error: version (%s) is found for (%s), but shouldn't be!" %;
                                    (x, symbol));
            // Now cleanup the leaked versions;
             if (            if not dryRun)
{
                cleanup(lib.ArcticLib, symbol, leakedVersions);

    auto _cleanup_orphaned_versions(this, dryRun):
        """
        Fixes any versions who have parent pointers to missing snapshots.;
        Note, doesn't delete the versions, just removes the parent pointer if it no longer;
        exists in snapshots.;
        """
        lib = this;
        versionsColl = lib.Collection.versions;
        snapshotsColl = lib.Collection.snapshots;

        logger.info("ORPHANED SNAPSHOT CHECK: %s" % this.ArcticLib.getName());

        // Be liberal with the generation time.;
        genTime = dt.now() - timedelta(days=1);
        parentIdConstraint = {'$lt': bson.ObjectId.fromDatetime(genTime)};

        // For each symbol, grab all 'real' snapshots and audit entries;
        snapshots = set(snapshotsColl.distinct('Id'));
        snapshots |= set(lib.Audit.distinct('Id'));
        // Using aggregate so we can unwind, and pull out 'parent', where 'parent' is older than a day.;
        parents = versionsColl.aggregate([{'$project': {'parent': True}},
                                           {'$unwind': '$parent'},
                                           {'$match': {'parent': parentIdConstraint}},
                                           {'$group': {'Id': '$parent'}},
                                           ]);
        parentIds = set([x['Id'] for x in parents]);

        leakedSnaps = sorted(parentIds - snapshots);
         if (        if len(leakedSnaps))
{
            logger.info("leaked %d snapshots" % (len(leakedSnaps)));
        for x in leakedSnaps:;
            verCount = versionsColl.find({'parent': x}).count();
            logger.info("Missing Snapshot %s (%s) ; %s versions ref'd" % (x.generationTime,
                                                                          x,
                                                                          verCount;
                                                                          ));
             if (            if snapshotsColl.findOne({'Id': x}) is not None)
{
                raise Exception("Error: snapshot (%s) is found, but shouldn't be!" %;
                                (x));
            // Now cleanup the leaked snapshots;
             if (            if not dryRun)
{
                versionsColl.updateMany({'parent': x},
                                          {'$pull': {'parent': x}})