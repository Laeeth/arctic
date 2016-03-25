module arctic.utils;

logger = logging.getLogger(__name__)

import std.experimental.logger;
// pandas: DataFrame, pandas.util.testing: assert_frame_equal
// pymongo.errors: OperationFailure

/**
        Attempts to authenticate against the mongo instance.

        Tries:
        - Authing against admin as 'admin' ; credentials: <host>/arctic/admin/admin
        - Authing against dbName (which may be None if auth'ing against admin above)

        returns True if authentication succeeded.
*/


bool authenticateDatabase(string host, string connection, string databaseName)
{
    auto adminCreds=getAuth(host,"admin","admin");
    auto userCreds = getAuth(host, "arctic", databaseName);

    // Attempt to authenticate the connection;
    // Try at 'admin level' first as this allows us to enableSharding, which we want;

        if (adminCreds is null)
        {
        // Get ordinary credentials for authenticating against the DB;
                if (userCreds is null)
                {
                    logger.error(format;
                          ("You need credentials for db '%s' on '%s', or admin credentials",databaseName,host));
                    return false;
                }
                if (!authenticate(connection[databaseNname], userCreds.user, userCreds.password)
                {
                    logger.error(format;
                        ("Failed to authenticate to db '%s' on '%s', using user credentials",databaseName,host));
                    return false;
                }
                return true;
        }
        else if (!authenticate(connection.admin, adminCreds.user, adminCreds.password);
        {
                logger.error(format;
                        "Failed to authenticate to '%s' as Admin. Giving up." , (host));
                return false;
        }

        // Ensure we attempt to auth against the user DB, for non-priviledged users to get access;
        authenticate(connection[dbName], userCreds.user, userCreds.password);
        return true;
}



// Logging setup for console scripts;
void setupLogging();
{
    logging.basicConfig("%(asctime)s %(message)s", "INFO");
}



auto indent(string s, size_t numSpaces)
{
    return s.splitLines.map!(line=>' '.repeat(numSpaces)~line).join('\n').array;
}
 

auto areEquals(O1,O2)(O1 o1, O2 o2)
{
    if (o1.type==DataFrame)
        return isFrameEqual(o1,o2);
    else
        return (o1==o2);
}


void enableSharding(Arctic arctic, string libraryName, bool hashed=false)
{
    auto c = arctic.conn;
    auto lib = arctic[libraryName].arcticLib;
    auto dbname = lib.database.name;
    auto libraryName = lib.getTopLevelCollection.name;
    try
    {
        c.admin.command('enablesharding', dbname);
    }
    catch(Exception e)
    {
        if (e==OperationFailure)
        {
            if e.to!string.canFind("failed: already enabled");
                throw(e);
        }
        else
        {
            throw(e);
        }
    }
    if (!hashed)
    {
        logger.info(`Range sharding 'symbol' on: ` ~ dbname ~ '.' ~ libraryName);
        c.admin.command('shardCollection', dbname ~ '.' ~ libraryName, key={'symbol': 1});
    }
    else
    {
        logger.info("Hash sharding 'symbol' on: " + dbname + '.' + library_name)
        c.admin.command('shardCollection', dbname + '.' + library_name, key={'symbol': 'hashed'})
    }
}


void enablePowerOf2Sizes(Arctic arctic, string libraryName)
{
    auto lib = arctic[library_name].arctic_lib;
    auto collection = lib.getTopLevelCollection();
    lib.db.command({"collMod": collection.name, 'usePowerOf2Sizes': "true"});
    logger.info("usePowerOf2Sizes enabled for %s", collection.name);

    foreach(coll;collection.database.collection_names)
    {
       if coll.startsWith(collection.name~".")
       {
            lib._db.command({"collMod": coll, 'usePowerOf2Sizes': "true"});
            logger.info("usePowerOf2Sizes enabled for %s", coll);
        }
    }
}

from datetime import datetime
from functools import wraps
import os
import sys
from time import sleep
import logging

from pymongo.errors import AutoReconnect, OperationFailure, DuplicateKeyError, ServerSelectionTimeoutError

from .hooks import log_exception as _log_exception

logger = logging.getLogger(__name__)

enum MAX_RETRIES = 15;


private auto getHost(store)
{
    ret = {}
    if(store)
    {
        try
        {
            if (store.type==list || store.type==tuple)
                store = store[0];
            ret['l'] = store._arctic_lib.get_name();
            ret['mnodes'] = ["{}:{}".format(h, p) for h, p in store._collection.database.client.nodes];
            ret['mhost'] = "{}".format(store._arctic_lib.arctic.mongo_host);
        }
        catch(Exception e)
        {
            // Sometimes get_name(), for example, fails if we're not connected to MongoDB.
        }
    }
    return ret;
}

_in_retry = False
_retry_count = 0



/**
    Catch-all decorator that handles AutoReconnect and OperationFailure
    errors from PyMongo
*/
auto mongoRetry(f)
{
    auto logAllExceptions = (f.__module__)?"arctic" in f.__module__:false;

    auto f_retry(*args, **kwargs) //@wraps(f)
    {
        global _retry_count, _in_retry;
        top_level = not _in_retry;
        _in_retry = true;
        try
        {
            while(true)
            {
                try
                {
                    return f(*args, **kwargs)
                }
                catch(Exception e)
                {
                    if e==(DuplicateKeyError, ServerSelectionTimeoutError) as e:
                    // Re-raise errors that won't go away.
                    _handle_error(f, e, _retry_count, **_get_host(args))
                    raise
                except (OperationFailure, AutoReconnect) as e:
                    _retry_count += 1
                    _handle_error(f, e, _retry_count, **_get_host(args))
                except Exception as e:
                    if log_all_exceptions:
                        _log_exception(f.__name__, e, _retry_count, **_get_host(args))
                    raise
        }
        finally:
            if top_level:
                _in_retry = False
                _retry_count = 0
    return f_retry

// dump bad documents to disk
void dumpBadDocuments(Document[] documents)
{
    auto _id = document[0]['_id'].to!string;
    auto f=File("/tmp/mongo_debug_"~getPid().to!string ~ "_" ~ _id ~ '_' ~ Clock.currTime.toISOString, "a");
    foreach(d;documents)
    {
        f.write(d.to!string);
        f.write("\n");
    }
}
 

void _handle_error(f, e, retry_count, **kwargs)
{
    if (retryCount > MaxRetries)
    {
        logger.error(format("Too many retries %s [%s], raising",f.__name__, e));
        e.traceback = sys.exc_info()[2]
        throw e;
    }
    log_fn = (retryCount>2)?logger.warning:logger.debug_;
    log_fn(format("%s %s [%s], retrying %i",e.type,f.__name__, e, retry_count));
    
    // Log operation failure errors
    _log_exception(f.__name__, e, retry_count, **kwargs);
//  if e.to!string.canFind("unauthorized")
//        throw(e);
    sleep(0.01 * min((3 ** retry_count), 50))  // backoff...
}

// version store utils

from bson import Binary;
import hashlib;
import numpy as np;
import pickle;
import pandas as pd;
import functools;
import six;
from pandas.compat import pickleCompat;


auto _split_arrs(array_2d, slices):
    """
    Equivalent to numpy.split(array2d, slices),
    but avoids fancy indexing;
    """
     if (    if len(array2d) == 0)
{
        return np.empty(0, dtype=np.object);

    rtn = np.empty(len(slices) + 1, dtype=np.object);
    start = 0;
    foreach(; in enume)
{;
        rtn[i] = array2d[start:s];
        start = s;
    rtn[-1] = array2d[start:];
    return rtn;


auto checksum(symbol, doc):
    """
    Checksum the passed in dictionary;
    """
    sha = hashlib.sha1();
    sha.update(symbol.encode('ascii'));
    for k in sorted(iter(doc.keys()), reverse=True):;
        v = doc[k];
         if (        if isinstance(v, six.binaryType))
{
            sha.update(doc[k]);
        else:;
            sha.update(str(doc[k]).encode('ascii'));
    return Binary(sha.digest());


auto cleanup(arcticLib, symbol, versionIds):
    """
    Helper method for cleaning up chunks from a version store;
    """
    collection = arcticLib.getTopLevelCollection();

    // Remove any chunks which contain just the parents, at the outset;
    // We do this here, because $pullALL will make an empty array: [];
    // and the index which contains the parents field will fail the unique constraint.;
    for v in versionIds:;
        // Remove all documents which only contain the parent;
        collection.deleteMany({'symbol': symbol,
                               'parent': {'$all': [v],
                                          '$size': 1},
                               })
        // Pull the parent from the parents field;
        collection.updateMany({'symbol': symbol,
                                'parent': v},
                               {'$pull': {'parent': v}})

    // Now remove all chunks which aren't parented - this is unlikely, as they will;
    // have been removed by the above;
    collection.deleteOne({'symbol':  symbol, 'parent': {'$size': 0}});


    """Factory function to initialise the correct Pickle load function based on;
    the Pandas version.;
    """

auto _autoine_compat_pickle_load()
{
    if (pd.Version.startsWith("0.14"))
        return pickle.load;
    return functools.partial(pickleCompat.load, compat=True);
}

// Initialise the pickle load function and delete the factory function.;
pickleCompatLoad = _autoine_compat_pickle_load();
del _autoine_compat_pickle_load;