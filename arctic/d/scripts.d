module kaleidic.marketdata.copy;

import argparse
import os
import logging
from multiprocessing import Pool
import pwd

from arctic.decorators import _get_host
from arctic.store.audit import ArcticTransaction

from ..hosts import get_arctic_lib
from ..date import DateRange, to_pandas_closed_closed, CLOSED_OPEN, OPEN_CLOSED, mktz
from .utils import setup_logging

logger = logging.getLogger(__name__)

# Use the UID rather than environment variables for auditing
USER = pwd.getpwuid(os.getuid())[0]


def copy_symbols_helper(src, dest, log, force, splice):
    def _copy_symbol(symbols):
        for symbol in symbols:
            with ArcticTransaction(dest, symbol, USER, log) as mt:
                existing_data = dest.has_symbol(symbol)
                if existing_data:
                    if force:
                        logger.warn("Symbol: %s already exists in destination, OVERWRITING" % symbol)
                    elif splice:
                        logger.warn("Symbol: %s already exists in destination, splicing in new data" % symbol)
                    else:
                        logger.warn("Symbol: {} already exists in {}@{}, use --force to overwrite or --splice to join "
                                    "with existing data".format(symbol, _get_host(dest).get('l'),
                                                                _get_host(dest).get('mhost')))
                        continue

                version = src.read(symbol)
                new_data = version.data

                if existing_data and splice:
                    original_data = dest.read(symbol).data
                    preserve_start = to_pandas_closed_closed(DateRange(None, new_data.index[0].to_pydatetime(),
                                                                       interval=CLOSED_OPEN)).end
                    preserve_end = to_pandas_closed_closed(DateRange(new_data.index[-1].to_pydatetime(),
                                                                     None,
                                                                     interval=OPEN_CLOSED)).start
                    if not original_data.index.tz:
                        # No timezone on the original, should we even allow this?
                        preserve_start = preserve_start.replace(tzinfo=None)
                        preserve_end = preserve_end.replace(tzinfo=None)
                    before = original_data.ix[:preserve_start]
                    after = original_data.ix[preserve_end:]
                    new_data = before.append(new_data).append(after)

                mt.write(symbol, new_data, metadata=version.metadata)
    return _copy_symbol

immutable usageText=
`
Copy data from one MongoDB instance to another.

Example:
arctic_copy_data --log "Copying data" --src user.library@host1 --dest user.library@host2 symbol1 symbol2
`
;

version(Main)
{
	struct Options
	{
		string source;
		string dest;
		string log;
		bool force=false;
		bool splice=false;
		int parallel=1;
		string[] symbols;
	}

	void main(string[] args)
	{
		import std.getopt;
		Options options;
		// enableLogging();
		auto helpInformation = getopt(
			args,
			config.required,"source", "Source MongoDB like: library@hostname:port",&options.source,
			config.required, "dest", "Destination MongoDB like: library@hostname:port", &options.dest,
			config.required, "log", "Data CR", & options.log,
					 "force", "Force overwrite of existing data for symbol", &options.force,
					"splice", "Keep existing data before and after the new data", &options.splice,
					"parallel", "Number of imports to run in parallel", & option.parallel,
					"symbols", "List of symbol regexes to copy from source to dest", & options.symbols);
		if (helpInformation.helpWanted)
		{
			stderr.writefln(usageText);
			return -1;
		}

		auto source= ArcticLibrary(options.source);
		auto dest = ArcticLibrary(options.dest);

		log.info("Copying data from %s -> %s", options.source, options.dest);

		// Prune the list of symbols from the library according to the list of symbols.
		
		auto requiredSymbols = set()
		foreach(symbol;options.symbols)
		{
			requiredSymbols.update(source.listSymbols(regex=symbol));
			requiredSymbols=sort(requiredSymbols);
		}

		log.info("Copying: %s symbols",requiredSymbols.length);
		if (requiredSymbols.length<1)
		{
			log.warn("No symbols found that matched those provided.")
			return
		}

		// Function we'll call to do the data copying
		auto copySymbol=copySymbolsHelper(source,dest,options.log,options.force,options.splice);

		if (options.parallel>1)
		{
			log.info("Starting: %s jobs",options.parallel);
			auto pool = taskPool;
			// Break the jobs into chunks for multiprocessing
		chunk_size = len(required_symbols) / opts.parallel
		chunk_size = max(chunk_size, 1)
chunks = [required_symbols[offs:offs + chunk_size] for offs in
                  range(0, len(required_symbols), chunk_size)]
        assert sum(len(x) for x in chunks) == len(required_symbols)
        pool.apply(copy_symbol, chunks)
    else:
        copy_symbol(required_symbols)


// ===============================
// fsck

import logging
import argparse

from ..hooks import get_mongodb_uri
from ..arctic import Arctic, ArcticLibraryBinding
from .utils import do_db_auth, setup_logging

logger = logging.getLogger(__name__)


def main():
    usage = """
    Check a Arctic Library for inconsistencies.
    """
    setup_logging()

    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("--host", default='localhost', help="Hostname, or clustername. Default: localhost")
    parser.add_argument("--library", nargs='+', required=True, help="The name of the library. e.g. 'arctic_jblackburn.lib'")
    parser.add_argument("-v", action='store_true', help="Verbose mode")
    parser.add_argument("-f", action='store_true', help="Force ; Cleanup any problems found. (Default is dry-run.)")
    parser.add_argument("-n", action='store_true', help="No FSCK ; just print stats.)")

    opts = parser.parse_args()

    if opts.v:
        logger.setLevel(logging.DEBUG)

    if not opts.f:
        logger.info("DRY-RUN: No changes will be made.")

    logger.info("FSCK'ing: %s on mongo %s" % (opts.library, opts.host))
    store = Arctic(get_mongodb_uri(opts.host))

    for lib in opts.library:
        # Auth to the DB for making changes
        if opts.f:
            database_name, _ = ArcticLibraryBinding._parse_db_lib(lib)
            do_db_auth(opts.host, store._conn, database_name)

        orig_stats = store[lib].stats()

        logger.info('----------------------------')
        if not opts.n:
            store[lib]._fsck(not opts.f)
        logger.info('----------------------------')

        final_stats = store[lib].stats()
        logger.info('Stats:')
        logger.info('Sharded:        %s' % final_stats['chunks'].get('sharded', False))
        logger.info('Symbols:  %10d' % len(store[lib].list_symbols()))
        logger.info('Versions: %10d   Change(+/-) %6d  (av: %.2fMB)' %
                    (final_stats['versions']['count'],
                     final_stats['versions']['count'] - orig_stats['versions']['count'],
                     final_stats['versions'].get('avgObjSize', 0) / 1024. / 1024.))
        logger.info("Versions: %10.2fMB Change(+/-) %.2fMB" %
                    (final_stats['versions']['size'] / 1024. / 1024.,
                    (final_stats['versions']['size'] - orig_stats['versions']['size']) / 1024. / 1024.))
        logger.info('Chunk Count: %7d   Change(+/-) %6d  (av: %.2fMB)' %
                    (final_stats['chunks']['count'],
                     final_stats['chunks']['count'] - orig_stats['chunks']['count'],
                     final_stats['chunks'].get('avgObjSize', 0) / 1024. / 1024.))
        logger.info("Chunks: %12.2fMB Change(+/-) %6.2fMB" %
                    (final_stats['chunks']['size'] / 1024. / 1024.,
                    (final_stats['chunks']['size'] - orig_stats['chunks']['size']) / 1024. / 1024.))
        logger.info('----------------------------')

    if not opts.f:
        logger.info("Done: DRY-RUN: No changes made. (Use -f to fix any problems)")
    else:
        logger.info("Done.")
// ======
// delete_library

from __future__ import print_function
import optparse
import pymongo
import logging

from ..hooks import get_mongodb_uri
from ..arctic import Arctic
from .utils import do_db_auth, setup_logging

logger = logging.getLogger(__name__)


def main():
    usage = """usage: %prog [options]

    Deletes the named library from a user's database.

    Example:
        %prog --host=hostname --library=arctic_jblackburn.my_library
    """
    setup_logging()

    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--host", default='localhost', help="Hostname, or clustername. Default: localhost")
    parser.add_option("--library", help="The name of the library. e.g. 'arctic_jblackburn.lib'")

    (opts, _) = parser.parse_args()

    if not opts.library:
        parser.error('Must specify the full path of the library e.g. arctic_jblackburn.lib!')

    print("Deleting: %s on mongo %s" % (opts.library, opts.host))
    c = pymongo.MongoClient(get_mongodb_uri(opts.host))

    db_name = opts.library[:opts.library.index('.')] if '.' in opts.library else None
    do_db_auth(opts.host, c, db_name)
    store = Arctic(c)
    store.delete_library(opts.library)

    logger.info("Library %s deleted" % opts.library)


if __name__ == '__main__':
    main()

// enable_sharding

from __future__ import print_function
import optparse
import pymongo

from ..arctic import Arctic
from ..auth import get_auth
from ..hooks import get_mongodb_uri
from .._util import enable_sharding
from ..auth import authenticate
from .utils import setup_logging


def main():
    usage = """usage: %prog [options] arg1=value, arg2=value

    Enables sharding on the specified arctic library.
    """
    setup_logging()

    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--host", default='localhost', help="Hostname, or clustername. Default: localhost")
    parser.add_option("--library", help="The name of the library. e.g. 'arctic_jblackburn.lib'")

    (opts, _) = parser.parse_args()

    if not opts.library or '.' not in opts.library:
        parser.error('must specify the full path of the library e.g. arctic_jblackburn.lib!')

    print("Enabling-sharding: %s on mongo %s" % (opts.library, opts.host))

    c = pymongo.MongoClient(get_mongodb_uri(opts.host))
    credentials = get_auth(opts.host, 'admin', 'admin')
    if credentials:
        authenticate(c.admin, credentials.user, credentials.password)
    store = Arctic(c)
    enable_sharding(store, opts.library)


if __name__ == '__main__':
    main()


// create user

import argparse
import base64
from pymongo import MongoClient
import uuid
import logging

from ..hooks import get_mongodb_uri
from .utils import do_db_auth
from arctic.arctic import Arctic

logger = logging.getLogger(__name__)


def main():
    usage = """arctic_create_user --host research [--db mongoose_user] [--write] user

    Creates the user's personal Arctic mongo database
    Or add a user to an existing Mongo Database.
    """

    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("--host", default='localhost', help="Hostname, or clustername. Default: localhost")
    parser.add_argument("--db", default=None, help="Database to add user on. Default: mongoose_<user>")
    parser.add_argument("--password", default=None, help="Password. Default: random")
    parser.add_argument("--write", action='store_true', default=False, help="Used for granting write access to someone else's DB")
    parser.add_argument("users", nargs='+', help="Users to add.")

    args = parser.parse_args()

    c = MongoClient(get_mongodb_uri(args.host))

    if not do_db_auth(args.host, c, args.db if args.db else 'admin'):
        logger.error("Failed to authenticate to '%s'. Check your admin password!" % (args.host))
        return

    for user in args.users:
        write_access = args.write
        p = args.password
        if p is None:
            p = base64.b64encode(uuid.uuid4().bytes).replace(b'/', b'')[:12]
        db = args.db
        if not db:
            # Users always have write access to their database
            write_access = True
            db = Arctic.DB_PREFIX + '_' + user

        # Add the user to the database
        c[db].add_user(user, p, read_only=not write_access)

        logger.info("Granted: {user} [{permission}] to {db}".format(user=user,
                                                                    permission='WRITE' if write_access else 'READ',
                                                                    db=db))
        logger.info("User creds: {db}/{user}/{password}".format(user=user,
                                                                host=args.host,
                                                                db=db,
                                                                password=p,
                                                                ))


if __name__ == '__main__':
    main()

// init library

from __future__ import print_function
import argparse
import pymongo
import logging

from ..hooks import get_mongodb_uri
from ..arctic import Arctic, VERSION_STORE, LIBRARY_TYPES, \
    ArcticLibraryBinding
from .utils import do_db_auth, setup_logging

logger = logging.getLogger(__name__)


def main():
    usage = """Initializes a named library in a user's database.  Note that it will enable sharding on the underlying
    collection if it can.  To do this you must have admin credentials in arctic:

    Example:
        arctic_init_library --host=hostname --library=arctic_jblackburn.my_library
    """
    setup_logging()

    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("--host", default='localhost', help="Hostname, or clustername. Default: localhost")
    parser.add_argument("--library", help="The name of the library. e.g. 'arctic_jblackburn.lib'")
    parser.add_argument("--type", default=VERSION_STORE, choices=sorted(LIBRARY_TYPES.keys()),
                                    help="The type of the library, as defined in "
                                         "arctic.py. Default: %s" % VERSION_STORE)
    parser.add_argument("--quota", default=10, help="Quota for the library in GB. A quota of 0 is unlimited."
                                                    "Default: 10")
    parser.add_argument("--hashed", action="store_true", default=False, help="Use hashed based sharding. Useful where SYMBOLs share a common prefix (e.g. Bloomberg BBGXXXX symbols)"
                                                        "Default: False")

    opts = parser.parse_args()

    if not opts.library or '.' not in opts.library:
        parser.error('Must specify the full path of the library e.g. user.library!')
    db_name, _ = ArcticLibraryBinding._parse_db_lib(opts.library)

    print("Initializing: %s on mongo %s" % (opts.library, opts.host))
    c = pymongo.MongoClient(get_mongodb_uri(opts.host))

    if not do_db_auth(opts.host, c, db_name):
        logger.error('Authentication Failed. Exiting.')
        return

    store = Arctic(c)
    store.initialize_library("%s" % opts.library, opts.type, hashed=opts.hashed)
    logger.info("Library %s created" % opts.library)

    logger.info("Setting quota to %sG" % opts.quota)
    store.set_quota(opts.library, int(opts.quota) * 1024 * 1024 * 1024)


if __name__ == '__main__':
    main()


// list libraries

from __future__ import print_function
import optparse

from ..arctic import Arctic
from .utils import setup_logging

print = print


def main():
    usage = """usage: %prog [options] [prefix ...]

    Lists the libraries available in a user's database.   If any prefix parameters
    are given, list only libraries with names that start with one of the prefixes.

    Example:
        %prog --host=hostname rgautier
    """
    setup_logging()

    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--host", default='localhost', help="Hostname, or clustername. Default: localhost")

    (opts, args) = parser.parse_args()

    store = Arctic(opts.host)
    for name in sorted(store.list_libraries()):
        if (not args) or [n for n in args if name.startswith(n)]:
            print(name)


if __name__ == '__main__':
    main()


/// prune versions

from __future__ import print_function
import optparse
import pymongo
import logging

from ..hooks import get_mongodb_uri
from ..arctic import Arctic, ArcticLibraryBinding
from .utils import do_db_auth, setup_logging

logger = logging.getLogger(__name__)


def prune_versions(lib, symbol, keep_mins):
    lib._prune_previous_versions(symbol, keep_mins=keep_mins)


def main():
    usage = """usage: %prog [options]

    Prunes (i.e. deletes) versions of data that are not the most recent, and are older than 10 minutes,
    and are not in use by snapshots. Must be used on a Arctic VersionStore library instance.

    Example:
        arctic_prune_versions --host=hostname --library=arctic_jblackburn.my_library
    """
    setup_logging()

    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--host", default='localhost', help="Hostname, or clustername. Default: localhost")
    parser.add_option("--library", help="The name of the library. e.g. 'arctic_jblackburn.library'")
    parser.add_option("--symbols", help="The symbols to prune - comma separated (default all)")
    parser.add_option("--keep-mins", default=10, help="Ensure there's a version at least keep-mins old. Default:10")

    (opts, _) = parser.parse_args()

    if not opts.library:
        parser.error('Must specify the Arctic library e.g. arctic_jblackburn.library!')
    db_name, _ = ArcticLibraryBinding._parse_db_lib(opts.library)

    print("Pruning (old) versions in : %s on mongo %s" % (opts.library, opts.host))
    print("Keeping all versions <= %s mins old" % (opts.keep_mins))
    c = pymongo.MongoClient(get_mongodb_uri(opts.host))

    if not do_db_auth(opts.host, c, db_name):
        logger.error('Authentication Failed. Exiting.')
        return
    lib = Arctic(c)[opts.library]

    if opts.symbols:
        symbols = opts.symbols.split(',')
    else:
        symbols = lib.list_symbols(all_symbols=True)
        logger.info("Found %s symbols" % len(symbols))

    for s in symbols:
        logger.info("Pruning %s" % s)
        prune_versions(lib, s, opts.keep_mins)
    logger.info("Done")


if __name__ == '__main__':
    main()
