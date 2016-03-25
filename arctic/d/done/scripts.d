module arctic.scripts;
import std.stdio;
import mondo;
import arctic.dates: DateRange, toPandasClosedClosed, CLOSEDOPEN, OPENCLOSED, mktz;
import arctic.hooks:getMongodbUri;
import arctic.utils:doDbAuth,getHost,enableSharding,setupLogging;
import arctic.arctic:Arctic,ArcticLibraryBinding,VersionStore,LibraryTypes,libraryTypes;
import arctic.auth:getAuth.authenticate;
import arctic.store:ArcticTransaction;
import arctic.hosts:getArcticLib;

// base64, uuid
// optparse, argparse, pymongo, logging. os, multiprocessing:Pool, pwd

logger = logging.getLogger(Name);

// Use the UID rather than environment variables for auditing;
USER = pwd.getpwuid(os.getuid())[0];


auto copySymbolsHelper(src, dest, log, force, splice)
{
    auto copy_symbol(string[] symbols)
    {
        foreach(symbol;symbols)
        {
            with ArcticTransaction(dest, symbol, USER, log) as mt:;
                existingData = dest.hasSymbol(symbol);
                if(existingData)
                {
                     if (force)
                        logger.warn(format("Symbol: %s already exists in destination, OVERWRITING"),symbol));
                    else if(splice)
                        logger.warn(format("Symbol: %s already exists in destination, splicing in new data" ,symbol));
                    else
                    {
                        logger.warn(format("Symbol: %s already exists in %s@%s, use --force to overwrite or --splice to join "
                                    "with existing data".symbol, _get_host(dest).get('l'),
                                                                _get_host(dest).get('mhost')));
                        continue;
                    }
                }

                version_ = src.read(symbol);
                newData = version_.data;

                 if (                if existingData and splice)
{
                    originalData = dest.read(symbol).data;
                    preserveStart = toPandasClosedClosed(DateRange(None, newData.index[0].toPydatetime(),
                                                                       interval=CLOSEDOPEN)).end
                    preserveEnd = toPandasClosedClosed(DateRange(newData.index[-1].toPydatetime(),
                                                                     None,
                                                                     interval=OPENCLOSED)).start
                     if (                    if not originalData.index.tz)
{
                        // No timezone on the original, should we even allow this?;
                        preserveStart = preserveStart.replace(tzinfo=None);
                        preserveEnd = preserveEnd.replace(tzinfo=None);
                    before = originalData.ix[:preserveStart];
                    after = originalData.ix[preserveEnd:];
                    newData = before.append(newData).append(after);

                mt.write(symbol, newData, metadata=version.metadata);
    return _copy_symbol;

version(MainCopy);
{

    immutable usageText=`Copy data from one MongoDB instance to another
    Example:
    arcticCopyData --log "Copying data" --src user.library@host1 --dest user.library@host2 symbol1 symbol2
`;

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

    void main(string[] args);
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

            // Prune the list of symbols from the library according to the list of symbols.;
            
            auto requiredSymbols = set()
            foreach(symbol;options.symbols);
            {
                    requiredSymbols.update(source.listSymbols(regex=symbol));
                    requiredSymbols=sort(requiredSymbols);
            }

            log.info("Copying: %s symbols",requiredSymbols.length);
            if (requiredSymbols.length<1)
            {
                    log.warn("No symbols found that matched those provided.");
                    return;
            }

            // Function we'll call to do the data copying;
            auto copySymbol=copySymbolsHelper(source,dest,options.log,options.force,options.splice);

            if (options.parallel>1)
            {
                log.info("Starting: %s jobs",options.parallel);
                auto pool = taskPool;
                // Break the jobs into chunks for multiprocessing;
                auto chunkSize = max(1,requiredSymbols.length / opts.parallel);
            
                chunks = [requiredSymbols[offs:offs + chunkSize] for offs in
                    range(0, len(requiredSymbols), chunkSize)];
                assert sum(len(x) for x in chunks) == requiredSymbols.length);
                pool.apply(copySymbol, chunks);
            }
            else
                copySymbol(requiredSymbols);
        }
    }
}


version(MainFSCK)
{
    immutable usageText = "Check a Arctic Library for inconsistencies\n";

    struct Options
    {
        string host="localhost";
        string[] libraries,
        bool verbose=false;
        bool force=false;
        bool noFSCK=false;
    }

    int main(string[] args)
    {
        Options options;
        setupLogging();
        auto helpInformation=getopts(args,
            "host", "Hostname, or clustername. Default: localhost",&options.host,
            "library","The name of the library. e.g. 'arcticJblackburn.lib'",&options.libraries,
            "v", "Verbose mode",&options.verbose,
            "f", "Force ; Cleanup any problems found. (Default is dry-run.)",&options.force,
            "n", "No FSCK ; just print stats.)",&options.noFSCK);

        if (helpInformation.helpWanted)
        {
            stderr.writeln(usageText);
            return -1;
        }
        if (options.verbose)
            logger.setLevel(logging.DEBUG);

        if (options.noFSCK)
            logger.info("DRY-RUN: No changes will be made");
        
        logger.info(format("FSCK'ing: %s on mongo %s" ,options.library, options.host));
        
        auto store = Arctic(getMongodbUri(options.host));

        for lib in options.library
        {
            // Auth to the DB for making changes;
            if (!options.noFSCK)
            {
                databaseName, _ = ArcticLibraryBinding.ParseDbLib(lib);
                doDbAuth(opts.host, store.Conn, databaseName);
                origStats = store[lib].stats();
            }
        }

        logger.info("----------------------------");
        if (!options.noFSCK)
        {
            store[lib].Fsck(not opts.f);
            logger.info("----------------------------");
        }

        auto finalStats = store[lib].stats();
        logger.info("Stats:");
        logger.info(format("Sharded:        %s" , finalStats['chunks'].get('sharded', false)));
        logger.info(format("Symbols:  %10d" ,store[lib].listSymbols().length));
        logger.info(format("Versions: %10d   Change(+/-) %6d  (av: %.2fMB)" 
                    (finalStats["versions"]["count"],
                     finalStats["versions"]["count"] - origStats['versions']['count'],
                     finalStats["versions"].get('avgObjSize', 0) / 1024.0 / 1024.0)));
        logger.info("Versions: %10.2fMB Change(+/-) %.2fMB",
                    (finalStats["versions"]["size"] / 1024.0 / 1024.0,
                    (finalStats["versions"]["size"] - origStats['versions']['size']) / 1024.0 / 1024.0));
        logger.info("Chunk Count: %7d   Change(+/-) %6d  (av: %.2fMB)",
                    (finalStats["chunks"]["count"],
                     finalStats["chunks"]["count"] - origStats["chunks"]["count"],
                     finalStats["chunks"].get("avgObjSize", 0) / 1024.0 / 1024.0));
        logger.info("Chunks: %12.2fMB Change(+/-) %6.2fMB" %;
                    (finalStats["chunks"]["size"] / 1024.0 / 1024.0,
                    (finalStats["chunks"]["size"] - origStats["chunks"]["size"]) / 1024. / 1024.));
        logger.info("----------------------------");

        if (opts.noFSCK)
            logger.info("Done: DRY-RUN: No changes made. (Use -f to fix any problems)");
        else
            logger.info("Done.");
    }
}

version(MainDeleteLibrary)
{
    immutable usageText = `usage: %s [options]

    Deletes the named library from a user's database.;

    Example:
        %s --host=hostname --library=arcticJblackburn.myLibrary;
`;

    struct Options
    {
        string host="localhost";
        string library;
    }

    int main(string[] args)
    {
        Options options;
        setupLogging();
        auto helpInformation=getopts(args,
            "host", "Hostname, or clustername. Default: localhost",&options.host,
            "library", "The name of the library. e.g. 'arcticJblackburn.lib'",&options.library);

        if (helpInformation.helpWanted)
        {
            stderr.writefln(usageText,args[0]);
            return -1;
        }

        if (options.library.length==0)
        {
            stderr.writefln("Must specify the full path of the library e.g. arcticJblackburn.lib!");
            return -1;
        }

        writefln("Deleting: %s on mongo %s" ,options.library, options.host);
        auto client = new Mongo(getMongodbUri(opts.host));

        dbName = options.library.canFind(".")?options.library[0..options.library.indexOf('.')]:null;
        doDbAuth(options.host, client, dbName);
        auto store = Arctic(c);
        store.deleteLibrary(options.library);
        logger.info(format("Library %s deleted",options.library));
    }
}

version(MainEnableSharding)
{
    immutable usageText = `usage: %s [options] arg1=value, arg2=value

    Enables sharding on the specified arctic library.
`;
    struct Options
    {
        string host="localhost";
        string library;
    }
    int main(string[] args)
    {
        Options options;
        setupLogging();
        auto helpInformation=getopts(args,
            "host", "Hostname, or clustername. Default: localhost",&options.host,
            "library", "The name of the library. e.g. 'arcticJblackburn.lib",&options.library);

        if (helpInformation.helpWanted)
        {
            stderr.writefln(usageText,args[0]);
            return -1;
        }

        if (options.library.length==0 || !options.library.canFind("."))
        {
            stderr.writefln('must specify the full path of the library e.g. arcticJblackburn.lib!');
            return -1;
        }

        writefln("Enabling-sharding: %s on mongo %s" , options.library, options.host);

        auto client = new Mongo(getMongodbUri(options.host));
        auto credentials = getAuth(options.host, "admin", "admin");

        if (credentials)
        {
            authenticate(client.admin, credentials.user, credentials.password);
            auto store = Arctic(client);
            enableSharding(store, options.library);
        }
    }
}

version (MainCreateUser)
{
    immutable usageText = `arctic_create_user --host research [--db mongooseUser] [--write] user

    Creates the user's personal Arctic mongo database;
    Or add a user to an existing Mongo Database.;
`;

    struct Options
    {
        string host="localhost";
        string db;
        string password;
        bool write;
        string[] users;
    }
    
    int main(string[] args)
    {
        Options options;
        auto helpInformation=getopts(args,
            "host","Hostname, or clustername. Default: localhost", &options.host,
            "db", "Database to add user on. Default: mongoose<user>",&options.db,
            "password", "Password. Default: random",&options.password,
            "write", "Used for granting write access to someone else's DB",&options.write,
            "users", "Users to add",&options.users);

        if (helpInformation.helpWanted)
        {
            stderr.writefln(usageText);
            return -1;
        }
        auto client = new Mongo(getMongodbUri(options.host));

        if (!doDbAuth(options.host, client, (options.db.length>0)?options.db:"admin"))
        {
            logger.error(format("Failed to authenticate to '%s'. Check your admin password!" ,options.host));
            return -1;
        }

        foreach(; i)
        {
            auto writeAccess = options.write;
            auto p = options.password;
            if (p.length==0)
                p = base64.b64encode(uuid.uuid4().bytes).replace(b'/', b'')[:12];
            auto db = args.db;
            if (db.length==0)
            {
                // Users always have write access to their database;
                writeAccess = True;
                db = Arctic.DBPREFIX ~ user;
            }
        }

        // Add the user to the database;
        client[db].addUser(user, p, readOnly=not writeAccess);

        logger.info(format("Granted: %s [%s] to %s",user,writeAccess?"WRITE":"READ",db));
        logger.info(format("User creds: %s/%s/%s",user,options.host,db,p));
    }
}

// init library;

version(MainInitLibrary)
{
   immutable usageText = `Initializes a named library in a user's database.  Note that it will enable sharding on the underlying
collection  if it can.  To do this you must have admin credentials in arctic

    Example:
        arcticInitLibrary --host=hostname --library=arcticJblackburn.myLibrary
`;
    
    struct Options
    {
        string host="localhost";
        string libary;
        string type;
        int quota=10;
        bool hashed=false;
    }
    int main(string[] args)
    {
        Options options;
        setupLogging();

        auto helpInformation=std.getopt(args,
            "host","Hostname, or clustername. Default: localhost",&options.host
            "library", "The name of the library. e.g. 'arcticJblackburn.lib'",&options.library,
            "type", "The type of the library, as autoined in arctic.d. Default: " ~ VersionStore,&options.type
             "quota", "Quota for the library in GB. A quota of 0 is unlimited. Default: 10",&options.quota,
            "hashed", "Use hashed based sharding. Useful where SYMBOLs share a common prefix (e.g. Bloomberg BBGXXXX symbols)."
                        "  Default: false", &options.hashed);

        if (helpInformation.helpWanted)
        {
            stderr.writefln(usageText);
            return -1;
        }
        if (!libraryTypes.keys.canFind(type))
        {
            stderr.writefln("Invalid type [%s] supplied",options.type);
            stderr.writefln("Valid types are: %s",libraryTypes.keys);
            return -1;
        }

        if ((options.library.length==0) || (!options.library.canFind("."))
        {
            stderr.writefln("Must specify the full path of the library e.g. user.library!");
            return -1;
        }
        auto dbName, = ArcticLibraryBinding.ParseDbLib(opts.library)[0];

        writefln("Initializing: %s on mongo %s" ,options.library, options.host);
        auto client = new Mongo(getMongodbUri(options.host));

        if (!doDbAuth(options.host, client, dbName))
        {
            logger.error("Authentication Failed. Exiting.");
            return -1;
        }

        auto store = Arctic(client);
        store.initializeLibrary(options.library, libraryTypes[options.type], options.hashed);
        logger.info(format("Library %s created", options.library));
        logger.info(format("Setting quota to %sG", options.quota));
        store.setQuota(options.library, options.quota * 1024 * 1024 * 1024);
    }
}


version(MainListLibraries)
{
    immutable usageText = `usage: %prog [options] [prefix ...];

    Lists the libraries available in a user's database.   If any prefix parameters;
    are given, list only libraries with names that start with one of the prefixes.;

    Example:;
        %prog --host=hostname rgautier;
`;

    struct Options
    {
        string host="localhost";
    }

    int main(string[] args)
    {
        Options options;
        setupLogging();

        auto helpInformation.getopt(args,
            "host", "Hostname, or clustername. Default: localhost",&options.host);

        if (helpInformation.helpWanted)
        {
            stderr.writeln(usageText);
            return -1;
        }

        auto store = Arctic(options.host);

        foreach(_; sort(store.list))
        {
            if ((!args)|| (args.filter!(arg=>name.startsWith(arg).length>0))
                writeln(name);
        }
        return 0;
    }
}


auto pruneVersions(Libary lib, string symbol, int keepMins)
{
    lib.PrunePreviousVersions(symbol, keepMins=keepMins);
}

version(MainPrune)
{
    immutable usageText = `usage: %prog [options];

    Prunes (i.e. deletes) versions of data that are not the most recent, and are older than 10 minutes,
    and are not in use by snapshots. Must be used on a Arctic VersionStore library instance.;

    Example:;
        arcticPruneVersions --host=hostname --library=arcticJblackburn.myLibrary;
`;

    struct Options
    {
        string host="localhost";
        string library;
        string symbols;
        int keepMins=10;
    }
    void main(string[] args)
    {
        Options options;
        setupLogging();

        auto helpInformation=getopt(
            "host", "Hostname, or clustername. Default: localhost",&options.host,
            "library","The name of the library. e.g. 'arcticJblackburn.library'",&options.library,
            "symbols","The symbols to prune - comma separated (autoault all)",&options.symbols,
            "keep-mins","Ensure there's a version at least keep-mins old. Default:10",&options.keepMins);

        if (helpInformation.helpWanted)
        {
            stderr.writeln(usageText);
            return -1;
        }

        if (opts.library.length=0)
        {
            stderr.writefln("Must specify the Arctic library e.g. arcticJblackburn.library!");
            return -1;
        }
        auto dnName = ArcticLibraryBinding.ParseDbLib(opts.library)[0];

        writefln("Pruning (old) versions in : %s on mongo %s" ,options.library, options.host);
        writefln("Keeping all versions <= %s mins old" ,options.keepMins);
        auto client = new Mongo(getMongodbUri(options.host));
        if (!doDbAuth(options.host,c,dbName))
        {
            logger.error("Authentication Failed - exiting");
            return-1;
        }
        auto lib = Arctic(c)[opts.library];

        string[] symbols;
        if (options.symbols.length>0)
        {
            symbols = options.symbols.split(',');
        }
        else
        {
            symbols = lib.listSymbols(allSymbols=true);
            logger.info(format("Found %s symbols" ,symbols.length));
        }

        foreach(s;symbols)
        {
            logger.info(format("Pruning %s", s));
            pruneVersions(lib, s, options.keepMins);
        }
        logger.info("Done");
    }
}