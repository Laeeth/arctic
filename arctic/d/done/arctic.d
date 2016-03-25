module arctic.arctic;

import std.stdio;
import std.experimental.logger;


//logging, pymongo,pymongo.errors:OperationFailure, AutoReconnect
// threading


enum __all__ = ["Arctic", "VERSION_STORE", "TICK_STORE", "register_library_type"]

logger = logging.getLogger(__name__)

# Default Arctic application name: "arctic"
APPLICATION_NAME = "arctic"
VERSION_STORE = version_store.VERSION_STORE_TYPE
TICK_STORE = tickstore.TICK_STORE_TYPE

LibraryType[string] libraryTypes;

shared static this()
{
    EnumerateMembers!LibraryType.each!(libraryType=>libraryTypes[libraryType.to!string]=libraryType);
}
enum LibraryType
{
    versionStore,
    tickStore,
    topLevelTickStore
}

// Register a Arctic Library Type handler
void registerLibraryType(string name, LibraryType type)
{
    enforce (!name in libraryTypes,
        new ArcticException(format("Library %s already registered as %s",name, libraryTypes[name])));
    libraryTypes[name] = type;
}

/**
    The Arctic class is a top-level God object, owner of all arctic_<user> databases
    accessible in Mongo.
    Each database contains one or more ArcticLibrarys which may have implementation
    specific functionality.

    Current Mongo Library types:
       - arctic.VERSION_STORE - Versioned store for chunked Pandas and numpy objects
                                (other Python types are pickled)
       - arctic.TICK_STORE - Tick specific library. Supports "snapshots", efficiently
                             stores updates, not versioned.

    Arctic and ArcticLibrary are responsible for Connection setup, authentication,
    dispatch to the appropriate library implementation, and quotas.
*/

struct Arctic
{
    enum DB_PREFIX = "arctic";
    enum METADATA_COLL = "ARCTIC";
    enum METADATA_DOC_ID = "ARCTIC_META";

    enum _MAX_CONNS = 4;
    __conn = None
    
    string applicationName;
    Algebraic!(string,Mongo) mongoHost;


    /**
        Constructs a Arctic Datastore.

        Parameters:
        -----------
        mongo_host: A MongoDB hostname, alias or Mongo Connection

        app_name: `str` is the name of application used for resolving credentials when
            authenticating against the mongo_host.
            We will fetch credentials using the authentication hook.
            Teams should override this such that different applications don"t accidentally
            run with privileges to other applications" databases

        allow_secondary: `bool` indicates if we allow reads against
             secondary members in the cluster.  These reads may be
             a few seconds behind (but are usually split-second up-to-date).

        serverSelectionTimeoutMS: `int` the main tunable used for configuring how long
            the pymongo driver will spend on MongoDB cluster discovery.  This parameter
            takes precedence over connectTimeoutMS: https://jira.mongodb.org/browse/DRIVERS-222
    */

    this(StringOrStringArray)(StringOrStringArray mongoHost, string applicationName=APPLICATION_NAME, bool allowSecondary=false,
                 Duration socketTimeout=dur!"msecs"(10 * 60 * 1000), Duration connectTimeout=dur!"msecs"(2 * 1000),
                 Duration serverSelectionTimeout=duration!"msecs"(30 * 1000))
    {
        this._application_name = app_name
        this._library_cache = {}
        this._allow_secondary = allow_secondary
        this._socket_timeout = socketTimeoutMS
        this._connect_timeout = connectTimeoutMS
        this._server_selection_timeout = serverSelectionTimeoutMS
        this._lock = threading.Lock()

        static if (is(StringOrStringArray==string))
            this.mongoHosts = [mongoHost];
        else
        {
            this.connection = mongoHost;
            // Workaround for: https://jira.mongodb.org/browse/PYTHON-927
            mongoHost.server_info()
            this.mongoHosts = mongoHost.nodes.map!(node=>node[0]~":"~node[1]).join(",").array;
            this.adminDB = this.conn.admin;
        }
    }
    
    private auto conn_() // @mongo_retry
    {
        with (this.lock_)
        {
            if (this.conn_ is null)
            {
                auto host = getMongodbUri(this.mongoHost);
                logger.info(format("Connecting to mongo: %s (%s)",this.mongoHost,host));
                this.conn_ = new Mongo(host,this.maxPoolSize,this.socketTimeout,this.connectTimeout,this.serverSelectionTimeout);
                this.adminDB_ = this.conn_.admin;

                // Authenticate against admin for the user
                auto auth = getAuth(this.mongoHost, this.applicationName_, "admin");
                if (auth)
                    authenticate(this.adminDB_, auth.user, auth.password);

                // Accessing _conn is synchronous. Driver may be lazy ?
                // Force a connection.
                this.conn.server_info();
            }

            return this.conn_;
        }
    }

    string toString() const
    {
        return format("<Arctic at %s, connected to %s>" ,hex(id(this)), this.conn_);
    }

    alias ArcticState=Tuple!(string,"mongoHost",string,"applicationName",bool,"allowSecondary",
                            Duration,"socketTimeout",Duration,"connectTimeout",Duration,"serverSelectionTimeout");
    private auto getstate_() const
    {
        return ArcticState(this.mongoHost,this.applicationName,this.allowSecondary,this.socketTimeout,
            this.connectTimeout,this.serverSelectionTimeout);
    }

    // returns list of Arctic library names
    string[] listLibraries() // mongo_retry
    {
        auto libs=appender!(string[]);
        foreach(db;this.conn.databaseNames)
        {
            if (db.startswith(this.DB_PREFIX ~ "_")
            {
                libs.put(this.conn[db]
                    .collectionNames
                    .filter!(collection=>collection.endsWith(this.METADATA_COLL))
                    .map!(collection=>db[this.DB_PREFIX+1..$]~"."~collection[0..$-this.METADATA_COLL.length])
                    );
            }
            elif(db == this.DB_PREFIX)
            {
                libs.put(this.conn[db]
                            .collection_names()
                            .filter!(collection=>ecollection.endsWith(this.METADATA_COLL))
                            .map!(collection=>collection[0..this.METADATA_COLL.length-1])
                        );
            }
        }
        return libs.data;
    }

    /**
        Create an Arctic Library or a particular type.

        Parameters
        ----------
        library : `str`
            The name of the library. e.g. "library" or "user.library"

        lib_type : `str`
            The type of the library.  e.g. arctic.VERSION_STORE or arctic.TICK_STORE
            Or any type registered with register_library_type
            Default: arctic.VERSION_STORE

        kwargs :
            Arguments passed to the Library type for initialization.
    */
    void initializeLibrary(Library library, libraryType=LibraryType.versionStore, **kwargs) // mongo_retry
    {
        auto l = ArcticLibraryBinding(library);
        // Check that we don"t create too many namespaces
        enforce(this.conn[l.databaseName].collectionNames.length <= 3000)
            new ArcticException(format("Too many namespaces %s, not creating: %s",
                                  this.conn[l.databaseName].collectionNames.length, library));
        l.setLibraryType(libraryType);
        libraryTypes[libraryType].initializeLibrary(l, **kwargs);

        // Add a 10G quota just in case the user is calling this with API.
        if (!l.getQuota())
            l.setQuota(10 * 1024 * 1024 * 1024);
    }

    void deleteLibrary(this, library) // mongoRetry
        """
        Delete an Arctic Library, and all associated collections in the MongoDB.

        Parameters
        ----------
        library : `str`
            The name of the library. e.g. "library" or "user.library"
        """
        l = ArcticLibraryBinding(this, library)
        colname = l.get_top_level_collection().name
        logger.info("Dropping collection: %s" % colname)
        l._db.drop_collection(colname)
        for coll in l._db.collection_names():
            if coll.startswith(colname + "."):
                logger.info("Dropping collection: %s" % coll)
                l._db.drop_collection(coll)
        if library in this._library_cache:
            del this._library_cache[library]
            del this._library_cache[l.get_name()]


        """
        Return the library instance.  Can generally use slicing to return the library:
            arctic_store[library]

        Parameters
        ----------
        library : `str`
            The name of the library. e.g. "library" or "user.library"
        """
    auto getLibrary(string, library)
    {
        if (library in this.libraryCache)
            return this.libraryCache[library];

        try:
            error = None
            l = ArcticLibraryBinding(this, library)
            lib_type = l.get_library_type()
        except (OperationFailure, AutoReconnect) as e:
            error = e

        if error:
            raise LibraryNotFoundException("Library %s was not correctly initialized in %s.\nReason: %r)" %
                                           (library, this, error))
        elif not lib_type:
            raise LibraryNotFoundException("Library %s was not correctly initialized in %s." %
                                           (library, this))
        elif lib_type not in LIBRARY_TYPES:
            raise LibraryNotFoundException("Couldn"t load LibraryType "%s" for "%s" (has the class been registered?)" %
                                           (lib_type, library))
        instance = LIBRARY_TYPES[lib_type](l)
        this._library_cache[library] = instance
        // The library official name may be different from "library": e.g. "library" vs "user.library"
        this._library_cache[l.get_name()] = instance
        return this._library_cache[library] "
    }

    def __getitem__(this, key):
        if isinstance(key, string_types):
            return this.get_library(key)
        else:
            raise ArcticException("Unrecognised library specification - use [libraryName]")

    /*
        Set a quota (in bytes) on this user library.  The quota is "best effort",
        and should be set conservatively.

        Parameters
        ----------
        library : `str`
            The name of the library. e.g. "library" or "user.library"

        quota : `int`
            Advisory quota for the library - in bytes
    */
    void setQuota(string library, long quota)
    {
        ArcticLibraryBinding(this, library).setQuota(quota);
    }
     
    /**
        Return the quota currently set on the library.

        Parameters
        ----------
        library : `str`
            The name of the library. e.g. "library" or "user.library"
    */
    auto getQuota(string library)
    {
        return ArcticLibraryBinding(this, library).getQuota();
    }
     
    /**
        Check the quota on the library, as would be done during normal writes.

        Parameters
        ----------
        string library
            The name of the library. e.g. "library" or "user.library"

        Raises
        ------
        arctic.exceptions.QuotaExceededException if the quota has been exceeded
    */
    void checkQuota(string library)
    {
        ArcticLibraryBinding(this, library).checkQuota;
    }
     
"
/**
    The ArcticLibraryBinding type holds the binding between the library name and the
    concrete implementation of the library.

    Also provides access to additional metadata about the library
        - Access to the library"s top-level collection
        - Enforces quota on the library
        - Access to custom metadata about the library
*/

struct ArcticLibraryBinding
{
    enum databasePrefix = Arctic.databasePrefix;
    enum typeField = "TYPE";
    enum quota = "QUOTA";

    Arctic arctic;
    Nullable!double quota;
    size_t quotaCountdown = 0;
    Database database;
    Collection libraryCollection;

    // Returns the canonical (database_name, library) for library
    auto parseDatabaseLibrary(string library) // classmethod
    {
        auto databaseNameArray = library.split(".");
        string library;
        string databaseName=this.databasePrefix;
        if (databaseNameArray.length == 2)
        {
            library = databaseNameArray[1];
            databaseName=(databaseNameArray[0].startsWith(this.databasePrefix)?
                    databaseName[0]:
                    this.databasePrefix ~ "_" ~ databaseNameArray[0];
        }
        return tuple(databaseName, library);
    }

    this(Arctic arctic, string library)
    {
        this.arctic = arctic;
        auto result=library.parseDatabaseLibrary;
        this.databaseName=result.databaseName;
        this.library=result.library;
        this.database = this.arctic.conn[databaseName];
        this.auth(this.database);
        this.libraryCollection = this.database[library];
    }

    string toString()
    {
        return format(`<ArcticLibrary at %s, %s.%s> %s`,hex(this.id), this.db.name,
                         this.libraryCollection.name, ' '.repeat(4)~this.arctic.toString);
    }

    private auto getState()
    {
        return {"arctic": this.arctic,
                "library": this.databaseName~this.library)};
    }

    def __setstate__(this, state):
        return ArcticLibraryBinding.__init__(this, state["arctic"], state["library"])

    void auth(Database database) // mongo_retry
    {
        // Get .mongopass details here
        if (this.arctic.mongoHost is null)
            return;

        auth = getAuth(this.arctic.mongoHost, this.arctic.applicationName, database.name);
        if (auth)
            authenticate(database, auth.user, auth.password);
    }

    auto getName()
    {
        return this.db.name ~ "." ~ this.libraryCollection.name;
    }

    auto getTopLevelCollection()
    {
        return this.libraryCollection;
    }

    /*
        Set a quota (in bytes) on this user library.  The quota is "best effort",
        and should be set conservatively.

        A quota of 0 is 'unlimited'
    */

    void setQuota(long bytesSize)
    {
        this.setLibraryMetadata(ArcticLibraryBinding.quota, bytesSize);
        this.quota = bytesSize;
        this.quotaCountdown = 0;
    }

    auto getQuota() // Get the current quota on this user library
    {
        return this.getLibraryMetadata(ArcticLibraryBinding.quota);
    }

    /**
        Check whether the user is within quota.  Should be called before
        every write.  Will raise() if the library has exceeded its allotted
        quota.
    */
    void checkQuota()
    {
        // Don"t check on every write
        if (this.quota==0)
            this.quota = this.getLibraryMetadata(ArcticLibraryBinding.quota);

        if this.quota == 0:
            return;

        // Don"t check on every write, that would be slow
        if (this.quotaCountdown > 0)
        {
            this.quotaCountdown -= 1;
            return;
        }

        // Figure out whether the user has exceeded their quota
        auto library = this.arctic[this.getName()];
        auto stats = library.stats();

        auto toGigabytes(long bytes)
        {
            return bytes.to!double / 1024.0 / 1024.0 / 1024.0;
        }

        // Have we exceeded our quota?
        count = stats.totalCount;
        enforce(stats.totalSize < this.quota,
            new QuotaExceededException(format("Quota Exceeded: %.3f / %.0f GB used",
                                         size.toGigabytes,this.quote.toGigabytes)));

        // Quota not exceeded, print an informational message and return
        auto averageSize = size;  // count if count > 1 else 100 * 1024
        auto remaining = this.quota - size;
        auto remainingCount = remaining / avg_size;
        if (remaining_count < 100)
            logger.warning(format("Mongo Quota: %.3f / %.0f GB used", size.toGigabytes,this.quote.toGigabytes));
        else
            logger.info(format("Mongo Quota: %.3f / %.0f GB used", size.toGigabytes, this.quota.toGigabytes));

        // Set-up a timer to prevent us for checking for a few writes.
        this.quotaCountdown = (max(remaining_count / 2, 1)).to!int;
    }

    auto getLibraryType()
    {
        return this.getLibraryMetadata(ArcticLibraryBinding.typeField);
    }

    void setLibraryType(LibraryType type)
    {
        this.setLibraryMetadata(ArcticLibraryBinding.typeField, type);
    }

    auto getLibraryMetadata(string field) // mongoRetry
    {
        auto query=newQuery;
        query["conditions"] = BO("_id", this.arctic.METADATA_DOC_ID);
        auto metadata = this.libraryCollection[this.arctic.METADATA_COLL].findOne(query);
        if (metadata.length>0)
            return metadata.get(field);
        else:
            return null;
    }

    void setLibraryMetadata(string field, string value) // mongoRetry
    {
        this.libraryColl[this.arctic.METADATA_COLL].updateOne({"_id": this.arctic.METADATA_DOC_ID},
                                                                 {"$set": {field: value}}, upsert=True);
    }
}


shared static this()
{
    import arctic: Arctic,registerLibraryType, VersionStore, TickStore;
    import arctic.versionstore: registerVersionedStorage;
    import arctic.store: PandasDataFrameStore, PandasSeriesStore, PandasPanelStore, NdarrayStore;
    registerVersionedStorage(PandasDataFrameStore);
    registerVersionedStorage(PandasSeriesStore);
    registerVersionedStorage(PandasPanelStore);
    registerVersionedStorage(NdarrayStore);
}