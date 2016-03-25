module arctic.hosts;
// re, weakref:WeakValueDictionary, six
/**
    Utilities to resolve a string to Mongo host, or a Arctic library.
*/
import std.regex;
import std.experimental.logger;

__all__ = ['get_arctic_lib', 'get_arctic_for_library']

logger = logging.getLogger(__name__)

# Application environment variables
arctic_cache = WeakValueDictionary()


CONNECTION_STR = re.compile(r"(^\w+\.?\w+)@([^\s:]+:?\w+)$")

/**
    Returns a mongo library for the given connection string

    Parameters
    ---------
    string connectionString
        Format must be one of the following:
            library@trading for known mongo servers
            library@hostname:port

    Returns:
    --------
    Arctic library
*/

auto getArcticLib(string connectionString, ** kwargs)
{
    import arctic:Arctic;
    auto m = CONNECTION_STR.match(connection_string);
    enforce(m,new ValueError("connection string incorrectly formed: " ~ connectionString));
    auto library=m.group(1);
    auto host = m.group(2);
    return getArctic(host, **kwargs)[library];
}


private auto get_arctic_(instance, **kwargs)
{
    // Consider any kwargs passed to the Arctic as discriminators for the cache
    key = instance, frozenset(six.iteritems(kwargs));

    // Don't create lots of Arctic instances
    arctic = arcticCache.get(key, null);
    if (arctic is null)
    {
        // Create the instance. Note that Arctic now connects
        // lazily so this doesn't connect until on creation.
        import arctic:Arctic;
        auto arctic = Arctic(instance, **kwargs);
        arcticCache[key] = arctic;
    }
    return arctic;
}
