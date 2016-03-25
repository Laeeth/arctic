

_resolve_mongodb_hook = lambda env: env
_log_exception_hook = lambda *args, **kwargs: None
_get_auth_hook = lambda *args, **kwargs: None


/**
   Return the MongoDB URI for the passed in host-alias / environment.

    Allows an indirection point for mapping aliases to particular
    MongoDB instances.
*/

auto getMongoDBUri(string host)
{
    return resolve_mongodb_hook_(host);
}


void registerResolveMongoDBHook(Hook)(Hook hook)
{
    resolve_mongodb_hook_ = hook;
}

// External exception logging hook
void logException(string functionName, Exception exception, int retryCount, **kwargs)
{
    log_exception_hook_(functionName, exception, retryCount, **kwargs);
}


void registerLogException_hook(Hook)(Hook hook)
{
    log_exception_hook_ = hook;
}

void registerGetAuthHook(Hook)(Hook hook)
{
    get_auth_hook_ = hook;
}
