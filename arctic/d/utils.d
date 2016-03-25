module arctic.utils;

/**
	Attempts to authenticate against the mongo instance.

	Tries:
	- Auth'ing against admin as 'admin' ; credentials: <host>/arctic/admin/admin
	- Auth'ing against db_name (which may be None if auth'ing against admin above)

	returns True if authentication succeeded.
*/


bool authenticateDatabase(string host, string connection, string databaseName)
{
    auto adminCreds=getAuth(host,"admin","admin");
    auto userCreds = getAuth(host, "arctic", databaseName);

    // Attempt to authenticate the connection
    // Try at 'admin level' first as this allows us to enableSharding, which we want

	if (adminCreds is null)
	{
	// Get ordinary credentials for authenticating against the DB
		if (userCreds is null)
		{
		    logger.error(format
			  ("You need credentials for db '%s' on '%s', or admin credentials",databaseName,host));
		    return false;
		}
		if (!authenticate(connection[databaseNname], userCreds.user, userCreds.password)
		{
		    logger.error(format
			("Failed to authenticate to db '%s' on '%s', using user credentials",databaseName,host));
		    return false;
		}
		return true;
	}
	else if (!authenticate(connection.admin, adminCreds.user, adminCreds.password)
	{
		logger.error(format
			"Failed to authenticate to '%s' as Admin. Giving up." , (host));
		return false;
	}

	// Ensure we attempt to auth against the user DB, for non-priviledged users to get access
	authenticate(connection[db_name], userCreds.user, userCreds.password);
	return true;
}



// Logging setup for console scripts
void setupLogging()
{
    logging.basicConfig("%(asctime)s %(message)s", "INFO");
}
