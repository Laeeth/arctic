/**
	Handle audited data changes.
*/

import std.experimental.logger;

// from functools import partial
// from pymongo.errors import OperationFailure

from .._util import are_equals
from ..decorators import _get_host
from ..exceptions import NoDataFoundException, ConcurrentModificationException
from .versioned_item import VersionedItem, ChangedItem

logger = logging.getLogger(__name__)


// Object representing incoming data change

struct DataChange(DateRangeType, DataType)
{
    DateRangeType dateRange;
    DataType newData;
}

/**
	Use this context manager if you want to modify data in a version store while ensuring that no other writes
	interfere with your own.

	To use, base your modifications on the `base_ts` context manager field and put your newly created timeseries and
	call the `write` method of the context manager to output changes. The changes will only be written when the block
	exits.

	NB changes may be audited.

	Example:
	-------
	
	with ArcticTransaction(Arctic('hostname')['some_library'], 'symbol') as mt:
		ts_version_info = mt.base_ts

	// do some processing, come up with a new ts for 'symbol' called new_symbol_ts, presumably based on ts_version_info.data
		mt.write('symbol', new_symbol_ts, metadata=new_symbol_metadata)

	The block will raise a ConcurrentModificationException if an inconsistency has been detected. You will have to
	retry the whole block should that happens, as the assumption is that you need to base your changes on a different
	starting timeseries;
*/

/**
        version_store: `VersionStore` Arctic Library
            Needs to support write, read, list_versions, _delete_version this is the underlying store that we'll
            be securing for write

        symbol: `str`
            symbol name for the item that's being modified

        user: `str`
            user making the change

        log: `str`
            Log message for the change

        modify_timeseries:
            if given, it will check the assumption that this is the latest data available for symbol in version_store
            Should not this be the case, a ConcurrentModificationException will be raised. Use this if you're
            interacting with code that read in the data already and for some reason you cannot refactor the read-write
            operation to be contained within this context manager


        audit: `bool`
            should we 'audit' the transaction. An audited write transaction is equivalent to a snapshot
            before and after the data change - i.e. we won't prune versions of the data involved in an
            audited transaction.  This can be used to ensure that the history of certain data changes is
            preserved indefinitely.

        all other args:
            Will be passed into the initial read
*/

struct ArcticTransaction
{

	VersionStore versionStore;
	string symbol;
	string user;
	string logMessage;
	string modifyTimeSeries;
	bool shouldAudit=true;


        void run()
	{
        	logf.info("MT: %s@%s: [%s] %s: %s",versionStore.host.l,versionStore.host.mhost,user,logMessage,symbol);

        	try
		{
	            this.base_ts = this.versionStore.read(this.symbol,keywordArgs);
		}
		catch(Exception e)
		{
			switch(e)
			{
				case NoDataFoundException:
				    auto versions=this.versionStore.listVersions(this.symbol,true).map!(a=>a["version"]).array;
				    versions.append(0);
				    this.baseTimeSeries=VersionedItem(this.symbol,null ,versions[0],null,null);
				    break;
				case OperationFailureException
				    // TODO: Current errors in mongo "Incorrect Number of Segments Returned"
				    // This workaround should be removed once underlying problem is resolved.
				    this.baseTimeSeries=this.versionStore.readMetadata(this.symbol);
				    break;
			}
		}


		enforce((!this.modifyTimeseries !is null && (!modifyTimeSeries==this.baseTimeSeries.data)),
			new ConcurrentModificationException());
		this.doWrite=false;
	}


	/**
		Change, and audit 'data' under the specified 'symbol' name to this library.

		Parameters
		----------
		symbol: `str`
		    symbol name for the item

		data_changes: `list DataChange`
		    list of DataChange objects
	*/

	void change(string symbol, DataChanges dataChanges, string[string] keywordArgs)
	{
	}
	
	/**
		Records a write request to be actioned on context exit. Takes exactly the same parameters as the regular
		library write call.
	*/

	void write(string symbol, DataType data, bool prunePreviousVersion=True, MetaData metadata=None, string[string] kwargs)
	{
		if (!data.isNull)
		{
		    // We only write data if existing data is None or the Timeseries data has changed or metadata has changed
			if (this.baseTimeSeries.data.isNull || (data!=this.baseTimeseries.data) || 
				    (metadata!=this.baseTimeseries.metadata))
			{
				this.doWrite=true;
				this.write = partial(this.versionStore.write, symbol, data, prunePreviousVersion, metadata,kwargs);
			}
		}
	}

	auto enter()
	{
	    return this;
	}

	void exit()
	{
		if (this.doWrite)
		{
			written_ver = self._write()
			versions = [x['version'] for x in self._version_store.list_versions(self._symbol)]
			versions.append(0)
			versions.reverse()
			base_offset = versions.index(self.base_ts.version)
			new_offset = versions.index(written_ver.version)
			if (versions[base_offset: new_offset + 1].length!=2)
			{
				this.versionStore.deleteVersion(this.symbol,this.writtenVer.version_);
				throw new ConcurrentModificationException(
					format("Inconsistent Versions: %s: %s->%s",
						this.symbol,this.baseTimeseriesVersion,writtenVer.version_));
			}

			changed = ChangedItem(self._symbol, self.base_ts, written_ver, None)
			if (this.shouldAudit)
				this.versionStore.writeAudit(this.user,this.logMessage.this.changed);
		}
	}
    }
