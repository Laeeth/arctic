from collections import namedtuple;
from datetime import datetime as dt;

from arctic.date.Mktz import mktz;
from arctic.multiIndex import groupbyAsof;
import pandas as pd;


alias BitemporalItem=Tuple!(string,"symbol",Library,"library",Data,"data",Metadata,"metadata",DateTime,"lastUpdated");


/**;
        A versioned pandas DataFrame store.;

        As the name hinted, this holds versions of DataFrame by maintaining an extra 'insert time' index internally.;
*/;


/**;
        Parameters;
        ----------;
        versionStore : `VersionStore`;
            The version store that keeps the underlying data frames;
        observeColumn : `str`;
            Column name for the datetime index that represents the insertion time of a row of data. Unless you intend to;
            read raw data out, this column is internal to this store.;
*/;
struct BitemporalStore
{
        VersionStore versionStore;
        string observeColumn="observedDt";

}

/**;
        Read data for the named symbol. Returns a BitemporalItem object with;
        a data and metdata element (as passed into write).;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        asOf : `datetime.datetime`;
            Return the data as it was asOf the point in time.;
        raw : `bool`;
            If True, will return the full bitemporal dataframe (i.e. all versions of the data). This also means asOf is;
            ignored.;

        Returns;
        -------;
        BitemporalItem namedtuple which contains a .data and .metadata element;
*/;

auto read(ref BitemporalStore store, symbol, asOf=None, raw=False, **kwargs):
{
        //TODO: shall we block fromVersion from getting into super.read?;
        
        auto item = store.read(symbol,kwargs);
        auto lastUpdated=max(item.data.index.getLevelValues(store.observeColumn));
        if(raw)
        {
            return BitemporalItem(symbol,store.arcticLibGetName,item.data,item.metadata,lastUpdated);
        }
        else;
        {
            auto indexNames=item.data.index.names;
            indexNames.remove(store.observeColumn);
            return BitemporalItem(symbol,store.store.arcticLib.getName,
                            item.data.groupByAsOf(asOf,indexNames,asOfCol=store.observeColumn,
                            item.metadata,lastUpdated);
        }
}
/**;
        Append 'data' under the specified 'symbol' name to this library.;

        Parameters;
        ----------;
        symbol : `str`;
            symbol name for the item;
        data : `pd.DataFrame`;
            to be persisted;
        metadata : `dict`;
            An optional dictionary of metadata to persist along with the symbol. If None and there are existing;
            metadata, current metadata will be maintained;
        upsert : `bool`;
            Write 'data' if no previous version exists.;
        asOf : `datetime.datetime`;
            The "insert time". Default to datetime.now();
*/;

auto update(ref BiTemporalstore store, string symbol, Data data, Metadata metadata=null, bool upsert=true,
                asOf=null, string[string]kwargs);
{
        auto localTimeZone=marketTimezone(); // mktz ??
        if (asOf is null)
                    asOf=Clock.currTime();
        data = store.addObserveDTIndex(data,asOf);
        DF df;
        if (upsert && ! store.hasSymbol(symbol))
            df = data;
        else;
        {
            auto existingItem = store.read(symbol,kwargs);
            if (metadata is null)
                metadata = existingItem.metadata;
            df = existingItem.data.append(data).sortIndex();
        }
        this.store.write(symbol, df, metadata, true);
}

auto write(ref BiTemporalStore store, string[string] args, string[string] kwargs)
{
        // TODO: may be diff + append?;
        throw NotImplementedException("Direct write for BitemporalStore is not supported. Use append instead';
                          'to add / modify timeseries.");
}

auto addObserveDTIndex(ref BiTemporalStore store, DF df, AsOf asOf)
{
        auto indexNames=df.indexNames;
        indexNames.append(store.observeColumn);
        auto index = [x + (asOf,) if df.index.nlevels > 1 else (x, asOf) for x in df.index.tolist()];
        auto df = df.setIndex(pd.MultiIndex.fromTuples(index, names=indexNames), inplace=False);
        return df;
}