alias VersionedItem=Tuple!(string,"symbol",Library,"library",Data,"data",
                string,"version",Metadata,"metadata");

// Class representing a Versioned object in VersionStore.;

auto metadataDict(VersionedItem item)
{
        string[string] ret;
        ret["symbol"]=item.symbol;
        ret["library"]=item.library;
        ret["version"]=item.version;
        return ret;
}

string toString(VersionedItem item)
{
        return format(
                "VersionedItem(symbol=%s,library=%s,data=%s,version=%s,metadata=%s",
            (item.symbol, item.library, item(item.data.type), item.version, item.metadata));
}

alias ChangedItem=Tuple!(string,"symbol",string,"originalVersion",string,"newVersion",
                string,"changes");
