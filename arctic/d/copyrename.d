import std.file;
import std.stdio;
import std.path;

void main(string[] args)
{
	foreach(file;dirEntries(".","*.py",SpanMode.shallow))
	{
		if (!file.isFile)
			continue;
		auto dFile=file.name.stripExtension~".d";
		if (dFile.exists)
		{
			writefln("skipping %s as it already exists",dFile);
			continue;
		}
		writefln("copying %s to %s",file.name,dFile);
		copy(file.name,dFile);
	}
}
