import std.exception;
import std.file;
import std.path;
import std.string;
import std.array:appender,array;
static import std.ascii;
import std.ascii:isWhite;
import std.stdio;
import std.algorithm;
import std.range;
import std.conv:to;

void main(string[] args)
{
	foreach(file;dirEntries(".","*.d",SpanMode.shallow))
	{
		if (!file.isFile)
			continue;
		auto text=cast(string) std.file.read(file.name);
		auto buf=appender!(string[]);
		foreach(line;text.splitLines)
		{
			size_t j,indent;
			while(j<line.length && line[j].isWhite)
			{
				indent+=(line[j]=='\t')?8:1;
				++j;
			}
			auto tokens=line.strip.split(' ');
			foreach(ref token;tokens)
			{
				auto i=token.indexOf("_");
				while(i!=-1)	
				{
					if (i<=0 || i==token.length-1)
						break;
					token=token[0..i]~std.ascii.toUpper(token[i+1])~
						token[i+2..$];
					i=token.indexOf("_");
				}
			}
			line=' '.repeat(indent).array.to!string ~ tokens.join(' ');
			if (line.strip.startsWith("#"))
			{
				auto p=line.indexOf("#");
				auto stub=(p<=0)?"":line[0..p];
				auto butt = (p==line.length-1)?"":line[p+1..$];
				line=stub~"//"~butt;
			}
			line=line.replace("(object)","");
			line=line.replace("class","struct");
			if (line.startsWith("struct") && (line.endsWith(":")))
				line=line[0..$-1]~"\n{\n";
			line=line.replace("def","auto");
			line=line.replace("self","this");
			line=line.fixFor();
			line=line.fixIf();
			if (line.needsSemicolon)
				line~=";";
			buf.put(line);
		}
		std.file.write(file.name~".done",buf.data.join("\n"));
	}
}

bool needsSemicolon(string line)
{
	line=line.strip;
	return (line.length>0 && !line.startsWith("def") && !line.startsWith("{") &&
			!line.startsWith("}") && !line.endsWith(";") &&
			!line.startsWith("auto") && !line.endsWith("&&") &&
			!line.endsWith("||") && (!line.endsWith("("))
			&& !line.endsWith(`"""`) && (!line.endsWith(","))
			&& !line.startsWith("if") && (!line.startsWith("#")) &&
			!line.startsWith("bool") && !line.startsWith("double") &&
			!line.startsWith("int") && (!line.startsWith("string")) &&
			!line.startsWith("struct") && (!line.startsWith("class")));
}

string fixFor(string line)
{
	auto i=line.indexOf("for ");
	if (i==-1)
		return line;
	try
	{
		auto stub=(i<=0)?"":line[0..i];
		auto j=line[i..$].indexOf(" in ");
		auto butt=line[j+" in ".length .. line[i+j..$].indexOf(":")];
		auto vars=line[i+"for ".length..j].strip.split(",");
		line=stub~"foreach("~vars[0..$].join(",")~";"~butt~")"~"\n"~"{";
	}
	catch(Error e)
	{
	}
	return line;
}

string fixIf(string line)
{
	auto i=line.indexOf("if");
	if (i==-1 || !line.strip.endsWith(":"))
		return line;
	line=line[0..i]~" if ("~line[0..$-1]~")\n{";
	return line;
}
