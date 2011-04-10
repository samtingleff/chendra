# Chendra #

## Introduction ##

Chendra is a lib for using [Thrift](http://thrift.apache.org/ "Thrift") objects as [Hadoop](http://hadoop.apache.org/ "Hadoop") key and value types. It is currently compatible with the 0.20 API.

To use, simply (1) generate thrift classes with the "hashcode" flag; (2) for use as keys set the mapred.output.key.comparator.class property; (3) set the io.serializations property; and (4) tell the input format what your key and value types are:

		conf.set("mapred.output.key.comparator.class", ThriftRawComparator.class.getName());
		conf.setStrings("io.serializations", new String[] {
				"org.apache.hadoop.io.serializer.WritableSerialization",
				ThriftCompactSerialization.class.getName() });

		ThriftCompactInputFormat.setKeyClass(conf, TInteger.class);
		ThriftCompactInputFormat.setValueClass(conf, TTweet.class);

We include simple types for: boolean, byte, double, integer, long, short, string and several list and map formulations of these.

The name comes from a seventeen-year-old Borneo elephant at the Oregon Zoo, the only Borneo elephant currently in the US. The Borneo elephant is thought by some to come from [Sulu](http://en.wikipedia.org/wiki/Sulu "Island of Sulu"), where it was imported form the island of [Java](http://en.wikipedia.org/wiki/Java "Island of Java"). They are a critically endangered species.
