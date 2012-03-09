package com.rubiconproject.data.thrift;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;

import com.rubiconproject.data.thrift.types.TNull;

public class ThriftCompactRecordWriterNullKey<V extends TBase> extends
		RecordWriter<TNull, V> {
	private ThriftCompactSerializer<V> serializer = new ThriftCompactSerializer<V>();

	private FSDataOutputStream out;

	public ThriftCompactRecordWriterNullKey(FSDataOutputStream out)
			throws IOException {
		this.out = out;
		this.serializer.open(out);
	}

	public void write(TNull key, V value) throws IOException {
		this.serializer.serialize(value);
	}

	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		this.serializer.close();
		this.out.close();
	}

}