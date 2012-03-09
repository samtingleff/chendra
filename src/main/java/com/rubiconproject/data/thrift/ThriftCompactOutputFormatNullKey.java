package com.rubiconproject.data.thrift;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.thrift.TBase;

import com.rubiconproject.data.thrift.types.TNull;

/**
 * OutputFormat for thrift objects with null keys.
 * 
 * @author stingleff
 * 
 * @param <K>
 * @param <V>
 */
public class ThriftCompactOutputFormatNullKey<V extends TBase> extends
		FileOutputFormat<TNull, V> {

	public FSDataOutputStream getOutputStream(TaskAttemptContext context)
			throws IOException {
		Path path = getDefaultWorkFile(context, ".data");
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream out = fs.create(path, false);
		return out;
	}

	@Override
	public RecordWriter<TNull, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new ThriftCompactRecordWriterNullKey<V>(getOutputStream(context));
	}
}
