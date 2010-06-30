package com.rubicon.data.thrift;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.thrift.TBase;

import com.rubicon.data.thrift.types.TNull;

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

	@Override
	public RecordWriter<TNull, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Path path = super.getDefaultWorkFile(context, ".data");
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream out = fs.create(path, false);
		return new ThriftRecordWriterNullKey<V>(out);
	}

	private static class ThriftRecordWriterNullKey<V extends TBase> extends
			RecordWriter<TNull, V> {
		private ThriftCompactSerializer<V> serializer = new ThriftCompactSerializer<V>();

		private FSDataOutputStream out;

		private ThriftRecordWriterNullKey(FSDataOutputStream out)
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
}
