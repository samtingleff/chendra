package com.rubicon.data.thrift;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.thrift.TBase;

/**
 * OutputFormat for thrift objects.
 * 
 * @author stingleff
 * 
 * @param <K>
 * @param <V>
 */
public class ThriftCompactOutputFormat<K extends TBase, V extends TBase>
		extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Path path = getDefaultWorkFile(context, ".data");
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream out = fs.create(path, false);
		return new ThriftRecordWriter<K, V>(out);
	}

	private static class ThriftRecordWriter<K extends TBase, V extends TBase>
			extends RecordWriter<K, V> {

		private ThriftCompactSerializer<K> keySerializer = new ThriftCompactSerializer<K>();

		private ThriftCompactSerializer<V> valueSerializer = new ThriftCompactSerializer<V>();

		private FSDataOutputStream out;

		private ThriftRecordWriter(FSDataOutputStream out) throws IOException {
			this.out = out;
			this.keySerializer.open(out);
			this.valueSerializer.open(out);
		}

		public void write(K key, V value) throws IOException {
			this.keySerializer.serialize(key);
			this.valueSerializer.serialize(value);
		}

		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			this.keySerializer.close();
			this.valueSerializer.close();
			this.out.close();
		}

	}
}
