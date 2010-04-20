package com.rubicon.data.thrift;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
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
	public RecordWriter<K, V> getRecordWriter(FileSystem fs, JobConf job,
			String name, Progressable progress) throws IOException {
		Path parent = super.getOutputPath(job);
		Path path = new Path(parent, name);
		FSDataOutputStream out = fs.create(path);
		return new ThriftRecordWriter(out);
	}

	private static class ThriftRecordWriter<K extends TBase, V extends TBase>
			implements RecordWriter<K, V> {

		private ThriftCompactSerializer<K> keySerializer = new ThriftCompactSerializer<K>();

		private ThriftCompactSerializer<V> valueSerializer = new ThriftCompactSerializer<V>();

		private FSDataOutputStream out;

		private ThriftRecordWriter(FSDataOutputStream out) throws IOException {
			this.out = out;
			this.keySerializer.open(out);
			this.valueSerializer.open(out);
		}

		public void close(Reporter reporter) throws IOException {
			this.keySerializer.close();
			this.valueSerializer.close();
			this.out.close();
		}

		public void write(K key, V value) throws IOException {
			this.keySerializer.serialize(key);
			this.valueSerializer.serialize(value);
		}

	}
}
