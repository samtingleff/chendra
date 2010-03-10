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
public class ThriftOutputFormat<K extends TBase, V extends TBase> extends
		FileOutputFormat<K, V> {

	private static Class keyClass;

	private static Class valueClass;

	public static void setKeyClass(Class cls) {
		keyClass = cls;
	}

	public static void setValueClass(Class cls) {
		valueClass = cls;
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(FileSystem fs, JobConf job,
			String name, Progressable progress) throws IOException {
		Path parent = super.getOutputPath(job);
		Path path = new Path(parent, name);
		FSDataOutputStream out = fs.create(path);
		return new ThriftRecordWriter(out, keyClass, valueClass);
	}

	private static class ThriftRecordWriter<K extends TBase, V extends TBase>
			implements RecordWriter<K, V> {
		private FSDataOutputStream out;

		private ThriftCompactSerializer<K> keySerializer;

		private ThriftCompactSerializer<V> valueSerializer;

		private ThriftRecordWriter(FSDataOutputStream out, Class<K> keyClass,
				Class<V> valueClass) throws IOException {
			this.out = out;
			this.keySerializer = new ThriftCompactSerializer<K>(keyClass);
			this.valueSerializer = new ThriftCompactSerializer<V>(valueClass);
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
