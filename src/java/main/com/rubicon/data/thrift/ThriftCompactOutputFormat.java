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
	private static final String KEY_CLASS_CONF = "rp.mapred.thrift.compact.outputformat.keyclass";

	private static final String VALUE_CLASS_CONF = "rp.mapred.thrift.compact.outputformat.valueclass";

	private Class<K> keyClass;

	private Class<V> valueClass;

	public static void setKeyClass(JobConf conf, Class<? extends TBase> cls) {
		conf.set(KEY_CLASS_CONF, cls.getCanonicalName());
	}

	public static void setValueClass(JobConf conf, Class<? extends TBase> cls) {
		conf.set(VALUE_CLASS_CONF, cls.getCanonicalName());
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(FileSystem fs, JobConf job,
			String name, Progressable progress) throws IOException {
		Path parent = super.getOutputPath(job);
		Path path = new Path(parent, name);
		FSDataOutputStream out = fs.create(path);
		return new ThriftRecordWriter(out, getKeyClass(job), getValueClass(job));
	}

	private Class<K> getKeyClass(JobConf conf) {
		Class<K> cls = null;
		if (keyClass != null)
			cls = keyClass;
		else {
			cls = (Class<K>) getClassFromJobConf(conf, KEY_CLASS_CONF);
			keyClass = cls;
		}
		return cls;
	}

	private Class<V> getValueClass(JobConf conf) {
		Class<V> cls = null;
		if (valueClass != null)
			cls = valueClass;
		else {
			cls = (Class<V>) getClassFromJobConf(conf, VALUE_CLASS_CONF);
			valueClass = cls;
		}
		return cls;
	}

	private Class<? extends TBase> getClassFromJobConf(JobConf conf,
			String param) {
		try {
			return (Class<? extends TBase>) Class.forName(conf.get(param));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
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
