package com.rubicon.data.thrift;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.TBase;

import com.rubicon.data.thrift.types.TNull;

/**
 * InputFormat for thrift objects with null keys.
 * 
 * @author stingleff
 * 
 * @param <K>
 * @param <V>
 */
public class ThriftCompactInputFormatNullKey<V extends TBase> extends
		FileInputFormat<TNull, V> {
	public static final String VALUE_CLASS_CONF = "rp.mapred.thrift.compact.inputformat.valueclass";

	private Class<V> valueClass;

	public static void setValueClass(Configuration conf,
			Class<? extends TBase> cls) {
		conf.set(VALUE_CLASS_CONF, cls.getCanonicalName());
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<TNull, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		return new ThriftRecordReaderNullKey<V>(conf, (FileSplit) split,
				getValueClass(conf));
	}

	private Class<V> getValueClass(Configuration conf) {
		Class<V> cls = null;
		if (valueClass != null)
			cls = valueClass;
		else {
			cls = (Class<V>) getClassFromConfiguration(conf, VALUE_CLASS_CONF);
			valueClass = cls;
		}
		return cls;
	}

	private Class<? extends TBase> getClassFromConfiguration(
			Configuration conf, String param) {
		try {
			return (Class<? extends TBase>) Class.forName(conf.get(param));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	private static class ThriftRecordReaderNullKey<V extends TBase> extends
			RecordReader<TNull, V> {
		private FileSplit split;

		private Class<V> valueClass;

		private FSDataInputStream in;

		private ThriftCompactDeserializer<V> valueDeserializer;

		private V value;

		private long start;

		private long pos;

		private long end;

		public ThriftRecordReaderNullKey(Configuration job, FileSplit split,
				Class<V> valueClass) throws IOException {
			this.split = split;
			this.valueClass = valueClass;
			this.valueDeserializer = new ThriftCompactDeserializer<V>(
					valueClass);
		}

		@Override
		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException {
			Path path = split.getPath();
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			this.in = fs.open(path);
			start = split.getStart();
			end = start + split.getLength();
			pos = start;
			this.valueDeserializer.open(in);
		}

		public synchronized long getPos() throws IOException {
			return pos;
		}

		public float getProgress() throws IOException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean result = false;
			try {
				if (getPos() < end) {
					value = createValue();
					valueDeserializer.deserialize(value);
					this.pos = in.getPos();
					result = true;
				}
			} catch (EOFException e) {
			}
			return result;
		}

		@Override
		public TNull getCurrentKey() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public V getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		public synchronized void close() throws IOException {
			this.valueDeserializer.close();
			this.in.close();
		}

		private V createValue() {
			try {
				return valueClass.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}
}
