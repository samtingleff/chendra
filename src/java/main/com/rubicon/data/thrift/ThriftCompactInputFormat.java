package com.rubicon.data.thrift;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.TBase;
import org.apache.thrift.transport.TTransportException;

/**
 * InputFormat for thrift objects.
 * 
 * @author stingleff
 * 
 * @param <K>
 * @param <V>
 */
public class ThriftCompactInputFormat<K extends TBase, V extends TBase> extends
		FileInputFormat<K, V> {
	private Class<K> keyClass;

	private Class<V> valueClass;

	public static void setKeyClass(Configuration conf,
			Class<? extends TBase> cls) {
		conf.set(Constants.KEY_CLASS, cls.getCanonicalName());
	}

	public static void setValueClass(Configuration conf,
			Class<? extends TBase> cls) {
		conf.set(Constants.VALUE_CLASS, cls.getCanonicalName());
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		return new ThriftRecordReader<K, V>(conf, (FileSplit) split,
				getKeyClass(conf), getValueClass(conf));

	}

	protected Class<K> getKeyClass(Configuration conf) {
		Class<K> cls = null;
		if (keyClass != null)
			cls = keyClass;
		else {
			cls = (Class<K>) ReflectionHelper.getClassFromConfiguration(conf,
					Constants.KEY_CLASS);
			keyClass = cls;
		}
		return cls;
	}

	protected Class<V> getValueClass(Configuration conf) {
		Class<V> cls = null;
		if (valueClass != null)
			cls = valueClass;
		else {
			cls = (Class<V>) ReflectionHelper.getClassFromConfiguration(conf,
					Constants.VALUE_CLASS);
			valueClass = cls;
		}
		return cls;
	}

	protected static class ThriftRecordReader<K extends TBase, V extends TBase>
			extends RecordReader<K, V> {
		private CompressionCodecFactory compressionCodecs = null;

		private FileSplit split;

		private Class<K> keyClass;

		private Class<V> valueClass;

		private FSDataInputStream in;

		private InputStream inputStream;

		private ThriftCompactDeserializer<K> keyDeserializer;

		private ThriftCompactDeserializer<V> valueDeserializer;

		private K key;

		private V value;

		private long start;

		private long pos;

		private long end;

		public ThriftRecordReader(Configuration job, FileSplit split,
				Class<K> keyClass, Class<V> valueClass) throws IOException {
			this.split = split;
			this.keyClass = keyClass;
			this.valueClass = valueClass;
			this.keyDeserializer = new ThriftCompactDeserializer<K>(keyClass);
			this.valueDeserializer = new ThriftCompactDeserializer<V>(
					valueClass);
		}

		@Override
		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException {
			Path path = split.getPath();
			compressionCodecs = new CompressionCodecFactory(context
					.getConfiguration());
			CompressionCodec codec = compressionCodecs.getCodec(path);
			FileSystem fs = path.getFileSystem(context.getConfiguration());

			this.in = fs.open(path);

			if (codec != null) {
				this.inputStream = codec.createInputStream(this.in);
			} else {
				this.inputStream = this.in;
			}

			start = split.getStart();
			end = start + split.getLength();
			pos = start;
			this.keyDeserializer.open(this.inputStream);
			this.valueDeserializer.open(this.inputStream);
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
				key = createKey();
				value = createValue();
				keyDeserializer.deserialize(key);
				valueDeserializer.deserialize(value);
				this.pos = this.in.getPos();
				result = true;
			} catch (TTransportException e) {
				if (e.getType() != TTransportException.END_OF_FILE) {
					throw new IOException(e);
				}
				e.printStackTrace();
			} catch (EOFException e) {
				e.printStackTrace();
			} catch (IOException e) {
				Throwable cause = e.getCause();
				if ((cause != null) && (cause instanceof TTransportException)) {
					TTransportException tt = (TTransportException) cause;
					if (tt.getType() != TTransportException.END_OF_FILE)
						throw e;
				} else
					throw e;
			}
			return result;
		}

		@Override
		public K getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public V getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		public synchronized void close() throws IOException {
			this.keyDeserializer.close();
			this.valueDeserializer.close();
			this.in.close();
		}

		private K createKey() {
			try {
				return keyClass.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
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
