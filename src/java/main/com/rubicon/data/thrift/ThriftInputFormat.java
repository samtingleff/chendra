package com.rubicon.data.thrift;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
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
public class ThriftInputFormat<K extends TBase, V extends TBase> extends
		FileInputFormat<K, V> {

	private static Class keyClass;

	private static Class valueClass;

	public static void setKeyClass(Class cls) {
		keyClass = cls;
	}

	public static void setValueClass(Class cls) {
		valueClass = cls;
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		reporter.setStatus(split.toString());
		return new ThriftRecordReader(job, (FileSplit) split, keyClass,
				valueClass);
	}

	private static class ThriftRecordReader<K extends TBase, V extends TBase>
			implements RecordReader<K, V> {
		private FileSplit split;

		private Class<K> keyClass;

		private Class<V> valueClass;

		private FSDataInputStream in;

		private ThriftDeserializer<K> keyDeserializer;

		private ThriftDeserializer<V> valueDeserializer;

		private boolean hasMore = true;

		private long start;

		private long pos;

		private long end;

		public ThriftRecordReader(Configuration job, FileSplit split,
				Class<K> keyClass, Class<V> valueClass) throws IOException {
			this.split = split;
			this.keyClass = keyClass;
			this.valueClass = valueClass;
			this.keyDeserializer = new ThriftDeserializer<K>(keyClass);
			this.valueDeserializer = new ThriftDeserializer<V>(valueClass);
			init(job);
		}

		private void init(Configuration job) throws IOException {
			Path path = split.getPath();
			FileSystem fs = path.getFileSystem(job);
			this.in = fs.open(path);
			start = split.getStart();
			end = start + split.getLength();
			pos = start;
			this.keyDeserializer.open(in);
			this.valueDeserializer.open(in);
			hasMore = true;
		}

		public K createKey() {
			try {
				return keyClass.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		public V createValue() {
			try {
				return valueClass.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
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

		public synchronized boolean next(K key, V value) throws IOException {
			if (!hasMore)
				return false;
			try {
				byte begin = in.readByte();
			} catch (EOFException e) {
				return false;
			}
			keyDeserializer.deserialize(key);
			valueDeserializer.deserialize(value);
			byte end = in.readByte();
			this.pos = in.getPos();
			if (end == ThriftFormatMagicBytes.END_FILE)
				hasMore = false;
			return true;
		}

		public synchronized void close() throws IOException {
			this.keyDeserializer.close();
			this.valueDeserializer.close();
			this.in.close();
		}

	}
}
