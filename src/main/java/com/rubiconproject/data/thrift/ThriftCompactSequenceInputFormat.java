package com.rubiconproject.data.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.thrift.TBase;

import com.rubiconproject.data.thrift.types.TLong;
import com.rubiconproject.data.thrift.types.TNull;
import com.rubiconproject.data.thrift.types.TString;

public class ThriftCompactSequenceInputFormat<K extends TBase, V extends TBase>
		extends FileInputFormat<K, V> {
	private static final PathFilter dataFileFilter = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			boolean v = (!name.startsWith("_") && !name.startsWith(".") && !name
					.endsWith(".index"));
			return v;
		}
	};

	@Override
	public boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	public List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();
		Path[] dirs = getInputPaths(job);
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		List<IOException> errors = new ArrayList<IOException>();

		for (int i = 0; i < dirs.length; ++i) {
			Path p = dirs[i];
			FileSystem fs = p.getFileSystem(job.getConfiguration());
			FileStatus[] matches = fs.globStatus(p, dataFileFilter);
			if (matches == null) {
				errors.add(new IOException("Input path does not exist: " + p));
			} else if (matches.length == 0) {
				errors.add(new IOException("Input Pattern " + p
						+ " matches 0 files"));
			} else {
				for (FileStatus globStat : matches) {
					if (globStat.isDir()) {
						for (FileStatus stat : fs.listStatus(
								globStat.getPath(), dataFileFilter)) {
							result.add(stat);
						}
					} else {
						result.add(globStat);
					}
				}
			}
		}

		if (!errors.isEmpty()) {
			throw new InvalidInputException(errors);
		}
		return result;
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new BlobRecordReader();
	}

	private static class BlobRecordReader<K extends TBase, V extends TBase>
			extends RecordReader<K, V> {

		private FSDataInputStream index;

		private ThriftCompactDeserializer indexDeserializer = new ThriftCompactDeserializer();

		private FSDataInputStream data;

		private ThriftCompactDeserializer dataDeserializer = new ThriftCompactDeserializer();

		private KeyPackage currentKey;

		private KeyPackage nextKey;

		private V value = null;

		private long start;

		private long pos;

		private long end;

		public BlobRecordReader() throws IOException {
		}

		@Override
		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			this.start = split.getStart();
			this.end = start + split.getLength();
			final Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);
			this.data = fs.open(split.getPath());
			this.index = fs.open(new Path(String.format("%1$s/%2$s.index",
					split.getPath().getParent().toString(), split.getPath()
							.getName())));

			this.dataDeserializer.open(this.data);
			this.indexDeserializer.open(this.index);

			this.pos = start;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (this.currentKey == null) {
				this.currentKey = readKeyPackage(this.indexDeserializer);
			}
			boolean result = hasValues(this.currentKey);
			if (result) {
				if (this.nextKey == null)
					this.nextKey = readKeyPackage(indexDeserializer);

				// am i at end of line for this key series?
				if (this.data.getPos() >= this.nextKey.getPos()) {
					this.currentKey = this.nextKey;
					result = hasValues(this.currentKey);
					if (result)
						this.nextKey = readKeyPackage(indexDeserializer);
				}
				if (result)
					this.value = readValue(this.dataDeserializer,
							this.currentKey.getValuesClass());
			}
			return result;
		}

		@Override
		public K getCurrentKey() throws IOException, InterruptedException {
			return (K) this.currentKey.getKey();
		}

		@Override
		public V getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		@Override
		public void close() throws IOException {
			this.indexDeserializer.close();
			this.index.close();
			this.dataDeserializer.close();
			this.data.close();
		}

		private boolean hasValues(KeyPackage kp) {
			return ((kp.getKeyClass().equals(TNull.class))
					&& (kp.getValuesClass().equals(TNull.class)) && (kp
					.getPos() == 0l)) ? false : true;
		}

		private KeyPackage readKeyPackage(ThriftCompactDeserializer deserializer)
				throws IOException {
			try {
				TString keyClassName = (TString) deserializer
						.deserialize(new TString());
				TString valueClassName = (TString) deserializer
						.deserialize(new TString());
				Class<? extends K> keyClass = (Class<? extends K>) Class
						.forName(keyClassName.getValue());
				Class<? extends V> valueClass = (Class<? extends V>) Class
						.forName(valueClassName.getValue());
				K k = (K) deserializer.deserialize(keyClass.newInstance());
				TLong pos = (TLong) deserializer.deserialize(new TLong());
				return new KeyPackage(keyClass, valueClass, k, pos.getValue());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		private V readValue(ThriftCompactDeserializer data,
				Class<? extends V> cls) {
			try {
				return (V) data.deserialize(cls.newInstance());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		private static class KeyPackage {
			private Class keyClass;

			private Class valuesClass;

			private Object key;

			private long pos;

			public KeyPackage(Class keyClass, Class valuesClass, Object key,
					long pos) {
				this.keyClass = keyClass;
				this.valuesClass = valuesClass;
				this.key = key;
				this.pos = pos;
			}

			public Class getKeyClass() {
				return keyClass;
			}

			public Class getValuesClass() {
				return valuesClass;
			}

			public Object getKey() {
				return key;
			}

			public long getPos() {
				return pos;
			}

			public String toString() {
				return key.toString() + ":" + getPos();
			}
		}
	}
}
