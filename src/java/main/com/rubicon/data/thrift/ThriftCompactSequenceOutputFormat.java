package com.rubicon.data.thrift;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.thrift.TBase;

import com.rubicon.data.thrift.types.TLong;
import com.rubicon.data.thrift.types.TNull;
import com.rubicon.data.thrift.types.TString;

public class ThriftCompactSequenceOutputFormat<K extends TBase, V extends TBase>
		extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		Path file = getDefaultWorkFile(job, ".data");
		Path indexFile = new Path(String.format("%1$s/%2$s.index", file
				.getParent().toString(), file.getName()));
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream indexOut = fs.create(indexFile, false);
		FSDataOutputStream fileOut = fs.create(file, false);
		return new BlobRecordWriter<K, V>(new DataOutputStream(indexOut),
				fileOut);
	}

	private static class BlobRecordWriter<K extends TBase, V extends TBase>
			extends RecordWriter<K, V> {

		private OutputStream index;

		private ThriftCompactSerializer indexSerializer = new ThriftCompactSerializer();

		private OutputStream data;

		private ThriftCompactSerializer dataSerializer = new ThriftCompactSerializer();

		private long lastKeyCount = 0;

		private K lastKey = null;

		public BlobRecordWriter(OutputStream index, OutputStream data)
				throws IOException {
			this.index = index;
			this.data = data;
			indexSerializer.open(index);
			dataSerializer.open(data);
			indexBegin();
		}

		@Override
		public void write(K k, V v) throws IOException, InterruptedException {
			if (lastKey == null)
				indexKeyBegin(k, v);
			else if (!lastKey.equals(k)) {
				indexKeyEnd();
				indexKeyBegin(k, v);
				lastKeyCount = 0;
			}

			dataSerializer.serialize(v);

			lastKey = k;
			++lastKeyCount;
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			indexKeyEnd();
			indexEnd();
			dataSerializer.close();
			data.close();
			indexSerializer.close();
			index.close();
		}

		private void indexBegin() throws IOException {
		}

		private void indexEnd() throws IOException {
			indexSerializer.serialize(new TString(TNull.class
					.getCanonicalName()));
			indexSerializer.serialize(new TString(TNull.class
					.getCanonicalName()));
			indexSerializer.serialize(new TNull());
			indexSerializer.serialize(new TLong(0l));
		}

		private void indexKeyBegin(K k, V v) throws IOException {
			indexSerializer.serialize(new TString(k.getClass()
					.getCanonicalName()));
			indexSerializer.serialize(new TString(v.getClass()
					.getCanonicalName()));
			indexSerializer.serialize(k);
		}

		private void indexKeyEnd() throws IOException {
			indexSerializer.serialize(new TLong(lastKeyCount));
		}
	}
}
