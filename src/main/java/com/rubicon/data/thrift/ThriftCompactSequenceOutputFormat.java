package com.rubicon.data.thrift;

import java.io.DataOutputStream;
import java.io.IOException;

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
		return new BlobRecordWriter<K, V>(indexOut, fileOut);
	}

	private static class BlobRecordWriter<K extends TBase, V extends TBase>
			extends RecordWriter<K, V> {

		private FSDataOutputStream indexFS;

		private DataOutputStream indexOut;

		private ThriftCompactSerializer indexSerializer = new ThriftCompactSerializer();

		private FSDataOutputStream dataFS;

		private DataOutputStream dataOut;

		private ThriftCompactSerializer dataSerializer = new ThriftCompactSerializer();

		private K lastKey = null;

		public BlobRecordWriter(FSDataOutputStream indexFS,
				FSDataOutputStream dataFS) throws IOException {
			this.indexFS = indexFS;
			this.indexOut = new DataOutputStream(indexFS);
			this.dataFS = dataFS;
			this.dataOut = new DataOutputStream(dataFS);
			indexSerializer.open(indexOut);
			dataSerializer.open(dataOut);
			indexBegin();
		}

		@Override
		public void write(K k, V v) throws IOException, InterruptedException {
			if (lastKey == null)
				indexKeyBegin(k, v);
			else if (!lastKey.equals(k)) {
				indexKeyEnd();
				indexKeyBegin(k, v);
			}

			dataSerializer.serialize(v);

			lastKey = k;
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			indexKeyEnd();
			indexEnd();
			dataSerializer.close();
			dataOut.close();
			dataFS.close();
			indexSerializer.close();
			indexOut.close();
			indexFS.close();
		}

		private void indexBegin() throws IOException {
		}

		private void indexEnd() throws IOException {
			indexWriteRecord(TNull.class, TNull.class, new TNull(), 0l);
		}

		private void indexKeyBegin(K k, V v) throws IOException {
			indexWriteRecord(k.getClass(), v.getClass(), k, dataFS.getPos());
		}

		private void indexKeyEnd() throws IOException {
		}

		private <Z extends TBase> void indexWriteRecord(Class keyClass,
				Class valueClass, Z k, long pos) throws IOException {
			indexSerializer.serialize(new TString(keyClass.getCanonicalName()));
			indexSerializer
					.serialize(new TString(valueClass.getCanonicalName()));
			indexSerializer.serialize(k);
			indexSerializer.serialize(new TLong(pos));
		}
	}
}
