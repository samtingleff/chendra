package com.rubicon.data.thrift;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TBase;

public class ThriftMultipleCompactOutputFormat<K extends TBase, V extends TBase>
		extends MultipleOutputFormat<K, V> {
	private ThriftCompactOutputFormat<K, V> output;

	@Override
	protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs,
			JobConf job, String name, Progressable progress) throws IOException {
		if (output == null) {
			output = new ThriftCompactOutputFormat<K, V>();
		}
		return output.getRecordWriter(fs, job, name, progress);
	}

}
