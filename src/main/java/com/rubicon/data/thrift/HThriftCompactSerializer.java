package com.rubicon.data.thrift;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;
import org.apache.thrift.TBase;

public class HThriftCompactSerializer<T extends TBase> implements
		Serializer<T>, Closeable {
	private ThriftCompactSerializer<T> delegate = new ThriftCompactSerializer<T>();

	public void open(OutputStream out) throws IOException {
		delegate.open(out);
	}

	public void close() throws IOException {
		delegate.close();
	}

	public void serialize(T obj) throws IOException {
		delegate.serialize(obj);
	}

}
