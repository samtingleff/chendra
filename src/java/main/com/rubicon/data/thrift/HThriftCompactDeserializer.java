package com.rubicon.data.thrift;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.thrift.TBase;

public class HThriftCompactDeserializer<T extends TBase> implements
		Deserializer<T>, Closeable {
	private ThriftCompactDeserializer<T> delegate;

	public HThriftCompactDeserializer(Class<T> cls) {
		delegate = new ThriftCompactDeserializer<T>(cls);
	}

	public void open(InputStream in) throws IOException {
		delegate.open(in);
	}

	public T deserialize(T obj) throws IOException {
		return delegate.deserialize(obj);
	}

	public void close() throws IOException {
		delegate.close();
	}

}
