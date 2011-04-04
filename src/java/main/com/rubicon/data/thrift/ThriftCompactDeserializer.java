package com.rubicon.data.thrift;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Deserializer for thrift objects.
 * 
 * @author stingleff
 * 
 * @param <T>
 */
public class ThriftCompactDeserializer<T extends TBase> implements Closeable {

	private Class<T> cls;

	private InputStream in;

	private TTransport transport;

	private TProtocol proto;

	public ThriftCompactDeserializer(Class<T> cls, InputStream in)
			throws IOException {
		this.cls = cls;
		open(in);
	}

	public ThriftCompactDeserializer(Class<T> cls) {
		this.cls = cls;
	}

	public ThriftCompactDeserializer() {
	}

	public void open(InputStream in) throws IOException {
		this.in = in;
		this.transport = new TIOStreamTransport(in);
		this.proto = new TCompactProtocol(transport);
	}

	public T deserialize(T obj) throws IOException, TTransportException {
		try {
			if (obj == null) {
				obj = cls.newInstance();
			}
			obj.read(this.proto);
			return obj;
		} catch (TException e) {
			throw new IOException(e);
		} catch (InstantiationException e) {
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			throw new IOException(e);
		} finally {
		}
	}

	public void close() throws IOException {
		this.transport.close();
		this.transport = null;
		this.proto = null;
	}
}
