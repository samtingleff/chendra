package com.rubiconproject.data.thrift;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

/**
 * Serializer for thrift objects.
 * 
 * @author stingleff
 * 
 * @param <T>
 */
public class ThriftCompactSerializer<T extends TBase> implements Closeable {

	private OutputStream out;

	private TTransport transport;

	private TProtocol proto;

	public ThriftCompactSerializer() {
	}

	public void open(OutputStream out) throws IOException {
		this.out = out;
		this.transport = new TIOStreamTransport(out);
		this.proto = new TCompactProtocol(transport);
	}

	public void close() throws IOException {
		this.transport.close();
		this.transport = null;
		this.proto = null;
	}

	public void serialize(T obj) throws IOException {
		try {
			obj.write(proto);
		} catch (TException e) {
			throw new IOException(e);
		}
	}
}
