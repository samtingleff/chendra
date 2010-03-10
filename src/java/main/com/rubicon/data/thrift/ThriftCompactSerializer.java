package com.rubicon.data.thrift;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;
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
public class ThriftCompactSerializer<T extends TBase> implements Serializer<T> {

	private Class<T> cls;

	private OutputStream out;

	private TTransport transport;

	private TProtocol proto;

	public ThriftCompactSerializer(Class<T> cls) {
		this.cls = cls;
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
