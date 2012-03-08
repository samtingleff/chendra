package com.rubicon.data.thrift;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.thrift.TBase;

/**
 * Serialization implementation for thrift objects.
 * 
 * @author stingleff
 * 
 * @param <T>
 */
public class ThriftCompactSerialization<T extends TBase> implements
		Serialization<T> {

	public boolean accept(Class<?> cls) {
		Class[] interfaces = cls.getInterfaces();
		for (Class c : interfaces) {
			if (c.equals(TBase.class))
				return true;
		}
		return false;
	}

	public Deserializer<T> getDeserializer(Class<T> cls) {
		return new HThriftCompactDeserializer<T>(cls);
	}

	public Serializer<T> getSerializer(Class<T> cls) {
		return new HThriftCompactSerializer<T>();
	}

}
