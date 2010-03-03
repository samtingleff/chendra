package com.rubicon.data.util.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ObjectSerializer {

	public static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.close();
		baos.close();
		return baos.toByteArray();
	}

	public static <V> V deserialize(byte[] bytes) throws IOException {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bais);
			V v = (V) ois.readObject();
			ois.close();
			bais.close();
			return v;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
