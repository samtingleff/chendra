package com.rubicon.data.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public abstract class SerializingHashFunction<K extends Serializable>
		implements HashFunction<K> {

	protected byte[] serialize(K o) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(o);
		oos.close();
		baos.close();
		return baos.toByteArray();
	}

}
