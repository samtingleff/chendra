package com.rubicon.data.util;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5HashFunction<K extends Serializable> extends
		SerializingHashFunction<K> implements HashFunction<K>, Serializable {
	private static final long serialVersionUID = -7186316145182160764L;

	public long hash(K k, int rep) {
		try {
			MessageDigest md5 = java.security.MessageDigest.getInstance("MD5");
			md5.reset();
			md5.update(super.serialize(k));
			md5.update(Integer.toString(rep).getBytes());
			byte[] bytes = md5.digest();
			BigInteger bi = new BigInteger(bytes);
			return bi.longValue();
		} catch (NoSuchAlgorithmException e) {
			throw new UnsupportedOperationException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
