package com.rubicon.data.util;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA1HashFunction<K extends Serializable> extends
		SerializingHashFunction<K> implements HashFunction<K>, Serializable {
	private static final long serialVersionUID = -7136080858878372346L;

	public long hash(K k, int rep) {
		try {
			MessageDigest sha1 = java.security.MessageDigest
					.getInstance("SHA1");
			sha1.reset();
			sha1.update(super.serialize(k));
			sha1.update(Integer.toString(rep).getBytes());
			byte[] bytes = sha1.digest();
			BigInteger bi = new BigInteger(bytes);
			return bi.longValue();
		} catch (NoSuchAlgorithmException e) {
			throw new UnsupportedOperationException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
