package com.rubicon.data.util;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class StringSHA1HashFunction implements HashFunction<String>, Serializable {
	private static final long serialVersionUID = -3909101866642248009L;

	public long hash(String str, int rep) {
		try {
			MessageDigest sha1 = java.security.MessageDigest
					.getInstance("SHA1");
			sha1.reset();
			sha1.update((str + rep).getBytes());
			byte[] bytes = sha1.digest();
			BigInteger bi = new BigInteger(bytes);
			return bi.longValue();
		} catch (NoSuchAlgorithmException e) {
			throw new UnsupportedOperationException(e);
		}
	}

}
