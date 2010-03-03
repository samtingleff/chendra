package com.rubicon.data.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Encoders {

	public static String md5(String text, int radix) {
		byte[] inputBytes = text.getBytes();
		byte[] md5bytes = md5(inputBytes);
		BigInteger bigInt = new BigInteger(1, md5bytes);
		return bigInt.toString(radix);
	}

	public static byte[] md5(byte[] bytes) {
		try {
			MessageDigest md5 = java.security.MessageDigest.getInstance("MD5");
			md5.reset();
			md5.update(bytes);
			byte[] result = md5.digest();
			return result;
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static long md5ToLong(final String key) {
		byte[] bytes = md5(key.getBytes());
		BigInteger bi = new BigInteger(bytes);
		return bi.longValue();
	}
}
