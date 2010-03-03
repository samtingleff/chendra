package com.rubicon.data.util;

import java.io.Serializable;

public class NumberHashFunction<K extends Number> implements HashFunction<K>, Serializable {
	private static final long serialVersionUID = -5937307228944767499L;

	private HashFunction<String> delegate = new StringSHA1HashFunction();

	public long hash(K n, int rep) {
		return delegate.hash(n.toString(), rep);
	}
}
