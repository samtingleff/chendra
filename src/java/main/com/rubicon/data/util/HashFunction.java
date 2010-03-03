package com.rubicon.data.util;

public interface HashFunction<K> {

	public long hash(K k, int rep);
}
