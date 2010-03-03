package com.rubicon.data.op;

public interface Operation<K, V, S> {

	public void init(K key);

	public void next(V value);

	public S result();
}
