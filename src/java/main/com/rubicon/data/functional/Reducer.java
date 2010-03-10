package com.rubicon.data.functional;

public interface Reducer<T1, T2> {
	public T2 reduce(Iterable<T1> values);
}
