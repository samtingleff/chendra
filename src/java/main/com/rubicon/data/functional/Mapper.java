package com.rubicon.data.functional;

public interface Mapper<T1, T2> {
	public void map(Iterable<T1> values, Collector<T2> collector);
}
