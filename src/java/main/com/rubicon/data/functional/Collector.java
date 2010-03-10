package com.rubicon.data.functional;

public interface Collector<T> {
	public void collect(T t);
}