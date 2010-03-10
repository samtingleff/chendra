package com.rubicon.data.parser;

public interface Parser<V> {
	public V parse(CharSequence chars);
}
