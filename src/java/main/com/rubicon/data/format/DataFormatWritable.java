package com.rubicon.data.format;

public interface DataFormatWritable<V extends DataFormat> {

	public V get();

	public void set(V v);
}
