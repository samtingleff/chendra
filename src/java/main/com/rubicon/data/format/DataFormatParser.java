package com.rubicon.data.format;


public interface DataFormatParser<V extends DataFormat> {

	public V parse(String line);
}
