package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface DataType<V> {
	public V read(DataInput in) throws IOException;

	public void write(DataOutput out, V obj) throws IOException;

	public int compare(V v1, V v2);
}
