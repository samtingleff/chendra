package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ShortType implements DataType<Short> {

	public static ShortType INSTANCE = new ShortType();

	public Short read(DataInput in) throws IOException {
		return in.readShort();
	}

	public void write(DataOutput out, Short obj) throws IOException {
		out.writeShort(obj);
	}

	public int compare(Short o1, Short o2) {
		return o1.compareTo(o2);
	}
}
