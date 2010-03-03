package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntType implements DataType<Integer> {

	public static IntType INSTANCE = new IntType();

	public Integer read(DataInput in) throws IOException {
		return in.readInt();
	}

	public void write(DataOutput out, Integer obj) throws IOException {
		out.writeInt(obj);
	}

	public int compare(Integer o1, Integer o2) {
		return o1.compareTo(o2);
	}
}
