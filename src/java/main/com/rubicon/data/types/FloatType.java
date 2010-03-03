package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FloatType implements DataType<Float> {

	public static FloatType INSTANCE = new FloatType();

	public Float read(DataInput in) throws IOException {
		return in.readFloat();
	}

	public void write(DataOutput out, Float obj) throws IOException {
		out.writeFloat(obj);
	}

	public int compare(Float o1, Float o2) {
		return o1.compareTo(o2);
	}
}
