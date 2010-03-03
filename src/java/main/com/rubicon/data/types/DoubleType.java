package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleType implements DataType<Double> {

	public static DoubleType INSTANCE = new DoubleType();

	public Double read(DataInput in) throws IOException {
		return in.readDouble();
	}

	public void write(DataOutput out, Double obj) throws IOException {
		out.writeDouble(obj);
	}

	public int compare(Double o1, Double o2) {
		return o1.compareTo(o2);
	}
}
