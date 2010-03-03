package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BooleanType implements DataType<Boolean> {

	public static BooleanType INSTANCE = new BooleanType();

	public Boolean read(DataInput in) throws IOException {
		return in.readBoolean();
	}

	public void write(DataOutput out, Boolean obj) throws IOException {
		out.writeBoolean(obj);
	}

	public int compare(Boolean o1, Boolean o2) {
		return o1.compareTo(o2);
	}
}
