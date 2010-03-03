package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteType implements DataType<Byte> {

	public static ByteType INSTANCE = new ByteType();

	public Byte read(DataInput in) throws IOException {
		return in.readByte();
	}

	public void write(DataOutput out, Byte obj) throws IOException {
		out.writeByte(obj);
	}

	public int compare(Byte o1, Byte o2) {
		return o1.compareTo(o2);
	}
}
