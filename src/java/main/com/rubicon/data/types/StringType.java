package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringType implements DataType<String> {

	public static StringType INSTANCE = new StringType();

	public String read(DataInput in) throws IOException {
		return in.readUTF();
	}

	public void write(DataOutput out, String obj) throws IOException {
		out.writeUTF(obj);
	}

	public int compare(String o1, String o2) {
		return o1.compareTo(o2);
	}
}
