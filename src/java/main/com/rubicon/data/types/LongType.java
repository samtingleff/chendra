package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongType implements DataType<Long> {

	public static LongType INSTANCE = new LongType();

	public Long read(DataInput in) throws IOException {
		return in.readLong();
	}

	public void write(DataOutput out, Long obj) throws IOException {
		out.writeLong(obj);
	}

	public int compare(Long o1, Long o2) {
		return o1.compareTo(o2);
	}
}
