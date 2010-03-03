package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongArrayType implements DataType<long[]> {

	public static LongArrayType INSTANCE = new LongArrayType();

	public long[] read(DataInput in) throws IOException {
		int size = in.readInt();
		long[] val = new long[size];
		for (int i = 0; i < size; ++i) {
			val[i] = in.readLong();
		}
		return val;
	}

	public void write(DataOutput out, long[] obj) throws IOException {
		out.writeInt(obj.length);
		for (long l : obj) {
			out.writeLong(l);
		}
	}

	public int compare(long[] o1, long[] o2) {
		int result = 0;
		if (o1.length < o2.length) {
			result = -1;
		} else if (o1.length > o2.length) {
			result = 1;
		} else {
			for (int i = 0; i < o1.length; ++i) {
				result = new Long(o1[i]).compareTo(new Long(o2[i]));
				if (result != 0)
					break;
			}
		}
		return result;
	}
}
