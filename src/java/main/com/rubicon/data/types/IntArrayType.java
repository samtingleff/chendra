package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntArrayType implements DataType<int[]> {

	public static IntArrayType INSTANCE = new IntArrayType();

	public int[] read(DataInput in) throws IOException {
		int size = in.readInt();
		int[] val = new int[size];
		for (int i = 0; i < size; ++i) {
			val[i] = in.readInt();
		}
		return val;
	}

	public void write(DataOutput out, int[] obj) throws IOException {
		out.writeInt(obj.length);
		for (int i : obj) {
			out.writeInt(i);
		}
	}

	public int compare(int[] o1, int[] o2) {
		int result = 0;
		if (o1.length < o2.length) {
			result = -1;
		} else if (o1.length > o2.length) {
			result = 1;
		} else {
			for (int i = 0; i < o1.length; ++i) {
				result = new Integer(o1[i]).compareTo(new Integer(o2[i]));
				if (result != 0)
					break;
			}
		}
		return result;
	}
}
