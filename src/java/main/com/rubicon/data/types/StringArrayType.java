package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringArrayType implements DataType<String[]> {

	public static StringArrayType INSTANCE = new StringArrayType();

	public String[] read(DataInput in) throws IOException {
		int size = in.readInt();
		String[] vals = new String[size];
		for (int i = 0; i < size; ++i) {
			vals[i] = in.readUTF();
		}
		return vals;
	}

	public void write(DataOutput out, String[] obj) throws IOException {
		out.writeInt(obj.length);
		for (String s : obj) {
			out.writeUTF(s);
		}
	}

	public int compare(String[] o1, String[] o2) {
		int result = 0;
		if (o1.length < o2.length) {
			result = -1;
		} else if (o1.length > o2.length) {
			result = 1;
		} else {
			for (int i = 0; i < o1.length; ++i) {
				result = o1[i].compareTo(o2[i]);
				if (result != 0)
					break;
			}
		}
		return result;
	}
}
