package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CharType implements DataType<Character> {

	public static CharType INSTANCE = new CharType();

	public Character read(DataInput in) throws IOException {
		return in.readChar();
	}

	public void write(DataOutput out, Character obj) throws IOException {
		out.writeChar(obj);
	}

	public int compare(Character o1, Character o2) {
		return o1.compareTo(o2);
	}
}
