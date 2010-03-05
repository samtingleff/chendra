package com.rubicon.data.io;

import com.rubicon.data.format.DataFieldGetter;
import com.rubicon.data.format.DataFieldSetter;
import com.rubicon.data.format.DataFormat;
import com.rubicon.data.types.IntArrayType;
import com.rubicon.data.util.ListUtils;

public class IntArrayDataFormat implements DataFormat {

	private static final int VERSION = 1;

	private static String separator = ",";

	private int[] values;

	public IntArrayDataFormat() {
		values = new int[0];
	}

	public IntArrayDataFormat(int[] values) {
		this.values = values;
	}

	@DataFieldGetter(name = "values", version = 1, type = IntArrayType.class)
	public int[] getValues() {
		return values;
	}

	@DataFieldSetter(name = "values", version = 1, type = IntArrayType.class)
	public void setValues(int[] values) {
		this.values = values;
	}

	public int version() {
		return VERSION;
	}

	public String join(String separator) {
		return ListUtils.join(separator, values);
	}

	public String toString() {
		return ListUtils.join(separator, values);
	}
}
