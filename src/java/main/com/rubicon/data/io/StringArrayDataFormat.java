package com.rubicon.data.io;

import java.io.Serializable;

import com.rubicon.data.format.DataFieldGetter;
import com.rubicon.data.format.DataFieldSetter;
import com.rubicon.data.format.DataFormat;
import com.rubicon.data.types.StringArrayType;
import com.rubicon.data.util.ListUtils;

public class StringArrayDataFormat implements DataFormat {

	private static final int VERSION = 1;

	private static String separator = ",";

	private String[] values;

	public StringArrayDataFormat() {
		values = new String[0];
	}

	public StringArrayDataFormat(String[] values) {
		this.values = values;
	}

	@DataFieldGetter(name = "values", version = 1, type = StringArrayType.class)
	public String[] getValues() {
		return values;
	}

	@DataFieldSetter(name = "values", version = 1, type = StringArrayType.class)
	public void setValues(String[] values) {
		this.values = values;
	}

	public int version() {
		return VERSION;
	}

	public String join(String separator) {
		return ListUtils.join(separator, (Serializable[]) values);
	}

	public String toString() {
		return ListUtils.join(separator, (Serializable[]) values);
	}
}
