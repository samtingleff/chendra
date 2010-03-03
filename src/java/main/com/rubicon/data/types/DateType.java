package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class DateType implements DataType<Date> {

	public static DateType INSTANCE = new DateType();

	public Date read(DataInput in) throws IOException {
		long l = in.readLong();
		return new Date(l);
	}

	public void write(DataOutput out, Date obj) throws IOException {
		out.writeLong(obj.getTime());
	}

	public int compare(Date o1, Date o2) {
		return o1.compareTo(o2);
	}
}
