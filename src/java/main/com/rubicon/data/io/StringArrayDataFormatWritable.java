package com.rubicon.data.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.rubicon.data.format.DataFormatWrapper;
import com.rubicon.data.format.DataFormatWritable;

/**
 * GENERATED CLASS! MODIFY AT YOUR OWN RISK!
 */
@DataFormatWrapper(wrapped = StringArrayDataFormat.class)
public class StringArrayDataFormatWritable implements DataFormatWritable<StringArrayDataFormat>,
			Writable, WritableComparable<StringArrayDataFormatWritable> {
	private StringArrayDataFormat data;

	public StringArrayDataFormatWritable() {
	}

	public StringArrayDataFormatWritable(StringArrayDataFormat data) {
		this.data = data;
	}

	public StringArrayDataFormat get() {
		return this.data;
	}

	public void set(StringArrayDataFormat data) {
		this.data = data;
	}

	public void readFields(DataInput in) throws IOException {
		this.data = new StringArrayDataFormat();
		this.data.setValues((String[]) com.rubicon.data.types.StringArrayType.INSTANCE.read(in));
	}

	public void write(DataOutput out) throws IOException {
		com.rubicon.data.types.StringArrayType.INSTANCE.write(out, this.data.getValues());
	}

	public int compareTo(StringArrayDataFormatWritable o) {
		int result = 0;
		StringArrayDataFormat odata = o.get();
		result = com.rubicon.data.types.StringArrayType.INSTANCE.compare(
			this.data.getValues(),
			odata.getValues()
		);
		if (result != 0)
			return result;
		return result;
	}

	public String toString() {
		return this.data.toString();
	}
}