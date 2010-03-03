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
@DataFormatWrapper(wrapped = StringDataFormat.class)
public class StringDataFormatWritable implements DataFormatWritable<StringDataFormat>,
			Writable, WritableComparable<StringDataFormatWritable> {
	private StringDataFormat data;

	public StringDataFormatWritable() {
	}

	public StringDataFormatWritable(StringDataFormat data) {
		this.data = data;
	}

	public StringDataFormat get() {
		return this.data;
	}

	public void set(StringDataFormat data) {
		this.data = data;
	}

	public void readFields(DataInput in) throws IOException {
		this.data = new StringDataFormat();
		this.data.setText((java.lang.String) com.rubicon.data.types.StringType.INSTANCE.read(in));
	}

	public void write(DataOutput out) throws IOException {
		com.rubicon.data.types.StringType.INSTANCE.write(out, this.data.getText());
	}

	public int compareTo(StringDataFormatWritable o) {
		int result = 0;
		StringDataFormat odata = o.get();
		result = com.rubicon.data.types.StringType.INSTANCE.compare(
			this.data.getText(),
			odata.getText()
		);
		if (result != 0)
			return result;
		return result;
	}

	public String toString() {
		return this.data.toString();
	}
}