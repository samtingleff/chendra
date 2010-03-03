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
@DataFormatWrapper(wrapped = IntArrayDataFormat.class)
public class IntArrayDataFormatWritable implements DataFormatWritable<IntArrayDataFormat>,
			Writable, WritableComparable<IntArrayDataFormatWritable> {
	private IntArrayDataFormat data;

	public IntArrayDataFormatWritable() {
	}

	public IntArrayDataFormatWritable(IntArrayDataFormat data) {
		this.data = data;
	}

	public IntArrayDataFormat get() {
		return this.data;
	}

	public void set(IntArrayDataFormat data) {
		this.data = data;
	}

	public void readFields(DataInput in) throws IOException {
		this.data = new IntArrayDataFormat();
		this.data.setValues((int[]) com.rubicon.data.types.IntArrayType.INSTANCE.read(in));
	}

	public void write(DataOutput out) throws IOException {
		com.rubicon.data.types.IntArrayType.INSTANCE.write(out, this.data.getValues());
	}

	public int compareTo(IntArrayDataFormatWritable o) {
		int result = 0;
		IntArrayDataFormat odata = o.get();
		result = com.rubicon.data.types.IntArrayType.INSTANCE.compare(
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