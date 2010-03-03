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
@DataFormatWrapper(wrapped = LongArrayDataFormat.class)
public class LongArrayDataFormatWritable implements DataFormatWritable<LongArrayDataFormat>,
			Writable, WritableComparable<LongArrayDataFormatWritable> {
	private LongArrayDataFormat data;

	public LongArrayDataFormatWritable() {
	}

	public LongArrayDataFormatWritable(LongArrayDataFormat data) {
		this.data = data;
	}

	public LongArrayDataFormat get() {
		return this.data;
	}

	public void set(LongArrayDataFormat data) {
		this.data = data;
	}

	public void readFields(DataInput in) throws IOException {
		this.data = new LongArrayDataFormat();
		this.data.setValues((long[]) com.rubicon.data.types.LongArrayType.INSTANCE.read(in));
	}

	public void write(DataOutput out) throws IOException {
		com.rubicon.data.types.LongArrayType.INSTANCE.write(out, this.data.getValues());
	}

	public int compareTo(LongArrayDataFormatWritable o) {
		int result = 0;
		LongArrayDataFormat odata = o.get();
		result = com.rubicon.data.types.LongArrayType.INSTANCE.compare(
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