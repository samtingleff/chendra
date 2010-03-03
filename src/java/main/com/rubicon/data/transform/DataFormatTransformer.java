package com.rubicon.data.transform;

import java.util.List;

import org.apache.hadoop.io.Writable;

import com.rubicon.data.format.DataFormat;

public interface DataFormatTransformer<In extends DataFormat, KeyOut extends Writable, ValueOut extends Writable> {
	public List<KeyOut> getKeys(In input);

	public List<ValueOut> getValues(In input);
}
