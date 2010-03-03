package com.rubicon.data.op;

import org.apache.hadoop.io.Writable;

public class SumOperation implements Operation<Writable, Number, Long> {

	private long total = 0;

	public void init(Writable key) {	
	}

	public void next(Number value) {
		total += value.longValue();
	}

	public Long result() {
		return total;
	}

}
