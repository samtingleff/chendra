package com.rubicon.data.op;

import org.apache.hadoop.io.Writable;

public class CountOperation implements Operation<Writable, Object, Long> {

	private long total = 0;

	public void init(Writable key) {	
	}

	public void next(Object value) {
		++total;
	}

	public Long result() {
		return total;
	}

}
