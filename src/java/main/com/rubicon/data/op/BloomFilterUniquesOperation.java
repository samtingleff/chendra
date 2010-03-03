package com.rubicon.data.op;

import java.io.Serializable;

import org.apache.hadoop.io.Writable;

import com.rubicon.data.util.BloomFilter;

public class BloomFilterUniquesOperation implements
		Operation<Writable, Serializable, Long> {

	private BloomFilter<Serializable> bf = new BloomFilter<Serializable>();

	private long total = 0;

	public void init(Writable key) {
	}

	public void next(Serializable value) {
		if (bf.add(value))
			++total;
	}

	public Long result() {
		return total;
	}
}
