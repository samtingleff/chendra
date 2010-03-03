package com.rubicon.data.util.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.rubicon.data.util.BloomFilter;
import com.rubicon.data.util.NumberHashFunction;

import junit.framework.TestCase;

public class BloomFilterTestCase extends TestCase {

	public void testLongBloomFilter() {
		int expectedElements = 100000;
		double allowableFalsePositives = 0.01;
		BloomFilter<Long> bf = new BloomFilter<Long>(
				new NumberHashFunction<Long>(), expectedElements,
				allowableFalsePositives);
		Map<Long, Boolean> map = new HashMap<Long, Boolean>();
		Random r = new Random();
		int mapCount = 0, bfCount = 0;
		for (int i = 0; i < expectedElements; ++i) {
			long n = r.nextLong();
			if (map.put(n, new Boolean(true)) == null)
				++mapCount;
			if (bf.add(new Long(n)))
				++bfCount;
		}
		// should be within 1%
		assertTrue(((double) (mapCount - bfCount)) <= (((double) expectedElements) * allowableFalsePositives));
	}
}
