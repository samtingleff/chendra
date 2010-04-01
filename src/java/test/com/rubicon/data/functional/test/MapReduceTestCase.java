package com.rubicon.data.functional.test;

import java.util.Arrays;
import java.util.List;

import com.rubicon.data.functional.Collector;
import com.rubicon.data.functional.MapReduce;
import com.rubicon.data.functional.Mapper;
import com.rubicon.data.functional.Reducer;

import junit.framework.TestCase;

public class MapReduceTestCase extends TestCase {

	public void testMap() {
		Integer[] intArray = new Integer[] { 10, 12, 1233, 1234, 123, 903434,
				245234 };
		List<Long> longs = new MapReduce<Integer, Long, Integer>().map(Arrays
				.asList(intArray), new Mapper<Integer, Long>() {
			public void map(Iterable<Integer> values, Collector<Long> collector) {
				for (Integer i : values) {
					collector.collect(i.longValue());
				}
			}
		});
		assertNotNull(longs);
		assertEquals(longs.size(), intArray.length);
	}

	public void testReduce() {
		Integer[] intArray = new Integer[] { 10, 12, 1233, 1234, 123, 903434,
				245234 };
		Long l = new MapReduce<Integer, Integer, Long>().reduce(Arrays
				.asList(intArray), new Reducer<Integer, Long>() {
			public Long reduce(Iterable<Integer> values) {
				long sum = 0;
				for (Integer i : values) {
					sum += i.intValue();
				}
				return new Long(sum);
			}
		});
		assertNotNull(l);
		assertEquals(l.longValue(), (long) 10 + 12 + 1233 + 1234 + 123 + 903434
				+ 245234);
	}

	public void testMapReduce() {
		Integer[] intArray = new Integer[] { 10, 12, 1233, 1234, 123, 903434,
				245234 };
		List<Integer> input = Arrays.asList(intArray);
		MapReduce<Integer, Integer, Integer> mr = new MapReduce<Integer, Integer, Integer>();
		Integer sum = mr.mapReduce(input, new Mapper<Integer, Integer>() {
			public void map(Iterable<Integer> values,
					Collector<Integer> collector) {
				for (Integer i : values) {
					collector.collect(i);
				}
			}
		}, new Reducer<Integer, Integer>() {
			public Integer reduce(Iterable<Integer> values) {
				int sum = 0;
				for (Integer i : values) {
					sum += i.intValue();
				}
				return new Integer(sum);
			}
		});
		assertNotNull(sum);
		assertEquals(sum.intValue(), 10 + 12 + 1233 + 1234 + 123 + 903434
				+ 245234);
	}
}
