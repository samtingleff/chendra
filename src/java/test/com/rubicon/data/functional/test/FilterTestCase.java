package com.rubicon.data.functional.test;

import java.util.Arrays;
import java.util.List;

import com.rubicon.data.functional.Filter;
import com.rubicon.data.functional.Predicate;

import junit.framework.TestCase;

public class FilterTestCase extends TestCase {

	public void testNullFilter() {
		List<Integer> inputs = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		List<Integer> filtered = new Filter<Integer>().filter(inputs,
				new Predicate<Integer>() {
					public boolean evaluate(Integer e) {
						return true;
					}
				});
		assertEquals(inputs, filtered);
	}

	public void testModFilter() {
		List<Integer> inputs = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		List<Integer> filtered = new Filter<Integer>().filter(inputs,
				new Predicate<Integer>() {
					public boolean evaluate(Integer e) {
						return e.intValue() % 2 == 0;
					}
				});
		assertEquals(Arrays.asList(0, 2, 4, 6, 8), filtered);
	}

	public void testMultipleFilters() {
		List<Integer> inputs = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		List<Integer> filtered = new Filter<Integer>().filter(inputs,
				new Predicate<Integer>() {
					public boolean evaluate(Integer e) {
						return e.intValue() % 2 == 0;
					}
				}, new Predicate<Integer>() {
					public boolean evaluate(Integer e) {
						return e.intValue() == 4;
					}
				});
		assertEquals(Arrays.asList(4), filtered);
	}
}
