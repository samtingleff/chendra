package com.rubicon.data.functional;

import java.util.LinkedList;
import java.util.List;

public class MapReduce<T1, T2, T3> {
	public List<T2> map(Iterable<T1> values, Mapper<T1, T2> mapper) {
		SimpleCollector<T2> collector = new SimpleCollector<T2>();
		mapper.map(values, collector);
		return collector.values();
	}

	public T3 reduce(Iterable<T2> values, Reducer<T2, T3> reducer) {
		return reducer.reduce(values);
	}

	public T3 mapReduce(Iterable<T1> values,
			Mapper<T1, T2> mapper, Reducer<T2, T3> reducer) {
		List<T2> mapped = map(values, mapper);
		T3 result = reduce(mapped, reducer);
		return result;
	}

	private static class SimpleCollector<T2> implements Collector<T2> {

		private LinkedList<T2> list = new LinkedList<T2>();

		public void collect(T2 t) {
			list.add(t);
		}

		List<T2> values() {
			return list;
		}
	}
}
