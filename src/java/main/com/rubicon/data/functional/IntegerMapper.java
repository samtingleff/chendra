package com.rubicon.data.functional;

public class IntegerMapper implements Mapper<String, Integer> {
	public void map(Iterable<String> values, Collector<Integer> collector) {
		for (String s : values) {
			try {
				collector.collect(new Integer(Integer.parseInt(s)));
			} catch (Exception e) {
			}
		}
	}
}
