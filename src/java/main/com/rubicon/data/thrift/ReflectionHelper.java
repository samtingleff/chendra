package com.rubicon.data.thrift;

import org.apache.hadoop.conf.Configuration;

public class ReflectionHelper {

	private ReflectionHelper() {
	}

	public static <T> Class<? extends T> getClassFromConfiguration(
			Configuration conf, String param) {
		try {
			System.err.println("value class conf: "
					+ conf.get(Constants.VALUE_CLASS));
			System.err.println(conf);
			return (Class<T>) Class.forName(conf.get(param));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
