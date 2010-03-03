package com.rubicon.data.mr;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;

import com.rubicon.data.format.DataFieldMethod;
import com.rubicon.data.format.DataFieldSetter;
import com.rubicon.data.format.DataFormat;
import com.rubicon.data.format.DataFormatWrapper;
import com.rubicon.data.format.DataFormatWritable;

public class GenericMapReduceBase extends MapReduceBase {

	protected static Class<? extends DataFormatWritable> outputKeyWrapperClass;

	protected static Class<? extends DataFormat> outputKeyClass;

	protected static Class<? extends DataFormatWritable> outputValueWrapperClass;

	protected static Class<? extends DataFormat> outputValueClass;

	protected Map<String, Method> keySetterMap;

	protected Map<String, Method> valueSetterMap;

	public static void setOutputKeyClass(
			Class<? extends DataFormatWritable> outputKeyFormat) {
		outputKeyWrapperClass = outputKeyFormat;
	}

	public static void setOutputValueClass(
			Class<? extends DataFormatWritable> outputValueFormat) {
		outputValueWrapperClass = outputValueFormat;
	}

	public void configure(JobConf job) {
		outputKeyClass = outputKeyWrapperClass.getAnnotation(
				DataFormatWrapper.class).wrapped();
		outputValueClass = outputKeyWrapperClass.getAnnotation(
				DataFormatWrapper.class).wrapped();
		keySetterMap = buildSetterMap(outputKeyWrapperClass);
		valueSetterMap = buildSetterMap(outputValueWrapperClass);
	}

	protected Map<String, Method> buildSetterMap(
			Class<? extends DataFormatWritable> cls) {
		DataFormatWrapper wrapper = cls.getAnnotation(DataFormatWrapper.class);
		Class<? extends DataFormat> c = wrapper.wrapped();
		return buildSetterMapWrapped(c);
	}

	private Map<String, Method> buildSetterMapWrapped(
			Class<? extends DataFormat> cls) {
		Map<String, Method> map = new HashMap<String, Method>();
		Method[] methods = cls.getMethods();
		for (Method m : methods) {
			DataFieldSetter setter = m.getAnnotation(DataFieldSetter.class);
			DataFieldMethod method = m.getAnnotation(DataFieldMethod.class);
			if (setter != null)
				map.put(setter.name(), m);
			else if (method != null)
				map.put(method.name(), m);
		}
		return map;
	}
}
