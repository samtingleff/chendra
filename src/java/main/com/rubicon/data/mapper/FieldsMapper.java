package com.rubicon.data.mapper;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.rubicon.data.filter.MapFilter;
import com.rubicon.data.format.DataFieldGetter;
import com.rubicon.data.format.DataFieldMethod;
import com.rubicon.data.format.DataFieldSetter;
import com.rubicon.data.format.DataFormat;
import com.rubicon.data.format.DataFormatParser;
import com.rubicon.data.format.DataFormatWrapper;
import com.rubicon.data.format.DataFormatWritable;
import com.rubicon.data.mr.Counters;
import com.rubicon.data.mr.GenericMapReduceBase;

public class FieldsMapper extends GenericMapReduceBase implements
		Mapper<LongWritable, Text, Writable, Writable> {

	private static DataFormatParser parser;

	private static Map<String, String> outputKeyFields = new HashMap<String, String>();

	private static Map<String, String> outputValueFields = new HashMap<String, String>();

	private static List<MapFilter> filters = new LinkedList<MapFilter>();

	public static void setParser(DataFormatParser p) {
		parser = p;
	}

	public static void setOutputKeyFields(Map<String, String> fields) {
		outputKeyFields = fields;
	}

	public static void setOutputValueFields(Map<String, String> fields) {
		outputValueFields = fields;
	}

	public static void addFilter(MapFilter filter) {
		filters.add(filter);
	}

	public void configure(JobConf job) {
		super.configure(job);
		for (MapFilter filter : filters) {
			filter.configure(job);
		}
	}

	public void map(LongWritable offset, Text line, OutputCollector output,
			Reporter reporter) throws IOException {
		reporter.incrCounter(Counters.MapRecords, 1);
		try {
			DataFormat df = parser.parse(line.toString());
			if (df != null) {
				reporter.incrCounter(Counters.MapRecordMatches, 1);

				if (!allow(df)) {
					reporter.incrCounter(Counters.MapRecordSkipped, 1);
					return;
				}

				DataFormat key = outputKeyClass.newInstance();
				DataFormat value = outputValueClass.newInstance();

				Method[] methods = df.getClass().getMethods();
				for (int i = 0; i < methods.length; ++i) {
					DataFieldGetter getter = methods[i]
							.getAnnotation(DataFieldGetter.class);
					if (getter != null) {
						Object val = methods[i].invoke(df);

						String outputKeyField = outputKeyFields.get(getter
								.name());
						if (outputKeyField != null) {
							// call the setter named getter.name() with val
							keySetterMap.get(outputKeyField).invoke(key, val);
						}
						String outputValueField = outputValueFields.get(getter
								.name());
						if (outputValueField != null) {
							// call the setter named getter.name() with val
							valueSetterMap.get(outputValueField).invoke(value,
									val);
						}
					}
				}
				DataFormatWritable keyWritable = outputKeyWrapperClass.getConstructor(outputKeyClass).newInstance(key);
				DataFormatWritable valueWritable = outputValueWrapperClass.getConstructor(outputValueClass).newInstance(value);
				output.collect(keyWritable, valueWritable);
			} else
				reporter.incrCounter(Counters.MapRecordMismatches, 1);
		} catch (Exception e) {
			e.printStackTrace();
			reporter.incrCounter(Counters.MapRecordErrors, 1);
		}
	}

	private boolean allow(DataFormat df) {
		boolean result = true;
		if (filters.size() > 0) {
			for (MapFilter filter : filters) {
				result = filter.accept(df);
				if (!result)
					break;
			}
		}
		return result;
	}
}
