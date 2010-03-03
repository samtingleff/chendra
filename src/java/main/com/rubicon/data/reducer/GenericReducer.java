package com.rubicon.data.reducer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.rubicon.data.format.DataFormat;
import com.rubicon.data.format.DataFormatWrapper;
import com.rubicon.data.format.DataFormatWritable;
import com.rubicon.data.mr.Counters;
import com.rubicon.data.mr.GenericMapReduceBase;
import com.rubicon.data.op.Operation;

public class GenericReducer extends GenericMapReduceBase
		implements
		Reducer<Writable, DataFormatWritable, DataFormatWritable, DataFormatWritable> {

	private static Map<String, Method> valueFields = new HashMap<String, Method>();

	private static Map<String, List<OperationOutput>> ops = new HashMap<String, List<OperationOutput>>();

	public static void addOperation(String valueField, Operation op,
			String outputKeyFieldMethod, String outputValueFieldMethod) {
		List<OperationOutput> list = ops.get(valueField);
		if (list == null) {
			list = new LinkedList<OperationOutput>();
			ops.put(valueField, list);
		}
		list.add(new OperationOutput(op, outputKeyFieldMethod,
				outputValueFieldMethod));
	}

	public void close() throws IOException {
	}

	public void configure(JobConf job) {
		super.configure(job);
		valueFields = buildSetterMap(outputValueWrapperClass);
	}

	public void reduce(Writable key, Iterator<DataFormatWritable> values,
			OutputCollector<DataFormatWritable, DataFormatWritable> output,
			Reporter reporter) throws IOException {
		reporter.incrCounter(Counters.ReducerRecords, 1);
		try {
			while (values.hasNext()) {
				reporter.incrCounter(Counters.ReducerRecordProcessed, 1);
				DataFormatWritable writable = values.next();
				DataFormat df = writable.get();
				try {
					for (Map.Entry<String, List<OperationOutput>> entry : ops
							.entrySet()) {
						// get the Object o from df from field entry.getKey()
						String field = entry.getKey();

						Object obj = null;
						for (OperationOutput op : entry.getValue()) {
							op.op.next(obj);
						}
					}
				} catch (Exception e) {
					reporter.incrCounter(Counters.ReducerErrors, 1);
				}
			}
			DataFormat outputKey = outputKeyClass.newInstance();
			DataFormat outputValue = outputValueClass.newInstance();
			for (Map.Entry<String, List<OperationOutput>> entry : ops
					.entrySet()) {
				for (OperationOutput op : entry.getValue()) {
					Object result = op.op.result();
					if (op.outputKeyFieldMethod != null) {
						// call outputKeyFieldMethod on object outputKey with
						// arg result
						keySetterMap.get(op.outputKeyFieldMethod).invoke(
								outputKey, result);
					}
					if (op.outputValueFieldMethod != null) {
						// call outputValueFieldMethod on object outputKey with
						// arg result
						valueSetterMap.get(op.outputValueFieldMethod).invoke(
								outputValue, result);
					}
				}
			}

			DataFormatWritable keyWritable = outputKeyWrapperClass
					.getConstructor(outputKeyClass).newInstance(outputKey);
			DataFormatWritable valueWritable = outputValueWrapperClass
					.getConstructor(outputValueClass).newInstance(outputValue);

			output.collect(keyWritable, valueWritable);

		} catch (Exception e) {
			e.printStackTrace();
			reporter.incrCounter(Counters.ReducerErrors, 1);
		}
	}

	private static class OperationOutput {
		public Operation op;

		public String outputKeyFieldMethod;

		public String outputValueFieldMethod;

		public OperationOutput(Operation op, String outputKeyFieldMethod,
				String outputValueFieldMethod) {
			this.op = op;
			this.outputKeyFieldMethod = outputKeyFieldMethod;
			this.outputValueFieldMethod = outputValueFieldMethod;
		}
	}
}
