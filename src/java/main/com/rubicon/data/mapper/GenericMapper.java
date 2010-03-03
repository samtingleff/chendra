package com.rubicon.data.mapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.rubicon.data.filter.MapFilter;
import com.rubicon.data.format.DataFormat;
import com.rubicon.data.format.DataFormatParser;
import com.rubicon.data.mr.Counters;
import com.rubicon.data.transform.DataFormatTransformer;

public class GenericMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Writable, Writable> {

	private static DataFormatParser parser;

	private static DataFormatTransformer transformer;

	private static List<MapFilter> filters = new LinkedList<MapFilter>();

	public void configure(JobConf job) {
		super.configure(job);
		try {
			parser = ((Class<? extends DataFormatParser>) job.getClass(
					"genericMapper.parser", null, DataFormatParser.class))
					.newInstance();
			transformer = ((Class<? extends DataFormatTransformer>) job
					.getClass("genericMapper.transformer", null,
							DataFormatTransformer.class)).newInstance();
			Class<? extends MapFilter>[] filterClasses = (Class<? extends MapFilter>[]) job
					.getClasses("genericMapper.filter", new Class[] {});
			if (filterClasses != null) {
				for (Class<? extends MapFilter> cls : filterClasses) {
					filters.add(cls.newInstance());
				}
				for (MapFilter filter : filters) {
					filter.configure(job);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
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

				List<Writable> keys = transformer.getKeys(df);
				List<Writable> values = transformer.getValues(df);
				for (int i = 0; i < keys.size(); ++i) {
					output.collect(keys.get(i), values.get(i));
				}
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
