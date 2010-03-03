package com.rubicon.data.filter;

import org.apache.hadoop.mapred.JobConf;

import com.rubicon.data.format.DataFormat;

public interface MapFilter<V extends DataFormat> {

	public void configure(JobConf job);

	public boolean accept(V v);
}
