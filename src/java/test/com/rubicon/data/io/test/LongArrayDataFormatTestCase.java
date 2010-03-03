package com.rubicon.data.io.test;

import com.rubicon.data.io.LongArrayDataFormat;
import com.rubicon.data.io.LongArrayDataFormatWritable;

import junit.framework.TestCase;

public class LongArrayDataFormatTestCase extends TestCase {

	public void testEquals() {
		LongArrayDataFormat df1 = new LongArrayDataFormat(new long[] { 10, 19,
				17, 23 });
		LongArrayDataFormat df2 = new LongArrayDataFormat(new long[] { 10, 19,
				17, 23 });
		LongArrayDataFormatWritable wdf1 = new LongArrayDataFormatWritable(df1);
		LongArrayDataFormatWritable wdf2 = new LongArrayDataFormatWritable(df2);
		assertEquals(wdf1.compareTo(wdf2), 0);
		assertEquals(wdf2.compareTo(wdf1), 0);
	}

	public void testSizeDifference() {
		LongArrayDataFormat df1 = new LongArrayDataFormat(new long[] { 10, 19,
				17, 23 });
		LongArrayDataFormat df2 = new LongArrayDataFormat(new long[] { 10, 19,
				17, 23, 19 });
		LongArrayDataFormatWritable wdf1 = new LongArrayDataFormatWritable(df1);
		LongArrayDataFormatWritable wdf2 = new LongArrayDataFormatWritable(df2);
		assertEquals(wdf1.compareTo(wdf2), -1);
		assertEquals(wdf2.compareTo(wdf1), 1);
	}

	public void testFieldDifference() {
		LongArrayDataFormat df1 = new LongArrayDataFormat(new long[] { 10, 19,
				17, 23 });
		LongArrayDataFormat df2 = new LongArrayDataFormat(new long[] { 10, 19,
				17, 22 });
		LongArrayDataFormatWritable wdf1 = new LongArrayDataFormatWritable(df1);
		LongArrayDataFormatWritable wdf2 = new LongArrayDataFormatWritable(df2);
		assertEquals(wdf1.compareTo(wdf2), 1);
		assertEquals(wdf2.compareTo(wdf1), -1);
	}
}
