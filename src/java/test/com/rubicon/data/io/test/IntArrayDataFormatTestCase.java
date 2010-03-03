package com.rubicon.data.io.test;

import com.rubicon.data.io.IntArrayDataFormat;
import com.rubicon.data.io.IntArrayDataFormatWritable;

import junit.framework.TestCase;

public class IntArrayDataFormatTestCase extends TestCase {

	public void testEquals() {
		IntArrayDataFormat df1 = new IntArrayDataFormat(new int[] { 10, 19, 17,
				23 });
		IntArrayDataFormat df2 = new IntArrayDataFormat(new int[] { 10, 19, 17,
				23 });
		IntArrayDataFormatWritable wdf1 = new IntArrayDataFormatWritable(df1);
		IntArrayDataFormatWritable wdf2 = new IntArrayDataFormatWritable(df2);
		assertEquals(wdf1.compareTo(wdf2), 0);
		assertEquals(wdf2.compareTo(wdf1), 0);
	}

	public void testSizeDifference() {
		IntArrayDataFormat df1 = new IntArrayDataFormat(new int[] { 10, 19, 17,
				23 });
		IntArrayDataFormat df2 = new IntArrayDataFormat(new int[] { 10, 19, 17,
				23, 19 });
		IntArrayDataFormatWritable wdf1 = new IntArrayDataFormatWritable(df1);
		IntArrayDataFormatWritable wdf2 = new IntArrayDataFormatWritable(df2);
		assertEquals(wdf1.compareTo(wdf2), -1);
		assertEquals(wdf2.compareTo(wdf1), 1);
	}

	public void testFieldDifference() {
		IntArrayDataFormat df1 = new IntArrayDataFormat(new int[] { 10, 19, 17,
				23 });
		IntArrayDataFormat df2 = new IntArrayDataFormat(new int[] { 10, 19, 17,
				22 });
		IntArrayDataFormatWritable wdf1 = new IntArrayDataFormatWritable(df1);
		IntArrayDataFormatWritable wdf2 = new IntArrayDataFormatWritable(df2);
		assertEquals(wdf1.compareTo(wdf2), 1);
		assertEquals(wdf2.compareTo(wdf1), -1);
	}
}
