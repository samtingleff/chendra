package com.rubicon.data.io.test;

import com.rubicon.data.io.StringArrayDataFormat;
import com.rubicon.data.io.StringArrayDataFormatWritable;

import junit.framework.TestCase;

public class StringArrayDataFormatTestCase extends TestCase {

	public void testEquals() {
		StringArrayDataFormat df1 = new StringArrayDataFormat(new String[] {
				"hello", "world", "short", "array" });
		StringArrayDataFormat df2 = new StringArrayDataFormat(new String[] {
				"hello", "world", "short", "array" });
		StringArrayDataFormatWritable wdf1 = new StringArrayDataFormatWritable(
				df1);
		StringArrayDataFormatWritable wdf2 = new StringArrayDataFormatWritable(
				df2);
		assertEquals(wdf1.compareTo(wdf2), 0);
		assertEquals(wdf2.compareTo(wdf1), 0);
	}

	public void testSizeDifference() {
		StringArrayDataFormat df1 = new StringArrayDataFormat(new String[] {
				"hello", "world", "short", "array" });
		StringArrayDataFormat df2 = new StringArrayDataFormat(new String[] {
				"hello", "world", "short", "array", "longer" });
		StringArrayDataFormatWritable wdf1 = new StringArrayDataFormatWritable(
				df1);
		StringArrayDataFormatWritable wdf2 = new StringArrayDataFormatWritable(
				df2);
		assertEquals(wdf1.compareTo(wdf2), -1);
		assertEquals(wdf2.compareTo(wdf1), 1);
	}

	public void testFieldDifference() {
		StringArrayDataFormat df1 = new StringArrayDataFormat(new String[] {
				"hello", "world", "short", "array" });
		StringArrayDataFormat df2 = new StringArrayDataFormat(new String[] {
				"hello", "world-10", "short", "array" });
		StringArrayDataFormatWritable wdf1 = new StringArrayDataFormatWritable(
				df1);
		StringArrayDataFormatWritable wdf2 = new StringArrayDataFormatWritable(
				df2);
		assertTrue(wdf1.compareTo(wdf2) < 0);
		assertTrue(wdf2.compareTo(wdf1) > 0);
	}

}
