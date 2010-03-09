package com.rubicon.data.thrift;

/**
 * Hackalicious marker for end of record and end of file.
 * 
 * @author stingleff
 * 
 */
public class ThriftFormatMagicBytes {
	public static final byte BEGIN_RECORD = 0;

	public static final byte END_RECORD = 1;

	public static final byte END_FILE = 2;
}
