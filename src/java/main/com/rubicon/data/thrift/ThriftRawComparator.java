package com.rubicon.data.thrift;

import org.apache.hadoop.io.RawComparator;
import org.apache.thrift.TBase;

/**
 * Raw comparator for thrift types. Not very smart.
 * 
 * @author stingleff
 *
 * @param <T>
 */
public class ThriftRawComparator<T extends TBase> implements RawComparator<T> {

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int result = (l1 > l2) ? 1 : ((l1 < l2) ? -1 : 0);
		if (result == 0) {
			for (int offset = 0; offset < l1; ++offset) {
				byte byte1 = b1[s1 + offset], byte2 = b2[s2 + offset];
				result = (byte1 > byte2) ? 1 : ((byte1 < byte2) ? -1 : 0);
				if (result != 0)
					break;
			}
		}
		return result;
	}

	public int compare(T o1, T o2) {
		return ((Comparable<T>) o1).compareTo(o2);
	}
}
