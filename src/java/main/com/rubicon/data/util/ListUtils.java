package com.rubicon.data.util;

import java.io.Serializable;
import java.util.Collection;

public class ListUtils {

	public static String join(Collection<? extends Serializable> coll,
			String separator) {
		StringBuffer sb = new StringBuffer();
		int index = 0;
		for (Serializable s : coll) {
			if (index != 0)
				sb.append(separator);
			sb.append(s);
			++index;
		}
		return sb.toString();
	}

	public static String join(String separator, Serializable... coll) {
		StringBuffer sb = new StringBuffer();
		int index = 0;
		for (Serializable s : coll) {
			if (index != 0)
				sb.append(separator);
			sb.append(s);
			++index;
		}
		return sb.toString();
	}
}
