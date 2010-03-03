package com.rubicon.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StringMapType implements DataType<Map<String, String>> {

	public static StringMapType INSTANCE = new StringMapType();

	public Map<String, String> read(DataInput in) throws IOException {
		int size = in.readInt();
		Map<String, String> vals = new HashMap<String, String>(size);
		for (int i = 0; i < size; ++i) {
			String key = in.readUTF();
			String val = in.readUTF();
			vals.put(key, val);
		}
		return vals;
	}

	public void write(DataOutput out, Map<String, String> obj) throws IOException {
		out.writeInt(obj.size());
		for (Map.Entry<String, String> entry : obj.entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeUTF(entry.getValue());
		}
	}

	public int compare(Map<String, String> o1, Map<String, String> o2) {
		int result = 0;
		if (o1.size() < o2.size()) {
			result = -1;
		} else if (o1.size() > o2.size()) {
			result = 1;
		} else {
			Iterator<Map.Entry<String, String>> iter1 = o1.entrySet().iterator();
			Iterator<Map.Entry<String, String>> iter2 = o2.entrySet().iterator();
			while (iter1.hasNext() && iter2.hasNext()) {
				Map.Entry<String, String> entry1 = iter1.next();
				Map.Entry<String, String> entry2 = iter2.next();
				int keyComparison = entry1.getKey().compareTo(entry2.getKey());
				int valueComparison = entry1.getValue().compareTo(entry2.getValue());
				result = (keyComparison == 0) ? valueComparison : keyComparison;
				if (result != 0)
					break;
			}
		}
		return result;
	}
}
