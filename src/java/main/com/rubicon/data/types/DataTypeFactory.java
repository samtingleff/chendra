package com.rubicon.data.types;

import java.util.HashMap;
import java.util.Map;

public class DataTypeFactory {
	private static final Map<Class<? extends DataType>, DataType> types = new HashMap<Class<? extends DataType>, DataType>() {
		{
			put(DateType.class, new DateType());
			put(IntType.class, new IntType());
			put(StringArrayType.class, new StringArrayType());
			put(StringMapType.class, new StringMapType());
			put(StringType.class, new StringType());
		}
	};

	private DataTypeFactory() {
	}

	public static DataType getDataType(Class<? extends DataType> cls) {
		return types.get(cls);
	}
}
