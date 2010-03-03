package com.rubicon.data.codegen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import com.rubicon.data.format.DataFieldGetter;
import com.rubicon.data.format.DataFieldSetter;
import com.rubicon.data.format.DataFormat;
import com.rubicon.data.io.IntArrayDataFormat;
import com.rubicon.data.io.LongArrayDataFormat;
import com.rubicon.data.io.StringDataFormat;
import com.rubicon.data.io.StringArrayDataFormat;

public class WritableTypeGenerator {
	private static Map<Class, String> primitiveArrayClassTypes = new HashMap<Class, String>() {
		{
			put(Byte.TYPE, "byte[]");
			put(Short.TYPE, "short[]");
			put(Integer.TYPE, "int[]");
			put(Long.TYPE, "long[]");
			put(Float.TYPE, "float[]");
			put(Double.TYPE, "double[]");
			put(Boolean.TYPE, "boolean[]");
			put(Character.TYPE, "char[]");
			put(String.class, "String[]");
		}
	};
	private static Map<Class, String> primitiveClassTypes = new HashMap<Class, String>() {
		{
			put(Byte.TYPE, "java.lang.Byte");
			put(Short.TYPE, "java.lang.Short");
			put(Integer.TYPE, "java.lang.Integer");
			put(Long.TYPE, "java.lang.Long");
			put(Float.TYPE, "java.lang.Float");
			put(Double.TYPE, "java.lang.Double");
			put(Boolean.TYPE, "java.lang.Boolean");
			put(Character.TYPE, "java.lang.Character");
		}
	};

	private VelocityEngine ve = new VelocityEngine();

	public WritableTypeGenerator() {
		Properties p = new Properties();
		p.setProperty("resource.loader", "classpath");
		p
				.setProperty("classpath.resource.loader.class",
						"org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
		p.setProperty("classpath.resource.loader.cache", "true");
		try {
			ve.init(p);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void generateWritable(DataFormat df, String srcDir) throws Exception {
		// generate a custom Writable class in dir srcDir
		Class cls = df.getClass();
		Method[] methods = cls.getMethods();
		List<Map> getFields = new LinkedList<Map>();
		List<Map> setFields = new LinkedList<Map>();

		for (int i = 0; i < methods.length; ++i) {
			DataFieldGetter getter = methods[i]
					.getAnnotation(DataFieldGetter.class);
			if (getter != null) {
				Map getMap = new HashMap();
				getMap.put("annotation", getter);
				getMap.put("method", methods[i]);
				getFields.add(getMap);
			}
		}
		for (int i = 0; i < methods.length; ++i) {
			DataFieldSetter setter = methods[i]
					.getAnnotation(DataFieldSetter.class);
			if (setter != null) {
				Map setMap = new HashMap();
				setMap.put("annotation", setter);
				setMap.put("method", methods[i]);
				setMap.put("setType", getSetterType(methods[i]
						.getParameterTypes()[0]));
				setFields.add(setMap);
			}
		}
		Comparator<Map> comp = new Comparator<Map>() {
			public int compare(Map o1, Map o2) {
				return ((Method) o1.get("method")).getName().compareTo(
						((Method) o2.get("method")).getName());
			}
		};
		Collections.sort(getFields, comp);
		Collections.sort(setFields, comp);

		String className = String.format("%1$s%2$s", cls.getSimpleName(),
				"Writable");
		File dest = new File(String.format("%1$s/%2$s/%3$s.java", srcDir, cls
				.getPackage().getName().replaceAll("\\.", "/"), className));
		File parent = dest.getParentFile();
		parent.mkdirs();
		FileOutputStream fos = new FileOutputStream(dest);
		PrintWriter pw = new PrintWriter(fos);
		try {
			VelocityContext context = new VelocityContext();
			context.put("package", cls.getPackage().getName());
			context.put("className", className);
			context.put("dataFormatClassName", df.getClass().getSimpleName());
			context.put("getters", getFields);
			context.put("setters", setFields);
			Template template = ve
					.getTemplate("/com/rubicon/data/codegen/DataFormatWritableTemplate.vm");
			template.merge(context, pw);
		} finally {
			pw.close();
			fos.close();
		}
	}

	private String getSetterType(Class type) {
		String resolved = null;
		if (type.isArray()) {
			resolved = primitiveArrayClassTypes.get(type.getComponentType());
			if (resolved == null)
				resolved = type.getName() + "[]";
		}
		else
			resolved = primitiveClassTypes.get(type);
		return (resolved != null) ? resolved : type.getName();
	}

	public static void main(String[] args) throws Exception {
		WritableTypeGenerator generator = new WritableTypeGenerator();
		generator.generateWritable(new StringDataFormat(), "gen");
		generator.generateWritable(new IntArrayDataFormat(), "gen");
		generator.generateWritable(new LongArrayDataFormat(), "gen");
		generator.generateWritable(new StringArrayDataFormat(), "gen");
	}
}
