package com.rubicon.data.util.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class InputStreamUtils {

	private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

	public static String getStringFileFile(File file) throws IOException {
		FileInputStream fis = new FileInputStream(file);
		try {
			return getStringFromInputStream(fis, DEFAULT_BUFFER_SIZE);
		} finally {
			fis.close();
		}
	}

	public static String getStringFromPath(Configuration conf, String path)
			throws IOException {
		InputStream is = getInputStreamFromPath(conf, path);
		try {
			return getStringFromInputStream(is, DEFAULT_BUFFER_SIZE);
		} finally {
			is.close();
		}
	}

	public static InputStream getInputStreamFromPath(Configuration conf,
			String path) throws IOException {
		InputStream is = InputStreamUtils.class.getResourceAsStream(path);
		// next look in hdfs
		if (is == null) {
			FileSystem fs = FileSystem.get(conf);
			Path inputPath = new Path(path);
			if ((fs.exists(inputPath)) && (fs.isFile(inputPath)))
				is = fs.open(inputPath);
		}
		if (is == null) {
			is = new FileInputStream(path);
		}
		return is;
	}

	private static String getStringFromInputStream(InputStream is,
			int bufferSize) throws IOException {
		InputStreamReader isr = new InputStreamReader(is);
		try {
			StringWriter sw = new StringWriter();
			char[] buffer = new char[bufferSize];
			int read = 0;
			while ((read = isr.read(buffer)) > 0) {
				sw.write(buffer, 0, read);
			}
			return sw.toString();
		} finally {
			isr.close();
		}
	}
}
