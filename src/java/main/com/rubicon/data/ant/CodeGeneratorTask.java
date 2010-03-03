package com.rubicon.data.ant;

import java.util.LinkedList;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import com.rubicon.data.codegen.WritableTypeGenerator;
import com.rubicon.data.format.DataFormat;

public class CodeGeneratorTask extends Task {

	private WritableTypeGenerator delegate = new WritableTypeGenerator();

	private List<NestedDataFormat> formats = new LinkedList<NestedDataFormat>();

	private String destDir;

	public void setDest(String dir) {
		this.destDir = dir;
	}

	public Object createDataformat() {
		return new NestedDataFormat();
	}

	public void addConfiguredDataformat(NestedDataFormat format) {
		formats.add(format);
	}

	public void execute() throws BuildException {
		try {
			for (NestedDataFormat format : formats) {
				Class<? extends DataFormat> cls = (Class<? extends DataFormat>) Class
						.forName(format.getClassName());
				DataFormat df = cls.newInstance();
				delegate.generateWritable(df, destDir);
			}
		} catch (Exception e) {
			throw new BuildException(e);
		}
	}

	public static class NestedDataFormat {
		private String className;

		public String getClassName() {
			return className;
		}

		public void setClassName(String className) {
			this.className = className;
		}
	}
}
