package com.rubicon.data.test;

import com.rubicon.data.util.StringSHA1HashFunction;

import junit.framework.TestCase;

public class StringSHA1HashFunctiontTestCase extends TestCase {

	public void testStringSHA1HashFunction() {
		String s = "Hello world this is a long piece of text you punk ass bastard. Here is even more text you mother funker.";
		StringSHA1HashFunction hash = new StringSHA1HashFunction();
		long l = hash.hash(s, 0);
		System.out.println(l);
	}
}
