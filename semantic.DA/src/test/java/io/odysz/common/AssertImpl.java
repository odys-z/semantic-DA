package io.odysz.common;

import static io.odysz.common.LangExt.isNull;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertImpl implements IAssert {

	@Override
	public <T> void equals(T a, T b, String... msg) throws Error {
		assertEquals(a, b, isNull(msg) ? null : msg[0]);
	}

	@Override
	public void equali(int a, int b, String... msg) throws Error {
		assertEquals(a, b, isNull(msg) ? null : msg[0]);
	}

	@Override
	public void fail(String e) throws Error {
		org.junit.jupiter.api.Assertions.fail(e);
	}

	@Override
	public void equall(long a, long b, String... msg) {
		assertEquals(a, b, isNull(msg) ? null : msg[0]);
	}
	
	public static void assertPathEquals(String expect, String actual) {
		try {
			assertEquals(expect, actual);
		} catch (AssertionError e) {
			assertEquals(expect.replaceAll("/", "\\\\"), actual);
		}
	}

}
