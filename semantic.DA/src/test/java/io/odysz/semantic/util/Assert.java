package io.odysz.semantic.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

public class Assert {
	
	public static <T extends Object> HashSet<T> assertIn(T id, T ... ids) {
		HashSet<T> s = new HashSet<T>(Arrays.asList(ids));
		assertTrue(s.contains(id));
		return s;
	}

	public static <T extends Object> HashSet<T> assertIn(T id, HashSet<T> set) {
		assertTrue(set.contains(id));
		return set;
	}

}
