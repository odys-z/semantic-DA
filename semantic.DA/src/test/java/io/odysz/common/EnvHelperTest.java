package io.odysz.common;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class EnvHelperTest {

	@Test
	void testReplace() {
		String home = System.getenv("HOME");
		assertEquals(home + "/v", EnvHelper.replace("$HOME/v"));
	}

	@Test
	void testIsRelativePath() {
		assertFalse(EnvHelper.isRelativePath("/"));
		assertFalse(EnvHelper.isRelativePath("$"));
		assertTrue(EnvHelper.isRelativePath("home"));
		assertFalse(EnvHelper.isRelativePath("$HOME/v"));
	}

}
