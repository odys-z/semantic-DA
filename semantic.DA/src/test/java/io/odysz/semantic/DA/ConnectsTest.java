package io.odysz.semantic.DA;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ConnectsTest {

	@BeforeAll
	void testInit() {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		System.out.println(path);
		Connects.init(path);
	}

	@Test
	void testGenId() {
	}

	@Test
	void testSelect() {
	}

}
