package io.odysz.common;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigsTest {
	static {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		System.out.println(path);
		Configs.init(path);
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testGetCfgString() {
		String v = Configs.getCfg("IrUser");
		assertEquals(v, "Class name of SUser Implementation");

		v = Configs.getCfg("tree-semantics", "templ-treegrid");
		assertEquals(v, "jsframe,checked,table,rec-id,parentId,text,fullpath,sibling-sort,paging-at-server");

		v = Configs.getCfg("cbb", "a-comm-link");
		assertEquals(v, "value of a-comm-link");

		// shouldn't trigger loading
		v = Configs.getCfg("cbb", "model");
		assertEquals(v, "value of model");
		v = Configs.getCfg("cbb", "gisorg");
		assertEquals(v, "value of gisorg");
	}

	@Test
	void testGetCfgStringString() {
		String v = Configs.getCfg("cbb", "gisorg");
		assertEquals(v, "value of gisorg");
	}

	@Test
	void testGetInt() {
		int v = Configs.getInt("ss-timeout-min", 0);
		assertEquals(v, 200);
	}

	@Test
	void testHasCfgString() {
		assertTrue(Configs.hasCfg("ss-timeout-min"));
	}

}
