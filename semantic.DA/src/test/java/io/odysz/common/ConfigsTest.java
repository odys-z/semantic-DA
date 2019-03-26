package io.odysz.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

public class ConfigsTest {
	static {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		System.out.println(path);
		Configs.init(path);
	}

	@Test
	public void testGetCfgString() {
		String v = Configs.getCfg("IrUser");
		assertEquals(v, "Class name of SUser Implementation");

		v = Configs.getCfg("tree-semantics", "templ-treegrid");
		assertEquals(v, "jsframe,checked,table,rec-id,parentId,text,fullpath,sibling-sort,paging-at-server");

		// shouldn't trigger loading
	}

	@Test
	public void testGetInt() {
		int v = Configs.getInt("ss-timeout-min", 0);
		assertEquals(v, 200);
	}

	@Test
	public void testHasCfgString() {
		assertTrue(Configs.hasCfg("ss-timeout-min"));
	}

}
