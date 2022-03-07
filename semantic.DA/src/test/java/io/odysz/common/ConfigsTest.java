package io.odysz.common;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import org.junit.jupiter.api.Test;

import io.odysz.common.Configs.keys;
import io.odysz.semantic.DASemantext;

public class ConfigsTest {
	static {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		System.out.println(path);
		Configs.init(path);
	}

	@Test
	public void testGetCfgString() {
		String v = Configs.getCfg(keys.irUser);
		assertEquals(v, "Class name of SUser Implementation");

		v = Configs.getCfg(keys.treeSemantics, "templ-treegrid");
		assertEquals(v, "jsframe,checked,table,rec-id,parentId,text,fullpath,sibling-sort,paging-at-server");

		// shouldn't trigger loading
	}

	@Test
	public void testGetInt() {
		int v = Configs.getInt(keys.timeoutMin, 0);
		assertEquals(v, 200);
	}

	@Test
	public void testHasCfgString() {
		assertTrue(Configs.hasCfg(keys.timeoutMin));
	}

	@Test
	public void testGetBase32() {
		String filesys = Configs.getCfg(keys.filesys);
		assertEquals(filesys, "windows");
		assertEquals("0000010", DASemantext.radix64_32(32));
		assertEquals("000010", Radix64.toString(64, 6));
		
		Configs.cfgs.get(keys.deftXTableId).remove(keys.filesys);
		assertEquals("000020", Radix32.toString(64, 6));
		assertEquals("0000010", DASemantext.radix64_32(32));
		
		DASemantext.file_sys = 0;
		Configs.cfgs.get(keys.deftXTableId).put(keys.idLen, "4");
		Configs.cfgs.get(keys.deftXTableId).put(keys.filesys, "linux");
		assertEquals("000-", DASemantext.radix64_32(63));
		assertEquals("----", DASemantext.radix64_32(16777215));

		Configs.cfgs.get(keys.deftXTableId).put(keys.idLen, "7");
		assertEquals("0------", DASemantext.radix64_32(68719476735L));

		Configs.cfgs.get(keys.deftXTableId).remove(keys.idLen);
		assertEquals("------", DASemantext.radix64_32(68719476735L));

	}
}
