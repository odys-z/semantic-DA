package io.oz.syn;

import static org.junit.jupiter.api.Assertions.*;

import static io.oz.syn.ExessionAct.*;

import org.junit.jupiter.api.Test;

class ExessionActTest {

	@Test
	void testNameOf() {
		assertEquals("close",     nameOf(close));
		assertEquals("restore",   nameOf(restore));
		assertEquals("unexpect",  nameOf(unexpect));
		assertEquals("ready",     nameOf(ready));
		assertEquals("init",      nameOf(init));
		assertEquals("exchange",  nameOf(exchange));
		assertEquals("close",     nameOf(close));
		assertEquals("trylater",  nameOf(trylater));
		assertEquals("lockerr",   nameOf(lockerr));
		assertEquals("deny",      nameOf(deny));
		assertEquals("signup",    nameOf(signup));
		assertEquals("setupDom",  nameOf(setupDom));
//		assertEquals("mode_server", nameOf(mode_server));
//		assertEquals("mode_client", nameOf(mode_client));
		assertEquals("ext_docref",  nameOf(ext_docref));
		assertEquals("NA",          nameOf(-3));
	}

}
