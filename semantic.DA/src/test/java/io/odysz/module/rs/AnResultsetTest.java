package io.odysz.module.rs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AnResultsetTest {

	@Test
	void testHasnext() throws Exception {
		AnResultset rs = new AnResultset(3, 4, "c-");
		
		rs.beforeFirst();
		assertTrue(rs.hasnext());
		rs.next();
		assertTrue(rs.hasnext());
		rs.next();
		assertTrue(rs.hasnext());
		rs.next();
		assertFalse(rs.hasnext());
	}

	@Test
	void testGetAtRow() throws Exception {
		AnResultset rs = new AnResultset(3, 4);
		assertEquals(0l, rs.getLongAtRow(0, 0));
		assertEquals(1l, rs.getLongAtRow(1, 0));
		assertEquals(4l, rs.getLongAtRow(0, 1));
		assertEquals(7l, rs.getLongAtRow(3, 1));
		assertEquals(11l, rs.getLongAtRow(3, 2));
		assertEquals(11l, rs.getLongAt("c-4", 2));
	}
}
