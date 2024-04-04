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
		
		rs.beforeFirst();
		assertFalse(rs.validx());

		rs.next();
		assertTrue(rs.validx());
		assertFalse(rs.hasprev());
		assertTrue(rs.hasnext());
		assertEquals("0", rs.getString("c-1"));
		assertEquals("1", rs.getString("c-2"));
		assertEquals("2", rs.getString("c-3"));
		assertEquals("3", rs.getString("c-4"));

		assertEquals("0", rs.getStringAtRow("c-1", 1));
		assertEquals("1", rs.getStringAtRow("c-2", 1));
		assertEquals("2", rs.getStringAtRow("c-3", 1));
		assertEquals("3", rs.getStringAtRow("c-4", 1));

		assertEquals("4", rs.nextString("c-1"));
		assertEquals("5", rs.nextString("c-2"));
		assertEquals("6", rs.nextString("c-3"));
		assertEquals("7", rs.nextString("c-4"));

		rs.next();
		assertTrue(rs.validx());
		assertTrue(rs.hasprev());
		assertTrue(rs.hasnext());
		assertEquals("4", rs.getString("c-1"));
		assertEquals("5", rs.getString("c-2"));
		assertEquals("6", rs.getString("c-3"));
		assertEquals("7", rs.getString("c-4"));

		assertEquals("0", rs.prevString("c-1"));
		assertEquals("1", rs.prevString("c-2"));
		assertEquals("2", rs.prevString("c-3"));
		assertEquals("3", rs.prevString("c-4"));

		assertEquals("8",  rs.nextString("c-1"));
		assertEquals("9",  rs.nextString("c-2"));
		assertEquals("10", rs.nextString("c-3"));
		assertEquals("11", rs.nextString("c-4"));

		rs.next();
		assertTrue(rs.validx());
		assertTrue(rs.hasprev());
		assertFalse(rs.hasnext());
		assertEquals("8",  rs.getString("c-1"));
		assertEquals("9",  rs.getString("c-2"));
		assertEquals("10", rs.getString("c-3"));
		assertEquals("11", rs.getString("c-4"));

		assertEquals("4", rs.prevString("c-1"));
		assertEquals("5", rs.prevString("c-2"));
		assertEquals("6", rs.prevString("c-3"));
		assertEquals("7", rs.prevString("c-4"));
		
		rs.next();
		assertFalse(rs.validx());
	}
}
