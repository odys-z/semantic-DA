package io.odysz.module.rs;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

class AnResultsetTest {

	@Test
	void testHasnext() throws SQLException {
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

}
