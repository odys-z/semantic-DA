package io.odysz.semantic.DA;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import org.junit.jupiter.api.Test;

import io.odysz.common.Configs;
import io.odysz.common.DateFormat;
import io.odysz.common.Utils;
import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DASemantextTest;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.x.TransException;

public class ConnectsTest {

	private static Transcxt st;

	static {
		File file = new File(DASemantextTest.rtroot);
		String abspath = file.getAbsolutePath();
		Utils.logi(abspath);
		Configs.init(abspath);
		Connects.init(abspath);

		try {
			DATranscxt.configRoot(null, abspath);
			st = new DATranscxt(DASemantextTest.connId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * A sqlite db created and connected after init(), not configured in connects.xml.
	 * @since 1.5.28
	 */
	@Test
	public void testAddSqlite() throws Exception {
		File db = new File(DASemantextTest.rtroot, "add-sqlite/added.db");
		db.delete();

		Connects.addSqlite("added-sqlite", db.getAbsolutePath(), "semantics.xml");
		assertTrue(db.exists());
		assertEquals(dbtype.sqlite, Connects.driverType("added-sqlite"));
		assertTrue(Connects.getSmtcsPath("added-sqlite").endsWith("semantics.xml"));

		ArrayList<String> sqls = new ArrayList<String>();
		sqls.add("drop table if exists t_added");
		sqls.add("create table t_added (id varchar(8) primary key)");
		sqls.add("insert into t_added values ('A')");
		Connects.commit("added-sqlite", null, sqls);
		assertEquals("A", Connects.select("added-sqlite", "select id from t_added").nxt().getString("id"));

		// an existing id is kept, the file isn't touched
		Connects.addSqlite("added-sqlite", "/no/such/dir/x.db", "semantics.xml");
		assertEquals(1, Connects.select("added-sqlite", "select count(*) c from t_added").nxt().getInt("c"));

		db.delete();
		db.getParentFile().delete();
	}

	@Test
	public void testSelect() throws SQLException, TransException {
		/*
		insert into a_functions (flags, funcId, funcName, url, parentId, sibling, fullpath) values
		('test00', '0001', 'System Title', null, null, '0', '0 0001'),
		('test00', '0002', 'Portal', 'views/portal.html', '0001', '1', '0 0001.1 0002'),
		('test00', '0003', 'User Info', 'views/user-info.html', '0001', '2', '0 0001.2 0003');
		*/
		AnResultset rs = Connects.select(
						"select * from a_functions where flags='test00' order by fullpath, sibling",
						AbsConnect.flag_nothing);

		rs.printSomeData(false, 3, "funcId", "funcName", "fullpath");
		assertEquals(rs.getRowCount(), 0);
		
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.select("a_functions", "f")
			.col("funcId")
			.col("funcName", "text")
			.col("fullpath")
			.orderby("fullpath")
			.orderby("sibling", "desc")
			.where("=", "flags", "'test00'")
			.commit(st.instancontxt(null, null), sqls); // using static semantext for testing

		rs = Connects.select(sqls.get(0));
		rs.printSomeData(false, 3, "funcId", "text", "fullpath");
		assertEquals(rs.getRowCount(), 0);
		
		assertEquals(sqls.get(0),
				"select funcId, funcName text, fullpath from a_functions f where flags = 'test00' order by fullpath asc, sibling desc");
	}

	@Test
	public void testInsert() throws TransException, SQLException {
		String flag = DateFormat.format(new Date());

		Utils.warn("Warning for null parent can be ignored here:");
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_functions")
			.nv("flags", flag)
			// .nv("funcId", "AUTO")
			.nv("funcName", "func - " + flag)
			.nv("parentId", "0")
			.commit(st.instancontxt(null, null), sqls); // using static semantext for testing
	}
	
	@Test
	public void testUri2Conn() throws SemanticException {
		assertEquals("sys-sqlite", Connects.uri2conn("session"));
		assertEquals("local-sqlite", Connects.uri2conn("/"));

		assertEquals("sys-sqlite", Connects.uri2conn("/sys/roles"));
		assertEquals("sys-sqlite", Connects.uri2conn("/sys/users"));

		assertEquals("north", Connects.uri2conn("/n/indicators"));
		assertEquals("north", Connects.uri2conn("/n/dashboard"));

		assertEquals("quiz", Connects.uri2conn("quiz/view"));
	}
}
