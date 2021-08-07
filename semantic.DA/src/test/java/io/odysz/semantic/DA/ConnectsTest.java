package io.odysz.semantic.DA;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.common.DateFormat;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.x.TransException;

public class ConnectsTest {

	private static Transcxt st;

	@BeforeAll
	public static void testInit() {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		Utils.logi(path);
		Connects.init(path);

		st = new Transcxt(null);
	}

	@Test
	public void testSelect() throws SQLException, TransException {
		/*
		insert into a_functions (flags, funcId, funcName, url, parentId, sibling, fullpath) values
		('test00', '0001', 'System Title', null, null, '0', '0 0001'),
		('test00', '0002', 'Portal', 'views/portal.html', '0001', '1', '0 0001.1 0002'),
		('test00', '0003', 'User Info', 'views/user-info.html', '0001', '2', '0 0001.2 0003');
		*/
		AnResultset rs = Connects.select("select * from a_functions where flags='test00' order by fullpath, sibling", Connects.flag_nothing);
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
		// Utils.logi(sqls);

		rs = Connects.select(sqls.get(0));
		rs.printSomeData(false, 3, "funcId", "text", "fullpath");
		assertEquals(rs.getRowCount(), 0);
		
		assertEquals(sqls.get(0),
				"select funcId, funcName text, fullpath from a_functions f where flags = 'test00' order by fullpath asc, sibling desc");
	}

	@Test
	public void testInsert() throws TransException, SQLException {
		String flag = DateFormat.format(new Date());

		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_functions")
			.nv("flags", flag)
			.nv("funcId", "AUTO")
			.nv("funcName", "func - " + flag)
			.commit(st.instancontxt(null, null), sqls); // using static semantext for testing
		
		// Utils.logi(sqls);
	}
}
