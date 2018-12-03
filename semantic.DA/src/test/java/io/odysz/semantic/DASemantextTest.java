package io.odysz.semantic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.common.DateFormat;
import io.odysz.common.Utils;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.DATranscxt;
import io.odysz.semantic.DA.DbLog;
import io.odysz.semantic.DA.drvmnger.SqliteDriver;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

class DASemantextTest {
	private static DATranscxt st;
	private static DbLog log;

	@BeforeAll
	static void testInit() throws SQLException {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		Utils.logi(path);
		Connects.init(path);

		ISemantext s = new DASemantext("src/test/res/semantics.xml");
		st = new DATranscxt(s);
		
		SUser usr = new SUser() {
			@Override public SemanticObject logout(Object header) { return null; }
			@Override public void removed() { }
			@Override public void touch() { }
			@Override public boolean login(Object jlogin) throws SemanticException, SQLException, IOException { return false; } 
			@Override public String getSessionId() { return null; } 
			@Override public long getLastAccessTime() { return 0; } 
			@Override public String getLogId() { return null; } 
			@Override public String getUserId() { return null; } 
			@Override public String getUserName() { return null; } 
			@Override public String getRoleName() { return null; } 
			@Override public String getRoleId() { return null; } 
			@Override public String getOrgName() { return null; } 
			@Override public String getOrgId() { return null; } 
			@Override public boolean isAdmin() { return false; } 
			@Override public String homepage() { return null; } 
		};

		SemanticObject jo = new SemanticObject();
		jo.put("usrAct", new SemanticObject());
		log = new DbLog(usr, jo);
		
		
		// initialize ir_autoseq
//		SqliteDriver drv = SqliteDriver.initConnection(jdbc, usr, pswd, dbg);
		SResultset rs = Connects.select("SELECT type, name, tbl_name FROM sqlite_master where type = 'table' and tbl_name = 'oz_autoseq'",
				Connects.flag_nothing);
		if (rs.getRowCount() == 0) {
			// create oz_autoseq
			ArrayList<String> sqls = new ArrayList<String>();
			sqls.add("CREATE TABLE oz_autoseq (\n" + 
					"  sid text(50),\n" + 
					"  seq INTEGER,\n" + 
					"  remarks text(200),\n" + 
					"  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid)\n" + 
					")");
			Connects.commit(log, sqls, Connects.flag_nothing);
		}

	}


	@Test
	void testGenId() {
	}


	@Test
	void testInsert() throws TransException, SQLException {
		String flag = DateFormat.format(new Date());

		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_functions")
			.nv("flags", flag)
			.nv("funcId", "AUTO")
			.nv("funcName", "func - " + flag)
			.commit(sqls);
		
		Utils.logi(sqls);
		
		Connects.commit(log , sqls);
	}

	@Test
	void testUpdate() {
	}

}
