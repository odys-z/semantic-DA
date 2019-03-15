package io.odysz.semantic;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import io.odysz.common.DateFormat;
import io.odysz.common.Utils;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.DATranscxt;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**Test basic semantics for semantic-jserv.<br>
 * To initialize oz_autoseq table:<pre>
CREATE TABLE oz_autoseq (
  sid text(50),
  seq INTEGER,
  remarks text(200),
  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid)
);</pre>
 * @author ody
 *
 */
class DASemantextTest {
	static final String connId = "local-sqlite";
	private static DATranscxt st;
	private static IUser usr;

	@BeforeAll
	static void testInit() throws SQLException, SemanticException, SAXException, IOException {
		Utils.printCaller(false);

		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		Utils.logi(path);
		Connects.init(path);

		ISemantext s = new DASemantext(connId, "src/test/res/semantics.xml");
		st = new DATranscxt(s);
		
		SemanticObject jo = new SemanticObject();
		jo.put("userId", "tester");
		SemanticObject usrAct = new SemanticObject();
		usrAct.put("funcId", "DASemantextTest");
		usrAct.put("funcName", "test ISemantext implementation");
		jo.put("usrAct", usrAct);
		usr = new TestUser("tester", jo);
		
		// initialize oz_autoseq - only for sqlite
		SResultset rs = Connects.select("SELECT type, name, tbl_name FROM sqlite_master where type = 'table' and tbl_name = 'oz_autoseq'",
				Connects.flag_nothing);
		if (rs.getRowCount() == 0) {
			// create oz_autoseq
			ArrayList<String> sqls = new ArrayList<String>();
			sqls.add("CREATE TABLE oz_autoseq (\n" + 
					"  sid text(50),\n" + 
					"  seq INTEGER,\n" + 
					"  remarks text(200),\n" + 
					"  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid))");
			sqls.add("CREATE TABLE a_logs (\n" +
					"  logId text(20),\n" + 
					"  funcId text(20),\n" + 
					"  funcName text(50),\n" + 
					"  oper text(20),\n" + 
					"  logTime text(20),\n" + 
					"  txt text(4000),\n" + 
					"  CONSTRAINT oz_logs_pk PRIMARY KEY (logId))");
			sqls.add("insert into oz_autoseq (sid, seq, remarks) values" +
					"('a_functions.funcId', 0, 'test')," +
					"('a_roles.roleId', 0, 'test')," + 
					"('a_users.userId', 0, 'test')," +
					"('a_logs.logId', 0, 'test')");
			sqls.add("insert into a_functions\n" +
					"(flags, funcId, funcName, fullpath) " + 
					"values ( '1911-10-10', '------', 'Sun Yet-sen', '-')");
			try { Connects.commit(usr, sqls, Connects.flag_nothing); }
			catch (Exception e) {
				Utils.warn("Make sure table oz_autoseq already exists, and only for testing aginst a sqlite DB.");
			}
		}
	}

	@Test
	void testInsert() throws TransException, SQLException {
		String flag = DateFormat.format(new Date());

		ArrayList<String> sqls = new ArrayList<String>(1);
		SemanticObject r = st.insert("a_functions")
			.nv("flags", flag)
			.nv("funcId", "AUTO")
			.nv("funcName", "func - " + flag)
			.nv("parentId", "------")
			.commit(sqls);
		
		Utils.logi(sqls);
		
		assertNotEquals(r.get("a_functions"), null);
		
		Connects.commit(usr , sqls);
		Utils.logi("New ID for a_functions: %s", st.resolvedVal("a_functions", "funcId"));
		
		// level 2
		r = st.insert("a_functions")
			.nv("flags", flag)
			.nv("funcId", "AUTO")
			.nv("funcName", "func - " + flag)
			.nv("parentId", st.resolvedVal("a_functions", "funcId"))
			.commit(sqls);
		
		Utils.logi(sqls);
		Connects.commit(usr , sqls);
		Utils.logi("New ID for a_functions: %s", st.resolvedVal("a_functions", "funcId"));

	}

	@Test
	void testUpdate() {
	}

}
