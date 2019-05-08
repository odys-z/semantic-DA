package io.odysz.semantic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import io.odysz.common.DateFormat;
import io.odysz.common.Utils;
import io.odysz.common.dbtype;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.parts.condition.Funcall;
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
public class DASemantextTest {
	static final String connId = "local-sqlite";
	private static DATranscxt st;
	private static IUser usr;
	private static HashMap<String, DASemantics> smtcfg;

	/**Use this to reset semantic-DA.db ( a sqlite3 db file).<pre>
drop TABLE a_logs;
drop TABLE oz_autoseq;
DELETE FROM a_functions;
DELETE FROM a_role_func;
DELETE from a_users;
DELETE from a_roles;</pre>
	 * 
	 * @throws SQLException
	 * @throws SemanticException
	 * @throws SAXException
	 * @throws IOException
	 */
	@Before
	public void testInit() throws SQLException, SemanticException, SAXException, IOException {
		Utils.printCaller(false);

		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		Utils.logi(path);
		Connects.init(path);

		// st = new DATranscxt(new DASemantext(connId, null, null));
		st = new DATranscxt(connId);
		smtcfg = DATranscxt.initConfigs(connId, "src/test/res/semantics.xml");
		
		SemanticObject jo = new SemanticObject();
		jo.put("userId", "tester");
		SemanticObject usrAct = new SemanticObject();
		usrAct.put("funcId", "DASemantextTest");
		usrAct.put("funcName", "test ISemantext implementation");
		jo.put("usrAct", usrAct);
		usr = new LoggingUser(connId, "src/test/res/semantic-log.xml", "tester", jo);
		
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
					"('crs_a.aid', 0, 'test')," + 
					"('crs_b.bid', 8, 'test')," +
					"('a_logs.logId', 0, 'test')");
			sqls.add("insert into a_functions\n" +
					"(flags, funcId, funcName, fullpath) " + 
					"values ( '1911-10-10', '------', 'Sun Yat-sen', '-')");
			try { Connects.commit(usr, sqls, Connects.flag_nothing); }
			catch (Exception e) {
				Utils.warn("Make sure table oz_autoseq already exists, and only for testing aginst a sqlite DB.");
			}
		}
	}

	@Test
	public void testInsert() throws TransException, SQLException, SAXException, IOException {
		String flag = DateFormat.format(new Date());

		DASemantext s0 = new DASemantext(connId, smtcfg, usr);
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_functions")
			.nv("flags", flag)
			.nv("funcId", "AUTO")	// let's support semantics.xml/smtc=pk
			.nv("funcName", "testInsert A - " + flag)
			.nv("parentId", "------")
			.commit(s0, sqls);
		
		// Utils.logi(sqls);
		
		// Utils.logi("New ID for a_functions: %s", s0.resulvedVal("a_functions", "funcId"));
		assertEquals(6, ((String) s0.resulvedVal("a_functions", "funcId")).length());
		
		// level 2
		DASemantext s1 = new DASemantext(connId, smtcfg, usr);
		st.insert("a_functions")
			.nv("flags", flag)
			// .nv("funcId", "AUTO")
			.nv("funcName", "testInsert B - " + flag)
			.nv("parentId", s0.resulvedVal("a_functions", "funcId"))
			.commit(s1, sqls);
		
		// Utils.logi(sqls);
		Connects.commit(usr , sqls);

		// Utils.logi("New ID for a_functions: %s", s1.resulvedVal("a_functions", "funcId"));
		assertEquals(6, ((String) s1.resulvedVal("a_functions", "funcId")).length());
	}

	@Test
	public void testBatch() throws TransException, SQLException, SAXException, IOException {
		DASemantext s0 = new DASemantext(connId, smtcfg, usr);
		ArrayList<String> sqls = new ArrayList<String>(1);
		Insert f1 = st.insert("a_role_func")
				.nv("funcId", "000001");
		Insert f2 = st.insert("a_role_func")
				.nv("funcId", "000002");
		Insert newuser = st.insert("a_roles")
				.nv("roleId", "AUTO") // overridden by semantics.xml
				.nv("roleName", "Co-funder")
				.post(f1)
				.post(f2);
		newuser.commit(s0, sqls);
		Connects.commit(usr , sqls);
		
		DASemantext s1 = new DASemantext(connId, smtcfg, usr);
		String newId = (String) s0.resulvedVal("a_roles", "roleId");
		SemanticObject s = st
				.select("a_role_func", "rf")
				.col("count(funcId)", "cnt")
				.where("=", "rf.roleId", "'" + newId + "'")
				.where("=", "rf.funcId", "'000001'")
				.rs(s1);
		SResultset slect = (SResultset) s.rs(0);
		slect.printSomeData(false, 2, "funcId");

		slect.beforeFirst().next();
		assertEquals(1, slect.getInt("cnt"));
	}
	
	/**Test cross referencing auto k.
	 * crs_a.aid, crs_b.bid are autok;<br>
	 * crs_a.afk referencing crs_b.bid,<br>
	 * crs_b.bfk referencing crs_a.aid,
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	public void testCrossAutoK() throws TransException, SQLException {
		DASemantext s0 = new DASemantext(connId, smtcfg, usr);
		ArrayList<String> sqls = new ArrayList<String>(1);
		Insert f1 = st.insert("crs_a")
				.nv("remarka", Funcall.now(dbtype.sqlite));
		st.insert("crs_b")
				.nv("remarkb", Funcall.now(dbtype.sqlite))
				.post(f1)
				.commit(s0, sqls);
	
		Connects.commit(usr , sqls);

		// insert into crs_b  (remarkb, bid, bfk) values (datetime('now'), '000017', '00001K')
		// insert into crs_a  (remarka, aid, afk) values (datetime('now'), '00001K', '000017')
		assertEquals("insert into crs_b  (remarkb, bid, bfk) values (",
					sqls.get(0).substring(0, 47));
		assertEquals("insert into crs_b  (remarkb, bid, bfk) values (",
					sqls.get(0).substring(0, 47));
	}

	/**This is used for testing semantics:<br>
	 * 1. auto key: a_users.userId<br>
	 * 2. default val: a_users.pswd<br>
	 * 3. de-encrypt: a_users.pswd<br>
	 * 4. parent-child on del: a_user, a_role_func<br>
	 * 5. check count on insert: a_user.userName<br>
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	public void testSmtxUsers() throws TransException, SQLException {
		String flag = DateFormat.formatime(new Date());
		String usrName = "01 " + flag;

		DASemantext s0 = new DASemantext(connId, smtcfg, usr);
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_users") // with default value: pswd = '123456'
			.nv("userName", usrName)
			.nv("roleId", "r01")
			.nv("orgId", "o-01")
			.nv("birthday", Funcall.now(dbtype.sqlite))
			.commit(s0, sqls);
		Connects.commit(usr , sqls);
		sqls.clear();
		
		String usrId = (String)s0.resulvedVal("a_users", "userId");
		
		st.select("a_users", "u")
			.where("=", "userId", "'" + usrId + "'")
			.commit(sqls, usr);

		// assert default value pswd = '123456'
		SResultset rs = Connects.select(sqls.get(0));
		rs.beforeFirst().next();
		assertEquals("123456", rs.getString("pswd"));
		
		// TODO de-encrypt, ...
		
		// 5. check count on insert: a_user.userName<br>
		sqls.clear();
		s0.clear();
		try {
			st.insert("a_users")
				.nv("userName", usrName)
				.commit(s0, sqls);
			fail("check count on insert: a_user.userName not working");
		} catch (SemanticException e) {
			assertEquals("Checking count on a_users.userId",
					e.getMessage().substring(0, 32));
		}
	}
}
