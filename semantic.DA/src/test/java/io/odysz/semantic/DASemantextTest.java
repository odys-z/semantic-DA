package io.odysz.semantic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
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
	private static HashMap<String, TableMeta> metas;

	static {
		try {
			Utils.printCaller(false);

			File file = new File("src/test/res");
			String path = file.getAbsolutePath();
			Utils.logi(path);
			Connects.init(path);

			// load metas, then semantics
			smtcfg = DATranscxt.initConfigs(connId, "src/test/res/semantics.xml");
			metas = DATranscxt.meta(connId);

			st = new DATranscxt(connId, metas);

			SemanticObject jo = new SemanticObject();
			jo.put("userId", "tester");
			SemanticObject usrAct = new SemanticObject();
			usrAct.put("funcId", "DASemantextTest");
			usrAct.put("funcName", "test ISemantext implementation");
			jo.put("usrAct", usrAct);
			usr = new LoggingUser(connId, "src/test/res/semantic-log.xml", "tester", jo);
		} catch (SemanticException | SQLException | SAXException | IOException e) {
			e.printStackTrace();
		}
	
	}
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
					"  cnt int,\n" + 
					"  txt text(4000),\n" + 
					"  CONSTRAINT oz_logs_pk PRIMARY KEY (logId))");
			sqls.add("insert into oz_autoseq (sid, seq, remarks) values" +
					"('a_functions.funcId', 0, 'test')," +
					"('a_roles.roleId', 0, 'test')," + 
					"('a_users.userId', 0, 'test')," +
					"('crs_a.aid', 0, 'test')," + 
					"('crs_b.bid', 8, 'test')," +
					"('b_alarms.alarmId', 0, 'cascade-ancestor')," +
					"('b_alarm_logic.logicId', 64 * 4, 'cascade-parent')," +
					"('b_logic_device.deviceLogId', 64 * 64, 'cascade-child')," +
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

		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr);
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_functions")
			.nv("flags", flag)
			.nv("funcId", "AUTO")	// let's support semantics.xml/smtc=pk
			.nv("funcName", "testInsert A - " + flag)
			.nv("parentId", "------")
			.commit(s0, sqls);
		
		// Utils.logi("New ID for a_functions: %s", s0.resulvedVal("a_functions", "funcId"));
		assertEquals(6, ((String) s0.resulvedVal("a_functions", "funcId")).length());
		
		// level 2
		DASemantext s1 = new DASemantext(connId, smtcfg, metas, usr);
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
		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr);
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
		
		DASemantext s1 = new DASemantext(connId, smtcfg, metas, usr);
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
	
	/**Test cross referencing auto k.<br>
	 * crs_a.aid, crs_b.bid are autok;<br>
	 * crs_a.afk referencing crs_b.bid,<br>
	 * crs_b.bfk referencing crs_a.aid.<br>
	 * Also, test int type's value (crs_a.testInt = 100) not single-quoted.<br>
	 * Test crs_a.fundDate(sqlite number) is quoted for both insert and update.
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	public void testCrossAutoK() throws TransException, SQLException {
		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr);
		ArrayList<String> sqls = new ArrayList<String>(1);
		Insert f1 = st.insert("crs_a")
				.nv("remarka", Funcall.now(dbtype.sqlite))
				.nv("fundDate", "1777-07-04")
				.nv("testInt", "100"); // testing that int shouldn't quoted
		st.insert("crs_b")
				.nv("remarkb", Funcall.now(dbtype.sqlite))
				.post(f1)
				.commit(s0, sqls);
		
		assertEquals(String.format(
			"update crs_b  set bfk='%s' where bid = '%s'",
			s0.resulvedVal("crs_a", "aid"), s0.resulvedVal("crs_b", "bid")),
			sqls.get(2));
		assertEquals(String.format(
			"insert into crs_a  (remarka, fundDate, testInt, aid, afk) values (datetime('now'), '1777-07-04', 100, '%s', '%s')",
			s0.resulvedVal("crs_a", "aid"), s0.resulvedVal("crs_b", "bid")),
			sqls.get(1));
		assertEquals(String.format(
			"insert into crs_b  (remarkb, bid) values (datetime('now'), '%s')",
			s0.resulvedVal("crs_b", "bid")),// s0.resulvedVal("crs_a", "aid")),
			sqls.get(0));

		Connects.commit(usr , sqls);

		st.update("crs_a")
			.nv("fundDate", "1911-10-10")
			.where("=", "testInt", "100")
			.commit(s0, sqls);
		assertEquals("update crs_a  set fundDate='1911-10-10' where testInt = 100",
					sqls.get(3));
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

		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr);
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

		// assert 2 default value pswd = '123456'
		SResultset rs = Connects.select(sqls.get(0));
		rs.beforeFirst().next();
		assertEquals("123456", rs.getString("pswd"));
		
		// TODO assert 3 de-encrypt, ...
		
		// assert 5. check count on insert: a_user.userName<br>
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
		
		testz04(usrId);
	}
		
	/**Test 4. parent-child on del
	 * @param usrId
	 * @throws TransException
	 * @throws SQLException
	 */
	private void testz04(String usrId) throws TransException, SQLException {
		// assert 4. del a_role_funcs
		String roleId = "role-u" + usrId;
		Insert rf1 = st.insert("a_role_func")
			.nv("roleId", roleId)
			.nv("funcId", "func-" + usrId + " 01");
		Insert rf2 = st.insert("a_role_func")
			.nv("roleId", roleId)
			.nv("funcId", "func-" + usrId + " 02");
			
		ISemantext s1 = st.instancontxt(usr);
		st.insert("a_roles", usr)
			.nv("roleName", roleId)
			.post(rf1).post(rf2)
			.ins(s1);
		
		String newRoleId = (String)s1.resulvedVal("a_roles", "roleId");
		SemanticObject cnt = st.select("a_roles", "r")
			.col("count(r.roleId)", "cnt")
			.j("a_role_func", "rf", "rf.roleId = r.roleId")
			.where_("=", "r.roleId", newRoleId)
			.rs(st.instancontxt(usr));
		SResultset rs = (SResultset) cnt.rs(0);
		rs.beforeFirst().next();
		// inserted two children
		assertEquals(2, rs.getInt("cnt"));
		
		st.delete("a_roles", usr)
			.where_("=", "roleId", newRoleId)
			.d(st.instancontxt(usr));
		
		cnt = st.select("a_role_func", "rf")
				.col("count(*)", "cnt")
				.where_("=", "rf.roleId", newRoleId)
				.rs(s1);

		rs = (SResultset) cnt.rs(0);
		rs.beforeFirst().next();
		assertEquals(0, rs.getInt("cnt"));
	}

	@Test
	public void testChkOnDel() throws TransException, SQLException {
		ISemantext s1 = st.instancontxt(usr);
		String typeId = "0201";		// Device Fault
		st.insert("b_alarms", usr)	// auto key id = 54
			.nv("typeId", typeId)
			.ins(s1);

		try {
		ISemantext s2 = st.instancontxt(usr);
		st.delete("a_domain", usr)
			.where_("=", "domainId", typeId)
			.d(s2);
		
		fail("ck-cnt-del not working");
		}
		catch (SemanticException e) {
			assertTrue(e.getMessage().startsWith("a_domain.checkSqlCountOnDel:"));
		}
	}
	
	@Test
	public void testCascadeInsert() throws TransException {
		testMultiChildInst();
	}
	
	private void testMultiChildInst() throws TransException {
			ArrayList<String> sqls = new ArrayList<String>(1);
		String dt = DateFormat.format(new Date());
		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr);
		st.insert("b_alarms")
				.nv("remarks", Funcall.now(dbtype.sqlite))
				.nv("typeId", "02-alarm")
				.post(st.insert("b_alarm_logic")	// child of b_alarms, auto key: logicId
						.nv("remarks", "R1 " + dt)
						.post(st.insert("b_alarm_logic")
								.nv("remarks", "L2 " + dt)
				)).commit(s0, sqls);

		assertEquals(String.format("insert into b_alarms  (remarks, typeId, alarmId) values (datetime('now'), '02-alarm', '%s')",
				s0.resulvedVal("b_alarms", "alarmId")),
				sqls.get(0));
		// the first insert b_alarm_logic must correct if following is ok.
		Utils.logi(sqls.get(1));
		assertEquals(String.format("insert into b_alarm_logic  (remarks, logicId, alarmId) values ('L2 %s', '%s', '%s')",
				dt, s0.resulvedVal("b_alarm_logic", "logicId"), s0.resulvedVal("b_alarms", "alarmId")),
				sqls.get(2));
	}
}
