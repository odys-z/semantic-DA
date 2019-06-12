package io.odysz.semantic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import io.odysz.common.AESHelper;
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
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**Test basic semantics for semantic-jserv.<br>
 * This source can be used as examples of how to use semantic-* java API.
 * 
 * @author odys-z@github.com
 *
 */
public class DASemantextTest {
	static final String connId = "local-sqlite";
	private static DATranscxt st;
	private static IUser usr;
	private static HashMap<String, DASemantics> smtcfg;
	private static HashMap<String, TableMeta> metas;

	private static String rtroot = "src/test/res/";

	static {
		try {
			Utils.printCaller(false);

			File file = new File("src/test/res");
			String path = file.getAbsolutePath();
			Utils.logi(path);
			Connects.init(path);

			// load metas, then semantics
			DATranscxt.configRoot(rtroot, rtroot);
			smtcfg = DATranscxt.loadSemantics(connId, "src/test/res/semantics.xml");
			st = new DATranscxt(connId);
			// metas = DATranscxt.meta(connId);
			metas = Connects.getMeta(connId);

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
					"('a_attaches.attId', 0, 'attachements')," +
					"('a_functions.funcId', 0, 'test')," +
					"('a_logs.logId', 0, 'test')," +
					"('a_orgs.orgId', 0, 'test')," + 
					"('a_roles.roleId', 0, 'test')," + 
					"('a_users.userId', 0, 'test')," +
					"('b_alarm_logic.logicId', 64 * 4, 'cascade-parent')," +
					"('b_alarms.alarmId', 0, 'cascade-ancestor')," +
					"('b_logic_device.deviceLogId', 64 * 64, 'cascade-child')," +
					"('crs_a.aid', 0, 'test')," + 
					"('crs_b.bid', 128 * 64, 'test')"
					);
			sqls.add("delete from a_attaches");
			sqls.add("delete from a_functions");
			sqls.add("delete from a_logs");
			sqls.add("delete from a_orgs");
			sqls.add("delete from a_role_func");
			sqls.add("delete from a_roles");
			sqls.add("delete from a_users");
			sqls.add("delete from b_alarm_logic");
			sqls.add("delete from b_alarms");
			sqls.add("delete from b_logic_device");
			sqls.add("delete from crs_a");
			sqls.add("delete from crs_b");
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

		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
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
		DASemantext s1 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
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
		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
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
		
		DASemantext s1 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
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
	 * crs_a.afk referencing crs_b.bid, (post-fk)<br>
	 * crs_b.bfk referencing crs_a.aid.<br>
	 * <p>Also, test int type's value (crs_a.testInt = 100) not single-quoted.<br>
	 * Test crs_a.fundDate(sqlite number) is quoted for both insert and update.</p>
	 * 
	 * <p>Also, test post-fk.<br>
	 * As post-fk is a weak wiring up, it should do nothing when updating. </p>
	 * 
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	public void testCrossAutoK() throws TransException, SQLException {
		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
		ArrayList<String> sqls = new ArrayList<String>(1);
		Insert f1 = st.insert("crs_a")
				.nv("remarka", Funcall.now(dbtype.sqlite))
				.nv("fundDate", "1777-07-04")
				.nv("testInt", "100"); // testing that int shouldn't quoted
		st.insert("crs_b")
				.nv("remarkb", Funcall.now(dbtype.sqlite))
				.post(f1)
				.commit(s0, sqls);
		
		String aid = (String) s0.resulvedVal("crs_a", "aid");
		String bid = (String) s0.resulvedVal("crs_b", "bid");

		assertEquals(String.format(
			"update crs_b  set bfk='%s' where bid = '%s'",
			aid, bid),
			sqls.get(2));
		assertEquals(String.format(
			"insert into crs_a  (remarka, fundDate, testInt, aid, afk) values (datetime('now'), '1777-07-04', 100, '%s', '%s')",
			aid, bid),
			sqls.get(1));
		assertEquals(String.format(
			"insert into crs_b  (remarkb, bid) values (datetime('now'), '%s')",
			bid),// s0.resulvedVal("crs_a", "aid")),
			sqls.get(0));

		Connects.commit(usr , sqls);

		// Test Case of Resulving: 
		// Using Resulving in child table to resulve parent's pk reference
		sqls.clear();
		st.update("crs_a")
			.nv("fundDate", "1911-10-10")
			.where("=", "testInt", "100")
			.commit(s0, sqls);
		assertEquals("update crs_a  set fundDate='1911-10-10' where testInt = 100",
					sqls.get(0));
		
		sqls.clear();
		DASemantext s1 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
		st.insert("crs_b")
			.nv("remarkb", "1911-10-10")
			.post(st.update("crs_a")
					.nv("remarka", "update child")
					.where("=", "bid", new Resulving("crs_b", "bid")))
			.commit(s1, sqls);

		// insert into crs_b  (remarkb, bid) values ('1911-10-10', '00000p')
		// update crs_a  set remarka='update child' where bid = '00000o'
		bid = (String) s1.resulvedVal("crs_b", "bid");
		assertEquals(2, sqls.size());
		assertEquals(String.format(
				"insert into crs_b  (remarkb, bid) values ('1911-10-10', '%s')",
				bid), sqls.get(0));
		assertEquals(String.format(
				"update crs_a  set remarka='update child' where bid = '%s'",
				bid), sqls.get(1));
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

		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
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
			// .nv("roleId", roleId)
			.nv("funcId", "func-" + usrId + " 01");
		Insert rf2 = st.insert("a_role_func")
			// .nv("roleId", roleId)
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
		String typeId = "02-fault";		// Device Fault
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
	
	/**Test multiple children auto key ({@link DASemantics.smtype#autoInc})<br>
	 * Test post-fk ({@link DASemantics.smtype#postFk})<br>
	 * Test cascading fk on insert ({@link DASemantics.smtype#fkIns};<br>
	 * using semantics:<pre>
   	&lt;s&gt;
  		&lt;id&gt;6a&lt;/id&gt;
  		&lt;smtc&gt;auto&lt;/smtc&gt;
  		&lt;tabl&gt;b_alarms&lt;/tabl&gt;
  		&lt;pk&gt;alarmId&lt;/pk&gt;
  		&lt;args&gt;alarmId&lt;/args&gt;
  	&lt;/s&gt;
  	&lt;s&gt;
  		&lt;id&gt;6b&lt;/id&gt;
  		&lt;smtc&gt;auto&lt;/smtc&gt;
  		&lt;tabl&gt;b_alarm_logic&lt;/tabl&gt;
  		&lt;pk&gt;logicId&lt;/pk&gt;
  		&lt;args&gt;logicId&lt;/args&gt;
  	&lt;/s&gt;
  	&lt;s&gt;
  		&lt;id&gt;6c&lt;/id&gt;
  		&lt;smtc&gt;fk&lt;/smtc&gt;
  		&lt;tabl&gt;b_alarm_logic&lt;/tabl&gt;
  		&lt;pk&gt;logicId&lt;/pk&gt;
  		&lt;args&gt;alarmId,b_alarms,alarmId&lt;/args&gt;
  	&lt;/s&gt;
	&lt;s&gt;
  		&lt;id&gt;6e&lt;/id&gt;
  		&lt;smtc&gt;auto&lt;/smtc&gt;
  		&lt;tabl&gt;b_logic_device&lt;/tabl&gt;
  		&lt;pk&gt;deviceLogId&lt;/pk&gt;
  		&lt;args&gt;deviceLogId&lt;/args&gt;
  	&lt;/s&gt;
	&lt;s&gt;
  		&lt;id&gt;6f&lt;/id&gt;
  		&lt;smtc&gt;fk&lt;/smtc&gt;
  		&lt;tabl&gt;b_logic_device&lt;/tabl&gt;
  		&lt;pk&gt;deviceLogId&lt;/pk&gt;
  		&lt;args&gt;logicId,b_alarm_logic,logicId&lt;/args&gt;
  	&lt;/s&gt;
	&lt;s&gt;
  		&lt;id&gt;6g&lt;/id&gt;
  		&lt;smtc&gt;fk&lt;/smtc&gt;
  		&lt;tabl&gt;b_logic_device&lt;/tabl&gt;
  		&lt;pk&gt;deviceLogId&lt;/pk&gt;
  		&lt;args&gt;alarmId,b_alarms,alarmId&lt;/args&gt;
  	&lt;/s&gt;
</pre>
	 * tested case:<pre>insert into b_alarms  (remarks, typeId, alarmId) values (datetime('now'), '02-alarm', '00000X')
insert into b_alarm_logic  (remarks, logicId, alarmId) values ('R1 2019-05-17', '00002l', '00000X')
insert into b_logic_device  (remarks, deviceLogId, logicId, alarmId) values ('R1''s device 1.1', '00004Y', '00002l', '00000X')
insert into b_logic_device  (remarks, deviceLogId, logicId, alarmId) values ('R1''s ddevice 1.2', '00004Z', '00002l', '00000X')
insert into b_alarm_logic  (remarks, logicId, alarmId) values ('L2 2019-05-17', '00002m', '00000X')
insert into b_logic_device  (remarks, deviceLogId, logicId, alarmId) values ('L2''s device 2.1', '00004a', '00002m', '00000X')
insert into b_logic_device  (remarks, deviceLogId, logicId, alarmId) values ('L2''s device 2.2', '00004b', '00002m', '00000X')</pre>
	 * @throws TransException
	 * @throws SQLException 
	 */
	@Test
	public void testCascadeInsert() throws TransException, SQLException {
		String dt = DateFormat.format(new Date());
		ISemantext s0 = st.instancontxt(usr);
		st.insert("b_alarms", usr)
				.nv("remarks", Funcall.now(dbtype.sqlite))
				.nv("typeId", "02-alarm")

				.post(st.insert("b_alarm_logic")	// child of b_alarms, auto key: logicId
						.nv("remarks", "R1 " + dt)
						.post(st.insert("b_logic_device")
								.nv("remarks", "R1''s device 1.1"))
						.post(st.insert("b_logic_device")
								.nv("remarks", "R1''s ddevice 1.2"))

						.post(st.insert("b_alarm_logic")
								.nv("remarks", "L2 " + dt)
								.post(st.insert("b_logic_device")
										.nv("remarks", "L2''s device 2.1"))
								.post(st.insert("b_logic_device")
										.nv("remarks", "L2''s device 2.2"))
				)).ins(s0);

		// let's findout the last inserted into b_logic_device
		SemanticObject res = st.select("b_logic_device", "d")
			.col("max(deviceLogId)", "dlid")
			.where_("=", "alarmId", s0.resulvedVal("b_alarms", "alarmId"))
			.rs(st.instancontxt(usr));
		SResultset rs = (SResultset) res.rs(0);
		rs.beforeFirst().next();
		// the max deviceLogId should be in s0.
		assertEquals(s0.resulvedVal("b_logic_device", "deviceLogId"), rs.getString("dlid"));
		Utils.warn("What's about update parent?");
	}
	
	@Test
	public void testMultiChildInst() throws TransException {
		ArrayList<String> sqls = new ArrayList<String>(1);
		String dt = DateFormat.format(new Date());
		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
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
		String alarmId = (String) s0.resulvedVal("b_alarms", "alarmId");
		assertEquals(String.format("insert into b_alarm_logic  (remarks, logicId, alarmId) values ('L2 %s', '%s', '%s')",
				dt, s0.resulvedVal("b_alarm_logic", "logicId"), alarmId),
				sqls.get(2));
		
		// test case
		// because b_alarm is updating, no auto key generated,
		// so child fk should provided by client, and won't been resulved.
		sqls.clear();
		DASemantext s1 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
		st.update("b_alarms")
			.nv("remarks", "updated")
			.where_("=", "alarmId", alarmId)
			.post(st.delete("b_alarm_logic")
					.where_("=", "alarmId", alarmId)
					.post(st.insert("b_alarm_logic")
							 .nv("remarks", "L3 " + dt)
							 .nv("alarmId", alarmId))) // because b_alarm is updating, no auto key there.
			.commit(s1, sqls);

		Utils.logi(sqls);
		// update b_alarms  set remarks='updated' where alarmId = '000010'
		// delete from b_alarm_logic where alarmId = '000010'
		// insert into b_alarm_logic  (remarks, alarmId, logicId) values ('L3 2019-05-20', '000010', '00003N')
		assertEquals(String.format("insert into b_alarm_logic  (remarks, alarmId, logicId) values ('L3 %s', '%s', '%s')",
				dt, alarmId, s1.resulvedVal("b_alarm_logic", "logicId")),
				sqls.get(2));
	}
	
	@Test
	public void testExtfile() throws TransException, SQLException, IOException {
		String flag = DateFormat.formatime(new Date());
		String usrName = "attached " + flag;

		DASemantext s0 = new DASemantext(connId, smtcfg, metas, usr, rtroot);
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_users") // with default value: pswd = '123456'
			.nv("userName", usrName)
			.nv("roleId", "attach-01")
			.nv("orgId", "R.C.")
			.nv("birthday", Funcall.toDate(dbtype.sqlite, "1866-12-12"))
			.post(st.insert("a_attaches")
					.nv("attName", "Sun Yet-sen Portrait.jpg")  // name: portrait
					.nv("busiTbl", "a_user")
					// .nv("busiId", new Resulving("a_users", "userId"))
					.nv("uri", readB64("src/test/res/Sun Yet-sen.jpg")))
			.commit(s0, sqls);

		// insert into a_users  (userName, roleId, orgId, birthday, userId, pswd) values ('attached 2019-06-12 18:20:33', 'attach-01', 'R.C.', datetime('1866-12-12'), '00001R', '123456')
		// insert into a_attaches  (attName, busiTbl, uri, attId, busiId, optime, oper)
		// values ('Sun Yet-sen Portrait.jpg', 'a_user', 'uploads/a_user/00001C Sun Yet-sen Portrait.jpg', '00001C', '00001R', datetime('now'), 'tester')
		assertEquals(String.format(
				"insert into a_attaches  (attName, busiTbl, uri, attId, busiId, optime, oper) " +
				"values ('Sun Yet-sen Portrait.jpg', 'a_user', 'uploads/a_user/%1$s Sun Yet-sen Portrait.jpg', '%1$s', '%2$s', datetime('now'), 'tester')",
				s0.resulvedVal("a_attaches", "attId"),
				s0.resulvedVal("a_users", "userId")),
				sqls.get(1));
		Connects.commit(usr , sqls);

		sqls.clear();
		
		String attId = (String)s0.resulvedVal("a_attaches", "attId");
		
		SResultset rs = (SResultset) st.select("a_attaches", "f")
			.col("uri").col("extFile(f.uri)", "b64")
			.whereEq("attId", attId)
			// .commit(sqls, usr);
			.rs(new DASemantext(connId, smtcfg, metas, usr, rtroot))
			.rs(0);

		// assert 2. verify file exists
		// SResultset rs = Connects.select(sqls.get(0));

		rs.beforeFirst().next(); // uri is relative path
		String fp = FilenameUtils.concat(rtroot, rs.getString("uri"));
		File f = new File(fp);
		assertTrue(f.exists());
		
		assertEquals("/9j/4AAQ",
				rs.getString("b64").substring(0, 8));
		assertEquals(2652, rs.getString("b64").length());
		
		st.delete("a_attaches", usr)
			.whereEq("attId", attId)
			.commit(sqls, usr)
			.d(s0.clone(usr));

		assertFalse(f.exists());
		
		// by the way, have a test on pc-del-tbl
		sqls.clear();
		st.delete("a_users")
			.whereEq("userId", "fake-userId")
			.commit(sqls);
		
		assertEquals(2, sqls.size());
		assertEquals("delete from a_attaches where busiId  in ( select userId from a_users  where userId = 'fake-userId' ) AND busiTbl = 'a_users'",
				sqls.get(0));
		assertEquals("delete from a_users where userId = 'fake-userId'",
				sqls.get(1));
	}

	private String readB64(String filename) throws IOException {
		Path p = Paths.get(filename);
		byte[] f = Files.readAllBytes(p);
		return AESHelper.encode64(f);
	}
}
