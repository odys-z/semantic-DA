package io.odysz.semantic;

import static io.odysz.common.CheapIO.readB64;
import static io.odysz.common.LangExt.eq;
import static io.odysz.common.Utils.loadTxt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import io.odysz.anson.Anson;
import io.odysz.common.AESHelper;
import io.odysz.common.Configs;
import io.odysz.common.DateFormat;
import io.odysz.common.EnvPath;
import io.odysz.common.FilenameUtils;
import io.odysz.common.Regex;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt.SemanticsMap;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.syn.T_DA_PhotoMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**
 * Test basic semantics for semantic-jserv.<br>
 * This source can be used as examples of how to use semantic-* java API.
 * 
 * @author odys-z@github.com
 */
public class DASemantextTest {
	public static final String connId = "local-sqlite";
	private static DATranscxt st;
	private static IUser usr;
	private static SemanticsMap smtcfg;

	public final static String rtroot = "src/test/res/";
	private static String runtimepath;
	private static T_DA_PhotoMeta phm;

	static {
		try {
			Utils.printCaller(false);

			File db = new File("src/test/res/semantic-DA.db");
			if (!db.exists())
				fail("Create res/semantic-DA.db, clean project and retry...");

			File file = new File(rtroot);
			runtimepath = file.getAbsolutePath();
			Utils.logi(runtimepath);
			Configs.init(runtimepath);
			Connects.init(runtimepath);

			// load metas, then semantics
			DATranscxt.configRoot(rtroot, runtimepath);
			String rootkey = System.getProperty("rootkey");
			DATranscxt.key("user-pswd", rootkey);

			smtcfg = DATranscxt.initConfigs(connId,// loadSemanticsXml(connId),
						(c) -> new SemanticsMap(c));
			st = new DATranscxt(connId);

			SemanticObject jo = new SemanticObject();
			jo.put("userId", "tester");
			SemanticObject usrAct = new SemanticObject();
			usrAct.put("funcId", "DASemantextTest");
			usrAct.put("funcName", "test ISemantext implementation");
			jo.put("usrAct", usrAct);
			usr = new LoggingUser(connId, "tester", jo);
			phm = new T_DA_PhotoMeta(connId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}

	/**
	 * Initializing testing DB (use this to reset semantic-DA.db - a sqlite3 db file).
	 * 
	 * @throws SQLException
	 * @throws SemanticException
	 * @throws SAXException
	 * @throws IOException
	 */
	@BeforeAll
	public static void testInit() throws SQLException, SemanticException, SAXException, IOException {
		ArrayList<String> sqls = new ArrayList<String>();

		try {
			// Thread.sleep(3000); // wait for previous tests
			for (String tbl : new String[] {
					// "oz_autoseq",
					"a_logs", "a_attaches",
					"a_domain", "a_functions", "a_orgs", "a_role_func", "a_roles", "a_users",
					"b_alarms", "b_alarm_logic", "b_logic_device",
					"crs_a", "crs_b", "h_photos", "doc_devices"}) {
				sqls.add("drop table if exists " + tbl);
				Connects.commit(usr, sqls, AbsConnect.flag_nothing);
				sqls.clear();
			}

			for (String tbl : new String[] {
					"oz_autoseq.ddl",/*"oz_autoseq.sql",*/   "a_logs.ddl",      "a_attaches.ddl",
					"a_domain.ddl",    "a_domain.sql",       "a_functions.ddl", "a_functions.sql",
					"a_orgs.ddl",      "a_orgs.sql",
					"a_role_func.ddl", "a_roles.ddl",        "a_users.ddl",     "b_alarm_logic.ddl",
					"b_alarms.ddl",    "b_logic_device.ddl", "crs_a.ddl",       "crs_b.ddl",
					"h_photos.ddl",    "doc_devices.ddl"}) {

				sqls.add(loadTxt(DASemantextTest.class, tbl));
				Connects.commit(usr, sqls, AbsConnect.flag_nothing);
				sqls.clear();
			}
//			Connects.reload(runtimepath); // reload metas
			st = new DATranscxt(connId);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testInsert() throws TransException, SQLException, SAXException, IOException {
		String flag = DateFormat.format(new Date());

		DASemantext s0 = new DASemantext(connId, smtcfg, usr, rtroot);
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_functions")
			.nv("flags", flag)
			// .nv("funcId", "AUTO") // Legacy
			.nv("funcName", "testInsert A - " + flag)
			.nv("parentId", "------")
			.commit(s0, sqls);
		
		// Utils.logi("New ID for a_functions: %s", s0.resulvedVal("a_functions", "funcId"));
		assertEquals(6, ((String) s0.resulvedVal("a_functions", "funcId", -1)).length());
		
		// level 2
		DASemantext s1 = new DASemantext(connId, smtcfg, usr, rtroot);
		st.insert("a_functions")
			.nv("flags", flag)
			// .nv("funcId", "AUTO")
			.nv("funcName", "testInsert B - " + flag)
			.nv("parentId", (String)s0.resulvedVal("a_functions", "funcId", -1))
			.commit(s1, sqls);
	
		// Utils.logi(sqls);
		Connects.commit(usr , sqls);

		// Utils.logi("New ID for a_functions: %s", s1.resulvedVal("a_functions", "funcId"));
		assertEquals(6, ((String) s1.resulvedVal("a_functions", "funcId", -1)).length());
	}

	@Test
	public void testBatch() throws TransException, SQLException, SAXException, IOException {
		DASemantext s0 = new DASemantext(connId, smtcfg,  usr, rtroot);
		ArrayList<String> sqls = new ArrayList<String>(1);
		Insert f1 = st.insert("a_role_func")
				.nv("funcId", "000001");
		Insert f2 = st.insert("a_role_func")
				.nv("funcId", "000002");
		Insert newuser = st.insert("a_roles")
				// .nv("roleId", "AUTO") // overridden by semantics.xml
				.nv("roleName", "Co-funder")
				.post(f1)
				.post(f2);
		newuser.commit(s0, sqls);
		Connects.commit(usr , sqls);
		
		DASemantext s1 = new DASemantext(connId, smtcfg, usr, rtroot);
		String newId = (String) s0.resulvedVal("a_roles", "roleId", -1);
		SemanticObject s = st
				.select("a_role_func", "rf")
				.col("count(funcId)", "cnt")
				.where("=", "rf.roleId", "'" + newId + "'")
				.where("=", "rf.funcId", "'000001'")
				.rs(s1);
		AnResultset slect = (AnResultset) s.rs(0);
		slect.printSomeData(false, 2, "cnt");

		slect.beforeFirst().next();
		assertEquals(1, slect.getInt("cnt"));
	}

	@Test
	public void testFullpath() throws TransException, SQLException {
		DASemantext s0 = new DASemantext(connId, smtcfg, usr, rtroot);
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_functions")
			// .nv("funcId", "AUTO")
			.nv("funcName", "testInsert A - ")
			.nv("parentId", "------")
			// for root inserted by a_functions.sql, fullpath = '-.000'
			.commit(s0, sqls);

		Connects.commit(usr , sqls); // must insert - parent exists when compose children's fullpaths
		
		String parentId = (String) s0.resulvedVal("a_functions", "funcId", -1);

		st.insert("a_functions")
			// .nv("funcId", "AUTO")
			.nv("funcName", "testInsert A - ")
			.nv("parentId", parentId)
			.nv("sibling", "1")
			.commit(s0, sqls);

		st.insert("a_functions")
			// .nv("funcId", "AUTO")
			.nv("funcName", "testInsert A - ")
			.nv("parentId", parentId)
			.nv("sibling", "2")
			.commit(s0, sqls);

		Regex reg = new Regex("insert into a_functions \\(funcName, parentId, funcId, fullpath\\) values \\('testInsert A - ', '------', '0000..', '-.000'\\)");
		assertTrue(reg.match(sqls.get(0)), sqls.get(0));

		reg = new Regex(".*000.001'\\)");
		assertTrue(reg.match(sqls.get(1)), ".*000.001'\\)");
		reg = new Regex(".*000.002'\\)");
		assertTrue(reg.match(sqls.get(2)), ".*000.002'\\)");
	}
	
	/**
	 * Test prefix.auto-key.
	 * 
	 * sk = semantics.xml/6h
	 * 
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	void testAutoKprefix() throws TransException, SQLException {
		String devname = DateFormat.formatime(new Date());
		
		String tbl = "doc_devices";

		st.insert(tbl, usr)
			.nv("synode0", "1.4.34") // varchar(6)
			.nv("devname", devname)
			.nv("owner",   usr.uid())
			.nv("org",     "zsu.ua")
			.ins(st.instancontxt(connId, usr));
	
		assertEquals("synode0",
			DATranscxt.getHandler(connId, "doc_devices", smtype.autoInc).args[2],
			"Check configuration: synode0");

		st.insert(tbl, usr)
			// synode0 == null, will hard code into 'synode0'
			.nv("devname", devname)
			.nv("owner",   usr.uid())
			.nv("org",     "zsu.ua")
			.ins(st.instancontxt(connId, usr));
	
		AnResultset rs = ((AnResultset) st
			.select(tbl)
			.col("device")
			.whereEq("devname", devname)
			.rs(st.instancontxt(connId, usr))
			.rs(0)).nxt();
		
		// oz_autoseq.sql: ('doc_devices.device', 64 * 64 * 4, 'device');
		assertTrue(eq("1.4.34.0004", rs.getString("device").subSequence(0, 11).toString())
				|| eq("1.4.34.000G", rs.getString("device").subSequence(0, 11).toString()), "000G01");
		rs.next();
		assertTrue(eq("synode0.0004", rs.getString("device").subSequence(0, 12).toString())
				|| eq("synode0.000G", rs.getString("device").subSequence(0, 12).toString()), "000402");
	}

	/**Test cross referencing auto k.<br>
	 * crs_a.aid, crs_b.bid are autok;<br>
	 * crs_a.afk referencing crs_b.bid, (pkref)<br>
	 * crs_b.bfk referencing crs_a.aid.<br>
	 * <p>Also, test int type's value (crs_a.testInt = 100) not single-quoted.<br>
	 * Test crs_a.fundDate(sqlite number) is quoted for both insert and update.</p>
	 * 
	 * <p>Also, test post-fk.<br>
	 * As pkref is a weak wiring up, it should do nothing when updating. </p>
	 * 
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	public void testCrossAutoK() throws TransException, SQLException {
		DASemantext s0 = new DASemantext(connId, smtcfg, usr, rtroot);
		ArrayList<String> sqls = new ArrayList<String>(1);
		Insert f1 = st.insert("crs_a")
				.nv("remarka", Funcall.now())
				.nv("fundDate", "1776-07-04")
				.nv("testInt", "100"); // testing that int shouldn't quoted
		st.insert("crs_b")
				.nv("remarkb", Funcall.now())
				.post(f1)
				.commit(s0, sqls);
		
		String aid = (String) s0.resulvedVal("crs_a", "aid", -1);
		String bid = (String) s0.resulvedVal("crs_b", "bid", -1);

		assertEquals(String.format(
			"update crs_b set bfk='%s' where bid = '%s'",
			aid, bid),
			sqls.get(2));
		assertEquals(String.format(
			"insert into crs_a (remarka, fundDate, testInt, aid, afk) values (datetime('now'), '1776-07-04', 100, '%s', '%s')",
			aid, bid),
			sqls.get(1));
		assertEquals(String.format(
			"insert into crs_b (remarkb, bid) values (datetime('now'), '%s')",
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
		assertEquals("update crs_a set fundDate='1911-10-10' where testInt = 100",
					sqls.get(0));
		
		sqls.clear();
		DASemantext s1 = new DASemantext(connId, smtcfg, usr, rtroot);
		st.insert("crs_b")
			.nv("remarkb", "1911-10-10")
			.post(st.update("crs_a")
					.nv("remarka", "update child")
					.where("=", "bid", new Resulving("crs_b", "bid")))
			.commit(s1, sqls);

		// insert into crs_b  (remarkb, bid) values ('1911-10-10', '00000p')
		// update crs_a  set remarka='update child' where bid = '00000o'
		bid = (String) s1.resulvedVal("crs_b", "bid", -1);
		assertEquals(2, sqls.size());
		assertEquals(String.format(
				"insert into crs_b (remarkb, bid) values ('1911-10-10', '%s')",
				bid), sqls.get(0));
		assertEquals(String.format(
				"update crs_a set remarka='update child' where bid = '%s'",
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
	 * @throws IOException 
	 * @throws GeneralSecurityException 
	 */
	@Test
	public void testSmtxUsers() throws TransException, SQLException, GeneralSecurityException, IOException {
		String flag = DateFormat.formatime(new Date());
		String usrName = "01 " + flag;

		DASemantext s0 = new DASemantext(connId, smtcfg, usr, rtroot);
		ArrayList<String> sqls = new ArrayList<String>(1);
		st.insert("a_users") // with default value: pswd = '123456'
			.nv("userName", usrName)
			.nv("roleId", "r01")
			.nv("orgId", "o-01")
			.nv("birthday", Funcall.now())
			.commit(s0, sqls);
		Connects.commit(usr , sqls);
		sqls.clear();
		
		String usrId = (String)s0.resulvedVal("a_users", "userId", -1);
		
		st.select("a_users", "u")
			.where("=", "userId", "'" + usrId + "'")
			.commit(sqls, usr);

		// assert 2 default value pswd = '123456' (iv = null)
		AnResultset rs = Connects.select(sqls.get(0));
		rs.beforeFirst().next();
		assertEquals("123456", rs.getString("pswd"));
		
		// assert 5. check count on insert: a_user.userName<br>
		sqls.clear();
		// s0.clear();
		s0 = new DASemantext(connId, smtcfg, usr, rtroot);

		try {
			st.insert("a_users")
				.nv("userName", usrName)
				.commit(s0, sqls);
			fail("check count on insert: a_user.userName not working");
		} catch (SemanticException e) {
			assertEquals("Checking count on a_users.userId",
					e.getMessage().substring(0, 32));
		}

		// assert 3 de-encrypt, (dencrypt with iv)
		// 3.1 insert with iv
		String clientKey = "odys-z.github.io";
		String rootK = DATranscxt.key("user-pswd");
		if (rootK == null) {
			// mvn clean test -Drootkey=*******
			Utils.warn("Please set a 16 bytes rootkey!\nFor maven testing: mvn test -Drootkey=*******");
			rootK = "odys-z.github.io";
		}

		byte[] iv = AESHelper.getRandom();
		String iv64 = AESHelper.encode64(iv);
		String pswdCipher = AESHelper.encrypt("abc123", clientKey, iv);
		// usr.sessionKey("odys-z.github.io");

		ISemantext s2 = st.instancontxt(connId, usr);
		String usr3 = ((SemanticObject) st.insert("a_users", usr)
			.nv("userName", "dencrypt " + flag)
			.nv("iv", iv64)
			.nv("pswd", pswdCipher)
			.ins(s2)).resulve("a_users", "userId", -1);
		
		// String usr3 = (String) s2.resulvedVal("a_users", "userId");
		rs = (AnResultset) st.select("a_users", "u")
			.col("pswd")
			.col("iv")
			.whereEq("userId", usr3)
			.rs(st.basictx())	// in case the semantics handler will decrypt it
			.rs(0);

		rs.beforeFirst().next();
		String pswd = rs.getString("pswd");
		assertNotEquals("abc123", pswd);

		iv = AESHelper.decode64(rs.getString("iv"));

		// 3.2 update with iv
		iv = AESHelper.getRandom();
		iv64 = AESHelper.encode64(iv);
		pswdCipher = AESHelper.encrypt("xyz999", clientKey, iv);

		ISemantext s3 = st.instancontxt(connId, usr);
		st.update("a_users", usr)
			.nv("userName", "dencrypt 2 " + flag)
			.nv("iv", iv64)
			.nv("pswd", pswdCipher)
			.whereEq("userId", usr3)
			.u(s3);

		rs = (AnResultset) st.select("a_users", "u")
			.col("pswd")
			.col("iv")
			.whereEq("userId", usr3)
			.rs(st.basictx())	// in case the semantics handler will decrypt it
			.rs(0);

		rs.beforeFirst().next();
		pswd = rs.getString("pswd");
		iv = AESHelper.decode64(rs.getString("iv"));
		assertNotEquals("abc123", AESHelper.decrypt(pswd, rootK, iv));

		testz04(usrId);
	}
		
	/**Test: parent-child on del
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
			
		ISemantext s1 = st.instancontxt(connId, usr);
		st.insert("a_roles", usr)
			.nv("roleName", roleId)
			.post(rf1).post(rf2)
			.ins(s1);
		
		String newRoleId = (String)s1.resulvedVal("a_roles", "roleId", -1);
		SemanticObject cnt = st.select("a_roles", "r")
			.col("count(r.roleId)", "cnt")
			.j("a_role_func", "rf", "rf.roleId = r.roleId")
			.where_("=", "r.roleId", newRoleId)
			.rs(st.instancontxt(connId, usr));
		AnResultset rs = (AnResultset) cnt.rs(0);
		rs.beforeFirst().next();
		// inserted two children
		assertEquals(2, rs.getInt("cnt"));
		
		st.delete("a_roles", usr)
			.where_("=", "roleId", newRoleId)
			.d(st.instancontxt(connId, usr));
		
		cnt = st.select("a_role_func", "rf")
				.col("count(*)", "cnt")
				.where_("=", "rf.roleId", newRoleId)
				.rs(s1);

		rs = (AnResultset) cnt.rs(0);
		rs.beforeFirst().next();
		assertEquals(0, rs.getInt("cnt"));
	}

	/**
	 * Test: parent-child on del check should working.
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	public void testChkOnDel() throws TransException, SQLException {
		ISemantext s1 = st.instancontxt(connId, usr);
		String typeId = "02-fault";	// Device Fault
		st.insert("b_alarms", usr)	// auto key id = 54
			.nv("typeId", typeId)
			.ins(s1);

		try {
			ISemantext s2 = st.instancontxt(connId, usr);
			st.delete("a_domain", usr)
				.where_("=", "domainId", typeId)
				.d(s2);
			
			Utils.warn("ever deleted d_domain table? fix with this:\n%s",
					"insert into a_domain(domainId, domainName, domainValue, sort, fullpath) values('02-fault', 'test: testChkOnDel()', null, 99, '99 02-fault')");
			fail("ck-cnt-del not working");
		}
		catch (SemanticException e) {
			assertTrue(e.getMessage().startsWith("a_domain.checkSqlCountOnDel: b_alarms "), e.getMessage());
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
		ISemantext s0 = st.instancontxt(connId, usr);
		st.insert("b_alarms", usr)
		  .nv("remarks", Funcall.now())
		  .nv("typeId", "02-alarm")

		  .post(st.insert("b_alarm_logic")	// child of b_alarms, auto key: logicId
				  .nv("remarks", "R1 " + dt)
				  .post(st.insert("b_logic_device")
						  .nv("remarks", "R1's device 1.1"))
				  .post(st.insert("b_logic_device")
						  .nv("remarks", "R1's ddevice 1.2"))

				  .post(st.insert("b_alarm_logic")
						  .nv("remarks", "L2 " + dt)
						  .post(st.insert("b_logic_device")
								  .nv("remarks", "L2's device 2.1"))
						  .post(st.insert("b_logic_device")
								  .nv("remarks", "L2's device 2.2"))
			)).ins(s0);

		// let's find out the last inserted into b_logic_device
		SemanticObject res = st.select("b_logic_device", "d")
			.col("max(deviceLogId)", "dlid")
			.where_("=", "alarmId", s0.resulvedVal("b_alarms", "alarmId", -1))
			.rs(st.instancontxt(connId, usr));
		AnResultset rs = (AnResultset) res.rs(0);
		rs.beforeFirst().next();
		// the max deviceLogId should be in s0.
		assertEquals(s0.resulvedVal("b_logic_device", "deviceLogId", -1), rs.getString("dlid"));
	}
	
	@Test
	public void testMultiChildInst() throws TransException, SQLException {
		ArrayList<String> sqls = new ArrayList<String>(1);
		String dt = DateFormat.format(new Date());
		DASemantext s0 = new DASemantext(connId, smtcfg, usr, rtroot);
		st.insert("b_alarms")
				.nv("remarks", Funcall.now())
				.nv("typeId", "02-alarm")
				.post(st.insert("b_alarm_logic")	// child of b_alarms, auto key: logicId
						.nv("remarks", "R1 " + dt)
						.post(st.insert("b_alarm_logic")
								.nv("remarks", "L2 " + dt)
				)).commit(s0, sqls);

		assertEquals(String.format("insert into b_alarms (remarks, typeId, alarmId) values (datetime('now'), '02-alarm', '%s')",
				s0.resulvedVal("b_alarms", "alarmId", -1)),
				sqls.get(0));
		// the first insert b_alarm_logic must correct if following is ok.
		// Utils.logi(sqls.get(1));
		String alarmId = (String) s0.resulvedVal("b_alarms", "alarmId", -1);
		assertEquals(String.format("insert into b_alarm_logic (remarks, logicId, alarmId) values ('L2 %s', '%s', '%s')",
				dt, s0.resulvedVal("b_alarm_logic", "logicId", -1), alarmId),
				sqls.get(2));
		
		// Note Jun 21, 2021, FIXME can we support this?
		// test case
		// because b_alarm is updating, no auto key generated,
		// so child fk should provided by client, and won't been resulved.
		sqls.clear();
		DASemantext s1 = new DASemantext(connId, smtcfg, usr, rtroot);
		st.update("b_alarms")
			.nv("remarks", "updated")
			.where_("=", "alarmId", alarmId)
			.post(st.delete("b_alarm_logic")
					.where_("=", "alarmId", alarmId)
					.post(st.insert("b_alarm_logic")
							 .nv("remarks", "L3 " + dt)
							 .nv("alarmId", alarmId))) // because b_alarm is updating, no auto key there.
			.commit(s1, sqls);

		// Utils.logi(sqls);
		// update b_alarms  set remarks='updated' where alarmId = '000010'
		// delete from b_alarm_logic where alarmId = '000010'
		// insert into b_alarm_logic  (remarks, alarmId, logicId) values ('L3 2019-05-20', '000010', '00003N')
		assertEquals(String.format("insert into b_alarm_logic (remarks, alarmId, logicId) values ('L3 %s', '%s', '%s')",
				dt, alarmId, s1.resulvedVal("b_alarm_logic", "logicId", -1)),
				sqls.get(2));
	}
	
	@SuppressWarnings("serial")
	@Test
	public void testMuiltiRowsInsert() throws TransException, SQLException, IOException {
		ArrayList<String> sqls = new ArrayList<String>(1);
		String dt = DateFormat.format(new Date());
		DASemantext s0 = new DASemantext(connId, smtcfg, usr, rtroot);
		st.insert("b_alarms")
				.nv("remarks", Funcall.now())
				.nv("typeId", "02-alarm")
				.post(st.insert("b_alarm_logic")	// child of b_alarms, auto key: logicId
						.cols(new String[] {"remarks"} )
						.value(new ArrayList<Object[]>() { {add(new String[] {"remarks", "R1 " + dt});} })
						.value(new ArrayList<Object[]>() { {add(new String[] {"remarks", "R2 " + dt});} })
						.post(st.insert("b_alarm_logic")
								.nv("remarks", "L2 " + dt)
				)).commit(s0, sqls);

		assertEquals(String.format("insert into b_alarms (remarks, typeId, alarmId) values (datetime('now'), '02-alarm', '%s')",
				s0.resulvedVal("b_alarms", "alarmId", -1)),
				sqls.get(0));
	}
	
	@Test
	public void testDeleteBatchSelect() throws TransException, SQLException {
		DASemantext ctx = new DASemantext(connId, smtcfg, usr, rtroot);
		String alarmA = ((SemanticObject) st.insert("b_alarms", usr)
			.nv("typeId", "batch-q-x")
			.nv("remarks", "-------")
			.ins(st.instancontxt(connId, usr)))
			.resulve("b_alarms", "alarmId", 0);

		st.insert("b_alarms", usr)
			.nv("typeId", "batch-q-y")
			.nv("remarks", "+++++++")
			.ins(st.instancontxt(connId, usr));
		
		ctx = new DASemantext(connId, smtcfg, usr, rtroot);
		AnResultset rs = (AnResultset) st.batchSelect("b_alarms", "a")
			.cols("typeId", "remarks")
			.before(st.delete("b_alarms")
					.whereEq("typeId", "batch-q-y"))
			.whereEq("typeId", "batch-q-y")
			.rs(ctx)
			.rs(0);
		
		assertFalse(rs.next());

		rs = ((AnResultset) st.batchSelect("b_alarms", "a")
			.cols("typeId", "remarks", "alarmId")
			.before(st.delete("b_alarms")
					.whereEq("typeId", "batch-q-y"))
			.whereEq("typeId", "batch-q-x")
			.rs(ctx)
			.rs(0))
			.nxt();
		
		assertEquals(1, rs.getRowCount());
		assertEquals(alarmA, rs.getString("alarmId"));
		assertEquals("batch-q-x", rs.getString("typeId"));
		assertEquals("-------", rs.getString("remarks"));
		
		
		rs = ((AnResultset) st.batchSelect("b_alarms", "a")
			.page(0, 2)
			.cols("typeId", "remarks", "alarmId")
			.before(st.delete("b_alarms")
					.whereEq("typeId", "batch-q-y"))
			.whereEq("typeId", "batch-q-x")
			.rs(ctx)
			.rs(0))
			.nxt();
			
		assertEquals(1, rs.getRowCount());
		assertEquals(alarmA, rs.getString("alarmId"));
		assertEquals("batch-q-x", rs.getString("typeId"));
		assertEquals("-------", rs.getString("remarks"));
		
		rs = ((AnResultset) st.select("b_alarms", "a")
			.page(0, 2)
			.cols("typeId", "remarks", "alarmId")
			.whereEq("typeId", "batch-q-x")
			.rs(ctx)
			.rs(0))
			.nxt();
			
		assertEquals(1, rs.getRowCount());
		assertEquals(alarmA, rs.getString("alarmId"));
		assertEquals("batch-q-x", rs.getString("typeId"));
		assertEquals("-------", rs.getString("remarks"));
				
		// TODO testpaging
		// TODO testpaging
		// TODO testpaging
		// TODO testpaging
		// TODO testpaging
		// TODO testpaging
	}

	@SuppressWarnings("serial")
	@Test
	public void testMuiltiInsOpertime() throws TransException, SQLException, IOException {
		DASemantext s0 = new DASemantext(connId, smtcfg, usr, rtroot);
		ArrayList<String> sqls = new ArrayList<String>(1);

		ArrayList<Object[]> r0 = (new ArrayList<Object[]>() { {
			add(new Object[] {"attName", "att0"});
			add(new Object[] {"busiTbl", "test 0"});
			add(new Object[] {"busiId", "deleting"}); }
		});
		ArrayList<Object[]> r1 = (new ArrayList<Object[]>() { {
			add(new Object[] {"attName", "att1"});
			add(new Object[] {"busiTbl", "test 1"});
			add(new Object[] {"busiId", "deleting"}); }
		});
		ArrayList<Object[]> r2 = (new ArrayList<Object[]>() { {
			add(new Object[] {"attName", "att2"});
			add(new Object[] {"busiTbl", "test 2"});
			add(new Object[] {"busiId", "deleting"}); }
		});

		st.insert("a_attaches")
			.cols("attName", "check optime", "busiTbl", "busiId")
			//.nv("uri", readB64("src/test/res/Sun Yet-sen.jpg"));
			.value(r0).value(r1).value(r2)
			.commit(s0, sqls);

		assertTrue(sqls.get(0).endsWith(String.format(
				"datetime('now'), 'tester'), ('att2', null, 'test 2', 'deleting', '%s', datetime('now'), 'tester')",
				s0.resulvedVal("a_attaches", "attId", -1))));
	}

	/**
	 * Test deserialize Anson instance from DB field.
	 * 
	 * <h6>Note:<br>
	 * New line character ('\n') is not the same with the value before saving.</h6>
	 * <pre>
	assertEquals("104° 0' 11.23\"", exif.exif.get("GPS:GPS Longitude"));
	assertEquals("E", exif.exif.get("GPS:GPS Longitude Ref"));
	assertEquals("30° 40' 11.88\"", exif.exif.get("GPS:GPS Latitude"));
	assertEquals("Below sea level", exif.exif.get("GPS:GPS Altitude Ref"));
	assertEquals("v\\nv", exif.exif.get("(RGB\\nabc\\n123)"));
	assertEquals("0 metres", exif.exif.get("Altitude"));
	 * </pre>
	 * @since 1.4.27
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	public void testAnsonField() throws TransException, SQLException {
		DASemantext s0 = new DASemantext(connId, smtcfg, usr, rtroot);

		st.delete("b_alarms", usr)
			.whereIn("typeId", new String[] {"02-photo", "03-photo"})
			.d(s0);

		st.insert("b_alarms", usr)
			.nv("remarks", new T_PhotoCSS(16, 9))
			.nv("typeId", "02-photo")
			.ins(s0);

		AnResultset rs = ((AnResultset) st.select("b_alarms")
			.col("remarks")
			.whereEq("typeId", "02-photo")
			.rs(s0)
			.rs(0))
			.nxt();
		
		T_PhotoCSS anson = rs.<T_PhotoCSS>getAnson("remarks");
		assertEquals(16, anson.w());
		assertEquals( 9, anson.h());

		st.update("b_alarms", usr)
			.nv("remarks", new T_Exifield()
					.add("GPS:GPS Longitude", "104° 0' 11.23\"")
					.add("GPS:GPS Longitude Ref", "E")
					.add("GPS:GPS Latitude", "30° 40' 11.88\"")
					.add("GPS:GPS Altitude Ref", "Below sea level")
					.add("(RGB\nabc\n123)", "v\nv")
					.add("Altitude", "0 metres"))
			.nv("typeId", "02-photo")
			.whereEq("typeId", "02-photo")
			.u(s0);

		rs = ((AnResultset) st.select("b_alarms")
			.col("remarks")
			.whereEq("typeId", "02-photo")
			.rs(s0)
			.rs(0))
			.nxt();

		T_Exifield exif = rs.<T_Exifield>getAnson("remarks");
		assertEquals("104° 0' 11.23\"", exif.exif.get("GPS:GPS Longitude"));
		assertEquals("E", exif.exif.get("GPS:GPS Longitude Ref"));
		assertEquals("30° 40' 11.88\"", exif.exif.get("GPS:GPS Latitude"));
		assertEquals("Below sea level", exif.exif.get("GPS:GPS Altitude Ref"));
		assertEquals("v\nv", exif.exif.get("(RGB\\nabc\\n123)"));
		assertEquals("0 metres", exif.exif.get("Altitude"));
		
		st.delete("b_alarms", usr)
			.whereIn("typeId", new String[] {"02-photo", "03-photo"})
			.d(s0);
	}
	
	/**
	 * Can't work on Windows.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testExtfilePathHandler() throws Exception {
		// setEnv2("VOLUME_HOME", "/home/ody/volume");
		EnvPath.extendEnv("VOLUME_HOME", "/home/ody/volume");

		String[] args = "$VOLUME_HOME/shares,uri,userId,cate,docName".split(",");
		String extroot = args[ShExtFilev2.ixExtRoot];
		
		// String encoded = EnvPath.encodeUri(extroot, "ody", "000001 f.txt");
		String encoded = FilenameUtils.concat(extroot, "ody", "000001 f.txt");
		assertTrue( eq("$VOLUME_HOME/shares/ody/000001 f.txt", encoded) ||
					eq("$VOLUME_HOME\\shares\\ody\\000001 f.txt", encoded));

		String abspath = EnvPath.decodeUri("", encoded);
		assertTrue( eq("/home/ody/volume/shares/ody/000001 f.txt", abspath) ||
					eq("\\home\\ody\\volume\\shares\\ody\\000001 f.txt", abspath));
		
		args = "upload,uri,userId,cate,docName".split(",");
		// encoded = EnvPath.encodeUri(extroot, "admin", "000002 f.txt");
		encoded = FilenameUtils.concat(extroot, "admin", "000002 f.txt");
		assertTrue( eq("$VOLUME_HOME/shares/admin/000002 f.txt", encoded) ||
					eq("$VOLUME_HOME\\shares\\admin\\000002 f.txt", encoded));

		abspath = EnvPath.decodeUri(rtroot, encoded);
		// assertEquals("src/test/res/upload/admin/000002 f.txt", abspath);
		assertTrue( eq("/home/ody/volume/shares/admin/000002 f.txt", abspath) ||
					eq("\\home\\ody\\volume\\shares\\admin\\000002 f.txt", abspath));

		args = "/home/ody/upload,uri,userId,cate,docName".split(",");
		// encoded = EnvPath.encodeUri(extroot, "admin", "000003 f.txt");
		encoded = FilenameUtils.concat(extroot, "admin", "000003 f.txt");
		// assertEquals("/home/ody/upload/admin/000003 f.txt", encoded);
		assertTrue( eq("$VOLUME_HOME/shares/admin/000003 f.txt", encoded) ||
					eq("$VOLUME_HOME\\shares\\admin\\000003 f.txt", encoded));

		abspath = EnvPath.decodeUri(rtroot, encoded);
		assertTrue( eq("/home/ody/volume/shares/admin/000003 f.txt", abspath) ||
					eq("\\home\\ody\\volume\\shares\\admin\\000003 f.txt", abspath));
		
		// Override
		System.setProperty("VOLUME_HOME", "/home/alice/vol");
		abspath = EnvPath.decodeUri(rtroot, encoded);
		assertTrue( eq("/home/alice/vol/shares/admin/000003 f.txt", abspath) ||
					eq("\\home\\alice\\vol\\shares\\admin\\000003 f.txt", abspath));
	}
	
	/**
	 * @throws TransException
	 * @throws SQLException
	 * @throws IOException
	 */
	@Test
	public void testExtfile() throws TransException, SQLException, IOException {
		String flag = DateFormat.formatime(new Date());
		String usrName = "attached " + flag;

		DASemantext s0 = new DASemantext(connId, smtcfg, usr, runtimepath);
		ArrayList<String> sqls = new ArrayList<String>(1);

		// 1
		st.insert("a_users") // with default value: pswd = '123456'
			.nv("userName", usrName)
			.nv("roleId", "attach-01")
			.nv("orgId", "R.C.")
			.nv("birthday", Funcall.toDate("1866-12-12"))
			.post(st.insert("a_attaches") // ext-paths updated since 1.5.18, see also DocRefTest.
					.nv("attName", "Sun Yet-sen Portrait.jpg")  // name: portrait
					.nv("busiTbl", "a_users")
					// Since attached file pattern no longer supported in DA v1.3.3, "busiId" are only used for sub folder and not for resulving FK.
					// .nv("busiId", new Resulving("a_users", "userId"))
					.nv("busiId", "res")
					.nv("uri", readB64("src/test/res/Sun Yet-sen.jpg")))
			.commit(s0, sqls);

		// insert into a_users  (userName, roleId, orgId, birthday, userId, pswd) values ('attached 2019-06-12 18:20:33', 'attach-01', 'R.C.', datetime('1866-12-12'), '00001R', '123456')
		// insert into a_attaches  (attName, busiTbl, uri, attId, busiId, optime, oper)
		// values ('Sun Yet-sen Portrait.jpg', 'a_user', 'uploads/a_user/00001C Sun Yet-sen Portrait.jpg', '00001C', '00001R', datetime('now'), 'tester')
		assertEquals(String.format(
				"insert into a_attaches (attName, busiTbl, busiId, uri, attId, optime, oper) " +
				"values ('Sun Yet-sen Portrait.jpg', 'a_users', '%1$s', 'uploads/a_users/%2$s/%3$s Sun Yet-sen Portrait.jpg', '%3$s', datetime('now'), 'tester')",
				s0.resulvedVal("a_users", "userId", -1),
				"res",
				s0.resulvedVal("a_attaches", "attId", -1)),
				sqls.get(1));
		Connects.commit(usr , sqls);

		sqls.clear();
		
		String attId = (String)s0.resulvedVal("a_attaches", "attId", -1);
		
		// v0.9.8 also test select union with extFile
		Query q = st.select("a_attaches", "f2")
			.col("uri").col("extFile(f2.uri)", "b64")
			.whereEq("attId", "union test")
			;
		AnResultset rs = (AnResultset) st.select("a_attaches", "f")
			.col("uri").col("extFile(f.uri)", "b64")
			.whereEq("attId", attId)
			.union(q)
			.rs(new DASemantext(connId, smtcfg, usr, rtroot))
			.rs(0);

		// assert 2. verify file exists
		// AnResultset rs = Connects.select(sqls.get(0));

		rs.beforeFirst().next(); // uri is relative path

		String uri1 = rs.getString("uri");
		String fp = EnvPath.decodeUri(rtroot, uri1);
		assertEquals(FilenameUtils.concat(rtroot, uri1), fp);

		File f = new File(fp);
		assertTrue(f.exists());
		
		assertEquals("/9j/4AAQ",
				rs.getString("b64").substring(0, 8));
		assertEquals(2652, rs.getString("b64").length());
		
		// 2
		st.update("a_attaches", usr)
		  .nv("attName", "Sun Yet-sen Portrait.jpg")
		  .nv("busiTbl", "a_folder2")
		  .nv("busiId", "res")
		  // .nv("uri", uri1)
		  .whereEq("attId", attId)
		  // .commit(sqls, usr)
		  .u(s0.clone(usr));
		
		assertFalse(f.exists());

		rs = (AnResultset) st.select("a_attaches", "f")
				.col("uri").col("extFile(f.uri)", "b64")
				.whereEq("attId", attId)
				.rs(new DASemantext(connId, smtcfg, usr, rtroot))
				.rs(0);
			
		rs.next();
		fp = EnvPath.decodeUri(rtroot, rs.getString("uri"));

		f = new File(fp);
		assertTrue(f.exists());

		// 3
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
		assertEquals("delete from a_attaches where busiId in  ( select userId from a_users  where userId = 'fake-userId' ) AND busiTbl = 'a_users'",
				sqls.get(0));
		assertEquals("delete from a_users where userId = 'fake-userId'",
				sqls.get(1));
	}

	@Test
	public void testExtfilev2() throws TransException, SQLException, IOException {
		// h_photo will triggering table stamps 
		SyncTestRobot usr = new SyncTestRobot("robot").device("test");

		DASemantext s0 = new DASemantext(connId, smtcfg, usr, runtimepath);
		ArrayList<String> sqls = new ArrayList<String>(1);

		// 1
		// <args>uploads,uri,family,shareby,month,docname</args>
		String content64 = readB64("src/test/res/Sun Yet-sen.jpg");

		st.insert(phm.tbl)
			.nv(phm.org, "zsu.ua")
			.nv(phm.shareby, "ody")
			.nv(phm.folder, "2022-10")
			.nv(phm.resname, "Sun Yet-sen.jpg")
			.nv(phm.uri, content64)
			.commit(s0, sqls);

		String pid = (String) s0.resulvedVal(phm.tbl, phm.pk, -1);

		assertEquals(String.format(
				"insert into h_photos (family, shareby, folder, docname, uri, pid) " +
				"values ('zsu.ua', 'ody', '2022-10', 'Sun Yet-sen.jpg', " +
				"'uploads/zsu.ua/ody/2022-10/%1$s Sun Yet-sen.jpg', " +
				"'%1$s')", pid),
				sqls.get(0));
		Connects.commit(usr , sqls);

		sqls.clear();
		
		AnResultset rs = (AnResultset) st
			.select(phm.tbl, "f2")
			.col(phm.uri).col("extFile(f2.uri)", "b64")
			.whereEq(phm.pk, pid)
			.rs(new DASemantext(connId, smtcfg, usr, rtroot))
			.rs(0);

		rs.beforeFirst().next(); // uri is relative path

		String uri1 = rs.getString("b64");
		assertEquals("/9j/4AAQ",
				rs.getString("b64").substring(0, 8));
		assertEquals(2652, uri1.length());
		
		String fp1 = EnvPath.decodeUri(rtroot, rs.getString(phm.uri));
		File f1 = new File(fp1);
		assertTrue(f1.exists(), fp1);
		
		// 2 read
		// uploads/zsu.ua/ody/2022-10/000001 Sun Yet-sen.jpg
		// 2.1 extfile()
		assertEquals(String.format("uploads/zsu.ua/ody/2022-10/%s Sun Yet-sen.jpg", pid),
				DAHelper.getValstr(st, connId, phm, phm.uri, phm.pk, pid));
		assertEquals(content64,
				DAHelper.getExprstr(st, connId, phm,
					Funcall.extfile(phm.uri), phm.uri,
					phm.pk, pid));
		
		// 2.2 refile()
		String refstr = (String) DAHelper.getExprstr(st, connId, phm,
					Funcall.refile(new T_SyndocRef("X29", phm, st.instancontxt(connId, usr))), phm.uri,
					phm.pk, pid);

		// 1.5.18, semantic-transact 1.5.60
		// test syndoc reference object
		T_SyndocRef ref = (T_SyndocRef) Anson.fromJson(refstr);
		assertEquals("X29", ref.synode);
		assertEquals("h_photos", ref.tbl);
		assertEquals(pid, ref.docId);
		assertEquals(null, ref.uids); // Tested in DBSyn2tableTest

		// 3 move
		st.update(phm.tbl, usr)
		  .nv(phm.resname, "Volodymyr Zelensky.jpg")
		  .nv(phm.org, "zsu.ua")
		  .nv(phm.folder, "2022-10")
		  .nv(phm.shareby, "Zelensky")
		  .whereEq(phm.pk, pid)
		  .u(s0.clone(usr));
		
		rs = (AnResultset) st.select(phm.tbl, "f")
				.col(phm.uri).col("extFile(f.uri)", "b64")
				.whereEq(phm.pk, pid)
				.rs(new DASemantext(connId, smtcfg, usr, rtroot))
				.rs(0);
			
		rs.next();
		String fp2 = EnvPath.decodeUri(rtroot, rs.getString(phm.uri));

		assertFalse(f1.exists(), fp1);
		File f2 = new File(fp2);
		assertTrue(f2.exists(), fp2);

		// 4 delete
		st.delete(phm.tbl, usr)
			.whereEq(phm.pk, pid)
			.commit(sqls, usr)
			.d(s0.clone(usr));

		assertFalse(f2.exists());
		
		assertEquals(String.format("delete from h_photos where pid = '%s'", pid),
				sqls.get(0));
	}
}
