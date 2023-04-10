package io.odysz.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static io.odysz.common.Utils.*;
import static io.odysz.semantic.CRUD.*;
import static io.odysz.semantic.util.Assert.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import io.odysz.common.Configs;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PhotoMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynCleanMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

class DBSyntextTest {
	public static final String conn0 = "synode-main";
	public static final String conn1 = "synode-s1";
	public static final String conn2 = "synode-s2";
	public static final String conn3 = "synode-s3";
	public static final String rtroot = "src/test/res/";

	public static final String father = "/test/res/Sun Yet-sen.jpg";
	public static final String s0 = "s0";
	public static final String s1 = "s1";
	public static final String s2 = "s2";
	public static final String s3 = "s3";

	static String runtimepath;

	static DBSynsactBuilder trb0;
	static DBSynsactBuilder trb1;
	static DBSynsactBuilder trb2;
	static DBSynsactBuilder trb3;
	static IUser robot;
	static HashMap<String, DBSynmantics> synms;
	static HashMap<String, TableMeta> metas;


	static PhotoMeta phm;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;
	static SynCleanMeta clnm;

	static {
		try {
			printCaller(false);

			// File file = new File("src/test/res");
			File file = new File(rtroot);
			runtimepath = file.getAbsolutePath();
			logi(runtimepath);
			Configs.init(runtimepath);
			Connects.init(runtimepath);

			// load metas, then semantics
			DATranscxt.configRoot(rtroot, runtimepath);
			String rootkey = System.getProperty("rootkey");
			DATranscxt.key("user-pswd", rootkey);

			// smtcfg = DBSynsactBuilder.loadSynmantics(conn0, "src/test/res/synmantics.xml", true);
			trb0 = new DBSynsactBuilder(conn0);
			trb1 = new DBSynsactBuilder(conn1);
			trb2 = new DBSynsactBuilder(conn2);
			trb3 = new DBSynsactBuilder(conn3);
			metas = Connects.getMeta(conn0);
			
			phm = new PhotoMeta();
			metas.put(phm.tbl, phm);
			
			chm = new SynChangeMeta();
			metas.put(chm.tbl, chm);

			clnm = new SynCleanMeta();
			metas.put(clnm.tbl, clnm);
			
			sbm = new SynSubsMeta();
			metas.put(sbm.tbl, sbm);

			SemanticObject jo = new SemanticObject();
			jo.put("userId", "tester");
			SemanticObject usrAct = new SemanticObject();
			usrAct.put("funcId", "DASemantextTest");
			usrAct.put("funcName", "test ISemantext implementation");
			jo.put("usrAct", usrAct);
			robot = new LoggingUser(conn0, "src/test/res/semantic-log.xml", "tester", jo);
		} catch (SemanticException | SQLException | SAXException | IOException e) {
			e.printStackTrace();
		}
	
	}

	@BeforeAll
	public static void testInit()
			throws SQLException, SAXException, IOException, TransException {

		ArrayList<String> sqls = new ArrayList<String>();
		sqls.add( "CREATE TABLE oz_autoseq (\n"
				+ "  sid text(50),\n"
				+ "  seq INTEGER,\n"
				+ "  remarks text(200),\n"
				+ "  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid))");
		sqls.add( "CREATE TABLE a_logs (\n"
				+ "  logId text(20),\n"
				+ "  funcId text(20),\n"
				+ "  funcName text(50),\n"
				+ "  oper text(20),\n"
				+ "  logTime text(20),\n"
				+ "  cnt int,\n"
				+ "  txt text(4000),\n"
				+ "  CONSTRAINT oz_logs_pk PRIMARY KEY (logId))");

		sqls.add(SynChangeMeta.ddl);
		sqls.add(SynSubsMeta.ddl);
		sqls.add(SynCleanMeta.ddl);
		sqls.add(PhotoMeta.ddl);

		sqls.add( "delete from oz_autoseq;\n"
				+ "insert into oz_autoseq (sid, seq, remarks) values\n"
				+ "('h_photos.pid', 0, 'photo'),\n"
				+ "('a_logs.logId', 0, 'test');"
				);

		sqls.add(String.format("delete from %s", phm.tbl));
		sqls.add("delete from a_logs");

		Connects.commit(conn0, robot, sqls, Connects.flag_nothing);
		Connects.commit(conn1, robot, sqls, Connects.flag_nothing);
		Connects.commit(conn2, robot, sqls, Connects.flag_nothing);
		Connects.commit(conn3, robot, sqls, Connects.flag_nothing);
	}

	@Test
	void testChangeLog() throws TransException, SQLException {
		Ck c0 = new Ck(conn0);
		Ck c1 = new Ck(conn1);
		Ck c2 = new Ck(conn2);
		Ck c3 = new Ck(conn3);

		// 1. insert h_photos
		String pid = insertPhoto0();
		
		// 2.1. check syn_change added CRUD.C
		c0.changeC(pid);
		// 2.2. check syn_subscribe added s1, s2, s3
		c0.subs(pid, null, CRUD.C, C, C);

		// 3. udpate h_photos
		updatePoto0(pid);
		
		// 4. check CRUD.C over ride CRUD.U
		c0.changeC(pid);
		c0.subs_CCC(pid);
		
		// 5. s1 pull
		s1pull(pid);
		
		// 6. update h_photos
		updatePoto0(pid);

		// 7. check CRUD.C over ride CRUD.U
		c0.changeU(pid);
		c0.subs(pid, null, C, null, C);

		// 8. s2 pull
		String s2pid = s2pull();
		c0.changeU(pid);
		c0.subs(pid, null, U, null, C);
		c2.changeU(s2pid);
		c2.subs(s2pid, null, U, null, C);

		// 9. s1, s3 pull
		s1pull();
		c1.changeX(pid);

		s3pull();
		c3.changeX(pid);

		c0.changeX(pid);

		// s0: null, s1: null, s2: null, s3: null
		c0.subs(pid, null, null, null, null);

		// 10. s1 delete m1:clientpath
		s1delm1f();
		pid = s1push("m1", father);
		c0.changeD(pid);
		c0.clean(pid, null, null, D, D);
	}

	String s1push(String synode, String clientpath) {
		return null;
	}

	/**
	 * s1 delete s1:father
	 */
	void s1delm1f() {
	}

	void s3pull() {
	}

	String s2pull() {
		return null;
	}

	void s1pull() {
	}

	void s1pull(String pid) throws TransException {
		((DBSyntext) trb1.instancontxt(conn1, robot))
			.synPull(phm);
	}

	void updatePoto0(String pid) throws TransException, SQLException {
		trb0.update(phm.tbl, robot)
			.nv(phm.clientpath, father)
			.whereEq(chm.pk, pid)
			.u(trb0.instancontxt(conn0, robot))
			;
	}

	String insertPhoto0() throws TransException, SQLException {
		return ((SemanticObject) trb0
			.insert(phm.tbl, robot)
			// .nv(phm.uri, "")
			.nv(phm.clientpath, father)
			.ins(trb0.instancontxt(conn0, robot)))
			.resulve(phm.tbl, phm.pk);
	}
	
	public static class Ck {
		private final String connId;
		
		public Ck(String conn) {
			this.connId = conn;
		}

		public void changeC(String pid) throws TransException, SQLException {
			AnResultset chg = (AnResultset) trb0
				.select(chm.tbl, "ch")
				.cols(chm.cols())
				.whereEq(chm.recTabl, phm.tbl)
				.whereEq(chm.pk, pid)
				.rs(trb0.instancontxt(conn0, robot))
				.rs(0);
			
			chg.next();
			
			assertEquals(CRUD.C, chg.getString(chm.crud));
			assertEquals(phm.tbl, chg.getString(chm.recTabl));
			assertEquals(robot.deviceId(), chg.getString(chm.synoder));
		}

		/**
		 * h_photos[pid].crud = D, syn_change[pid].crud = D
		 * 
		 * @param pid
		 */
		public void changeD(String pid) {
		}

		public void subs_CCC(String pid) throws TransException, SQLException {
			AnResultset subs = (AnResultset) trb0
				.select(sbm.tbl, "ch")
				.col(Funcall.count(sbm.recId), "cnt")
				.cols(sbm.cols())
				.whereEq(sbm.recTabl, phm.tbl)
				.whereEq(sbm.pk, pid)
				.rs(trb0.instancontxt(connId, robot))
				.rs(0);
			
			subs.next();
		
			assertEquals(3, subs.getInt("cnt"));
			assertEquals(phm.tbl, subs.getString(sbm.recTabl));
			
			HashSet<String> s = subs.set(sbm.synodee); 
			assertIn("s3",
					 assertIn("s2",
					 assertIn("s1", s)));
		}

		/**
		 * Verify pid's change log is as updated.
		 * @param pid
		 */
		public void changeU(String pid) {
		}

		public void changeX(String pid) {
		}

		/**verify subscriptions.
		 * @param pid
		 * @param s subscriptions for s0/s1/s2/s3
		 */
		public void subs(String pid, String... s) { }

		/**verify cleanings:
		 * s0: null, s1: null, s2: D, s3: D
		 * @param pid
		 */
		public void clean(String pid, String ... s) { }
	}

}
