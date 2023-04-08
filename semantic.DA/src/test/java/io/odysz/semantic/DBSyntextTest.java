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
	public static final String conn0 = "synode-0";
	public static final String conn1 = "synode-1";
	public static final String conn2 = "synode-2";
	public static final String conn3 = "synode-3";
	public static final String rtroot = "src/test/res/";

	public static final String father = "src/test/res/Sun Yet-sen.jpg";
	public static final int X = 0;
	public static final int Y = 1;
	public static final int Z = 2;
	public static final int W = 3;
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

	static Ck c0;
	static Ck c1;
	static Ck c2;
	static Ck c3;

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
		
		
		c0 = new Ck(conn0);
		c1 = new Ck(conn1);
		c2 = new Ck(conn2);
		c3 = new Ck(conn3);
	}

	/**
	 * <pre>
	 *    a b c
	 *  A 0 0 0
	 *  B 0 0 0
	 *  C 0 0 0
	 * 
	 * A                                       | B
	 * crud, synode, pid, nyquence, subscribe  | crud, synode, pid, nyquence, subscribe
	 *  I,   A,      pA0, 0,         B         |  I,   B,      pB0, 0,         A 
	 *                               C         |                               C 
	 *                               D         |                               D 
	 * 
	 *    a b c
	 *  A 1 0 0
	 *  B 0 1 0
	 *  C 0 0 0
	 *  
	 * A                                        =) B
	 * crud, synode, pid, nyquence, subscribe   |  crud, synode, pid, nyquence, subscribe
	 *  I,   A,      pA0, 1,        [ ]         |   I,    B,     pB0,  1,        A 
	 *                               C          |                                C 
	 *                               D          |                                D 
	 *                                          |   I,    A,     pA0,  1,        C 
	 *                                          |                                D 
	 *                                          |   (B.a = A.a)
	 *
	 *    a b c
	 *  A 1 0 0
	 *  B 1 1 0
	 *  C 0 0 0
	 *  
	 * A                                        =) B
	 * crud, synode, pid, nyquence, subscribe   |  crud, synode, pid, nyquence, subscribe
	 *  I,    A,     pA0, 1,        [ ]         |   I,   B,      pB0, 1,        [ ]
	 *                               C          |                                C 
	 *                               D          |                                D 
	 *  I,    B,     pB0, 1,         C          |   I,   A,      pA0, 1,         C 
	 *                               D          |                                D 
	 *  (A.b = B.b)
	 *
	 *    a b c
	 *  A 1 1 0
	 *  B 1 1 0
	 *  C 0 0 0
	 * </pre>
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	void test01InsertBasic() throws TransException, SQLException {

		// 1. insert A
		String pA0 = insertPhotoA();
		
		// 1.1. syn_change.curd = C
		c0.changeC(pA0);
		// 1.2. syn_subscribe.to = [B, C, D]
		c0.subs(pA0, null, C, C, C);

		// 2. insert B
		String pB0 = insertPhotoA();

		// 1.1. syn_change.curd = C
		c1.changeC(pB0);
		// 1.2. syn_subscribe.to = [A, C, D]
		c1.subs(pB0, C, null, C, C);
		
		// 3.
		B_pullA();
		// 3.1. syn_change.curd = C
		c1.changeC(pA0);
		// 3.2. syn_subscribe.to = [C, D]
		c1.subs(pA0, null, C, C, C);
		// 3.3. B.a = A.a
		int Aa = c0.nyquence(X);
		assertEquals(Aa, c1.nyquence(X));
		
		// 4.
		B_pushA();
		// 4.1. syn_change.curd = C
		c1.changeC(pA0);
		// 4.2. syn_subscribe.to = [C, D]
		c1.subs(pA0, null, C, C, C);
		// 4.3. A.b = B.b
		int Bb = c1.nyquence(Y);
		assertEquals(Bb, c0.nyquence(Y));
	}

	/**
	 * <pre>
	 * A                     | B                    | C                    | D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  I,   A, pA0, 1, [ ]  |  I,   B, pB0, 1, [ ] |                      | 
	 *                   C   |                   C  |                      |
	 *                   D   |                   D  |                      |
	 *  I,   B, pB0, 1,  C   |  I,   A, pA0, 1,  C  |                      |
	 *                   D   |                   D  |                      |
	 *                         [B.a = A.a]
	 *    a b c d
	 *  A 1 1 0 0
	 *  B 1 1 0 0
	 *  C 0 0 0 0
	 *  D 0 0 0 0
	 *  
	 * B vs. C: B.n=1 > C.b, B =) C
	 * A                     | B                   =) C                    | D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  I,   A, pA0, 1, [ ]  |  I,   B, pB0, 1, [ ] |  I,   B, pB0, 1,     |
	 *                   C   |                  [-] |                  [ ] |
	 *                   D   |                   D  |                   D  |
	 *  I,   B, pB0, 1,  C   |  I,   A, pA0, 1,  C  |  I,   A, pA0, 1,     |
	 *                   D   |                   D  |                   D  |
	 *                                                [C.a = max(B.a, C.a), C.b = B.b, C.c, C.d = max(B.d, C.d)]
	 *    a b c d
	 *  A 1 1 0 0
	 *  B 1 1 0 0
	 *  C 1 1 0 0
	 *  D 0 0 0 0
	 *  
	 * A update pA0
	 * A
	 * crud, s, pid, n, sub  |
	 *  U,   A, pA0, 2,  B   |
	 *                   C   |
	 *                   D   |
	 *  I,   B, pB0, 1,  C   |
	 *                   D   |
	 *    a b c d
	 *  A 2 1 0 0
	 *  B 1 1 0 0
	 *  C 1 1 0 0
	 *  D 0 0 0 0
	 * 
	 * A                                                                  A=)D
	 * crud, s, pid, n, sub  |                                             | crud, s, pid, n, sub
	 *  U,   A, pA0, 2,  B   |                                             |  U,   A, pA0, 2,  B   (2>D.b=0)
	 *                   C   |                                             |                   C   (2>D.c=0)
	 *                  [ ]  |                                             |
	 *  I,   B, pB0, 1,  C   |                                             |  I,   B, pB0, 1,  C   (1>D.c=0)
	 *                  [ ]  |                                             |
	 *                                                                       [D.a=A.a, D.b=A.b]
	 *    a b c d
	 *  A 2 1 0 0
	 *  B 1 1 0 0
	 *  C 1 1 0 0
	 *  D 2 1 0 0
	 *
	 * B update pB0, C insert pC0
	 * A                     | B                    | C                    | D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *                       |  U,   B, pB0, 2,  A  |  I,   B, pB0, 1,     | 
	 *                       |                   C  |                  [ ] |
	 *                       |                   D  |                   D  |
	 *                       |  I,   A, pA0, 1,  C  |  I,   B, pA0, 1,     |
	 *                       |                   D  |                   D  |
	 *                       |                      |  I,   C, pC0, 1,  A  |
	 *                       |                      |                   B  |
	 *                       |                      |                   D  |
	 *                       
	 * B update pB0, C insert pC0, A vs. C, B vs. D
	 * </pre>
	 * @throws TransException
	 * @throws SQLException
	 */
	void test02UpdateTransit() throws TransException, SQLException {
	}
	
	void B_pullA() throws TransException {
		((DBSyntext) trb1.instancontxt(conn1, robot))
			.synPull(phm);
	}

	void B_pushA() { }

	void updatePoto0(String pid) throws TransException, SQLException {
		trb0.update(phm.tbl, robot)
			.nv(phm.clientpath, father)
			.whereEq(chm.pk, pid)
			.u(trb0.instancontxt(conn0, robot))
			;
	}

	String insertPhotoA() throws TransException, SQLException {
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
			
			assertEquals(C, chg.getString(chm.crud));
			assertEquals(phm.tbl, chg.getString(chm.recTabl));
			assertEquals(robot.deviceId(), chg.getString(chm.synoder));
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
		
		public int nyquence(int synode) {
			return 0;
		}
	}

}
