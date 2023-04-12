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
	public static final String[] conns = new String[4];
	public static final String rtroot = "src/test/res/";

	public static final String father = "src/test/res/Sun Yet-sen.jpg";
	public static final int X = 0;
	public static final int Y = 1;
	public static final int Z = 2;
	public static final int W = 3;
	static String runtimepath;

	static DBSynsactBuilder trbs[];
	static IUser robot;
	static HashMap<String, DBSynmantics> synms;
	static HashMap<String, TableMeta> metas;


	static T_PhotoMeta phm;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;
	/** @deprecated */
	static SynCleanMeta clnm;

	static Ck[] c;

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
			for (int s = 0; s < 4; s++) {
				conns[s] = String.format("synode-%s", s);
				trbs[s] = new DBSynsactBuilder(conns[s]);
				c[s] = new Ck(s);
			}
			metas = Connects.getMeta(conns[0]);

			phm = new T_PhotoMeta();
			metas.put(phm.tbl, phm);

			chm = new SynChangeMeta();
			metas.put(chm.tbl, chm);

			sbm = new SynSubsMeta();
			metas.put(sbm.tbl, sbm);

			SemanticObject jo = new SemanticObject();
			jo.put("userId", "tester");
			SemanticObject usrAct = new SemanticObject();
			usrAct.put("funcId", "DASemantextTest");
			usrAct.put("funcName", "test ISemantext implementation");
			jo.put("usrAct", usrAct);
			robot = new LoggingUser(conns[X], "src/test/res/semantic-log.xml", "tester", jo);
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

		sqls.add(SynChangeMeta.ddlSqlite);
		sqls.add(SynSubsMeta.ddlSqlite);
		sqls.add(SynCleanMeta.ddlSqlite);
		sqls.add(T_PhotoMeta.ddlSqlite);

		sqls.add( "delete from oz_autoseq;\n"
				+ "insert into oz_autoseq (sid, seq, remarks) values\n"
				+ "('h_photos.pid', 0, 'photo'),\n"
				+ "('a_logs.logId', 0, 'test');"
				);

		sqls.add(String.format("delete from %s", phm.tbl));
		sqls.add("delete from a_logs");

		for (int s = 0; s < 4; s++)
			Connects.commit(conns[s], robot, sqls, Connects.flag_nothing);
	}

	/**
	 * <pre>
	 *    a b c
	 *  A 0 0 0
	 *  B 0 0 0
	 *  C 0 0 0
	 * 1.
	 * A                     | B
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub
	 *  I,   A, pA0, 0,  B   |  I,   B, pB0, 0, A
	 *                   C   |                  C
	 *                   D   |                  D
	 *  A.a++, B.b++
	 *    a b c
	 *  A 1 0 0
	 *  B 0 1 0
	 *  C 0 0 0
	 *  
	 * 2.
	 * A                     =) B
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub
	 *  I,   A, pA0, 1, [ ]  |  I,   B, pB0, 1,  A
	 *                   C   |                   C
	 *                   D   |                   D
	 *                       |  I,   A, pA0, 1,  C
	 *                       |                   D
	 *    a b c
	 *  A 1 0 0
	 *  B 1 1 0  [B.a = A.a]
	 *  C 0 0 0
	 *
	 * 3.
	 * A                    (=  B
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub
	 *  I,   A, pA0, 1, [ ]  |  I,   B, pB0, 1, [ ]
	 *                   C   |                   C
	 *                   D   |                   D
	 *  I,   B, pB0, 1,  C   |  I,   A, pA0, 1,  C
	 *                   D   |                   D
	 *    a b c
	 *  A 1 1 0  [A.b = B.b]
	 *  B 1 1 0
	 *  C 0 0 0
	 * </pre>
	 * @throws TransException
	 * @throws SQLException
	 */
	@Test
	void test01InsertBasic() throws TransException, SQLException {

		// 1.1 insert A
		String pA0 = insertPhoto(X);

		// syn_change.curd = C
		c[X].change(C, pA0);
		// syn_subscribe.to = [B, C, D]
		c[X].subs(pA0, -1, Y, X, Z);

		// 1.2 insert B
		String pB0 = insertPhoto(Y);

		// syn_change.curd = C
		c[Y].change(C, pB0);
		// syn_subscribe.to = [A, C, D]
		c[Y].subs(pB0, X, -1, Z, W);

		// 2.
		BvsA(X, Y);
		c[Y].change(C, pA0);
		c[Y].subs(pA0, -1, -1, Z, W);
		// B.a = A.a
		int Aa = c[X].nyquence(X);
		assertEquals(Aa, c[Y].nyquence(X));

		c[X].change(C, pA0);
		c[X].subs(pA0, -1, -1, Z, W);
		// A.b = B.b
		int Bb = c[Y].nyquence(Y);
		assertEquals(Bb, c[X].nyquence(Y));
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
	 *
	 *    a b c d
	 *  A 1 1 0 0
	 *  B 1 1 0 0
	 *  C 0 0 0 0
	 *  D 0 0 0 0
	 *
	 * B vs. C: B.ch.n=1 > C.b, B => C, ++C
	 * A                     | B                   =) C                    | D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  I,   A, pA0, 1, [ ]  |  I,   B, pB0, 1, [ ] |  I,   B, pB0, 1,     |
	 *                   C   |                  [-] |                  [ ] |
	 *                   D   |                   D  |                   D  |
	 *  I,   B, pB0, 1,  C   |  I,   A, pA0, 1,  C  |  I,   A, pA0, 1,     |
	 *                   D   |                   D  |                   D  |
	 *
	 *    a b c d
	 *  A 1 1 0 0
	 *  B 1 1 0 0
	 *  C 1 1 1 0   [C.a = max(ch[A].n), C.b = B.b, ++C.c, C.d]
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
	 *  A 2 1 0 0   ++A.a
	 *  B 1 1 0 0
	 *  C 1 1 0 0
	 *  D 0 0 0 0
	 *
	 * A vs D, max(ch[A].n)=2 > D.a, max(ch[B].n)=1 > D.b
	 * A                                                                  A=)D
	 * crud, s, pid, n, sub  |                                             | crud, s, pid, n, sub
	 *  U,   A, pA0, 2,  B   |                                             |  U,   A, pA0, 2,  B   (2>D.a=0)
	 *                   C   |                                             |                   C
	 *                  [ ]  |                                             |
	 *  I,   B, pB0, 1,  C   |                                             |  I,   B, pB0, 1,  C   (1>D.c=0)
	 *                  [ ]  |                                             |
	 *    a b c d
	 *  A 2 1 0 0 
	 *  B 1 1 0 0
	 *  C 1 1 0 0
	 *  D 2 1 0 1   [D.a=max(ch[A].n)=2, D.b=max(ch[B].n)=1, D.c (ch[C]==NULL), ++D.d]
	 *
	 * B update pA0 twice, C insert pC0
	 * A                     | B                    | C                    | D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  U,   A, pA0, 2,  B   |  I,   B, pB0, 1,  A  |  I,   B, pB0, 1,     |  U,   A, pA0, 2,  B
	 *                   C   |                   C  |                  [ ] |                   C
	 *                   D   |                   D  |                   D  |
	 *  I,   B, pB0, 1,  C   |  UU,  B, pA0, 3,  C  |  I,   A, pA0, 1,     |  I,   B, pB0, 1,  C
	 *                  [ ]  |                   D  |                   D  |
	 *                       |                      |  I,   C, pC0, 1,  A  |
	 *                       |                      |                   B  |
	 *                       |                      |                   D  |
	 *    a b c d
	 *  A 2 1 0 0
	 *  B 1 3 0 0   ++B.b, ++B.b
	 *  C 1 1 1 0   ++C.c
	 *  D 2 1 0 1
	 *
	 * A vs. C, A.ch[A].n=2 > C.a, A.ch[B]=1 = C.b; A.ch[C]=NULL, A (= C[C]
	 * A                     | B                    | C                    | D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  U,   A, pA0, 2,  B   |  I,   B, pB0, 1,  A  |  I,   B, pB0, 1,     |  U,   A, pA0, 2,  B
	 *                   C   |                   C  |                  [ ] |                   C
	 *                  [D]  |                   D  |                   D  |
	 *  I,   B, pB0, 1,  C   |  UU,  B, pA0, 3,  C  |  U,   A, pA0, 2,     |  I,   B, pB0, 1,  C
	 *                  [ ]  |                   D  |                   B  |
	 *                       |                      |                   D  |
	 *  I,   C, pC0, 1,      |                      |  I,   C, pC0, 1, [A] |
	 *                   B   |                      |                   B  |
	 *                   D   |                      |                   D  |
	 *    a b c d
	 *  A 2 1 2 0   ++A.a, A.c = C.c
	 *  B 1 3 0 0
	 *  C 2 1 2 0   ++C.c, C.a = A.a
	 *  D 2 1 0 0
	 *
	 * </pre>
	 * @throws TransException
	 * @throws SQLException
	 */
	void test02UpdateTransit() throws TransException, SQLException {
	}

	void BvsA(int A, int B) throws TransException, SQLException {
		// A pull B
		sync(B, A);

		// A push B
		sync(A, B);

	}

	static void sync(int src, int dst) throws TransException, SQLException {
		AnResultset ents = ((DBSyntext) trbs[src].instancontxt(conns[src], robot))
			.entities(phm);
		
		while(ents.next()) {
			AnResultset subs = (AnResultset) trbs[src]
					.select(sbm.tbl, "sub")
					// compond Id
					.whereEq(sbm.synoder, c[src].synode)
					.whereEq(sbm.clientpath, ents.getString(phm.clientpath))
					.whereEq(sbm.clientpath2, ents.getString(phm.clientpath2))
					.rs(trbs[src].instancontxt(conns[src], robot))
					.rs(0);

			SynEntity entA = new SynEntity();
			String skip = entA.synode;
			entA.format(ents)
				// lock concurrency
				.sync(conns[dst], trbs[dst], subs, skip, robot)
				// unlock
				;
		}
	}

	void updatePoto0(String pid) throws TransException, SQLException {
		trbs[0].update(phm.tbl, robot)
			.nv(phm.clientpath, father)
			.whereEq(chm.pk, pid)
			.u(trbs[0].instancontxt(conns[X], robot))
			;
	}

	String insertPhoto(int s) throws TransException, SQLException {
		return ((SemanticObject) trbs[s]
			.insert(phm.tbl, robot)
			// .nv(phm.uri, "")
			.nv(phm.clientpath, father)
			.ins(trbs[0].instancontxt(conns[0], robot)))
			.resulve(phm.tbl, phm.pk);
	}

	public static class Ck {
		public final String synode;
		private final String connId;
		private final DBSynsactBuilder trb;

		public Ck(int s) {
			this.connId = conns[s];
			this.trb = trbs[s];
			
			synode = String.format("s%s", s);
		}

		public void change(String crud, String pid) throws TransException, SQLException {
			AnResultset chg = (AnResultset) trb
				.select(chm.tbl, "ch")
				.cols(chm.cols())
				.whereEq(chm.recTabl, phm.tbl)
				.whereEq(chm.pk, pid)
				.rs(trb.instancontxt(connId, robot))
				.rs(0);

			chg.next();

			assertEquals(C, chg.getString(chm.crud));
			assertEquals(phm.tbl, chg.getString(chm.recTabl));
			assertEquals(robot.deviceId(), chg.getString(chm.synoder));
		}

		public void subs_CCC(String pid) throws TransException, SQLException {
			AnResultset subs = (AnResultset) trb
				.select(sbm.tbl, "ch")
				.col(Funcall.count(sbm.recId), "cnt")
				.cols(sbm.cols())
				.whereEq(sbm.recTabl, phm.tbl)
				.whereEq(sbm.pk, pid)
				.rs(trb.instancontxt(connId, robot))
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
		 * @param z 
		 * @param x 
		 * @param y 
		 */
		public void subs(String pid, int... s) { }

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
