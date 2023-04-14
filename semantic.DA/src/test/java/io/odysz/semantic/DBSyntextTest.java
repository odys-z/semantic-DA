package io.odysz.semantic;

import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.CRUD.C;
import static io.odysz.semantic.util.Assert.assertIn;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
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
				trbs[s] = new DBSynsactBuilder(conns[s], phm, chm, sbm);
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
			usrAct.put("funcId", "DBSyntextTest");
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
	 *  I,   A, A:0, 0,  B   |  I,   B, B:0, 0, A
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
	 *  I,   A, A:0, 1, [ ]  |  I,   B, B:0, 1,  A
	 *                   C   |                   C
	 *                   D   |                   D
	 *                       |  I,   A, A:0, 1,  C
	 *                       |                   D
	 *    a b c
	 *  A 1 0 0
	 *  B 1 1 0  [B.a = A.a]
	 *  C 0 0 0
	 *
	 * 3.
	 * A                    (=  B
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub
	 *  I,   A, A:0, 1, [ ]  |  I,   B, B:0, 1, [ ]
	 *                   C   |                   C
	 *                   D   |                   D
	 *  I,   B, B:0, 1,  C   |  I,   A, A:0, 1,  C
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
		String A_0 = insertPhoto(X);

		// syn_change.curd = C
		c[X].change(C, A_0);
		// syn_subscribe.to = [B, C, D]
		c[X].subs(A_0, -1, Y, X, Z);

		// 1.2 insert B
		String B_0 = insertPhoto(Y);

		// syn_change.curd = C
		c[Y].change(C, B_0);
		// syn_subscribe.to = [A, C, D]
		c[Y].subs(B_0, X, -1, Z, W);

		// 2.
		BvsA(X, Y);
		c[Y].change(C, A_0);
		c[Y].subs(A_0, -1, -1, Z, W);
		// B.a = A.a
		long Aa = c[X].nyquence(X).n;
		assertEquals(Aa, c[Y].nyquence(X).n);

		c[X].change(C, B_0);
		c[X].subs(B_0, -1, -1, Z, W);
		// A.b = B.b
		long Bb = c[Y].nyquence(Y).n;
		assertEquals(Bb, c[X].nyquence(Y).n);
	}

	/**
	 * <pre>
	 * A                     | B                    | C                    | D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  I,   A, A:0, 1, [ ]  |  I,   B, B:0, 1, [ ] |                      |
	 *                   C   |                   C  |                      |
	 *                   D   |                   D  |                      |
	 *  I,   B, B:0, 1,  C   |  I,   A, A:0, 1,  C  |                      |
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
	 *  I,   A, A:0, 1, [ ]  |  I,   B, B:0, 1, [ ] |  I,   B, B:0, 1,     |
	 *                   C   |                  [-] |                  [ ] |
	 *                   D   |                   D  |                   D  |
	 *  I,   B, B:0, 1,  C   |  I,   A, A:0, 1,  C  |  I,   A, A:0, 1,     |
	 *                   D   |                   D  |                   D  |
	 *
	 *    a b c d
	 *  A 1 1 0 0
	 *  B 1 1 0 0
	 *  C 1 1 1 0   [C.a = max(ch[A].n), C.b = B.b, ++C.c, C.d]
	 *  D 0 0 0 0
	 *
	 * A update A:0
	 * A
	 * crud, s, pid, n, sub  |
	 *  U,   A, A:0, 2,  B   |
	 *                   C   |
	 *                   D   |
	 *  I,   B, B:0, 1,  C   |
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
	 *  U,   A, A:0, 2,  B   |                                             |  U,   A, A:0, 2,  B   (2>D.a=0)
	 *                   C   |                                             |                   C
	 *                  [ ]  |                                             |
	 *  I,   B, B:0, 1,  C   |                                             |  I,   B, B:0, 1,  C   (1>D.c=0)
	 *                  [ ]  |                                             |
	 *    a b c d
	 *  A 2 1 0 0 
	 *  B 1 1 0 0
	 *  C 1 1 0 0
	 *  D 2 1 0 1   [D.a=max(ch[A].n)=2, D.b=max(ch[B].n)=1, D.c (ch[C]==NULL), ++D.d]
	 *
	 * B update A:0 twice, C insert pC0
	 * A                     | B                    | C                    | D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  U,   A, A:0, 2,  B   |  I,   B, B:0, 1,  A  |  I,   B, B:0, 1,     |  U,   A, A:0, 2,  B
	 *                   C   |                   C  |                  [ ] |                   C
	 *                   D   |                   D  |                   D  |
	 *  I,   B, B:0, 1,  C   |  UU,  B, A:0, 3,  C  |  I,   A, A:0, 1,     |  I,   B, B:0, 1,  C
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
	 *  U,   A, A:0, 2,  B   |  I,   B, B:0, 1,  A  |  I,   B, B:0, 1,     |  U,   A, A:0, 2,  B
	 *                   C   |                   C  |                  [ ] |                   C
	 *                  [D]  |                   D  |                   D  |
	 *  I,   B, B:0, 1,  C   |  UU,  B, A:0, 3,  C  |  U,   A, A:0, 2,     |  I,   B, B:0, 1,  C
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
	@Test
	void test02UpdateTransit() throws TransException, SQLException {
	}

	/**
	 * <pre>
	 * A                     |  B                   |  C
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  
	 *    a b c
	 *  A 2 1 0
	 *  B 1 1 0
	 *  C 0 0 0
	 * 
	 * D <=> A
	 * A                     |  B                   |  C                   |  D
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub | crud, s, pid, n, sub | crud, s, pid, n, sub
	 *  i,   A, A:D, 3,  B   |                      |                      |  i,   D, D:a, 1,  B
	 *                   C   |                      |                      |                   C
	 *  
	 *    a b c d
	 *  A 2 1 0 0
	 *  B 1 1 0 
	 *  C 0 0 0 
	 *  D 0     0
	 *  
	 *    a b   d
	 *  A 2 1 | 2
	 *  B 1 1 | 1
	 * [] ----+--
	 *  D 2 1 | 0
	 * </pre>
	 */
	@Test
	void testSynodeManage() {
	}
	
	void BvsA(int A, int B) throws TransException, SQLException {
		// A pull B
		sync(B, A);

		// A push B
		sync(A, B);

	}

	@SuppressWarnings("serial")
	static void sync(int src, int dst) throws TransException, SQLException {
		AnResultset ents = ((DBSyntext) trbs[src].instancontxt(conns[src], robot))
			.entities(phm);
		
		while(ents.next()) {
			// say, entA = trb.loadEntity(phm)
			AnResultset subs = (AnResultset) trbs[src]
					.select(chm.tbl, "ch")
					.je("ch", sbm.tbl, "sb", chm.entFk, sbm.entId)
					.whereEq("ch", chm.entbl, phm.tbl)
					.whereEq("sb", sbm.entbl, phm.tbl)
					// compond Id
					.whereEq(chm.synoder, c[src].synode)
					.whereEq(chm.clientpath, ents.getString(phm.clientpath))
					.whereEq(chm.clientpath2, ents.getString(phm.clientpath2))
					.rs(trbs[src].instancontxt(conns[src], robot))
					.rs(0);

			SynEntity entA = new SynEntity(ents, phm, chm, sbm);
			String skip = entA.synode;
			entA.format(ents)
				// lock concurrency
				.syncWith(conns[dst], trbs[dst], subs, new HashSet<String>() {{add(skip);}}, robot)
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
			.nv(phm.uri, "")
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

		/**
		 * Verify change record.
		 * 
		 * @param crud
		 * @param eid
		 * @throws TransException
		 * @throws SQLException
		 */
		public void change(String crud, String eid) throws TransException, SQLException {
			AnResultset chg = (AnResultset) trb
				.select(chm.tbl, "ch")
				.cols(chm.cols())
				.whereEq(chm.recTabl, phm.tbl)
				.whereEq(chm.pk, eid)
				.rs(trb.instancontxt(connId, robot))
				.rs(0);

			chg.next();

			assertEquals(C, chg.getString(chm.crud));
			assertEquals(phm.tbl, chg.getString(chm.recTabl));
			assertEquals(robot.deviceId(), chg.getString(chm.synoder));
		}

		/**verify subscriptions.
		 * @param conn 
		 * @param pid
		 * @param sub subscriptions for X/Y/Z/W, -1 if not exists
		 * @throws SQLException 
		 * @throws TransException 
		 */
		public void subs(String pid, int ... sub) throws SQLException, TransException {
			AnResultset subs = trb.subscripts(connId, pid, robot);

			subs.next();

			assertEquals(3, subs.getInt("cnt"));
			assertEquals(phm.tbl, subs.getString(sbm.entbl));

			HashSet<String> synodes = subs.set(sbm.subs);
			
			for (int n : sub)
				if (n >= 0)
					assertIn(c[n].synode, synodes);
		}

		public Nyquence nyquence(int synode) {
			return trbs[synode].nyquence(conns[synode]);
		}
	}

}
