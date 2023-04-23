package io.odysz.semantic.syn;

import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.CRUD.C;
import static io.odysz.semantic.util.Assert.assertIn;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.transact.sql.parts.condition.Funcall.compound;
import static io.odysz.transact.sql.parts.condition.Funcall.compoundVal;
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
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.LoggingUser;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.sql.parts.condition.Predicate;
import io.odysz.transact.x.TransException;

public class DBSyntextTest {
	public static final String[] conns = new String[4];
	public static final String rtroot = "src/test/res/";
	public static final String h_photos = "h_photos";
	public static final String father = "src/test/res/Sun Yet-sen.jpg";

	public static final int X = 0;
	public static final int Y = 1;
	public static final int Z = 2;
	public static final int W = 3;

	static String runtimepath;

	public static Ck[] c;
	public static DBSynsactBuilder trbs[];

	static HashMap<String, DBSynmantics> synms;
	// static HashMap<String, TableMeta> metas;

	static SynodeMeta snm;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;
	static T_PhotoMeta phm;

	static {
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


//			metas = Connects.getMeta(conns[0]);
//
//			phm = new T_PhotoMeta(conns[0]);
//			metas.put(phm.tbl, phm);
//
//			chm = new SynChangeMeta();
//			metas.put(chm.tbl, chm);
//
//			sbm = new SynSubsMeta();
//			metas.put(sbm.tbl, sbm);

	}

	@BeforeAll
	public static void testInit()
			throws SQLException, SAXException, IOException, TransException, ClassNotFoundException {

		ArrayList<String> sqls = new ArrayList<String>();
		/*
		sqls.add( "CREATE TABLE if not exists oz_autoseq (\n"
				+ "  sid text(50),\n"
				+ "  seq INTEGER,\n"
				+ "  remarks text(200),\n"
				+ "  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid))");
		*/
		sqls.add(Utils.loadTxt("../oz_autoseq.sql"));

//		sqls.add( "CREATE TABLE a_logs (\n"
//				+ "  logId text(20),\n"
//				+ "  funcId text(20),\n"
//				+ "  funcName text(50),\n"
//				+ "  oper text(20),\n"
//				+ "  logTime text(20),\n"
//				+ "  cnt int,\n"
//				+ "  txt text(4000),\n"
//				+ "  CONSTRAINT oz_logs_pk PRIMARY KEY (logId))");

		sqls.add(SynChangeMeta.ddlSqlite);
		sqls.add(SynSubsMeta.ddlSqlite);
		sqls.add(T_PhotoMeta.ddlSqlite);

		sqls.add( "delete from oz_autoseq;\n"
				+ "insert into oz_autoseq (sid, seq, remarks) values\n"
				+ "('h_photos.pid', 0, 'photo'),\n"
				+ "('a_logs.logId', 0, 'test');"
				);

		phm = new T_PhotoMeta("syn.00").replace();
		sqls.add(String.format("delete from %s", phm.tbl));
		sqls.add("delete from a_logs");

		for (int s = 0; s < 4; s++)
			Connects.commit(conns[s], c[s].robot, sqls, Connects.flag_nothing);
		
		// smtcfg = DBSynsactBuilder.loadSynmantics(conn0, "src/test/res/synmantics.xml", true);
		for (int s = 0; s < 4; s++) {
			conns[s] = String.format("syn.%02x", s);
			trbs[s] = new DBSynsactBuilder(conns[s]);
			c[s] = new Ck(s);
		}
		
		assertEquals("syn.00", c[0].connId);
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
		c[X].change(C, A_0, phm);
		// syn_subscribe.to = [B, C, D]
		c[X].subs(A_0, -1, Y, X, Z);

		// 1.2 insert B
		String B_0 = insertPhoto(Y);

		// syn_change.curd = C
		c[Y].change(C, B_0, phm);
		// syn_subscribe.to = [A, C, D]
		c[Y].subs(B_0, X, -1, Z, W);

		// 2.
		BvisitA(X, Y);
		c[Y].change(C, A_0, phm);
		c[Y].subs(A_0, -1, -1, Z, W);
		// B.a = A.a
		long Aa = c[X].nyquence(X).n;
		assertEquals(Aa, c[Y].nyquence(X).n);

		c[X].change(C, B_0, phm);
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
	 * Test W join with X.
	 * 
	 * <pre>
	 * X                     |  Y                   |  Z
	 * crud, s, sid, n, sub  | crud, s, sid, n, sub | crud, s, sid, n, sub
	 *  
	 *    x y z
	 *  X 2 1 0
	 *  Y 1 1 0
	 *  Z 0 0 0
	 * 
	 * W <=> X, sid[X:w]: X added W
	 * X: syn_node           |  Y                   |  Z                   |  W
	 * crud, s, sid, n, sub  | crud, s, sid, n, sub | crud, s, sid, n, sub | crud, s, sid, n, sub
	 *  i,   X,  w , 3,  Y   |                      |                      |  i,   X,  w , 3,  Y
	 *                   Z   |                      |                      |                   Z
	 *  i,   W,  x , 1,  Y   |                      |                      |  i,   W,  x , 1,  Y
	 *                   Z   |                      |                      |                   Z
	 *    x y z w
	 *  X 3 1 0 1   ++X.x, X.w = W.w
	 *  Y 1 1 0 
	 *  Z 0 0 0 
	 *  W 3     1   ++W.w, W.x = X.x
	 *  
	 *  X <=> Y, X.ch[X].n=3 > Y.x, X.ch[W].n=1 > Y.w=NULL
	 *  Z <=> W, Z.w < W.ch[X].n=3, Z.w=NULL < W.ch[W].n=1
	 * X: syn_node           |  Y                   |  Z                   |  W
	 * crud, s, sid, n, sub  | crud, s, sid, n, sub | crud, s, sid, n, sub | crud, s, sid, n, sub
	 *  i,   X,  w , 3, [Y]  |  i,   X,  w , 3,     |  i,   X,  w , 3,  Y  |  i,   X,  w , 3,  Y
	 *                   Z   |                   Z  |                      |                  [Z]
	 *  i,   W,  x , 1, [Y]  |  i,   W,  x , 1,     |  i,   W,  x , 1,  Y  |  i,   W,  x , 1,  Y
	 *                   Z   |                   Z  |                      |                  [Z]
	 *    x y z w
	 *  X 3 1 0 1
	 *  Y 1 1 0 1  Y.w = max(X.ch[W].n)
	 *  Z 0 0 0 1  Z.w = W.w
	 *  W 3   0 1  W.z = Z.z
	 *  
	 *  Z leaved, told X
	 * X: syn_node           |  Y                   |  Z                   |  W
	 * crud, s, sid, n, sub  | crud, s, sid, n, sub | crud, s, sid, n, sub | crud, s, sid, n, sub
	 *  i,   X,  w , 3,  Z   |  i,   X,  w , 3,  Z  |  -    -  ---  -   -  |  i,   X,  w , 3,  Y
	 *  d,   X,  z , 4,  Y   |                      |                      |
	 *                   W   |                      |                      |
	 *  i,   W,  x , 1,  Z   |  i,   W,  x , 1,  Z  |                      |  i,   W,  x , 1,  Y
	 *  
	 *    x y z w
	 *  X 4 1[0]1   ++X.x
	 *  Y 1 1 0 1
	 *  ------+--
	 *  W 3   0 1
	 *  
	 * </pre>
	 * @throws SQLException 
	 * @throws TransException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	void testSynodeManage() throws TransException, SQLException, ClassNotFoundException, IOException {

		initSynodes(0);

		c[X].synodes(X, Y, Z, -1);
		join(X, W);
		c[X].synodes(X, Y, Z, W);

		Nyquence n = c[X].nyquence(X);
		
		c[X].change(C, c[X].synode, phm);
		// i X  w  3
		c[X].chgEnt(C, c[X].synode, c[W].synode, phm);
		// Y, Z
		c[X].subs(c[W].synode, -1, Y, Z, -1);
		// And more ...
	}
	
	static void initSynodes(int s)
			throws SQLException, TransException, ClassNotFoundException, IOException {
		String sqls = Utils.loadTxt("syn_nodes.sql");
		Connects.commit(c[s].robot, sqls);
	}

	void join(int admin, int apply) throws TransException, SQLException {
		c[admin].trb.addSynode(
				c[admin].connId,  // into admin's db
				new Synode(c[apply].synode, c[admin].robot.orgId()),
				c[admin].robot);
		
		pull(admin, apply);
	}

	void BvisitA(int A, int B) throws TransException, SQLException {
		// A pull B
		pull(B, A);

		// B push A
		push(B, A);
	}

	public static void push(int src, int dst) throws TransException, SQLException {
		SynEntity anObj = trbs[src].loadEntity(c[src].synode, c[src].connId, c[src].robot, phm);
		anObj.syncInto(conns[dst], trbs[dst], anObj.subs(), null, c[dst].robot);
	}

	@SuppressWarnings("serial")
	public static void pull(int src, int dst) throws TransException, SQLException {
		AnResultset ents = trbs[src].entities(phm, c[src].connId, c[src].robot);
		
		while(ents.next()) {
			// say, entA = trb.loadEntity(phm)
			AnResultset subs = (AnResultset) trbs[src]
					.select(chm.tbl, "ch")
					.je("ch", sbm.tbl, "sb", chm.entfk, sbm.uids, chm.entbl, phm.tbl, sbm.entbl, phm.tbl)
					.whereEq("ch", chm.org, c[src].robot.orgId())
					.whereEq("ch", chm.synoder, c[src].synode)
					.whereEq("ch", chm.uids, ents.getString(chm.uids))
					.rs(trbs[src].instancontxt(conns[src], c[src].robot))
					.rs(0);

			SynEntity anObjA = new SynEntity(ents, phm, chm, sbm);
			String skip = anObjA.synode();
			anObjA.format(ents)
				// lock concurrency
				.syncInto(conns[dst], trbs[dst], subs, new HashSet<String>() {{add(skip);}}, c[dst].robot)
				// unlock
				;
			
			trbs[dst].incNyquence(c[dst].connId, phm.tbl, skip, c[dst].robot);
		}
	}

	void updatePoto(int s, String pid) throws TransException, SQLException {
		trbs[s].update(phm.tbl, c[s].robot)
			.nv(chm.uids, father) // clientpath
			.whereEq(chm.pk, pid)
			.u(trbs[s].instancontxt(conns[X], c[s].robot))
			;
	}

	String insertPhoto(int s) throws TransException, SQLException {
		return ((SemanticObject) trbs[s]
			.insert(phm.tbl, c[s].robot)
			.nv(phm.uri, "")
			.nv(chm.uids, father)
			.ins(trbs[s].instancontxt(conns[s], c[s].robot)))
			.resulve(phm.tbl, phm.pk);
	}

	public static class Ck {
		public IUser robot;
		public final String synode;
		private final String connId;
		final DBSynsactBuilder trb;

		public Ck(int s) throws SQLException, TransException, ClassNotFoundException, IOException {
			this(conns[s], trbs[s], String.format("s%s", s), "rob-" + s);
			initSynodes(s);
		}

		/**
		 * Verify all synodes information here is as expected.
		 * 
		 * @param x X presented here, -1 for disappearing
		 * @param y
		 * @param z
		 * @param w
		 */
		public void synodes(int x, int y, int z, int w) {
		}

		public Ck(String conn, DBSynsactBuilder trb, String synid, String uid)
				throws SQLException, TransException, ClassNotFoundException, IOException {
			this.connId = conn;
			this.trb = trb;
			
			synode = synid;


			SemanticObject jo = new SemanticObject();
			jo.put("userId", "tester");
			SemanticObject usrAct = new SemanticObject();
			usrAct.put("funcId", "DBSyntextTest");
			usrAct.put("funcName", "test ISemantext implementation");
			jo.put("usrAct", usrAct);
			robot = new LoggingUser(connId, "src/test/res/semantic-log.xml", uid, jo);
		}

		/**
		 * Verify entity's change log
		 * 
		 * @param crud
		 * @param synoder
		 * @param entId
		 * @param entm
		 */
		public void chgEnt(String crud, String synoder, String entId, SyntityMeta entm) {

		}

		/**
		 * Verify change flag, crud, where tabl = entabl, entity-id = eid.
		 * 
		 * @param crud flag to be verified
		 * @param eid  entity id
		 * @param entm entity table meta
		 * @throws TransException
		 * @throws SQLException
		 */
		public void change(String crud, String eid, SyntityMeta entm) throws TransException, SQLException {
			AnResultset chg = (AnResultset) trb
				.select(chm.tbl, "ch")
				.cols(chm.cols())
				.whereEq(chm.entbl, entm.tbl)
				.whereEq(chm.entfk, eid)
				.rs(trb.instancontxt(connId, robot))
				.rs(0);

			chg.next();

			assertEquals(C, chg.getString(chm.crud));
			assertEquals(phm.tbl, chg.getString(chm.entbl));
			assertEquals(robot.deviceId(), chg.getString(chm.synoder));
		}

		/**verify subscriptions.
		 * @param pid
		 * @param sub subscriptions for X/Y/Z/W, -1 if not exists
		 * @throws SQLException 
		 * @throws TransException 
		 */
		public void subs(String pid, int ... sub) throws SQLException, TransException {
			ArrayList<String> toIds = new ArrayList<String>();
			for (int n : sub)
				if (n >= 0)
					toIds.add(c[n].synode);
			subs(pid, (String[])toIds.toArray());
		}

		public void subs(String pid, String ... toIds) throws SQLException, TransException {
			AnResultset subs = trb.subscripts(connId, pid, phm, robot);

			subs.next();

			assertEquals(3, subs.getInt("cnt"));
			assertEquals(phm.tbl, subs.getString(sbm.entbl));
			
			HashSet<String> synodes = subs.set(sbm.subs);
			
			int size = c.length;
			for (String n : toIds) {
				assertIn(n, synodes);
				size--;
			}
			assertEquals(0, size);
		}

		/**
		 * get nyquence
		 * @param synode
		 * @return
		 * @throws TransException 
		 * @throws SQLException 
		 */
		public Nyquence nyquence(int synode) throws SQLException, TransException {
			return trbs[synode].nyquence(conns[synode], robot.orgId(), robot.deviceId(), phm.tbl);
		}

//		public AnResultset entities(int s, String nodeId, String png) throws TransException, SQLException {
//			AnResultset ents = c[s].trb.entities(phm, png, robot);
//			return ents;
//		}

		/**
		 * Verify if and only if one instance exists on this node.
		 * 
		 * @param synoder
		 * @param clientpath
		 */
		public void verifile(String synoder, String clientpath, T_PhotoMeta phm) {
			trb.select(phm.tbl)
				.col(count(phm.pk), "c")
				.where(new Predicate(op.eq, compound(chm.uids), compoundVal(synoder, clientpath)))
				;
		}
	}

}
