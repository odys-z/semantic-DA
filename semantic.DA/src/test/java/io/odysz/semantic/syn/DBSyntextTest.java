package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.compoundVal;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.eq;
import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.CRUD.C;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.compound;
import static io.odysz.transact.sql.parts.condition.Funcall.concatstr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.transact.sql.parts.condition.Funcall.now;
import static io.odysz.semantic.syn.Nyquence.maxn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.anson.Anson;
import io.odysz.common.Configs;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.condition.Predicate;
import io.odysz.transact.x.TransException;

/**
 * Full-duplex mode for exchanging logs are running.
 * 
 * <pre>
 * 1. Synodes initialized with ++n0.
 * 
 *   a  b c  d
 * A x0
 * B   y0
 * C      z
 * D         w
 * 
 * --------------------------------------------------------
 * 2. A vs. B
 * A[s=A, A.b ≤ n] ⇨ B, where n ∈ {x0}
 * A ⇦ B[s=B, B.a ≤ n], where n ∈ {y0}
 * A.b = B.n0, B.a = A.n0
 * A.n0++, B.n0++
 * 
 *   a  b  c  d
 * A x1 y0
 * B x0 y1
 * C       z  
 * D          w
 * 
 * --------------------------------------------------------
 * 3. A vs. B
 * A[s=A, A.b ≤ n] ⇨ B
 * A handled B's response, buf B failed of getting response of A, A ⇦ B[s=B, B.a ≤ y1]
 * B roll back pending operations (buffered pending commitments), but A committed.
 * ++A.n0, A.b = B.n0
 * 
 *   a  b   c   d
 * A x3 y1			A can update A.a while communicating with C/D
 * B x0 y2 != x3	B can update B.b while communicating with C/D
 * C        z 
 * D            w
 * 
 * --------------------------------------------------------
 * 4. A vs. B
 * On initiation, B found buffered commitments whit n <= A.b, commit first
 * 
 *   a  b   c   d
 * A x3 y1
 * B x0 y3           y3 = x3
 * C        z 
 * D            w
 * 
 * </pre>
 * @author Ody
 */
public class DBSyntextTest {
	public static final String[] conns;
	public static final String[] testers;
	public static final String logconn = "log";
	public static final String rtroot = "src/test/res/";
	public static final String father = "src/test/res/Sun Yet-sen.jpg";

	public static final int X = 0;
	public static final int Y = 1;
	public static final int Z = 2;
	public static final int W = 3;

	static String runtimepath;

	public static Ck[] ck = new Ck[4];

	static HashMap<String, DBSynmantics> synms;

	static SynodeMeta snm;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;

	static {
		printCaller(false);
		conns = new String[] { "syn.00", "syn.01", "syn.02", "syn.03" };
		testers = new String[] { "odyx", "odyy", "odyz", "odyw" };

		File file = new File(rtroot);
		runtimepath = file.getAbsolutePath();
		logi(runtimepath);
		Configs.init(runtimepath);
		Connects.init(runtimepath);

		// load metas, then semantics
		DATranscxt.configRoot(rtroot, runtimepath);
		String rootkey = System.getProperty("rootkey");
		DATranscxt.key("user-pswd", rootkey);
	}

	static T_PhotoMeta phm;

	@BeforeAll
	public static void testInit() throws Exception {
		// DDL
		// Debug Notes:
		// Sqlite won't commit multiple (ignore following) sql in one batch, and quit silently!
		// Similar report: https://sqlite-users.sqlite.narkive.com/JqAIbcSi/running-multiple-ddl-statements-in-a-batch-via-jdbc
		// To verify this, uncomment the first line in ddl.

		for (int s = 0; s < 4; s++) {
			conns[s] = String.format("syn.%02x", s);
			Connects.commit(conns[s], DATranscxt.dummyUser(),
				"CREATE TABLE if not exists a_logs (\n"
				+ "  logId text(20),\n"
				+ "  funcId text(20),\n"
				+ "  funcName text(50),\n"
				+ "  oper text(20),\n"
				+ "  logTime text(20),\n"
				+ "  cnt int,\n"
				+ "  txt text(4000),\n"
				+ "  CONSTRAINT oz_logs_pk PRIMARY KEY (logId)\n"
				+ ");" );
			
			 Connects.commit(conns[s], DATranscxt.dummyUser(),
				"CREATE TABLE if not exists oz_autoseq (\r\n"
				 + "  sid text(50),\r\n"
				 + "  seq INTEGER,\r\n"
				 + "  remarks text(200),\r\n"
				 + "  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid));");
		}

		ck = new Ck[4];
		String[] synodeIds = new String[] { "X", "Y", "Z", "W" };
		// new for triggering ddl loading - some error here FIXME
		// nyqm = new NyquenceMeta("");
		chm = new SynChangeMeta();
		sbm = new SynSubsMeta();


		for (int s = 0; s < 4; s++) {
			String conn = conns[s];
			
			snm = new SynodeMeta(conn, null);
			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", snm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), snm.ddlSqlite);

			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", chm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), chm.ddlSqlite);

			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", sbm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), sbm.ddlSqlite);

			// JUserMeta usm = new JUserMeta(conn);
			// Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", usm.tbl));
			// Connects.commit(conn, DATranscxt.dummyUser(), usm.ddlSqlite);

			T_PhotoMeta phm = new T_PhotoMeta(conn).replace();

			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", phm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), phm.ddlSqlite);

			ArrayList<String> sqls = new ArrayList<String>();
			sqls.addAll(Arrays.asList(Utils.loadTxt("../oz_autoseq.sql").split(";-- --\n")));
			sqls.add(String.format("update oz_autoseq set seq = %d where sid = 'h_photos.pid'", (long) Math.pow(64, s+1)));

			sqls.add(String.format("delete from %s", snm.tbl));
			if (s != W)
				sqls.add(Utils.loadTxt("syn_nodes.sql"));

			sqls.add(String.format("delete from %s", phm.tbl));
			// sqls.add(String.format("delete from %s", usm.tbl));
			// sqls.add(Utils.loadTxt("a_users.sql"));

			Connects.commit(conn, DATranscxt.dummyUser(), sqls);

			ck[s] = new Ck(s, new DBSynsactBuilder(conn, synodeIds[s]).loadNyquvect0(conn), "zsu");
			snm = new SynodeMeta(conn, ck[s].trb).autopk(false).replace();
			ck[s].synm = snm;
			if (s != W)
				ck[s].trb.incNyquence();

			ck[s].trb.registerEntity(conn, ck[s].phm);
			ck[s].trb.registerEntity(conn, snm);
		}

		phm = new T_PhotoMeta(conns[0]).replace(); // all entity table is the same in this test

		assertEquals("syn.00", ck[0].connId());
	}

	@Test
	void testChangeLogs() throws Exception {
		test01InsertBasic();
		testJoinChild();
	}

	/**
	 * <pre>
	 *  ++A.a, ++B.b
	 *     a  b  c
	 *  A +1     
	 *  B    +1   
	 *  C       +1
	 *  
	 * 1.
	 * A                     | B
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub
	 *  I,   A, A:0, 1,  B   |  I,   B, B:0, 1, A
	 *                   C   |                  C
	 *                   D   |                  D
	 *
	 *    a b c d
	 *  A x      
	 *  B   y    
	 *  C     z  
	 *  D       w
	 *---------------------------------------------------------------
	 * 2. A.a > B.a, B don't know A:0 [B]/C/D, with n=x > B.a | s=A
	 * 
	 *  A select n in range (A.b, A.a = x]
	 *  B merge with local DB, with records sorted by n, s, pid.
	 *  
	 * A                     =) B
	 * crud, s, uids, n, sub  | crud, s, uids, n, sub
	 *  I,   A, A:0,  x, [B]  |  I,   B, B:0,  y,  A
	 *                    C   |                    C
	 *                    D   |                    D
	 *                        |
	 *                        |  I,   A, A:0,  x,  C   b.a < chg.n = x
	 *                        |                    D   b.a < chg.n = x
	 *                        
	 *
	 * A.b < B.b, A don't know B:0 [A]/C/D, with n=y > A.b | s=B
	 * A                    (=  B
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub
	 *  I,   A, A:0, x, [ ]  |  I,   B, B:0, y, [ ]
	 *                   C   |                   C
	 *                   D   |                   D
	 *  I,   B, B:0, y,  C   |  I,   A, A:0, x,  C
	 *                   D   |                   D
	 *                   
	 *    a  b  c  d
	 *  A x1 x                  [A.b = B.b, ++A.a ( = max(B.a, ++A.a))]
	 *  B x  y1        y1 = x1, [B.a = A.a, ++B.b (= max(++B.b, A.b))]
	 *  C       z
	 *  D          w
	 *  
	 *---------------------------------------------------------------
	 * 3. A insert A:1
	 * A                    (=  B
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub
	 *  I,   A, A:0, x, [ ]  |  I,   B, B:0, y, [ ]
	 *                   C   |                   C
	 *                   D   |                   D
	 *  I,   B, B:0, y,  C   |  I,   A, A:0, x,  C
	 *                   D   |                   D
	 *  I,   A, A:1, x1, B   |  
	 *                   C   |  
	 *                   D   |  
	 *                   
	 *    a  b  c  d
	 *  A x1 x
	 *  B x  y1
	 *  C       z
	 *  D          w
	 *  
	 *---------------------------------------------------------------
	 * 4. A Update A:1
	 * 
	 * A select n in range (A.b, A.a = A.n0]
	 * B merge with local DB, with records sorted by n, s, pid, only merge with n in range (B.a, B.b = B.n0]
	 * 
	 * A                    (=  B
	 * crud, s, pid, n, sub  | crud, s, pid, n, sub
	 *  I,   A, A:0, x, [ ]  |  I,   B, B:0, y, [ ]
	 *                   C   |                   C
	 *                   D   |                   D
	 *  I,   B, B:0, y,  C   |  I,   A, A:0, x,  C
	 *                   D   |                   D
	 *  U,   A, A:1, x1, B   |  
	 *                   C   |  
	 *                   D   |  
	 *                       |  I,   A, A:1, x1, C 
	 *                       |                   D 
	 *                   
	 *    a  b  c  d
	 *  A x2 x1
	 *  B x1 y2
	 *  C       z
	 *  D          w
	 *
	 * =============================================================
	 * If the response to A lost, B won't commit but will send the
	 * operations into a buffer, to discard if requested again.
	 *  
	 * </pre>
	 * @throws TransException
	 * @throws SQLException
	 * @throws IOException 
	 */
	void test01InsertBasic() throws TransException, SQLException, IOException {
		Utils.logi("\n=== === %s === ===", new Object(){}.getClass().getEnclosingMethod().getName());

		HashMap<String, Nyquence> nvx = ck[X].trb.nyquvect;
		long Aa_ = nvx.get(ck[X].trb.synode()).n;
		long Ab_ = nvx.get(ck[Y].trb.synode()).n;
		String x = ck[X].trb.synode();

		HashMap<String, Nyquence> nvy = ck[Y].trb.nyquvect;
		long Ba_ = nvy.get(ck[X].trb.synode()).n;
		long Bb_ = nvy.get(ck[Y].trb.synode()).n;
		String y = ck[Y].trb.synode();

		HashMap<String, Nyquence> nvz = ck[Z].trb.nyquvect;
		long Ca_ = nvx.get(ck[Z].trb.synode()).n;
		long Cb_ = nvy.get(ck[Z].trb.synode()).n;
		String z = ck[Z].trb.synode();


		// 1.1 insert A
		Utils.logi("1.1 ----------------- insert A -----------------");
		String[] A_0_uids = insertPhoto(X);
		String A_0 = A_0_uids[0];

		// syn_change.curd = C
		ck[X].change(1, C, A_0, ck[X].phm);
		// syn_subscribe.to = [B, C, D]
		ck[X].psubs(2, A_0_uids[1], -1, Y, Z, -1);

		// 1.2 insert B
		Utils.logi("\n1.2 ----------------- insert B -----------------");
		String[] B_0_uids = insertPhoto(Y);
		String B_0 = B_0_uids[0];

		// syn_change.curd = C
		ck[Y].change(1, C, B_0, ck[Y].phm);
		// syn_subscribe.to = [A, C, D]
		ck[Y].psubs(2, B_0_uids[1], X, -1, Z, -1);
		
		printChangeLines(ck);
		printNyquv(ck);

		// 2. X <= Y
		Utils.logi("\n2 ----------------- X <= Y ----------------- ");
		exchangePhotos(X, Y);
		ck[Y].change(1, C, ck[Y].trb.synode(), B_0, ck[Y].phm);
		ck[Y].change(1, C, ck[X].trb.synode(), A_0, ck[Y].phm);
		ck[Y].psubs(1, B_0_uids[1], -1, -1, Z, -1);
		ck[Y].psubs(1, A_0_uids[1], -1, -1, Z, -1);

		// B.b++, A.b = B.b, B.a = A.a
		long Ab = nvx.get(y).n;
		long Bb = ck[Y].trb.n0().n;
	
		assertnv(  Bb,     Bb_ + 1, Ab_ + 1, Ab + 1,
			 nvy.get(y).n, Bb,      Ab,      Bb);

		long Aa = nvx.get(x).n;
		long Ba = nvy.get(x).n;

		assertnv(   Aa,      Aa_ + 1, Ba_ + 1,
			ck[X].trb.n0().n, Aa,     Ba);

		Ab_ = Ab;
		Bb_ = Bb;
		Aa_ = Aa;
		Ba_ = Ba;
		
		// 3. Y <= Z
		long Bc_ = nvy.get(z).n;
		long Cc_ = nvz.get(z).n;

		Utils.logi("\n3 ----------------- Y <= Z -----------------");
		exchangePhotos(Y, Z);
		ck[Z].change(0, C, A_0, ck[Z].phm);
		ck[Z].change(0, C, ck[X].trb.synode(), A_0, ck[Z].phm);
		ck[Z].psubs(0, A_0_uids[1], -1, -1, Z, -1);

		ck[Z].change(0, C, B_0, ck[Y].phm);
		ck[Z].change(0, C, ck[Y].trb.synode(), B_0, ck[Z].phm);
		ck[Z].psubs(0, B_0_uids[1], -1, -1, Z, -1);
		
		ck[Y].change(0, C, A_0, ck[Y].phm);
		ck[Y].change(0, C, B_0, ck[Y].phm);
		ck[Y].psubs(0, A_0_uids[1], -1, -1, Z, -1);
		ck[Y].psubs(0, B_0_uids[1], -1, -1, Z, -1);

		Ca_ = Ba;
		Cb_ = Bb;

		long Bc = nvy.get(x).n;
		long Ca = nvz.get(x).n;
		long Cb = nvz.get(y).n;
		long Cc = ck[Z].trb.n0().n;

		assertEquals(Cc, nvz.get(z).n);
		assertnv( Ca_, Cb_, Cc_ + 2,
				  Ca,  Cb,  Cc );
		assertnv( Ba_, Bb_, Bc_ + 1,
				  Ba,  Bb,  Bc );

		Ab_ = Ab;
		Bb_ = Bb;
		Aa_ = Aa;
		Ba_ = Ba;
		Bc_ = Bc;
		Ca_ = Ca;
		Cb_ = Cb;
		Cc_ = Cc;
		
		// 4. X <= Y
		Utils.logi("\n4 ----------------- X <= Y -----------------");
		exchangePhotos(X, Y);
		ck[X].change(0, C, A_0, ck[X].phm);
		ck[X].change(0, C, B_0, ck[X].phm);
		ck[X].psubs(0, A_0_uids[1], -1, -1, Z, -1);
		ck[X].psubs(0, B_0_uids[1], -1, -1, Z, -1);
	}

	/**
	 * <pre>
	 * A                      | B                    
	 * crud, s, pid, n,  sub  | crud, s, pid, n,  sub
	 *  D,   A, A:0, x0,  B   |  D,   B, B:0, y0,  A  
	 *                    C   |                    C 
	 *  I,   A, B:0, x0,  C   |  I,   B, A:0, y0,  C 
	 *  U,   A, C:0, x0,  C   |  U,   B, C:0, y0,  C
	 *
	 *    a   b   c    d
	 *  A x  
	 *  B     y
	 *  C         z
	 *  D             w
	 *----------------------------------------------
	 * Sync-range: [x, A.b] vs [y, B.a]
	 * A                      | B                    
	 * crud, s, pid, n,  sub  | crud, s, pid, n,  sub
	 *  D,   A, A:0, x,  [B]x |  D,   B, B:0, y,  [A] x 
	 *                    C   |                    C 
	 *  I,   A, B:0, x0,  C   |  I,   B, A:0, y0,  C 
	 *  U,   A, C:0, x0,  C   |  U,   B, C:0, y0,  C
	 *  
	 *    a   b   c    d
	 *  A x1  y0
	 *  B x0  y1
	 *  C         z0
	 *  D             w0
	 *  
	 *-----------------------------------------------------------------------
	 * B = C
	 * A                      | B                       | C
	 * crud, s, pid, n,  sub  | crud, s, pid, n,  sub   | crud, s, pid, n, sub
	 *  D,   A, A:0, x,  [ ]x |  D,   B, B:0, x,  [ ]   |  
	 *                    C   |                   [C] x |
	 *  I,   A, B:0, x0,  C   |  I,   B, A:0, y0, [C] x |
	 *  U,   A, C:0, x0,  C   |  U,   B, C:0, y0, [C] x |
	 *  
	 *    a   b   c    d
	 *  A x1  y0
	 *  B x0  y2  z0
	 *  C x0  y1  z2         z2 = y2
	 *  D             w0
	 *  
	 *----------------------------------------------------------------------- 
	 * A = B
	 * A                      | B                      | C
	 * crud, s, pid, n,  sub  | crud, s, pid, n, sub   | crud, s, pid, n, sub
	 *  D,   A, A:0, x,  [ ]  |                        |
	 *                   [C]x |                        |
	 *  I,   A, B:0, x0, [C]x |                        |
	 *  U,   A, C:0, x0, [C]x |                        |
	 *  
	 * A clean for each subscribe i, such that chg[sub=i].n ≤ B.a and A.nv[i] < B.nv[i]
	 * - B:0[C].n=x0 = B.a, A.c < B.c=z0 because A can't have A.c later than or equal to B.c,
	 *   so override A with B for subscribe C, i.e. delete B:0[c], (s=A, n=x0),
	 * - C:0[C].n=x0 = B.a, A.c < B.c=z0,
	 *   override A with B for subscribe C, i.e. delete C:0(s=A, n=x0)
	 *  
	 *  NOTE
	 *  B:0.n[sub=C] always less than, not equal to, B.c in a deletion propagation,
	 *  because B know about C later than A.
	 *  
	 *    a   b   c    d
	 *  A x3  y2  z0        x3 = y3
	 *  B x1  y3  z0
	 *  C x0  y1  z2    
	 *  D             w0
	 * </pre>
	 * @throws Exception
	 */
	void test02delete() throws Exception {
		String A_0 = deletePhoto(chm, X);
		String B_0 = deletePhoto(chm, Y);

		ck[X].change(2, CRUD.D, A_0, ck[X].phm);
		ck[Y].change(2, CRUD.D, B_0, ck[Y].phm);
		ck[X].psubs(2, A_0, -1,  Y, Z, W);
		ck[Y].psubs(2, B_0,  X, -1, Z, W);
		
		exchangePhotos(X, Y);
		ck[X].psubs(2, A_0, -1, -1, Z, W);
		ck[Y].psubs(2, A_0, -1, -1, Z, W);

		ck[X].psubs(2, B_0, -1, -1, Z, W);
		ck[Y].psubs(2, B_0, -1, -1, Z, W);
		
		exchangePhotos(Y, Z);
		ck[X].psubs(2, A_0, -1, -1,  Z, W);
		ck[Y].psubs(2, A_0, -1, -1, -1, W);
		ck[Z].psubs(2, A_0, -1, -1, -1, W);

		ck[X].psubs(2, B_0, -1, -1,  Z, W);
		ck[Y].psubs(2, B_0, -1, -1, -1, W);
		ck[Z].psubs(2, B_0, -1, -1, -1, W);

		exchangePhotos(X, Y);
		ck[X].psubs(2, A_0, -1, -1, -1, W);
		ck[Y].psubs(2, A_0, -1, -1, -1, W);
		ck[Z].psubs(2, A_0, -1, -1, -1, W);

		ck[X].psubs(2, B_0, -1, -1, -1, W);
		ck[Y].psubs(2, B_0, -1, -1, -1, W);
		ck[Z].psubs(2, B_0, -1, -1, -1, W);
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
	 * B vs. C:
	 * B =) C,
	 *     [B, B:0, 1] for S=B, n=1 > C.b=0; clear SUB=C/B, C.b=B.b
	 *     [A, A:0, 1] for S=A, n=1 > C.a=0; clear SUB=C/B, C.a=B.a
	 *     B.c=C.c
	 * 
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
	 *  C 1 1 1 0   [C.a = max(B.a, C.a), C.b = max(B.b, C.b), C.c++, C.d=max(B.d, C.d)]
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
	void test02UpdateTransit() throws TransException, SQLException {
	}

	/**
	 * Test W join with X
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
	 * W (=) X, sid[X:w]: X added W
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
	void testJoinChild() throws Exception {
		Utils.logi("\n=== === %s === ===", new Object(){}.getClass().getEnclosingMethod().getName());

		ck[X].synodes(X, Y, Z, -1);

		Utils.logi("1.1 -------- Y on W joining ---------");
		@SuppressWarnings("unused")
		ChangeLogs w_ack = joinSubtree(Y, W);

		ck[Y].synodes(X,  Y,  Z, W);
		ck[W].synodes(-1, Y, -1, W);

		ck[Y].change(1, C, "W", ck[Y].synm);
		ck[Y].synsubs(2, "Y,W", X, -1, Z, -1);
		
		Utils.logi("1.2 ------------- X vs Y ------------");
		exchangeSynodes(X, Y);
		ck[X].synodes(X, Y, Z, W);
		ck[X].change(1, C, "Y", "W", ck[X].synm);
		ck[X].synsubs(1, "Y,W", -1, -1, Z, -1);

		ck[Z].synodes(X, Y, Z, -1);
		ck[Z].synsubs(0, "Y,W", -1, -1, -1, -1);
		
		
		Utils.logi("2.1 ------------- X create photos ------------");
		String[] A_0_uids = insertPhoto(X);
		String A_0 = A_0_uids[0];
		
		Utils.logi("2.2 ----------------- X vs Z -----------------");
		exchangeSynodes(X, Z);
		
		Utils.logi("2.3 ----------------- Z vs W -----------------");
		try { exchangeSynodes(Z, W); }
		catch (SemanticException e){
			Utils.logi(e.getMessage());
			return;
		}
		fail("W is not roamingable");
	}

	/**
	 * <pre>
	 * 1. A insert A:0, update C:0; B insert B:0, update C:0
	 * A                     | B                    
	 * crud, s, pid, n,  sub  | crud, s, pid, n,  sub
	 *  I,   A, A:0, x1,  B   |  I,   B, B:0, y1,  A 
	 *                    C   |                    C 
	 *  U,   A, C:0, x1,  B   |  U,   B, C:0, y1,  A 
	 *                    C   |                    C 
	 *
	 *    a  b
	 *  A x1
	 *  B    y1
	 *-----------------------------------------------------------------------
	 * 
	 * Action:
	 * B insert A:0 for B.a < A:0[s=A, sub=B].n=x1
	 * A insert B:0 for A.b=0 < B:0[s=B, sub=A].n=y1
	 * A replace C:0 with B:C:0, because B has priority
	 * 
	 * A                      | B                    
	 * crud, s, pid, n,  sub  | crud, s, pid, n,  sub
	 *  I,   A, A:0, x1, [B] x|  I,   B, B:0, y1, [A] x
	 *                    C   |                    C 
	 *  I,   A, B:0, x1,  C   |  I,   B, A:0, y1,  C 
	 *  U,   A, C:0, y1, [B]  |  U,   B, C:0, y1, [A] 
	 *                    C   |                    C 
	 *
	 *    a  b
	 *  A x2 y1
	 *  B x1 y2
	 *-----------------------------------------------------------------------
	 * B (=) C
	 * A                      | B                       | C
	 * crud, s, pid, n,  sub  | crud, s, pid, n,  sub   | crud, s, pid, n, sub
	 *  I,   A, A:0, x1, [ ] x|  I,   B, B:0, y1, [ ]   |  
	 *                    C   |                   [C] x |
	 *  I,   A, B:0, x1,  C   |  I,   B, A:0, y1, [C] x |
	 *  U,   A, C:0, y1, [ ]  |  U,   B, C:0, y1, [ ]   |
	 *                    C   |                   [C] x |
	 *
	 *    a  b  c
	 *  A x2 y1  
	 *  B x1 y3 z0
	 *  C x1 y2 z3
	 *  
	 *-----------------------------------------------------------------------
	 * A (=) B
	 * A                      | B                      | C
	 * crud, s, pid, n,  sub  | crud, s, pid, n, sub   | crud, s, pid, n, sub
	 *  I,   A, A:0, x1,      |                        |  
	 *                   [C] x|                        |
	 *  I,   A, B:0, x1, [C] x|                        |
	 *  U,   A, C:0, y1,      |                        |
	 *                   [C] x|                        |
	 * 
	 * Actions:
	 * A clear changes as chg.n=x1 = B.a, and A.c < B.c (A merge with C earlier than B with C) 
	 * A.n0 = max(B.a, B.b, B.c, ++A.n0) 
	 * B.a = A.n0, by A.ack
	 *
	 *    a  b  c
	 *  A x4 y3 z0   A.c = max(A.c, B.c) = z0 
	 *  B x2 y4 z0
	 *  C x1 y2 z3
	 * </pre>
	 */
	void test04conflict() throws Exception {
	}

	/**
	 * apply.nv = admin.nv
	 * admin.n0++
	 * insert synode(apply)
	 * 
	 * @param admin
	 * @param apply
	 * @return [synid, uids]
	 * @throws TransException
	 * @throws SQLException
	 */
	ChangeLogs joinSubtree(int admin, int apply) throws TransException, SQLException {
		DBSynsactBuilder atb = ck[admin].trb;
		DBSynsactBuilder ctb = ck[apply].trb;

		ExchangeContext ax = new ExchangeContext(chm, atb, ctb.synode());
		ExchangeContext cx = new ExchangeContext(chm, ctb, atb.synode());

		// admin
		Utils.logi("(.1) %s accept %s", atb.synode(), ctb.synode());
		ChangeLogs resp = atb.addChild(ax, ctb.synode(), SynodeMode.child, ck[admin].robot(), Ck.org, ck[admin].domain);
		printChangeLines(ck);
		printNyquv(ck);

		// applicant
		Utils.logi("(.2) %s initiate domain", ctb.synode());
		ChangeLogs ack  = ctb.initDomain(cx, ctb.synode(), resp);
		printChangeLines(ck);
		printNyquv(ck);

		// admin
		Utils.logi("(.3) %s ack initiation", atb.synode());
		// atb.incN0(maxn(ack.nyquvect));
		printChangeLines(ck);
		printNyquv(ck);

		// applicant
		Utils.logi("(.4) %s closing application", ctb.synode());
		// ctb.nyquvect = Nyquence.clone(atb.nyquvect);
		HashMap<String, Nyquence> closenv = ctb.closeJoining(cx, atb.synode(), Nyquence.clone(atb.nyquvect));
		printChangeLines(ck);
		printNyquv(ck);

		Utils.logi("(.5) %s on closing", atb.synode());
		atb.oncloseJoiningp(ax, ctb.synode(), closenv);

		printChangeLines(ck);
		printNyquv(ck);

		return ack;
	}
	
	/**
	 * Go through logs' exchange, where client initiate the process.
	 * 
	 * @param srv hub
	 * @param cli client
	 * @throws SQLException 
	 * @throws TransException 
	 * @throws IOException 
	 */
	void exchangePhotos(int srv, int cli) throws TransException, SQLException, IOException {
		DBSynsactBuilder ctb = ck[cli].trb;
		DBSynsactBuilder stb = ck[srv].trb;

		SyntityMeta sphm = new T_PhotoMeta(stb.basictx().connId()).replace();
		SyntityMeta cphm = new T_PhotoMeta(ctb.basictx().connId()).replace();
		
		exchange(sphm, stb, cphm, ctb);
	}

	void exchangeSynodes(int srv, int cli) throws TransException, SQLException, IOException {
		DBSynsactBuilder ctb = ck[cli].trb;
		DBSynsactBuilder stb = ck[srv].trb;

		SyntityMeta ssnm = new SynodeMeta(stb.basictx().connId(), stb).replace();
		SyntityMeta csnm = new SynodeMeta(ctb.basictx().connId(), ctb).replace();
		
		exchange(ssnm, stb, csnm, ctb);
	}

	static void exchange(SyntityMeta sphm, DBSynsactBuilder stb, SyntityMeta cphm, DBSynsactBuilder ctb)
			throws TransException, SQLException, IOException {

		ExchangeContext cx = new ExchangeContext(chm, ctb, stb.synode());
		ExchangeContext sx = new ExchangeContext(chm, stb, ctb.synode());

		// 0, X init
		Utils.logi("\n(0.0): %s initiate", ctb.synode());
		ChangeLogs req = ctb.initExchange(cx, stb.synode());
		assertNotNull(req);
		
		if (eq(stb.synode(), "Y") && eq(ctb.synode(), "X")) {
			if (req.challenges() >= 3 || req.enitities(cphm.tbl) >= 2)
				Utils.warn("ERROR, req.challenges() >= 3 || req.enitities(cphm.tbl) >= 2,"
						+ "but it's not planned to fix this problem in half-duplex mode");
		}

		Utils.logi("(0.1): %s initiate\tchanges: %d\tentities: %d",
				ctb.synode(), req.challenges(), req.enitities(cphm.tbl));

		while (req != null) {
			// server
			Utils.logi("\n(1.0): %s on exchange", stb.synode());
			ChangeLogs resp = stb.onExchange(sx, ctb.synode(), ctb.nyquvect, req);
			Utils.logi("(1.1): %s on exchange response\tchanges: %d\tentities: %d\tanswers: %d",
					stb.synode(), resp.challenges(), resp.enitities(), resp.answers());
			printChangeLines(ck);
			printNyquv(ck);

			// client
			Utils.logi("\n(2.0): %s ack exchange", ctb.synode());
			ChangeLogs ack = ctb.ackExchange(cx, resp, stb.synode());
			Utils.logi("(2.1): %s ack exchange acknowledge\tchanges: %d\tentities: %d\tanswers: %d",
					ctb.synode(), ack.challenges(), ack.enitities(), ack.answers());
			printChangeLines(ck);
			printNyquv(ck);
			
			// server
			Utils.logi("\n(3.0): %s on ack", stb.synode());
			stb.onAck(sx, ack, ctb.synode(), sphm);
			printChangeLines(ck);
			printNyquv(ck);

			// client
			Utils.logi("\n(4.0): %s initiate again", ctb.synode());
			req = ctb.initExchange(cx, stb.synode());
			Utils.logi("(4.1): %s initiate again\tchanges: %d\tentities: %d",
					ctb.synode(), req.challenges(), req.enitities());
			printChangeLines(ck);
			printNyquv(ck);
			
			if (req.challenges() == 0)
				break;
		}

		assertNotNull(req);
		assertEquals(0, req.challenge == null ? 0 : req.challenge.size());

		Utils.logi("\n(5.0): %s closing exchange", ctb.synode());
		HashMap<String, Nyquence> nv = ctb.closexchange(cx, stb.synode(), Nyquence.clone(stb.nyquvect));

		printChangeLines(ck);
		printNyquv(ck);

		Utils.logi("(5.1) %s on closing exchange", stb.synode());
		// FIXME what if server don't agree?
		stb.onclosexchange(sx, ctb.synode(), nv);
		printChangeLines(ck);
		printNyquv(ck);

		if (req.challenges() > 0)
			fail("Shouldn't has any more challenge here.");
	
	}

	void updatePhoto(int s, String pid) throws TransException, SQLException {
		SyntityMeta entm = ck[s].phm;
		String conn = conns[s];
		String synoder = ck[s].trb.synode();
		DBSynsactBuilder trb = ck[s].trb;
		SyncRobot robot = (SyncRobot) ck[s].robot();
		
		trb.update(entm.tbl, robot)
			.nv(entm.synoder, synoder)
			.whereEq(chm.pk, pid)
			.u(trb.instancontxt(conn, robot))
			;
		
		trb.insert(chm.tbl)
			.nv(chm.crud, CRUD.U)
			.nv(chm.synoder, synoder)
			.nv(chm.uids, synoder + chm.UIDsep + pid)
			.nv(chm.nyquence, trb.n0().n)
			.post(trb
				.delete(sbm.tbl)
				.whereEq(sbm.entbl, entm.tbl)
				.whereEq(sbm.synodee, synoder)
				.whereEq(sbm.uids, concatstr(synoder, chm.UIDsep, pid))
				.post(trb.insert(sbm.tbl)
					.cols(sbm.entbl, sbm.synodee, sbm.uids)
					.select(trb.select(sbm.tbl)
						.col(constr(entm.tbl)).col(constr(synoder))
						.col(concatstr(synoder, chm.UIDsep, pid)))))
						// FIXME should be posted as
						// concat(constr(synoder), constr(chm.UIDsep), new Resulving(pid, ""))
			.ins(trb.instancontxt(conn, robot));
	}
	
	String[] insertPhoto(int s) throws TransException, SQLException {
		SyntityMeta entm = ck[s].phm;
		String conn = conns[s];
		String synoder = ck[s].trb.synode();
		DBSynsactBuilder trb = ck[s].trb;
		SyncRobot robot = (SyncRobot) ck[s].robot();
		
		T_PhotoMeta m = ck[s].phm;
		String pid = ((SemanticObject) trb
			.insert(m.tbl, robot)
			.nv(m.uri, "")
			.nv(m.fullpath, father)
			.nv(m.org(), ZSUNodesDA.family)
			.nv(m.device(), robot.uid())
			.nv(m.folder, robot.uid())
			.nv(m.shareDate, now())
			.ins(trb.instancontxt(conn, robot)))
			.resulve(entm);
		
		assertFalse(isblank(pid));
		
		trb .insert(chm.tbl, robot)
			.nv(chm.entfk, pid)
			.nv(chm.entbl, m.tbl)
			.nv(chm.crud, CRUD.C)
			.nv(chm.synoder, synoder)
			.nv(chm.uids, concatstr(synoder, chm.UIDsep, pid))
			.nv(chm.nyquence, trb.n0().n)
			.nv(chm.org, robot.orgId)
			.post(trb.insert(sbm.tbl)
				.cols(sbm.entbl, sbm.synodee, sbm.uids, sbm.org)
				.select((Query) trb
					.select(snm.tbl)
					.col(constr(entm.tbl))
					.col(snm.synoder)
					.col(concatstr(synoder, chm.UIDsep, pid))
					.col(constr(robot.orgId))
					.where(op.ne, snm.synoder, constr(trb.synode()))
					.whereEq(snm.domain, robot.orgId)))
			.ins(trb.instancontxt(conn, robot))
			;
		
		// return pid;
		return new String[] {pid, chm.uids(synoder, pid)};
	}
	
	String deletePhoto(SynChangeMeta chgm, int s)
			throws TransException, SQLException {

		T_PhotoMeta m = ck[s].phm;
		AnResultset slt = ((AnResultset) ck[s].trb
				.select(chgm.tbl, conns)
				.orderby(m.pk, "desc")
				.limit(1)
				.rs(ck[s].trb.instancontxt(ck[s].connId(), ck[s].robot()))
				.rs(0))
				.nxt();
		String pid = slt.getString(m.pk);

		pid = ((SemanticObject) ck[s].trb
			.delete(m.tbl, ck[s].robot())
			.whereEq(chgm.uids, pid)
			.d(ck[s].trb.instancontxt(conns[s], ck[s].robot())))
			.resulve(ck[s].phm.tbl, ck[s].phm.pk);
		
		assertFalse(isblank(pid));
		return pid;
	}

	/**
	 * Checker of each Synode.
	 * @author Ody
	 */
	public static class Ck {
		public static final String org = "URA";

		public T_PhotoMeta phm;
		public SynodeMeta synm;

		final DBSynsactBuilder trb;

		final String domain;

		public IUser robot() { return trb.synrobot(); }
		String connId() { return trb.basictx().connId(); }

		public Ck(int s, DBSynsactBuilder trb, String org) throws SQLException, TransException, ClassNotFoundException, IOException {
			this(conns[s], trb, org, String.format("s%s", s), "rob-" + s);
			phm = new T_PhotoMeta(conns[s]);
		}

		/**
		 * Verify all synodes information here are as expected.
		 * 
		 * @param x X presented here, -1 for disappearing
		 * @param y
		 * @param z
		 * @param w
		 */
		public void synodes(int x, int y, int z, int w) {
		}

		public Ck(String conn, DBSynsactBuilder trb, String org, String synid, String usrid)
				throws SQLException, TransException, ClassNotFoundException, IOException {
			this.trb = trb;
			this.domain = org;
		}

		public HashMap<String, Nyquence> cloneNv() {
			HashMap<String, Nyquence> nv = new HashMap<String, Nyquence>(4);
			for (String n : trb.nyquvect.keySet())
				nv.put(n, new Nyquence(trb.nyquvect.get(n).n));
			return nv;
		}

		/**
		 * Verify change flag, crud, where tabl = entm.tbl, entity-id = eid.
		 * 
		 * @param count 
		 * @param crud flag to be verified
		 * @param eid  entity id
		 * @param entm entity table meta
		 * @return nyquence.n
		 * @throws TransException
		 * @throws SQLException
		 */
		public long change(int count, String crud, String eid, SyntityMeta entm)
				throws TransException, SQLException {
			return change(count, crud, trb.synode(), eid, entm);
		}

		public long change(int count, String crud, String synoder, String eid, SyntityMeta entm)
				throws TransException, SQLException {
			AnResultset chg = (AnResultset) trb
				.select(chm.tbl, "ch")
				.cols(chm.cols())
				.whereEq(chm.org, domain)
				.whereEq(chm.entbl, entm.tbl)
				.whereEq(chm.synoder, synoder)
				.whereEq(chm.uids, synoder + chm.UIDsep + eid)
				.rs(trb.instancontxt(connId(), robot()))
				.rs(0);
			
			if (!chg.next() && count > 0)
				fail("Some change logs are missing...");

			assertEquals(count, chg.getRowCount());
			if (count > 0) {
				assertEquals(crud, chg.getString(chm.crud));
				assertEquals(entm.tbl, chg.getString(chm.entbl));
				assertEquals(synoder, chg.getString(chm.synoder));
				return chg.getLong(chm.nyquence);
			}
			return 0;
		}

		/**
		 * verify h_photos' subscription.
		 * @param uids
		 * @param sub subscriptions for X/Y/Z/W, -1 if not exists
		 * @throws SQLException 
		 * @throws TransException 
		 */
		public void psubs(int subcount, String uids, int ... sub) throws SQLException, TransException {
			ArrayList<String> toIds = new ArrayList<String>();
			for (int n : sub)
				if (n >= 0)
					toIds.add(ck[n].trb.synode());
			subsCount(phm, subcount, uids, toIds.toArray(new String[0]));
		}

		public void synsubs(int subcount, String uids, int ... sub) throws SQLException, TransException {
			ArrayList<String> toIds = new ArrayList<String>();
			for (int n : sub)
				if (n >= 0)
					toIds.add(ck[n].trb.synode());
			subsCount(synm, subcount, uids, toIds.toArray(new String[0]));
		}

		public void subsCount(SyntityMeta entm, int subcount, String uids, String ... toIds)
				throws SQLException, TransException {
			if (isNull(toIds)) {
				AnResultset subs = trb.subscribes(connId(), domain, uids, entm, robot());
				assertEquals(subcount, subs.getRowCount());
				// assertEquals(entm.tbl, subs.getString(sbm.entbl));
			}
			else {
				int cnt = 0;
				AnResultset subs = trb.subscribes(connId(), domain, uids, entm, robot());
				subs.beforeFirst();
				while (subs.next()) {
					if (indexOf(toIds, subs.getString(sbm.synodee)) >= 0)
						cnt++;
				}

				if (subs.beforeFirst().next())
					assertEquals(entm.tbl, subs.getString(sbm.entbl));
				assertEquals(subcount, cnt);
			}
		}

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

	public static void printNyquv(Ck[] ck) {
		Utils.logi(Stream.of(ck).map(c -> { return c.trb.synode();})
				.collect(Collectors.joining("    ", "      ", "")));

		for (int cx = 0; cx < ck.length; cx++) {
			DBSynsactBuilder t = ck[cx].trb;
			Utils.logi(
				t.synode() + " [ " +
				Stream.of(X, Y, Z, W)
				.map((nx) -> {
					String n = ck[nx].trb.synode();
					return String.format("%3s",
						t.nyquvect.containsKey(n) ?
						t.nyquvect.get(n).n : "");
					})
				.collect(Collectors.joining(", ")) +
				" ]");

//				t.nyquvect.keySet().stream()
//				 .map((String n) -> {return String.format("%3s", t.nyquvect.get(n).n);})
//				 .collect(Collectors.joining(", ")) +
//				" ]");
		}
	}
	
	static class ChangeLine extends Anson {
		String s;
		public ChangeLine(AnResultset r) throws SQLException {
			this.s = String.format("%1$1s %2$2s %3$9s %4$4s %5$2s %6$2s",
				r.getString(chm.crud),
				r.getString(chm.synoder),
				r.getString(chm.uids),
				r.getString(chm.nyquence),
				r.getString(sbm.synodee),
				r.getString(ChangeLogs.ChangeFlag, " "));
		}
		
		@Override
		public String toString() { return s; }
	}
	
	public static void printChangeLines(Ck[] ck)
			throws TransException, SQLException {

		HashMap<String, ChangeLine[]> uidss = new HashMap<String, ChangeLine[]>();

		for (int cx = 0; cx < ck.length; cx++) {
			DBSynsactBuilder b = ck[cx].trb;
			HashMap<String, ChangeLine> idmap = ((AnResultset) b
					.select(chm.tbl, "ch")
					.cols("ch.*", sbm.synodee)
					.je("ch", sbm.tbl, "sub", chm.entbl, sbm.entbl, chm.org, sbm.org, chm.uids, sbm.uids)
					.rs(b.instancontxt(b.basictx().connId(), b.synrobot()))
					.rs(0))
					.<ChangeLine>map(new String[] {chm.uids, sbm.synodee}, (r) -> new ChangeLine(r));

			for(String uids : idmap.keySet()) {
				if (!uidss.containsKey(uids))
					uidss.put(uids, new ChangeLine[4]);

				uidss.get(uids)[cx] = idmap.get(uids);
			}
		}
		
		Utils.logi(Stream.of(ck).map(c -> strcenter(c.trb.synode(), 27)).collect(Collectors.joining("")));

		Utils.logi(uidss.values().stream().map(
			(ChangeLine[] line) -> {
				return Stream.of(line)
					.map(c -> c == null ? String.format("%25s",  " ") : c.s)
					.collect(Collectors.joining(" | "));
			})
			.collect(Collectors.joining("\n")));
	}
	
	public static String strcenter(String text, int len){
	    String out = String.format("%"+len+"s%s%"+len+"s", "",text,"");
	    float mid = (out.length()/2);
	    float start = mid - (len/2);
	    float end = start + len; 
	    return out.substring((int)start, (int)end);
	}

	public static void assertnv(long... nvs) {
		if (nvs == null || nvs.length == 0 || nvs.length % 2 != 0)
			fail("Invalid arguments to assert.");
		
		for (int i = 0; i < nvs.length/2; i++) {
			assertEquals(nvs[i], nvs[i + nvs.length/2], String.format("nv[%d] %d : %d", i, nvs[i], nvs[i + nvs.length/2]));
		}
	}
}
