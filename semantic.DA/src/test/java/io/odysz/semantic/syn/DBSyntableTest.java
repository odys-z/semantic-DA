package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.compoundVal;
import static io.odysz.common.LangExt.ifnull;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.strcenter;
import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.CRUD.C;
import static io.odysz.semantic.CRUD.U;
import static io.odysz.semantic.syn.ExessionAct.*;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.compound;
import static io.odysz.transact.sql.parts.condition.Funcall.concat;
import static io.odysz.transact.sql.parts.condition.Funcall.concatstr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.transact.sql.parts.condition.Funcall.now;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

import io.odysz.anson.x.AnsonException;
import io.odysz.common.Configs;
import io.odysz.common.LangExt;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.Predicate;
import io.odysz.transact.x.TransException;

/**
 * Full-duplex mode for exchanging logs are running.
 * 
 * See test/res/console-print.txt
 * 
 * @author Ody
 */
public class DBSyntableTest {
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
	static SynchangeBuffMeta xbm;
	static PeersMeta prm;

	static T_PhotoMeta phm;

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

	@BeforeAll
	public static void testInit() throws Exception {
		// DDL
		// Debug Notes:
		// Sqlite won't commit multiple (ignore following) sql in one batch, and quit silently!
		// Similar report: https://sqlite-users.sqlite.narkive.com/JqAIbcSi/running-multiple-ddl-statements-in-a-batch-via-jdbc
		// To verify this, uncomment the first line in ddl.
		Utils.tabwidth = 8;

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
		sbm = new SynSubsMeta(chm);
		xbm = new SynchangeBuffMeta(chm);
		prm = new PeersMeta();

		for (int s = 0; s < 4; s++) {
			String conn = conns[s];
			
			snm = new SynodeMeta(conn);
			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", snm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), snm.ddlSqlite);

			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", chm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), chm.ddlSqlite);

			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", sbm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), sbm.ddlSqlite);

			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", xbm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), xbm.ddlSqlite);

			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", prm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), prm.ddlSqlite);

			T_PhotoMeta phm = new T_PhotoMeta(conn); //.replace();

			Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", phm.tbl));
			Connects.commit(conn, DATranscxt.dummyUser(), phm.ddlSqlite);
			phm.replace();

			ArrayList<String> sqls = new ArrayList<String>();
			sqls.addAll(Arrays.asList(Utils.loadTxt("../oz_autoseq.sql").split(";-- --\n")));
			sqls.add(String.format("update oz_autoseq set seq = %d where sid = 'h_photos.pid'", (long) Math.pow(64, s+1)));

			sqls.add(String.format("delete from %s", snm.tbl));
			if (s != W)
				sqls.add(Utils.loadTxt("syn_nodes.sql"));
			else
				sqls.add(Utils.loadTxt("syn_nodes_w.sql"));

			sqls.add(String.format("delete from %s", phm.tbl));

			Connects.commit(conn, DATranscxt.dummyUser(), sqls);

			ck[s] = new Ck(s, new DBSyntableBuilder(conn, synodeIds[s], "zsu",
					s != W ? DBSyntableBuilder.peermode : DBSyntableBuilder.leafmode)
					.loadNyquvect0(conn));
			snm = (SynodeMeta) new SynodeMeta(conn).autopk(false); // .replace();
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
		printNyquv(ck);

		int no = 0;
		test01InsertBasic(++no);
		testJoinChild(++no);
		testBranchPropagation(++no);
		test02Update(++no);
	}

	void test01InsertBasic(int section)
			throws TransException, SQLException, IOException {
		Utils.logrst(new Object(){}.getClass().getEnclosingMethod().getName(), section);
		int subsect = 0;

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
		Utils.logrst("insert X", section, ++subsect);
		String[] A_0_uids = insertPhoto(X);
		String A_0 = A_0_uids[0];

		ck[X].changelog(1, C, A_0, ck[X].phm);
		ck[X].psubs(2, A_0_uids[1], -1, Y, Z, -1);

		// 1.2 insert B
		Utils.logrst("insert Y", section, ++subsect);
		String[] B_0_uids = insertPhoto(Y);
		String B_0 = B_0_uids[0];

		ck[Y].changelog(1, C, B_0, ck[Y].phm);
		ck[Y].psubs(2, B_0_uids[1], X, -1, Z, -1);
		
		printChangeLines(ck);
		printNyquv(ck);

		// 1.3. X <= Y
		Utils.logrst("X <= Y", section, ++subsect);
		exchangePhotos(X, Y, section, subsect);
		ck[Y].buf_change(0, C, ck[Y].trb.synode(), B_0, ck[Y].phm);
		ck[Y].change_log(1, C, ck[Y].trb.synode(), B_0, ck[Y].phm);
		ck[X].buf_change(0, C, ck[X].trb.synode(), A_0, ck[Y].phm);
		ck[X].change_log(1, C, ck[X].trb.synode(), A_0, ck[Y].phm);
		ck[Y].psubs(1, B_0_uids[1], -1, -1, Z, -1);
		ck[Y].psubs(1, A_0_uids[1], -1, -1, Z, -1);
		ck[X].psubs(1, B_0_uids[1], -1, -1, Z, -1);
		ck[X].psubs(1, A_0_uids[1], -1, -1, Z, -1);

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
		
		// 1.4 Y <= Z
		long Bc_ = nvy.get(z).n;
		long Cc_ = nvz.get(z).n;

		Utils.logrst("Y <= Z", section, ++subsect);
		exchangePhotos(Y, Z, section, subsect);
		ck[Z].buf_change(0, C, A_0, ck[Z].phm);
		ck[Z].buf_change(0, C, ck[X].trb.synode(), A_0, ck[Z].phm);
		ck[Z].psubs(0, A_0_uids[1], -1, -1, Z, -1);

		ck[Z].buf_change(0, C, B_0, ck[Y].phm);
		ck[Z].buf_change(0, C, ck[Y].trb.synode(), B_0, ck[Z].phm);
		ck[Z].psubs(0, B_0_uids[1], -1, -1, Z, -1);
		
		ck[Y].buf_change(0, C, A_0, ck[Y].phm);
		ck[Y].buf_change(0, C, B_0, ck[Y].phm);
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
		
		// 1.5 X <= Y
		Utils.logrst("X <= Y", section, ++subsect);
		exchangePhotos(X, Y, section, subsect);
		ck[X].buf_change(0, C, A_0, ck[X].phm);
		ck[X].buf_change(0, C, B_0, ck[X].phm);
		ck[X].psubs(0, A_0_uids[1], -1, -1, Z, -1);
		ck[X].psubs(0, B_0_uids[1], -1, -1, Z, -1);
	}

	void testJoinChild(int section) throws Exception {
		Utils.logrst(new Object(){}.getClass().getEnclosingMethod().getName(), section);

		int no = 0;
		
		ck[X].synodes(X, Y, Z, -1);

		Utils.logrst("Y on W joining", section, ++no);
		@SuppressWarnings("unused")
		String[] w_ack = joinSubtree(Y, W, section, no);

		ck[X].synodes(X,  Y,  Z, -1);
		ck[Y].synodes(X,  Y,  Z, W);
		ck[Z].synodes(X,  Y,  Z, -1);
		ck[W].synodes(-1, Y, -1, W);

		ck[Y].change_log(1, C, "Y", "W", ck[Y].synm);
		ck[Y].buf_change(0, C, "W", ck[Y].synm);
		ck[Y].synsubs(2, "Y,W", X, -1, Z, -1);
		
		Utils.logrst("X vs Y", section, ++no);
		exchangeSynodes(X, Y, section, 2);
		ck[X].synodes(X, Y, Z, W);
		ck[X].change_log(1, C, "Y", "W", ck[X].synm);
		ck[X].buf_change(0, C, "Y", "W", ck[X].synm);
		ck[X].synsubs(1, "Y,W", -1, -1, Z, -1);

		ck[Z].synodes(X, Y, Z, -1);
		ck[Z].synsubs(0, "Y,W", -1, -1, -1, -1);
		
		Utils.logrst("X create photos", section, ++no);
		String[] x_uids = insertPhoto(X);
		printChangeLines(ck);
		printNyquv(ck);

		ck[X].change_log(1, C, "X", x_uids[0], ck[X].phm);
		ck[X].buf_change(0, C, x_uids[0], ck[X].phm);
		ck[X].psubs (3, x_uids[1], -1, Y, Z, W);

		
		Utils.logrst("X vs Z", section, ++no);
		exchangeSynodes(X, Z, section, no);
		Utils.logrst("On X-Z: Now Z know X:3[Y], not X,W", section, no, 1);
		
		Utils.logrst("Z vs W", section, ++no);
		try { exchangeSynodes(Z, W, section, no); }
		catch (ExchangeException ex_at_w){
			DBSyntableBuilder ctb = ck[W].trb;
			DBSyntableBuilder stb = ck[Z].trb;

			Utils.logi(ex_at_w.getMessage());

			ExchangeBlock req = ctb.abortExchange(ex_at_w.persist);

			stb.onAbort(req);

			assertEquals(ck[W].trb.n0().n, ctb.stamp.n);
			assertEquals(ck[Z].trb.n0().n, stb.stamp.n);
			return;
		}
		fail("W is unable to roaming with Z.");
	}

	void testBranchPropagation(int section)
			throws TransException, SQLException, IOException {

		Utils.logrst(new String[] { new Object(){}.getClass().getEnclosingMethod().getName(),
							"- must call testJoinChild() first"}, section);
		int no = 0;

		ck[X].synodes(X,  Y,  Z, -1);
		ck[Y].synodes(X,  Y,  Z, W);
		ck[Z].synodes(X,  Y,  Z, -1);
		ck[W].synodes(-1, Y, -1, W);

		String z = ck[Z].trb.synode();

		// Utils.logi("\n(.1) -------- Z create photos ---------");
		Utils.logrst("Z create photos", section, ++no);
		printNyquv(ck);

		String[] z_uids = insertPhoto(Z);

		ck[Z].buf_change(0, C, z_uids[0], ck[Y].phm);
		ck[Z].change_log(1, C, "Z", z_uids[0], ck[Y].phm);
		ck[Z].psubs(3, z_uids[1], X, Y, -1, W);

		printChangeLines(ck);
		printNyquv(ck);
		
		Utils.logrst("Y vs Z", section, ++no);
		exchangePhotos(Y, Z, section, 2);
		assertEquals(ck[Z].trb.n0().n, ck[Z].trb.stamp.n);
		ck[Y].buf_change(0, C, z, z_uids[0], ck[Y].phm);
		ck[Y].change_log(1, C, z, z_uids[0], ck[Y].phm);
		ck[Y].psubs(2, z_uids[1], X, -1, -1, W);
		ck[Z].psubs(2, z_uids[1], X, -1, -1, W);

		Utils.logrst("Y vs W", section, ++no);
		exchangePhotos(Y, W, section, 3);
		ck[Y].buf_change(0, C, z, z_uids[0], ck[X].phm);
		ck[Y].change_log(1, C, z, z_uids[0], ck[X].phm);
		ck[Y].psubs(1, z_uids[1], X, -1, -1, -1);
		ck[W].buf_change(0, C, z, z_uids[0], ck[X].phm);
		ck[W].psubs(0, z_uids[1], -1, -1, -1, -1);

		Utils.logrst("X vs Y", section, ++no);
		exchangePhotos(X, Y, section, 4);
		ck[X].buf_change(0, C, null, null, ck[X].phm);
		ck[X].psubs(0, null, X, -1, -1, -1);
		ck[Y].buf_change(0, C, null, null, ck[X].phm);
		ck[Y].psubs(0, null, -1, -1, -1, -1);

		Utils.logrst("Y vs Z", section, ++no);
		exchangePhotos(Y, Z, section, 4);
		ck[Y].buf_change(0, C, null, null, ck[X].phm);
		ck[Y].psubs(0, null, X, -1, -1, -1);
		ck[Z].buf_change(0, C, null, null, ck[X].phm);
		ck[Z].psubs(0, null, -1, -1, -1, -1);
	}

	void test02Update(int section) throws Exception {
		Utils.logrst(new Object(){}.getClass().getEnclosingMethod().getName(), section);

		Utils.logrst("X update photos", section, 1);
		String[] xu = updatePname(X);
		printChangeLines(ck);
		printNyquv(ck);

		ck[X].buf_change(1, U, xu[0], ck[X].phm);
		ck[X].psubs(3, xu[1], -1, Y, Z, W);

		Utils.logrst("Y update photos", section, 2);
		String[] yu = updatePname(Y);
		printChangeLines(ck);
		printNyquv(ck);

		ck[Y].buf_change(1, U, yu[0], ck[Y].phm);
		ck[Y].psubs(3, yu[1], X, -1, Z, W);
		
		Utils.logrst("X => Y", section, 3);
		exchangePhotos(Y, X, section, 3);
		printChangeLines(ck);
		printNyquv(ck);

		ck[X].buf_change(1, U, null, ck[X].phm);
		ck[X].psubs(4, null, X, Y, Z, W);
		ck[X].psubs(4, null, -1, -1, Z, W);
		// ck[X].psubs(2, null, -1, -1, Z, -1);

		ck[Y].buf_change(1, U, null, ck[Y].phm);
		ck[Y].psubs(4, null, X, Y, Z, W);
		ck[Y].psubs(4, null, -1, -1, Z, W);
		// ck[Y].psubs(2, null, -1, -1, Z, -1);
	}

	void testBreakAck(int section) throws Exception {
		Utils.logrst(new Object(){}.getClass().getEnclosingMethod().getName(), section);

		int no = 0;

		Utils.logrst("X update, Y insert", section, ++no);
		String[] xu = updatePname(X);
		printChangeLines(ck);
		printNyquv(ck);

		ck[X].buf_change(1 + 1, // already have 1
				U, xu[0], ck[X].phm);
		ck[X].psubs(4 + 3,  // already have 4 
				null, -1, Y, Z, W);
		ck[X].psubs(3, xu[1], -1, Y, Z, W);

		String[] yi = insertPhoto(Y);
		ck[Y].buf_change(1, C, yi[0], ck[Y].phm);
		ck[Y].psubs(3, yi[1], X, -1, Z, W);

		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst("X <= Y", section, ++no);
		exchange_break(ck[X].phm, ck[X].trb, ck[Y].phm, ck[Y].trb, section, no);

		ck[X].buf_change(1, C, ck[X].trb.synode(), xu[0], ck[X].phm);
		ck[X].buf_change(1, C, ck[Y].trb.synode(), yi[0], ck[X].phm);
		ck[X].psubs(2, xu[1], -1, -1, Z, W);
		ck[X].psubs(2, yi[1], -1, -1, Z, W);
		ck[Y].buf_change(1, C, ck[X].trb.synode(), xu[0], ck[Y].phm);
		ck[Y].buf_change(1, C, ck[Y].trb.synode(), yi[0], ck[Y].phm);
		ck[Y].psubs(2, xu[1], -1, -1, Z, W);
		ck[Y].psubs(2, yi[1], -1, -1, Z, W);
	}
	
	/**
	 * insert synode(apply)
	 * 
	 * @param adminx
	 * @param applyx
	 * @return [server-session-id, client-session-id]
	 * @throws TransException
	 * @throws SQLException
	 */
	String[] joinSubtree(int adminx, int applyx, int testix, int sect)
			throws TransException, SQLException {
		DBSyntableBuilder admb = ck[adminx].trb;
		String admin = ck[adminx].trb.synode();
		DBSyntableBuilder cltb = ck[applyx].trb;

		int no = 0;

		// sign up as a new domain
		ExessionPersist cltp = new ExessionPersist(cltb, chm, sbm, xbm, admin);
		Utils.logrst(String.format("sign up by %s", cltb.synode()), testix, sect, ++no);

		ExchangeBlock req  = cltb.domainSignup(cltp, admin);
		ExessionPersist admp = new ExessionPersist(cltb, chm, sbm, xbm, cltb.synode(), req);

		// admin on sign up request
		Utils.logrst(String.format("%s on sign up", admin), testix, sect, ++no);
		// ExchangeBlock resp = atb.addChild(ap, ctb.synode(), SynodeMode.child, ck[admin].robot(), Ck.org, ck[admin].domain);
		// response with new domain id
		ExchangeBlock resp = admb.addMyChild(admp, req, Ck.org);
		Utils.logrst(String.format("sign up by %s : %s", admb.synode(), resp.session), testix, sect, ++no);
		printChangeLines(ck);
		printNyquv(ck);

		// applicant
		Utils.logrst(String.format("%s initiate domain", cltb.synode()), testix, sect, ++no);
		ExchangeBlock ack  = cltb.initDomain(cltp, admin, resp);
		printChangeLines(ck);
		printNyquv(ck);

		// admin
		Utils.logrst(String.format("%s ack initiation", admb.synode()), testix, sect, ++no);
		// atb.incN0(maxn(ack.nyquvect));
		printChangeLines(ck);
		printNyquv(ck);

		// applicant
		Utils.logrst(new String[] {cltb.synode(), "closing application"}, testix, sect, ++no);
		// ctb.nyquvect = Nyquence.clone(atb.nyquvect);
		HashMap<String, Nyquence> closenv = cltb.closeJoining(cltp, Nyquence.clone(admb.nyquvect));
		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst(new String[] {admb.synode(), "on closing"}, testix, sect, ++no);
		admb.closeJoining(admp, closenv);

		printChangeLines(ck);
		printNyquv(ck);

		return new String[] {admp.session(), cltp.session()};
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
	void exchangePhotos(int srv, int cli, int test, int subno)
			throws TransException, SQLException, IOException {
		DBSyntableBuilder ctb = ck[cli].trb;
		DBSyntableBuilder stb = ck[srv].trb;

		SyntityMeta sphm = new T_PhotoMeta(stb.basictx().connId());
		SyntityMeta cphm = new T_PhotoMeta(ctb.basictx().connId());
		
		exchange(sphm, stb, cphm, ctb, test, subno);
	}

	void exchangeSynodes(int srv, int cli, int test, int subno)
			throws TransException, SQLException, IOException {
		DBSyntableBuilder ctb = ck[cli].trb;
		DBSyntableBuilder stb = ck[srv].trb;

		SyntityMeta ssnm = new SynodeMeta(stb.basictx().connId()).replace();
		SyntityMeta csnm = new SynodeMeta(ctb.basictx().connId()).replace();
		
		exchange(ssnm, stb, csnm, ctb, test, subno);
	}

	static void exchange(SyntityMeta sphm, DBSyntableBuilder stb, SyntityMeta cphm,
			DBSyntableBuilder ctb, int test, int subno)
			throws TransException, SQLException, IOException {

		int no = 0;
		Utils.logrst(new String[] {ctb.synode(), "initiate"}, test, subno, ++no);
		ExessionPersist cp = new ExessionPersist(ctb, chm, sbm, xbm, stb.synode());
		ExchangeBlock ini = ctb.initExchange(cp, stb.synode());
		// assertTrue(ini.totalChallenges > 0);
		Utils.logrst(String.format("%s initiate: changes: %d    entities: %d",
				ctb.synode(), ini.totalChallenges, ini.enitities(cphm.tbl)), test, subno, no, 1);

		Utils.logrst(new String[] {stb.synode(), "on initiate"}, test, subno, ++no);
		ExessionPersist sp = new ExessionPersist(stb, chm, sbm, xbm, ctb.synode(), ini);
		ExchangeBlock rep = stb.onInit(sp, ini);
		Utils.logrst(String.format("%s on initiate: changes: %d    entities: %d",
				stb.synode(), rep.totalChallenges, rep.enitities(cphm.tbl)), test, subno, no, 1);

		//
		challengeAnswerLoop(sp, stb, cp, ctb, rep, test, subno, ++no);

		Utils.logrst(new String[] {ctb.synode(), "closing exchange"}, test, subno, ++no);
		ExchangeBlock req = ctb.closexchange(cp, rep);
		assertEquals(req.nv.get(ctb.synode()).n + 1, ctb.stamp.n);
		assertEquals(ready, cp.exstate());

		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst(new String[] {stb.synode(), "on closing exchange"}, test, subno, ++no);
		// FIXME what if the server doesn't agree?
		rep = stb.onclosexchange(sp, req);
		assertEquals(rep.nv.get(ctb.synode()).n + 1, stb.stamp.n);
		assertEquals(ready, sp.exstate());

		printChangeLines(ck);
		printNyquv(ck);

//		if (req.challenges() > 0)
//			fail("Shouldn't has any more challenge here.");
	}

	static void exchange_break(SyntityMeta sphm, DBSyntableBuilder stb, SyntityMeta cphm,
			DBSyntableBuilder ctb, int test, int subno)
			throws TransException, SQLException, IOException {

		int no = 0;
		ExessionPersist cp = new ExessionPersist(stb, chm, sbm, xbm, stb.synode());

		Utils.logrst(new String[] {ctb.synode(), "initiate"}, test, subno, ++no);
		ExchangeBlock ini = ctb.initExchange(cp, stb.synode());
		assertTrue(ini.totalChallenges > 0);

		ExessionPersist sp = new ExessionPersist(ctb, chm, sbm, xbm, ctb.synode(), ini);

		ctb.abortExchange(cp, stb.synode(), null);
		ini = ctb.initExchange(cp, stb.synode());
		Utils.logrst(String.format("%s initiate changes: %d",
				ctb.synode(), ini.totalChallenges), test, subno, ++no);
		
		ExchangeBlock rep = stb.onInit(sp, ini);
		Utils.logrst(String.format("%s on initiate: changes: %d    entities: %d",
				ctb.synode(), rep.totalChallenges, rep.enitities(cphm.tbl)), test, subno, ++no);

		ExchangeBlock req = null;

		req = ctb.exchangePage(cp, rep);
		Utils.logrst(new String[] {stb.synode(), "on exchange"}, test, subno, ++no);
		rep = stb.onExchange(sp, ctb.synode(), req);
		Utils.logrst(String.format("%s on exchange response    changes: %d    entities: %d    answers: %d",
				stb.synode(), rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, ++no);
		printChangeLines(ck);
		printNyquv(ck);
		// assertEquals(ExessionAct.exchanging, sp.exstate());
	
		// server had sent reply but client haven't got it
		// challenges & answers are saved at server
		ctb.restorexchange();

		if (cp.hasNextChpages(ctb)) {
			// client
			cp.nextChpage();
			req = ctb.exchangePage(cp, null);
			// server
			Utils.logrst(new String[] {stb.synode(), "on exchange"}, test, subno, ++no);
			try {
				// server found the client is trying an already replied challenge
				rep = stb.onExchange(sp, ctb.synode(), req);
				fail("Not here");
			}
			catch (ExchangeException exp) {
				// server reply with saved answers
				rep = stb.requirestore(sp, ctb.synode());

				Utils.logrst(String.format("%s reuires restore    changes: %d    entities: %d    answers: %d",
					stb.synode(), rep.chpage, rep.enitities(), rep.answers()), test, subno, ++no);
				ctb.onRequires(cp, rep);
			}

			challengeAnswerLoop(sp, stb, cp, ctb, rep, test, subno, ++no);

			assertNotNull(req);
			assertEquals(0, req.chpage);

			Utils.logrst(new String[] {ctb.synode(), "closing exchange"}, test, subno, ++no);
			req = ctb.closexchange(cp, rep);

			printChangeLines(ck);
			printNyquv(ck);
			// assertEquals(ExessionAct.ready, cp.exstate());

			Utils.logrst(new String[] {stb.synode(), "on closing exchange"}, test, subno, ++no);
			// FIXME what if server don't agree?
			stb.onclosexchange(sp, req);
			printChangeLines(ck);
			printNyquv(ck);
			// assertEquals(ExessionAct.ready, sp.exstate());

			if (req.totalChallenges > 0)
				fail("Shouldn't has any more challenge here.");

		}
		else fail("Not here");
	}

	private static void challengeAnswerLoop(ExessionPersist sp, DBSyntableBuilder stb, 
				ExessionPersist cp, DBSyntableBuilder ctb, ExchangeBlock rep, int test, int subno, int step)
				throws SQLException, TransException {
		int no = 0;

		Utils.logrst(new String[] {"exchanges", stb.synode(), "<=>", ctb.synode()}, test, subno, step);

		ExchangeBlock req = null;
		// ExchangeBlock rep = null;
		while (cp.hasNextChpages(ctb)
			|| rep != null && rep.act == init // force to go on the initiation respond
			|| rep!= null && rep.hasmore()) {
			// client
			Utils.logrst(new String[] {ctb.synode(), "exchange"}, test, subno, step, ++no);
			cp.nextChpage();
			req = ctb.exchangePage(cp, rep);
			Utils.logrst(String.format("%s exchange challenge    changes: %d    entities: %d    answers: %d",
					ctb.synode(), req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, 1);
			printChangeLines(ck);
			printNyquv(ck);

			// server
			Utils.logrst(new String[] {stb.synode(), "on exchange"}, test, subno, step, ++no);
			sp.nextChpage();
			rep = stb.onExchange(sp, ctb.synode(), req);

			Utils.logrst(String.format("%s on exchange response    changes: %d    entities: %d    answers: %d",
					stb.synode(), rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, step, no, 1);
			printChangeLines(ck);
			printNyquv(ck);
		}
	}

	/**
	 * insert photo
	 * @param s
	 * @return [photo-id, change-id, uids]
	 * @throws TransException
	 * @throws SQLException
	 */
	String[] insertPhoto(int s) throws TransException, SQLException {
		SyntityMeta entm = ck[s].phm;
		String conn = conns[s];
		String synoder = ck[s].trb.synode();
		DBSyntableBuilder trb = ck[s].trb;
		SyncRobot robot = (SyncRobot) ck[s].robot();
		
		T_PhotoMeta m = ck[s].phm;
		String pid = ((SemanticObject) trb
			.insert(m.tbl, robot)
			.nv(m.uri, "")
			.nv(m.resname, "photo-x")
			.nv(m.fullpath, father)
			.nv(m.org(), robot.domain())
			.nv(m.device(), robot.deviceId())
			.nv(m.folder, robot.uid())
			.nv(m.shareDate, now())
			// TODO .post(insert chid)?
			.ins(trb.instancontxt(conn, robot)))
			.resulve(entm);
		
		assertFalse(isblank(pid));
		
		String chid = ((SemanticObject) trb
			.insert(chm.tbl, robot)
			.nv(chm.entfk, pid)
			.nv(chm.entbl, m.tbl)
			.nv(chm.crud, CRUD.C)
			.nv(chm.synoder, synoder)
			.nv(chm.uids, concatstr(synoder, chm.UIDsep, pid))
			.nv(chm.nyquence, trb.stamp.n)
			.nv(chm.domain, robot.domain())
			.post(trb.insert(sbm.tbl)
				.cols(sbm.insertCols())
				.select((Query) trb
					.select(snm.tbl)
					.col(new Resulving(chm.tbl, chm.pk))
					.col(snm.synoder)
					.where(op.ne, snm.synoder, constr(trb.synode()))
					.whereEq(snm.domain, robot.domain)))
			.ins(trb.instancontxt(conn, robot)))
			.resulve(chm);
		
		// return pid;
		return new String[] {pid, chid, chm.uids(synoder, pid)};
	}
	
	String deletePhoto(SynChangeMeta chgm, int s) throws TransException, SQLException {
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
			// TODO .post(null)
			.d(ck[s].trb.instancontxt(conns[s], ck[s].robot())))
			.resulve(ck[s].phm.tbl, ck[s].phm.pk);
		
		assertFalse(isblank(pid));
		return pid;
	}
	
	String[] updatePname(int s)
			throws SQLException, TransException, AnsonException, IOException {
		T_PhotoMeta entm = ck[s].phm;
		DBSyntableBuilder t = ck[s].trb;
		AnResultset slt = ((AnResultset) t
				.select(entm.tbl, "ch")
				.whereEq(entm.synoder, t.synrobot().deviceId())
				.orderby(entm.pk, "desc")
				.limit(1)
				.rs(ck[s].trb.instancontxt(ck[s].connId(), ck[s].robot()))
				.rs(0))
				.nxt();
		
		if (slt == null)
			throw new SemanticException("No entity found: synode=%s, ck=%d",
					t.synode(), s);

		String pid   = slt.getString(entm.pk);
		String synodr= slt.getString(entm.synoder);
		String pname = slt.getString(entm.resname);

		String chgid = t.updateEntity(synodr, pid, entm,
			entm.resname, String.format("%s,%04d", (pname == null ? "" : pname), t.n0().n),
			entm.createDate, now());

		return new String[] {pid, chgid, synodr + chm.UIDsep + pid};
	}
	
	/**
	 * Checker of each Synode.
	 * @author Ody
	 */
	public static class Ck {
		public static final String org = "URA";

		public T_PhotoMeta phm;
		public SynodeMeta synm;

		final DBSyntableBuilder trb;

		final String domain;

		public IUser robot() { return trb.synrobot(); }
		String connId() { return trb.basictx().connId(); }

		public Ck(int s, DBSyntableBuilder dbSyntableBuilder) throws SQLException, TransException, ClassNotFoundException, IOException {
			this(conns[s], dbSyntableBuilder, String.format("s%s", s), "rob-" + s);
			phm = new T_PhotoMeta(conns[s]);
		}

		/**
		 * Verify all synodes information here are as expected.
		 * 
		 * @param nx ck index
		 * @throws TransException 
		 * @throws SQLException 
		 */
		public void synodes(int ... nx) throws TransException, SQLException {
			ArrayList<String> nodes = new ArrayList<String>();
			int cnt = 0;
			for (int x = 0; x < nx.length; x++) {
				if (nx[x] >= 0) {
					nodes.add(ck[x].trb.synode());
					cnt++;
				}
			}

			AnResultset rs = (AnResultset) trb.select(synm.tbl)
				.col(synm.synoder)
				.distinct(true)
				.whereIn(synm.synoder, nodes)
				.rs(trb.instancontxt(trb.basictx().connId(), trb.synrobot()))
				.rs(0);
			assertEquals(cnt, rs.getRowCount());
		}

		public Ck(String conn, DBSyntableBuilder dbSyntableBuilder, String synid, String usrid)
				throws SQLException, TransException, ClassNotFoundException, IOException {
			trb = dbSyntableBuilder;
			this.domain = trb.domain();
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
		public long buf_change(int count, String crud, String eid, SyntityMeta entm)
				throws TransException, SQLException {
			return buf_change(count, crud, trb.synode(), eid, entm);
		}

		public long change_log(int count, String crud, String synoder, String eid, SyntityMeta entm)
				throws TransException, SQLException {
			Query q = trb
					.select(chm.tbl, "ch")
					.cols((Object[])chm.insertCols())
					.whereEq(chm.domain, domain)
					.whereEq(chm.entbl, entm.tbl);
				if (synoder != null)
					q.whereEq(chm.synoder, synoder);
				if (eid != null)
					q.whereEq(chm.uids, synoder + chm.UIDsep + eid);

				AnResultset chg = (AnResultset) q
						.rs(trb.instancontxt(connId(), robot()))
						.rs(0);
				
				if (!chg.next() && count > 0)
					fail(String.format("Expecting count == %d, but is actual 0", count));

				assertEquals(count, chg.getRowCount());
				if (count > 0) {
					assertEquals(crud, chg.getString(chm.crud));
					assertEquals(entm.tbl, chg.getString(chm.entbl));
					assertEquals(synoder, chg.getString(chm.synoder));
					return chg.getLong(chm.nyquence);
				}
				return 0;
		}

		public long buf_change(int count, String crud, String synoder, String eid, SyntityMeta entm)
				throws TransException, SQLException {
			Query q = trb
				.select(chm.tbl, "ch")
				.je_(xbm.tbl, "xb", chm.pk, xbm.changeId, chm.synoder, xbm.peer)
				.cols((Object[])chm.insertCols())
				.whereEq(chm.domain, domain)
				.whereEq(chm.entbl, entm.tbl);
			if (synoder != null)
				q.whereEq(chm.synoder, synoder);
			if (eid != null)
				q.whereEq(chm.uids, synoder + chm.UIDsep + eid);

			AnResultset chg = (AnResultset) q
					.rs(trb.instancontxt(connId(), robot()))
					.rs(0);
			
			if (!chg.next() && count > 0)
				fail(String.format("Expecting count == %d, but is actual 0", count));

			assertEquals(count, chg.getRowCount());
			if (count > 0) {
				assertEquals(crud, chg.getString(chm.crud));
				assertEquals(entm.tbl, chg.getString(chm.entbl));
				assertEquals(synoder, chg.getString(chm.synoder));
				return chg.getLong(chm.nyquence);
			}
			return 0;
		}

		public long changelog(int count, String crud, String eid, SyntityMeta entm)
				throws TransException, SQLException {
			return changelog(count, crud, trb.synode(), eid, entm);
		}

		public long changelog(int count, String crud, String synoder, String eid, SyntityMeta entm)
				throws TransException, SQLException {
			Query q = trb
				.select(chm.tbl, "ch")
				.cols((Object[])chm.insertCols())
				.whereEq(chm.domain, domain)
				.whereEq(chm.entbl, entm.tbl);
			if (synoder != null)
				q.whereEq(chm.synoder, synoder);
			if (eid != null)
				q.whereEq(chm.uids, synoder + chm.UIDsep + eid);

			AnResultset chg = (AnResultset) q
					.rs(trb.instancontxt(connId(), robot()))
					.rs(0);
			
			if (!chg.next() && count > 0)
				fail(String.format("Expecting count == %d, but is actual 0", count));

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
		 * @param chgid
		 * @param sub subscriptions for X/Y/Z/W, -1 if not exists
		 * @throws SQLException 
		 * @throws TransException 
		 */
		public void psubs(int subcount, String chgid, int ... sub) throws SQLException, TransException {
			ArrayList<String> toIds = new ArrayList<String>();
			for (int n : sub)
				if (n >= 0)
					toIds.add(ck[n].trb.synode());
			subsCount(phm, subcount, chgid, toIds.toArray(new String[0]));
		}

		public void synsubs(int subcount, String uids, int ... sub) throws SQLException, TransException {
			ArrayList<String> toIds = new ArrayList<String>();
			for (int n : sub)
				if (n >= 0)
					toIds.add(ck[n].trb.synode());

			// subsCount(synm, subcount, chgId, toIds.toArray(new String[0]));
				int cnt = 0;
				// AnResultset subs = trb.subscribes(connId(), domain, uids, entm, robot());
				AnResultset subs = (AnResultset) trb
						.select(chm.tbl, "ch")
						.je_(sbm.tbl, "sb", chm.pk, sbm.changeId)
						.cols_byAlias("sb", sbm.cols())
						.whereEq(chm.uids, uids)
						.rs(trb.instancontxt(connId(), robot()))
						.rs(0);
						;

				subs.beforeFirst();
				while (subs.next()) {
					if (indexOf(toIds.toArray(new String[0]), subs.getString(sbm.synodee)) >= 0)
						cnt++;
				}

				assertEquals(subcount, cnt);
				assertEquals(subcount, subs.getRowCount());
		}

		public void subsCount(SyntityMeta entm, int subcount, String chgId, String ... toIds)
				throws SQLException, TransException {
			if (isNull(toIds)) {
				// AnResultset subs = trb.subscribes(connId(), domain, uids, entm, robot());
				AnResultset subs = subscribes(connId(), chgId, entm, robot());
				assertEquals(subcount, subs.getRowCount());
			}
			else {
				int cnt = 0;
				// AnResultset subs = trb.subscribes(connId(), domain, uids, entm, robot());
				AnResultset subs = subscribes(connId(), chgId, entm, robot());
				subs.beforeFirst();
				while (subs.next()) {
					if (indexOf(toIds, subs.getString(sbm.synodee)) >= 0)
						cnt++;
				}

				assertEquals(subcount, cnt);
				assertEquals(subcount, subs.getRowCount());
			}
		}

		/**
		 * Verify subscribes
		 * @param connId
		 * @param chgId
		 * @param entm
		 * @param robot
		 * @return results
		 */
		private AnResultset subscribes(String connId, String chgId, SyntityMeta entm, IUser robot) 
			throws TransException, SQLException {
			Query q = trb.select(sbm.tbl, "ch")
					.cols((Object[])sbm.cols())
					;
			if (!isblank(chgId))
				q.whereEq(sbm.changeId, chgId) ;

			return (AnResultset) q
					.rs(trb.instancontxt(connId, robot))
					.rs(0);
		}
		
		/**
		 * Verify if and only if one instance exists on this node.
		 * 
		 * @param synoder
		 * @param clientpath
		 */
		public void verifile(String synoder, String clientpath, T_PhotoMeta phm) {
			trb.select(phm.tbl	)
				.col(count(phm.pk), "c")
				.where(new Predicate(op.eq, compound(chm.uids), compoundVal(synoder, clientpath)))
				;
		}
	}

	public static void printNyquv(Ck[] ck) {
		Utils.logi(Stream.of(ck).map(c -> { return c.trb.synode();})
			.collect(Collectors.joining("    ", "      ", "")));

		for (int cx = 0; cx < ck.length; cx++) {
			DBSyntableBuilder t = ck[cx].trb;
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
		}
	}
	
	static String changeLine(AnResultset r) throws SQLException {
		String seq = r.getString(xbm.seq, " ");

		return String.format("%1$1s %2$9s %3$9s %4$2s %5$2s [%6$4s]",
				r.getString(chm.crud),
				r.getString(chm.pk),
				r.getString(chm.uids),
				r.getString(chm.nyquence),
				r.getString(sbm.synodee),
				seq == null ? " " : seq
				);
	}
	
	public static void printChangeLines(Ck[] ck)
			throws TransException, SQLException {

		HashMap<String, String[]> uidss = new HashMap<String, String[]>();

		for (int cx = 0; cx < ck.length; cx++) {
			DBSyntableBuilder b = ck[cx].trb;
			HashMap<String,String> idmap = ((AnResultset) b
					.select(chm.tbl, "ch")
					.cols("ch.*", sbm.synodee).col(concat(ifnull(xbm.peer, " "), "':'", xbm.seq), xbm.seq)
					// .je("ch", sbm.tbl, "sub", chm.entbl, sbm.entbl, chm.domain, sbm.domain, chm.uids, sbm.uids)
					.je_(sbm.tbl, "sub", chm.pk, sbm.changeId)
					.l_(xbm.tbl, "xb", chm.pk, xbm.changeId)
					.orderby(xbm.seq, chm.entbl, chm.uids)
					.rs(b.instancontxt(b.basictx().connId(), b.synrobot()))
					.rs(0))
					.<String>map(new String[] {chm.pk, sbm.synodee}, (r) -> changeLine(r));

			for(String cid : idmap.keySet()) {
				if (!uidss.containsKey(cid))
					uidss.put(cid, new String[ck.length]);

				uidss.get(cid)[cx] = idmap.get(cid);
			}
		}
		
		Utils.logi(Stream.of(ck).map(c -> strcenter(c.trb.synode(), 36)).collect(Collectors.joining("|")));
		Utils.logi(Stream.of(ck).map(c -> LangExt.repeat("-", 36)).collect(Collectors.joining("+")));

		Utils.logi(uidss.keySet().stream().map(
			(String linekey) -> {
				return Stream.of(uidss.get(linekey))
					.map(c -> c == null ? String.format("%34s",  " ") : c)
					.collect(Collectors.joining(" | ", " ", " "));
			})
			.collect(Collectors.joining("\n")));
	}
	
	static String buffChangeLine(AnResultset r) throws SQLException {
		return String.format("%1$1s %2$9s %3$9s %4$4s %5$2s",
				r.getString(chm.crud),
				r.getString(chm.pk),
				r.getString(chm.uids),
				r.getString(chm.nyquence),
				r.getString(sbm.synodee)
				);
	}

	public static void assertnv(long... nvs) {
		if (nvs == null || nvs.length == 0 || nvs.length % 2 != 0)
			fail("Invalid arguments to assert.");
		
		for (int i = 0; i < nvs.length/2; i++) {
			assertEquals(nvs[i], nvs[i + nvs.length/2], String.format("nv[%d] %d : %d", i, nvs[i], nvs[i + nvs.length/2]));
		}
	}
}
