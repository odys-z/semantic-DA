package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.CRUD.C;
import static io.odysz.semantic.CRUD.U;
import static io.odysz.semantic.syn.Docheck.assertI;
import static io.odysz.semantic.syn.Docheck.assertnv;
import static io.odysz.semantic.syn.Docheck.ck;
import static io.odysz.semantic.syn.Docheck.printChangeLines;
import static io.odysz.semantic.syn.Docheck.printNyquv;
import static io.odysz.semantic.syn.ExessionAct.init;
import static io.odysz.semantic.syn.ExessionAct.ready;
import static io.odysz.semantic.syn.ExessionPersist.loadNyquvect;
import static io.odysz.semantic.util.DAHelper.getNstamp;
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
import java.util.HashMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.anson.x.AnsonException;
import io.odysz.common.AssertImpl;
import io.odysz.common.Configs;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.AutoSeqMeta;
import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SemanticTableMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
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
	public static final String rtroot  = "src/test/res/";
	public static final String father  = "src/test/res/Sun Yet-sen.jpg";
	public static final String ukraine = "src/test/res/Ukraine.png";
	
	static final String zsu = "zsu";

	public static final int X = 0;
	public static final int Y = 1;
	public static final int Z = 2;
	public static final int W = 3;

	static String runtimepath;

	// public static Docheck[] ck; // = new Docheck[4];

	static SynodeMeta snm;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;
	static SynchangeBuffMeta xbm;
	static SynSessionMeta ssm;
	static PeersMeta prm;

	// static T_PhotoMeta phm;
	static String[] synodes;

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
			
			AutoSeqMeta autom = new AutoSeqMeta(conns[s]);
			Connects.commit(conns[s], DATranscxt.dummyUser(), autom.ddlSqlite);
		}

		ck = new Docheck[4];
		synodes = new String[] { "X", "Y", "Z", "W" };
		// new for triggering ddl loading - some error here FIXME
		// nyqm = new NyquenceMeta("");
		chm = new SynChangeMeta();
		sbm = new SynSubsMeta(chm);
		xbm = new SynchangeBuffMeta(chm);
		ssm = new SynSessionMeta();
		prm = new PeersMeta();

		for (int s = 0; s < 4; s++) {
			String conn = conns[s];
			snm = new SynodeMeta(conn);
			T_DA_PhotoMeta phm = new T_DA_PhotoMeta(conn); //.replace();

			SemanticTableMeta.setupSqliTables(conn, snm, chm, sbm, xbm, prm, ssm, phm);

			phm.replace();

			ArrayList<String> sqls = new ArrayList<String>();
			sqls.add("delete from oz_autoseq;");
			sqls.add(Utils.loadTxt("../oz_autoseq.sql"));
			sqls.add(String.format("update oz_autoseq set seq = %d where sid = 'h_photos.pid'", (long) Math.pow(64, s+1)));

			sqls.add(String.format("delete from %s", snm.tbl));
			if (s != W)
				sqls.add(Utils.loadTxt("syn_nodes.sql"));
			else
				sqls.add(Utils.loadTxt("syn_nodes_w.sql"));

			sqls.add(String.format("delete from %s", phm.tbl));

			Connects.commit(conn, DATranscxt.dummyUser(), sqls);

			Docheck.ck[s] = new Docheck(new AssertImpl(), s != W ? zsu : null, conn, synodes[s],
					s != DBSyntableTest.W ? SynodeMode.peer : SynodeMode.leaf, phm);
			
			// ck[s].synm = snm;
			if (s != W)
				Docheck.ck[s].trb.incNyquence0();

			DBSyntableBuilder.registerEntity(conn, Docheck.ck[s].docm);
		}
		
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
		test03delete(++no);
	}

	void test01InsertBasic(int section)
			throws TransException, SQLException, IOException {
		@SuppressWarnings("unchecked")
		HashMap<String, Nyquence>[] nvs = (HashMap<String, Nyquence>[]) new HashMap[] {
				// ck[X].nyquvect(), ck[Y].nyquvect(), ck[Z].nyquvect()
				loadNyquvect(ck[X].trb),
				loadNyquvect(ck[Y].trb),
				loadNyquvect(ck[Z].trb)};

		HashMap<String, Nyquence>[] nvs_ = Nyquence.clone(nvs);

		int no = 0;
		String x = synodes[X];

		// 1 insert A
		Utils.logrst("insert A", section, ++no);
		String[] X_0_uids = insertPhoto(X);
		String X_0 = X_0_uids[0];

		// syn_change.curd = C
		ck[X].change_log(1, C, synodes[X], X_0, ck[X].docm);
		// syn_subscribe.to = [B, C, D]
		ck[X].psubs(2, X_0_uids[1], -1, Y, Z, -1);

		// 2 insert B
		Utils.logrst("insert B", section, ++no);
		String[] B_0_uids = insertPhoto(Y);
		String B_0 = B_0_uids[0];

		ck[Y].change_log(1, C, synodes[Y], B_0, ck[Y].docm);
		ck[Y].psubs(2, B_0_uids[1], X, -1, Z, -1);
		
		printChangeLines(ck);
		printNyquv(ck);

		// 3. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeDocs(X, Y, section, no);
		printChangeLines(ck);
		nvs = printNyquv(ck);
		ck[Y].change_doclog(1, C, B_0);
		ck[Y].change_doclog(1, C, x, X_0);
		ck[Y].psubs(1, B_0_uids[1], -1, -1, Z, -1);
		ck[Y].psubs(1, X_0_uids[1], -1, -1, Z, -1);

		assertI(ck, nvs);
		assertnv(nvs_[X], nvs[X], 1, 1, 0);
		assertnv(nvs_[Y], nvs[Y], 1, 1, 0);
		assertnv(nvs_[Z], nvs[Z], 0, 0, 0);

		// 4. Y <= Z
		nvs_ = nvs.clone();
		Utils.logrst("Y <= Z", section, ++no);
		exchangeDocs(Y, Z, section, no);
		nvs = printNyquv(ck);

		ck[Z].change_log(0, C, synodes[Z], X_0, ck[Z].docm);
		ck[Z].change_log(0, C, x, X_0, ck[Z].docm);
		ck[Z].psubs(0, X_0_uids[1], -1, -1, Z, -1);

		ck[Z].change_doclog(0, C, B_0);
		ck[Z].change_doclog(0, C, B_0);
		ck[Z].psubs(0, B_0_uids[1], -1, -1, Z, -1);
		
		ck[Y].change_doclog(0, C, X_0);
		ck[Y].change_doclog(0, C, B_0);
		ck[Y].psubs(0, X_0_uids[1], -1, -1, Z, -1);
		ck[Y].psubs(0, B_0_uids[1], -1, -1, Z, -1);

		assertI(ck, nvs);
		assertnv(nvs_[X], nvs[X], 0, 0, 0);
		assertnv(nvs_[Y], nvs[Y], 0, 1, 2);
		assertnv(nvs_[Z], nvs[Z], 1, 2, 2);

		nvs_ = nvs.clone();
		
		// 4. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeDocs(X, Y, section, no);
		ck[X].change_doclog(0, C, X_0);
		ck[X].change_doclog(0, C, B_0);
		ck[X].psubs(0, X_0_uids[1], -1, -1, Z, -1);
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

		ck[Y].change_log(1, C, "Y", "W", ck[Y].trb.synm);
		ck[Y].buf_change(0, C, "W", ck[Y].trb.synm);
		ck[Y].synsubs(2, "Y,W", X, -1, Z, -1);
		
		Utils.logrst("X vs Y", section, ++no);
		exchangeSynodes(X, Y, section, 2);
		ck[X].synodes(X, Y, Z, W);
		ck[X].change_log(1, C, "Y", "W", ck[X].trb.synm);
		ck[X].buf_change(0, C, "Y", "W", ck[X].trb.synm);
		ck[X].synsubs(1, "Y,W", -1, -1, Z, -1);

		ck[Z].synodes(X, Y, Z, -1);
		ck[Z].synsubs(0, "Y,W", -1, -1, -1, -1);
		
		Utils.logrst("X create photos", section, ++no);
		String[] x_uids = insertPhoto(X);
		printChangeLines(ck);
		printNyquv(ck);

		ck[X].change_log(1, C, "X", x_uids[0], ck[X].docm);
		ck[X].buf_change(0, C, x_uids[0], ck[X].docm);
		ck[X].psubs (3, x_uids[1], -1, Y, Z, W);

		
		Utils.logrst("X <= Z", section, ++no);
		exchangeSynodes(X, Z, section, no);
		ck[Z].synodes(X, Y, Z, W);
		ck[Z].synsubs(0, "Y,W", -1, -1, -1, -1);
		Utils.logrst("On X-Z: Now Z knows W", section, ++no);
		
		Utils.logrst("Z vs W", section, ++no);
		try { exchangeSynodes(Z, W, section, no); }
		catch (ExchangeException ex_at_w){
			DBSyntableBuilder ctb = ck[W].trb;
			DBSyntableBuilder stb = ck[Z].trb;

			Utils.logi(ex_at_w.getMessage());

			ExchangeBlock req = ctb.abortExchange(ex_at_w.persist);

			stb.onAbort(req);

			assertEquals(ck[W].n0().n, ck[W].stamp());
			assertEquals(ck[Z].n0().n, ck[Z].stamp());
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

		Utils.logrst("Z create photos", section, ++no);
		printNyquv(ck);

		String[] z_uids = insertPhoto(Z);

		printChangeLines(ck);
		printNyquv(ck);

		ck[Z].buf_change(0, C, z_uids[0], ck[Y].docm);
		ck[Z].change_log(1, C, "Z", z_uids[0], ck[Y].docm);
		ck[Z].psubs(3, z_uids[1], X, Y, -1, W);
		
		Utils.logrst("Y vs Z", section, ++no);
		exchangeDocs(Y, Z, section, 2);
		assertEquals(ck[Z].n0().n, ck[Z].stamp());
		ck[Y].buf_change(0, C, z, z_uids[0], ck[Y].docm);
		ck[Y].change_log(1, C, z, z_uids[0], ck[Y].docm);
		ck[Y].psubs(2, z_uids[1], X, -1, -1, W);
		ck[Z].psubs(2, z_uids[1], X, -1, -1, W);

		Utils.logrst("Y vs W", section, ++no);
		exchangeDocs(Y, W, section, 3);
		ck[Y].buf_change(0, C, z, z_uids[0], ck[X].docm);
		ck[Y].change_log(1, C, z, z_uids[0], ck[X].docm);
		ck[Y].psubs(1, z_uids[1], X, -1, -1, -1);
		ck[W].buf_change(0, C, z, z_uids[0], ck[X].docm);
		ck[W].psubs(0, z_uids[1], -1, -1, -1, -1);

		Utils.logrst("X vs Y", section, ++no);
		exchangeDocs(X, Y, section, 4);
		ck[X].buf_change(0, C, null, null, ck[X].docm);
		ck[X].psubs(0, null, X, -1, -1, -1);
		ck[Y].buf_change(0, C, null, null, ck[X].docm);
		ck[Y].psubs(0, null, -1, -1, -1, -1);

		Utils.logrst("Y vs Z", section, ++no);
		exchangeDocs(Y, Z, section, 4);
		ck[Y].buf_change(0, C, null, null, ck[X].docm);
		ck[Y].psubs(0, null, X, -1, -1, -1);
		ck[Z].buf_change(0, C, null, null, ck[X].docm);
		ck[Z].psubs(0, null, -1, -1, -1, -1);
	}

	void test02Update(int section) throws Exception {
		Utils.logrst(new Object(){}.getClass().getEnclosingMethod().getName(), section);
		int no = 0;

		Utils.logrst("X update photos", section, ++no);
		String[] xu = updatePname(X);
		printChangeLines(ck);
		printNyquv(ck);

		ck[X].change_doclog(1, U, xu[0]);
		ck[X].buf_change(0, U, xu[0], ck[X].docm);
		ck[X].psubs(3, xu[1], -1, Y, Z, W);

		Utils.logrst("Y update photos", section, ++no);
		String[] yu = updatePname(Y);
		printChangeLines(ck);
		printNyquv(ck);

		ck[Y].change_doclog(1, U, yu[0]);
		ck[Y].buf_change(0, U, yu[0], ck[Y].docm);
		ck[Y].psubs(3, yu[1], X, -1, Z, W);
		
		Utils.logrst("X => Y", section, ++no);
		exchangeDocs(Y, X, section, no);
		printChangeLines(ck);
		printNyquv(ck);

		ck[X].change_doclog(1, U, null);
		ck[X].buf_change(0, U, null, ck[X].docm);
		ck[X].psubs(4, null, X, Y, Z, W);
		ck[X].psubs(4, null, -1, -1, Z, W);

		ck[Y].change_doclog(1, U, null);
		ck[Y].buf_change_p(0, U, null);
		ck[Y].psubs(4, null, X, Y, Z, W);
		ck[Y].psubs(4, null, -1, -1, Z, W);

		Utils.logrst("Y <= Z", section, ++no);
		exchangeDocs(Y, Z, section, no);
		printChangeLines(ck);
		printNyquv(ck);

		ck[Y].change_doclog(1, U, null);
		ck[Y].psubs(2, null, X, Y, Z, W);
		ck[Y].psubs(2, null, -1, -1, -1, W);
		ck[Y].psubs(1, yu[1], -1, -1, -1, W);
		ck[Y].psubs(1, xu[1], -1, -1, -1, W);
	}

	void test03delete(int test) throws Exception {
		Utils.logrst(new Object(){}.getClass().getEnclosingMethod().getName(), test);
		int no = 0;

		int x = ck[X].docs();
		int y = ck[Y].docs();

		Utils.logrst("X delete a photo", test, ++no);
		Object[] xd = deletePhoto(X);
		printChangeLines(ck);
		printNyquv(ck);
		assertFalse(isNull(xd));
		assertEquals(1, xd[1]);
		ck[X].doc(0, (String)xd[0]);
		ck[X].doc(x-1);
		Utils.logrst(new String[] {"X deleted", (String) xd[0]},
				test, ++no, 1);


		Utils.logrst("Y delete a photo", test, ++no);
		Object[] yd = deletePhoto(Y);
		printChangeLines(ck);
		printNyquv(ck);
		assertFalse(isNull(yd));
		assertEquals(1, yd[1]);
		ck[Y].doc(0, (String)yd[0]);
		ck[Y].doc(y-1);
		Utils.logrst(new String[] {"Y deleted", (String) yd[0]},
				test, ++no, 1);

		Utils.logrst("X <= Y", test, ++no);
		exchangeDocs(X, Y, test, no);
		printChangeLines(ck);
		printNyquv(ck);

		ck[Y].doc(0, (String)xd[0]);
		ck[Y].doc(y-2);

		ck[X].doc(0, (String)yd[0]);
		ck[X].doc(x-2);
	}
	
	void testBreakAck(int section) throws Exception {
		Utils.logrst(new Object(){}.getClass().getEnclosingMethod().getName(), section);

		int no = 0;

		Utils.logrst("X update, Y insert", section, ++no);
		String[] xu = updatePname(X);
		printChangeLines(ck);
		printNyquv(ck);

		ck[X].buf_change(1 + 1, // already have 1
				U, xu[0], ck[X].docm);
		ck[X].psubs(4 + 3,  // already have 4 
				null, -1, Y, Z, W);
		ck[X].psubs(3, xu[1], -1, Y, Z, W);

		String[] yi = insertPhoto(Y);
		ck[Y].buf_change(1, C, yi[0], ck[Y].docm);
		ck[Y].psubs(3, yi[1], X, -1, Z, W);

		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst("X <= Y", section, ++no);
		exchange_break(ssm, ck[X].docm, ck[X].trb, ck[Y].docm, ck[Y].trb, section, no);

		ck[X].buf_change(1, C, ck[X].trb.synode(), xu[0], ck[X].docm);
		ck[X].buf_change(1, C, ck[Y].trb.synode(), yi[0], ck[X].docm);
		ck[X].psubs(2, xu[1], -1, -1, Z, W);
		ck[X].psubs(2, yi[1], -1, -1, Z, W);
		ck[Y].buf_change(1, C, ck[X].trb.synode(), xu[0], ck[Y].docm);
		ck[Y].buf_change(1, C, ck[Y].trb.synode(), yi[0], ck[Y].docm);
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
		ExessionPersist cltp = new ExessionPersist(cltb, admin);
		Utils.logrst(String.format("sign up by %s", cltb.synode()), testix, sect, ++no);

		ExchangeBlock req  = cltb.domainSignup(cltp, admin);
		ExessionPersist admp = new ExessionPersist(admb, cltb.synode(), req);

		// admin on sign up request
		Utils.logrst(String.format("%s on sign up", admin), testix, sect, ++no);
		ExchangeBlock resp = admb.domainOnAdd(admp, req, Docheck.org);
		Utils.logrst(String.format("sign up by %s : %s", admb.synode(), resp.session), testix, sect, ++no);
		printChangeLines(ck);
		printNyquv(ck);

		// applicant
		Utils.logrst(String.format("%s initiate domain", cltb.synode()), testix, sect, ++no);

		ExchangeBlock ack  = cltb.domainitMe(cltp, admin, resp);
		Utils.logi(ack.nv);

		printChangeLines(ck);
		printNyquv(ck);

		// admin
		Utils.logrst(String.format("%s ack initiation", admb.synode()), testix, sect, ++no);
		printChangeLines(ck);
		printNyquv(ck);

		// applicant
		Utils.logrst(new String[] {cltb.synode(), "closing application"}, testix, sect, ++no);
		req = cltb.domainCloseJoin(cltp, resp);
		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst(new String[] {admb.synode(), "on closing"}, testix, sect, ++no);
		admb.domainCloseJoin(admp, req);

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
	void exchangeDocs(int srv, int cli, int test, int subno)
			throws TransException, SQLException, IOException {
		DBSyntableBuilder ctb = ck[cli].trb;
		DBSyntableBuilder stb = ck[srv].trb;

		SyntityMeta sphm = new T_DA_PhotoMeta(stb.basictx().connId());
		SyntityMeta cphm = new T_DA_PhotoMeta(ctb.basictx().connId());
		
		exchange(ssm, sphm, stb, cphm, ctb, test, subno);
	}

	void exchangeSynodes(int srv, int cli, int test, int subno)
			throws TransException, SQLException, IOException {
		DBSyntableBuilder ctb = ck[cli].trb;
		DBSyntableBuilder stb = ck[srv].trb;

		SyntityMeta ssnm = new SynodeMeta(stb.basictx().connId()).replace();
		SyntityMeta csnm = new SynodeMeta(ctb.basictx().connId()).replace();
		
		exchange(ssm, ssnm, stb, csnm, ctb, test, subno);
	}

	static void exchange(SynSessionMeta ssm, SyntityMeta sphm, DBSyntableBuilder stb, SyntityMeta cphm, 
			DBSyntableBuilder ctb, int test, int subno)
			throws TransException, SQLException, IOException {

		int no = 0;
		Utils.logrst(new String[] {ctb.synode(), "initiate"}, test, subno, ++no);
		ExessionPersist cp = new ExessionPersist(ctb, stb.synode());
		ExchangeBlock ini = ctb.initExchange(cp);
		Utils.logrst(String.format("%s initiate: changes: %d    entities: %d",
				ctb.synode(), ini.totalChallenges, ini.enitities(cphm.tbl)), test, subno, no, 1);

		Utils.logrst(new String[] {stb.synode(), "on initiate"}, test, subno, ++no);
		ExessionPersist sp = new ExessionPersist(stb, ctb.synode(), ini);
		ExchangeBlock rep = stb.onInit(sp, ini);
		Utils.logrst(String.format(
				"%s on initiate: changes: %d",
				stb.synode(), rep.totalChallenges),
				test, subno, no, 1);

		//
		challengeAnswerLoop(sp, stb, cp, ctb, rep, test, subno, ++no);

		Utils.logrst(new String[] {ctb.synode(), "closing exchange"}, test, subno, ++no);
		ExchangeBlock req = ctb.closexchange(cp, rep);
		assertEquals(req.nv.get(ctb.synode()).n + 1, getNstamp(ctb).n);
		assertEquals(ready, cp.exstate());

		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst(new String[] {stb.synode(), "on closing exchange"}, test, subno, ++no);
		// FIXME what if the server doesn't agree?
		rep = stb.onclosexchange(sp, req);
		assertEquals(rep.nv.get(ctb.synode()).n + 1, getNstamp(stb).n);
		assertEquals(ready, sp.exstate());

		printChangeLines(ck);
		printNyquv(ck);
	}

	static void exchange_break(SynSessionMeta ssm, SyntityMeta sphm, DBSyntableBuilder stb,
			SyntityMeta cphm, DBSyntableBuilder ctb, int test, int subno)
			throws TransException, SQLException, IOException {

		int no = 0;
		Utils.logrst(new String[] {ctb.synode(), "initiate"}, test, subno, ++no);

		ExessionPersist cp = new ExessionPersist(stb, stb.synode());
		ExchangeBlock ini = ctb.initExchange(cp);
		assertTrue(ini.totalChallenges > 0);


		ctb.abortExchange(cp, stb.synode(), null);
		ini = ctb.initExchange(cp);
		Utils.logrst(String.format("%s initiate changes: %d",
				ctb.synode(), ini.totalChallenges), test, subno, ++no);
		
		ExessionPersist sp = new ExessionPersist(ctb, ctb.synode(), ini);
		ExchangeBlock rep = stb.onInit(sp, ini);

		Utils.logrst(String.format("%s on initiate: changes: %d    entities: %d",
				ctb.synode(), rep.totalChallenges, rep.enitities(cphm.tbl)), test, subno, ++no);

		ExchangeBlock req = null;

		req = cp.nextExchange(rep);
		Utils.logrst(new String[] {stb.synode(), "on exchange"}, test, subno, ++no);
		rep = stb.onExchange(sp, ctb.synode(), req);
		Utils.logrst(String.format("%s on exchange response    changes: %d    entities: %d    answers: %d",
				stb.synode(), rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, ++no);
		printChangeLines(ck);
		printNyquv(ck);
	
		// server had sent reply but client haven't got it
		// challenges & answers are saved at server
		ctb.restorexchange();

		if (cp.hasNextChpages(ctb)) {
			// client
			req = cp.nextExchange(req);
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

			if (req.totalChallenges > 0)
				fail("Shouldn't has any more challenge here.");

		}
		else fail("Not here");
	}

	static void challengeAnswerLoop(ExessionPersist sp, DBSyntableBuilder stb, 
				ExessionPersist cp, DBSyntableBuilder ctb, ExchangeBlock rep,
				int test, int subno, int step)
				throws SQLException, TransException {
		int no = 0;
		
		if (rep != null) {
			Utils.logrst(new String[] {"exchange loops", stb.synode(), "<=>", ctb.synode()},
				test, subno, step);
			
			ctb.onInit(cp, rep);

			while (cp.hasNextChpages(ctb)
				|| rep.act == init // force to go on the initiation respond
				|| rep.hasmore()) {
				// client
				Utils.logrst(new String[] {ctb.synode(), "exchange"}, test, subno, step, ++no);

				ExchangeBlock req = cp.nextExchange(rep);
				Utils.logrst(String.format("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						ctb.synode(), req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, 1);
				req.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);

				// server
				Utils.logrst(new String[] {stb.synode(), "on exchange"}, test, subno, step, ++no);
				// rep = sp.onextExchange(ctb.synode(), req);
				rep = sp.nextExchange(req);

				Utils.logrst(String.format("%s on exchange response    changes: %d    entities: %d    answers: %d",
						stb.synode(), rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, step, no, 1);
				rep.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
			}
		}
	}

	/**
	 * insert photo
	 * @param s
	 * @return [photo-id, change-id, uids]
	 * @throws TransException
	 * @throws SQLException
	 * @throws IOException 
	 */
	String[] insertPhoto(int s) throws TransException, SQLException, IOException {
		DBSyntableBuilder trb = ck[s].trb;
		ExpDocTableMeta m = ck[s].docm;
		String synoder = trb.synode();
		IUser rob = trb.synrobot();

		String[] pid_chid = trb.insertEntity(m, new T_Photo(trb.synconn(), zsu)
				.create(ukraine)
				.device(rob.deviceId())
				.folder(rob.uid()));
		
		return new String[] {pid_chid[0], pid_chid[1],
							SynChangeMeta.uids(synoder, pid_chid[0])};
	}
	
	/**
	 * @param chgm
	 * @param s checker index
	 * @return [synuid, 1/0]
	 * @throws TransException
	 * @throws SQLException
	 */
	Object[] deletePhoto(int s) throws TransException, SQLException {
		DBSyntableBuilder t = ck[s].trb;
		ExpDocTableMeta entm = ck[s].docm;
		AnResultset slt = ((AnResultset) ck[s].trb
				.select(entm.tbl)
				.limit(1)
				.rs(t.instancontxt(t.synconn(), t.synrobot()))
				.rs(0))
				.nxt();

		String suid = slt.getString(entm.synuid);

		return new Object[] {suid, t.deleteEntityBySynuid(entm, suid)};
	}
	
	/**
	 * Update {@link ExpDocTableMeta#resname} and {@link ExpDocTableMeta#createDate} (also know as pdate)
	 * @param s
	 * @return [entity-id, change-id, syn-uid]
	 * @throws SQLException
	 * @throws TransException
	 * @throws AnsonException
	 * @throws IOException
	 */
	String[] updatePname(int s)
			throws SQLException, TransException, AnsonException, IOException {
		ExpDocTableMeta entm = ck[s].docm;
		DBSyntableBuilder t = ck[s].trb;
		AnResultset slt = ((AnResultset) t
				.select(entm.tbl, "ch")
				.whereEq(entm.device, t.synrobot().deviceId())
				.orderby(entm.pk, "desc")
				.limit(1)
				.rs(ck[s].trb.instancontxt(ck[s].connId(), ck[s].robot()))
				.rs(0))
				.nxt();
		
		if (slt == null)
			throw new SemanticException("No entity found: synode=%s, ck=%d",
					t.synode(), s);

		String pid   = slt.getString(entm.pk);
		String synuid= slt.getString(entm.synuid);
		// String synodr= slt.getString(entm.synoder);
		String pname = slt.getString(entm.resname);

		String chgid = t.updateEntity(t.synode(), synuid, entm,
			entm.resname, String.format("%s,%04d", (pname == null ? "" : pname), ck[s].stamp()),
			entm.createDate, now());

		return new String[] {pid, chgid, synuid };
	}
	

}
