package io.oz.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.f;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.CRUD.C;
import static io.odysz.semantic.CRUD.U;
import static io.odysz.transact.sql.parts.condition.Funcall.now;
import static io.oz.syn.DBSyn2tableTest.chpageSize;
import static io.oz.syn.DBSyn2tableTest.ura;
import static io.oz.syn.DBSyn2tableTest.zsu;
import static io.oz.syn.Docheck.assertI;
import static io.oz.syn.Docheck.assertnv;
import static io.oz.syn.Docheck.ck;
import static io.oz.syn.Docheck.printChangeLines;
import static io.oz.syn.Docheck.printNyquv;
import static io.oz.syn.ExessionAct.init;
import static io.oz.syn.ExessionAct.ready;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.anson.AnsonException;
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
import io.odysz.semantic.meta.SynDocRefMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;
import io.oz.syn.registry.Syntities;

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
	
	public static final int X = 0;
	public static final int Y = 1;
	public static final int Z = 2;
	public static final int W = 3;

	static String runtimepath;

	static SynodeMeta snm;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;
	static SynchangeBuffMeta xbm;
	static SynDocRefMeta rfm;
	static SynSessionMeta ssm;
	static PeersMeta prm;

	static String[] synodes = new String[] { "X", "Y", "Z", "W" };

	static {
		printCaller(false);
		conns = new String[] { "syn.00", "syn.01", "syn.02", "syn.03" };
		testers = new String[] { "odyx", "odyy", "odyz", "odyw" };

		File file = new File(rtroot);
		runtimepath = file.getAbsolutePath();
		logi(runtimepath);

		Configs.init(runtimepath);
		Connects.init(runtimepath);

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
			conns[s] = f("syn.%02x", s);
			Connects.commit(conns[s], DATranscxt.dummyUser(),
					"drop table if exists a_logs");

			AutoSeqMeta autom = new AutoSeqMeta(conns[s]);
			Connects.commit(conns[s], DATranscxt.dummyUser(), autom.ddlSqlite);
		}

		ck = new Docheck[4];
		chm = new SynChangeMeta();
		sbm = new SynSubsMeta(chm);
		xbm = new SynchangeBuffMeta(chm);
		rfm = new SynDocRefMeta();
		ssm = new SynSessionMeta();
		prm = new PeersMeta();

		for (int s = 0; s < synodes.length; s++) {
			String conn = conns[s];
			snm = new SynodeMeta(conn);
			T_DA_PhotoMeta phm = new T_DA_PhotoMeta(conn); //.replace();
			T_DA_DevMeta   dvm = new T_DA_DevMeta(conn);  // .replace();

			SemanticTableMeta.setupSqliTables(conn, true, snm, chm, sbm, xbm, rfm, prm, ssm, phm, dvm);

			ArrayList<String> sqls = new ArrayList<String>();
			// sqls.add("delete from oz_autoseq;");
			// sqls.add(Utils.loadTxt("../oz_autoseq.sql"));
			assertTrue(DAHelper.count(new DATranscxt(conn), conn, "oz_autoseq", "sid", "h_photos.pid") == 1);
			sqls.add(f("update oz_autoseq set seq = %d where sid = 'h_photos.pid'", (long) Math.pow(64, s+1)));

			sqls.add(f("delete from %s", snm.tbl));
			if (s != W)
				sqls.add(Utils.loadTxt("syn_nodes.sql"));
			else
				sqls.add(Utils.loadTxt("syn_nodes_w.sql"));

			sqls.add(f("delete from %s", phm.tbl));

			Connects.commit(conn, DATranscxt.dummyUser(), sqls);

			// Do this before loading syndomx.
			// This is necessary as the initial state is unspecified about last synchronization's results,
			// and increasing n-stamp only will lead to confusion later when initiating exchanges. 
			SyndomContext.incN0Stamp(conn, snm, synodes[s], zsu);

			Syntities syntities = Syntities.load(runtimepath, f("syntity-%s.json", s), 
					(synreg) -> {
						if (eq(synreg.table, "h_photos"))
							return new T_DA_PhotoMeta(conn);
						else
							throw new SemanticException("TODO %s", synreg.table);
					});

			// MEMO
			// See also docsync.jser/Syngleton.setupSyntables(), 2.1 injecting synmantics after syn-tables have been set.
			// phm.replace();
			for (SyntityMeta m : syntities.metas.values())
				m.replace();

			Docheck.ck[s] = new Docheck(new AssertImpl(),
					// s != W ? zsu : null,
					zsu,
					conn, synodes[s],
					// s != DBSyntableTest.W ? SynodeMode.peer : SynodeMode.leaf,
					SynodeMode.peer,
					chpageSize, phm, dvm,
					Connects.getDebug(conn));
		}
		
		SyndomContext.forceExceptionStamp2n0  = true;
		
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
		SyndomContext xx = ck[X].synb.syndomx;
		SyndomContext yx = ck[Y].synb.syndomx;
		SyndomContext zx = ck[Z].synb.syndomx;

		@SuppressWarnings("unchecked")
		HashMap<String, Nyquence>[] nvs = (HashMap<String, Nyquence>[]) new HashMap[] {
				xx.loadNvstamp(ck[X].synb),
				yx.loadNvstamp(ck[Y].synb),
				zx.loadNvstamp(ck[Z].synb)};

		HashMap<String, Nyquence>[] nvs_ = Nyquence.clone(nvs);

		int no = 0;
		String x = synodes[X];

		// 1 insert A
		Utils.logrst("insert X", section, ++no);
		String[] X_0_uids = insertPhoto(X);
		String X_0 = X_0_uids[0];

		// syn_change.curd = C
		ck[X].change_log(1, C, synodes[X], X_0, ck[X].docm);
		// syn_subscribe.to = [B, C, D]
		ck[X].psubs(2, X_0_uids[1], -1, Y, Z, -1);

		// 2 insert B
		Utils.logrst("insert Y", section, ++no);
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
		ck[Y].psubs_uid(1, X_0_uids[2], -1, -1, Z, -1);

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
		assertnv(nvs_[Y], nvs[Y], 0, 1, 1);
		// 0, 0, 1 => 1, 2, 2
		assertnv(nvs_[Z], nvs[Z], 1, 2, 1);

		nvs_ = nvs.clone();
		
		// 4. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeDocs(X, Y, section, no);
		ck[X].change_doclog(0, C, X_0);
		ck[X].change_doclog(0, C, B_0);
		ck[X].psubs(0, X_0_uids[1], -1, -1, Z, -1);
		ck[X].psubs(0, B_0_uids[1], -1, -1, Z, -1);

		assertEquals(ck[X].docs(), ck[Y].docs());
	}

	void testJoinChild(int section) throws Exception {
		Utils.logrst(new Object(){}.getClass().getEnclosingMethod().getName(), section);

		int no = 0;
		
		ck[X].synodes(X, Y, Z, -1);

		printChangeLines(ck);
		printNyquv(ck, true);
		Utils.logrst("Y on W joining", section, ++no);
		
		SyndomContext wx = ck[W].synb.syndomx;
		@SuppressWarnings("unused")
		HashMap<String, Nyquence> wnv = wx.loadNvstamp(ck[W].synb);

		@SuppressWarnings("unused")
		String[] w_ack = joinSubtree(Y, W, section, no);

		ck[X].synodes(X,  Y,  Z, -1);
		ck[Y].synodes(X,  Y,  Z, W);
		ck[Z].synodes(X,  Y,  Z, -1);
		ck[W].synodes(-1, Y, -1, W);

		ck[Y].change_log_uids(1, C, "Y", "W,W", ck[Y].synb.syndomx.synm);
		ck[Y].change_log_uids(0, C, "Y", "Y,W", ck[Y].synb.syndomx.synm);
		ck[Y].buf_change(0, C, "W", ck[Y].synb.syndomx.synm);
		ck[Y].synsubs(2, "W,W", X, -1, Z, -1);
		
		Utils.logrst("X vs Y", section, ++no);
		exchangeSynodes(X, Y, section, 2);
		ck[X].synodes(X, Y, Z, W);
		ck[X].change_log_uids(1, C, "Y", "W,W", ck[X].synb.syndomx.synm);
		ck[X].buf_change(0, C, "Y", "W,W", ck[X].synb.syndomx.synm);
		ck[X].synsubs(1, "W,W", -1, -1, Z, -1);

		ck[Z].synodes(X, Y, Z, -1);
		ck[Z].synsubs(0, "W,W", -1, -1, -1, -1);
		
		Utils.logrst("X create photos", section, ++no);
		String[] x_uids = insertPhoto(X);
		printChangeLines(ck);
		printNyquv(ck, true);

		ck[X].change_log(1, C, "X", x_uids[0], ck[X].docm);
		ck[X].buf_change(0, C, x_uids[0], ck[X].docm);
		ck[X].psubs (3, x_uids[1], -1, Y, Z, W);

		
		Utils.logrst("X <= Z", section, ++no);
		exchangeSynodes(X, Z, section, no);
		ck[Z].synodes(X, Y, Z, W);
		ck[Z].synsubs(0, "W,W", -1, -1, -1, -1);
		Utils.logrst("On X-Z: Now Z knows W", section, ++no);
		
		Utils.logrst("Z vs W", section, ++no);
		try { exchangeSynodes(Z, W, section, no); }
		catch (ExchangeException ex_at_w){
			DBSyntableBuilder ctb = ck[W].synb;
			DBSyntableBuilder stb = ck[Z].synb;

			Utils.logi(ex_at_w.getMessage());

			ExchangeBlock req = ctb.abortExchange(ex_at_w.persist);

			stb.onAbort(req);

			printChangeLines(ck);
			printNyquv(ck, true);
			assertEquals(ck[W].n0().n, ck[W].stamp());
			assertEquals(ck[Z].n0().n, ck[Z].stamp());
			return;
		}
		fail("Not knowing Z, W is supposed unable to roaming on to Z.");
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

		String z = ck[Z].synb.syndomx.synode;

		Utils.logrst("Z create photos", section, ++no);
		printNyquv(ck);

		String[] z_uids = insertPhoto(Z);
		String eid = z_uids[0];
		String chg_z = z_uids[1];
		String uid = z_uids[2];

		printChangeLines(ck);
		printNyquv(ck);

		ck[Z].buf_change(0, C, eid, ck[Y].docm);
		ck[Z].change_log(1, C, "Z", eid, ck[Y].docm);
		ck[Z].psubs(3, chg_z, X, Y, -1, W);
		
		Utils.logrst("Y vs Z", section, ++no);
		exchangeDocs(Y, Z, section, 2);
		assertEquals(ck[Z].n0().n, ck[Z].stamp());
		ck[Y].buf_change(0, C, z, eid, ck[Y].docm);
		ck[Y].change_log(1, C, z, eid, ck[Y].docm);
		ck[Y].psubs_uid(2, uid, X, -1, -1, W);
		ck[Z].psubs(2, chg_z, X, -1, -1, W);

		Utils.logrst("Y vs W", section, ++no);
		exchangeDocs(Y, W, section, 3);
		ck[Y].buf_change(0, C, z, eid, ck[X].docm);
		ck[Y].change_log(1, C, z, eid, ck[X].docm);
		ck[Y].synsubs(1, uid, X, -1, -1, -1);
		ck[W].buf_change(0, C, z, eid, ck[X].docm);
		ck[W].synsubs(0, uid, -1, -1, -1, -1);

		Utils.logrst("X vs Y", section, ++no);
		exchangeDocs(X, Y, section, 4);
		ck[X].buf_change(0, C, null, null, ck[X].docm);
		ck[X].synsubs(0, null, X, -1, -1, -1);
		ck[Y].buf_change(0, C, null, null, ck[X].docm);
		ck[Y].synsubs(0, null, -1, -1, -1, -1);

		Utils.logrst("Y vs Z", section, ++no);
		exchangeDocs(Y, Z, section, 4);
		ck[Y].buf_change(0, C, null, null, ck[X].docm);
		ck[Y].synsubs(0, null, X, -1, -1, -1);
		ck[Z].buf_change(0, C, null, null, ck[X].docm);
		ck[Z].psubs(0, null, -1, -1, -1, -1);
		ck[Z].synsubs(0, null, -1, -1, -1, -1);
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

		ck[X].change_doclog(2, U, null);
		ck[X].buf_change(0, U, null, ck[X].docm);
		ck[X].psubs(6, null, X, Y, Z, W);
		ck[X].psubs(6, null, -1, -1, Z, W);

		ck[Y].change_doclog(1, U, null);
		ck[Y].buf_change_p(0, U, null);
		ck[Y].psubs(6, null, X, Y, Z, W);
		ck[Y].psubs(6, null, -1, -1, Z, W);

		Utils.logrst("Y <= Z", section, ++no);
		exchangeDocs(Y, Z, section, no);
		printChangeLines(ck);
		printNyquv(ck);

		ck[Y].change_doclog(1, U, null);
		ck[Y].psubs(3, null, X, Y, Z, W);
		ck[Y].psubs(3, null, -1, -1, -1, W);
		ck[Y].psubs(1, yu[1], -1, -1, -1, W);
		ck[Y].psubs_uid(1, xu[2], -1, -1, -1, W);
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

		Utils.logrst("Z <= Y", test, ++no);
		exchangeDocs(Z, Y, test, no);
		Utils.logrst("X <= Y", test, ++no);
		exchangeDocs(X, Y, test, no);
		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst("Z <= Y", test, ++no);
		exchangeDocs(Z, Y, test, no);
		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst("W <= Y", test, ++no);
		exchangeDocs(W, Y, test, no);

		Utils.logrst("W <= X", test, ++no);
		exchangeDocs(W, X, test, no);

		printChangeLines(ck);
		printNyquv(ck);

		Utils.logrst("Y <= X", test, ++no);
		exchangeDocs(Y, X, test, no);

		printChangeLines(ck);
		printNyquv(ck);

		assertEquals(ck[X].docs(), ck[Y].docs());
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
		DBSyntableBuilder admb = ck[adminx].synb;
		SyndomContext syxa = ck[adminx].synb.syndomx;

		String admin = ck[adminx].synb.syndomx.synode;
		DBSyntableBuilder cltb = ck[applyx].synb;
		SyndomContext syxc = ck[applyx].synb.syndomx;

		int no = 0;

		// sign up as a new domain
		ExessionPersist cltp = new ExessionPersist(cltb, admin);
		Utils.logrst(f("sign up by %s", syxc.synode), testix, sect, ++no);

		ExchangeBlock req  = cltb.domainSignup(cltp, admin);
		ExessionPersist admp = new ExessionPersist(admb, syxc.synode, req);

		// admin on sign up request
		Utils.logrst(f("%s on sign up", admin), testix, sect, ++no);
		ExchangeBlock resp = admb.domainOnAdd(admp, req, Docheck.org);
		Utils.logrst(f("sign up by %s : %s", syxa.synode, resp.session), testix, sect, ++no);
		printChangeLines(ck);
		printNyquv(ck, true);

		// applicant
		Utils.logrst(f("%s initiate domain", syxc.synode), testix, sect, ++no);

		ExchangeBlock ack  = cltb//.domain("zsu")
				.domainitMe(cltp, admin, "jserv/not-used-in-test", zsu, resp);
		Utils.logi(ack.nv, syxc.synode, ".Ack.nv: ");

		printChangeLines(ck);
		printNyquv(ck, true);

		// admin
		Utils.logrst(f("%s ack initiation", syxa.synode), testix, sect, ++no);
		printChangeLines(ck);
		printNyquv(ck, true);

		// applicant
		Utils.logrst(new String[] {syxc.synode, "closing application"}, testix, sect, ++no);
		req = cltb.domainCloseJoin(cltp, resp); // Debug Notes: resp.nv is polluted, but should be safely dropped.
		printChangeLines(ck);
		printNyquv(ck, true);

		Utils.logrst(new String[] {syxa.synode, "on closing"}, testix, sect, ++no);
		admb.domainCloseJoin(admp, req); // Debug Notes: req.nv is polluted, but should be safely dropped.

		printChangeLines(ck);
		printNyquv(ck, true);

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
	static void exchangeDocs(int srv, int cli, int test, int subno)
			throws TransException, SQLException, IOException {
		DBSyntableBuilder ctb = ck[cli].synb;
		DBSyntableBuilder stb = ck[srv].synb;

		SyntityMeta sphm = new T_DA_PhotoMeta(stb.basictx().connId());
		SyntityMeta cphm = new T_DA_PhotoMeta(ctb.basictx().connId());
		
		exchange(ssm, sphm, cphm, stb, ctb, test, subno);
	}

	static void exchangeSynodes(int srv, int cli, int test, int subno)
			throws TransException, SQLException, IOException {
		DBSyntableBuilder ctb = ck[cli].synb;
		DBSyntableBuilder stb = ck[srv].synb;

		SyntityMeta ssnm = new SynodeMeta(stb.basictx().connId()).replace();
		SyntityMeta csnm = new SynodeMeta(ctb.basictx().connId()).replace();
		
		exchange(ssm, ssnm, csnm, stb, ctb, test, subno);
	}

	static void exchange(SynSessionMeta ssm, SyntityMeta sphm, SyntityMeta cphm,
			DBSyntableBuilder stb,
			DBSyntableBuilder ctb, int test, int subno)
			throws TransException, SQLException, IOException {

		int no = 0;
		SyndomContext cltx = ctb.syndomx; 
		String clientnid = cltx.synode;

		SyndomContext srvx = stb.syndomx;
		String servnid = srvx.synode;
		
		Utils.logrst(new String[] {clientnid, "initiate"}, test, subno, ++no);
		ExessionPersist cp = new ExessionPersist(ctb, servnid);
		ExchangeBlock ini = ctb.initExchange(cp);
		Utils.logrst(f("%s initiate: changes: %d    entities: %d",
				clientnid, ini.totalChallenges, ini.enitities(cphm.tbl)), test, subno, no, 1);

		Utils.logrst(new String[] {servnid, "on initiate"}, test, subno, ++no);
		ExessionPersist sp = new ExessionPersist(stb, clientnid, ini);
		ExchangeBlock rep = stb.onInit(sp, ini);
		Utils.logrst(f("%s on initiate: changes: %d",
				servnid, rep.totalChallenges),
				test, subno, no, 1);

		//
		challengeAnswerLoop(sp, stb, cp, ctb, rep, test, subno, ++no);

		Utils.logrst(new String[] {clientnid, "closing exchange"}, test, subno, ++no);
		ExchangeBlock req = ctb.closexchange(cp, rep);
		assertEquals(ready, cp.exstate());
		assertEquals(0, DAHelper.count(ctb, ctb.syndomx.synconn, ssm.tbl, ssm.peer, stb.syndomx.synode));

		printChangeLines(ck);
		printNyquv(ck);
		printNyquv(ck, true);

		Utils.logrst(new String[] {servnid, "on closing exchange"}, test, subno, ++no);
		// FIXME what if the server doesn't agree?
		rep = stb.onclosexchange(sp, req);
		assertEquals(ready, sp.exstate());
		assertEquals(0, DAHelper.count(stb, stb.syndomx.synconn, ssm.tbl, ssm.peer, stb.syndomx.synode));

		printChangeLines(ck);
		printNyquv(ck);
		printNyquv(ck, true);
	}

	static void challengeAnswerLoop(ExessionPersist sp, DBSyntableBuilder stb, 
				ExessionPersist cp, DBSyntableBuilder ctb, ExchangeBlock rep,
				int test, int subno, int step) throws SQLException, TransException {
		int no = 0;

		
		if (rep != null) {
			SyndomContext syxc = ctb.syndomx; 
			String cid = syxc.synode;

			SyndomContext syxs = stb.syndomx; 
			String sid = syxs.synode;

			Utils.logrst(new String[] {"exchange loops", sid, "<=>", cid},
				test, subno, step);
			
			ctb.onInit(cp, rep);

			while (cp.hasNextChpages(ctb)
				|| rep.act == init // force to go on the initiation respond
				|| rep.hasmore()) {
				// client
				Utils.logrst(new String[] {cid, "exchange"}, test, subno, step, ++no);

				ExchangeBlock req = cp.nextExchange(rep);
				Utils.logrst(f("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						cid, req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, 1);
				req.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);

				// server
				Utils.logrst(new String[] {sid, "on exchange"}, test, subno, step, ++no);
				rep = sp.nextExchange(req);

				Utils.logrst(f("%s on exchange response    changes: %d    entities: %d    answers: %d",
						sid, rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, step, no, 1);
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
	static String[] insertPhoto(int s) throws TransException, SQLException, IOException {
		DBSyntableBuilder trb = ck[s].synb;
		ExpDocTableMeta m = ck[s].docm;
		String synoder = trb.syndomx.synode;

		SyncUser usr = new SyncUser(synoder, synoder, "doc owner@" + synoder, "dev client of " + s);

		String[] pid_chid = trb.insertEntity(ck[s].synb.syndomx, m,
								new T_Photo(ck[s].synb.syndomx.synconn, ura)
				.create(ukraine)
				.device(usr.deviceId())
				.folder(usr.uid()));
		
		ck[s].sessionUsr = usr; 

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
	static Object[] deletePhoto(int s) throws TransException, SQLException {
		DBSyntableBuilder t = ck[s].synb;
		ExpDocTableMeta entm = ck[s].docm;
		AnResultset slt = ((AnResultset) ck[s].synb
				.select(entm.tbl)
				.limit(1)
				.rs(t.instancontxt(ck[s].synb.syndomx.synconn, t.synrobot()))
				.rs(0))
				.nxt();

		String suid = slt.getString(entm.io_oz_synuid);

		return new Object[] {suid, t.deleteEntityBySynuid(ck[s].synb.syndomx, entm, suid)};
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
	static String[] updatePname(int s)
			throws SQLException, TransException, AnsonException, IOException {
		ExpDocTableMeta entm = ck[s].docm;
		DBSyntableBuilder t = ck[s].synb;
		String synode = ck[s].synb.syndomx.synode;

		AnResultset slt = ((AnResultset) t
				.select(entm.tbl, "ch")
				.whereEq(entm.device, ck[s].sessionUsr.deviceId())
				.orderby(entm.pk, "desc")
				.limit(1)
				.rs(ck[s].synb.instancontxt())
				.rs(0))
				.nxt();
		
		if (slt == null)
			throw new SemanticException("No entity found: synode=%s, ck=%d",
					synode, s);

		String pid    = slt.getString(entm.pk);
		String device = slt.getString(entm.device);
		String clientpath = slt.getString(entm.fullpath);
		String pname  = slt.getString(entm.resname);

		String chgid = t.updateEntity(ck[s].synb.syndomx, synode, new String[] { device, clientpath }, entm,
			entm.resname, f("%s,%04d", (pname == null ? "" : pname), ck[s].stamp()),
			entm.createDate, now());

		return new String[] {pid, chgid, slt.getString(entm.io_oz_synuid) };
	}
}
