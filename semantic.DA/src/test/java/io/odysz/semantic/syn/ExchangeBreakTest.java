package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.f;
import static io.odysz.common.LangExt.len;
import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.CRUD.C;
import static io.odysz.semantic.syn.Docheck.assertI;
import static io.odysz.semantic.syn.Docheck.assertnv;
import static io.odysz.semantic.syn.Docheck.ck;
import static io.odysz.semantic.syn.Docheck.printChangeLines;
import static io.odysz.semantic.syn.Docheck.printNyquv;
import static io.odysz.semantic.syn.ExessionAct.init;
import static io.odysz.semantic.syn.ExessionAct.ready;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.odysz.semantic.syn.DBSyn2tableTest.zsu;
import static io.odysz.semantic.syn.DBSyn2tableTest.ura;
import static io.odysz.semantic.syn.DBSyn2tableTest.chpageSize;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.common.AssertImpl;
import io.odysz.common.Configs;
import io.odysz.common.Utils;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.AutoSeqMeta;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SemanticTableMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynDocRefMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.syn.registry.Syntities;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
* Full-duplex mode for exchanging logs are running.
* 
* See test/res/console-print.txt
* 
* @author Ody
*/
public class ExchangeBreakTest {
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
		synodes = new String[] { "X", "Y", "Z", "W" };
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
			SyndomContext.incN0Stamp(conn, snm, synodes[s]);

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
					s != ExchangeBreakTest.W ? SynodeMode.peer : SynodeMode.leaf, chpageSize, phm, dvm,
					Connects.getDebug(conn));
		}
		
		SyndomContext.forceExceptionStamp2n0  = true;
		
		assertEquals("syn.00", ck[0].connId());
	}

	@Test
	void testChangeLogs() throws Exception {
		printNyquv(ck);

		int no = 0;
		testPageBreak(++no);
	}

	void testPageBreak(int section)
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

		// 1 insert A
		Utils.logrst("insert X", section, ++no);
		String[][] X_33_uids = insertDevices(33, X);

		assertEquals(33, len(X_33_uids));

		for (String[] xuids : X_33_uids) {
			String X_0 = xuids[0];
			ck[X].change_log(1, C, synodes[X], X_0, ck[X].devm);
			ck[X].psubs(2, xuids[1], -1, Y, Z, -1);
		}

		// 2 insert B
		Utils.logrst("insert Y", section, ++no);
		String[][] B_15_uids = insertDevices(15, Y);
		
		printChangeLines(ck);
		printNyquv(ck);

		for (String[] yuids : B_15_uids) {
			String B_0 = yuids[0];

			ck[Y].change_log(1, C, synodes[Y], B_0, ck[Y].devm);
			ck[Y].psubs(2, yuids[1], X, -1, Z, -1);

			ck[Y].change_devlog(1, C, B_0);
//			ck[Y].change_devlog(1, C, x, X_0);
			ck[Y].psubs(1, yuids[1], -1, -1, Z, -1);
			ck[Y].psubs_uid(1, yuids[2], -1, -1, Z, -1);
		}

		// 3. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangePage(X, Y, section, no);

		printChangeLines(ck);
		nvs = printNyquv(ck);

		restart(X);

		exchangePage(X, Y, section, no);
		printChangeLines(ck);
		nvs = printNyquv(ck);

		restart(Y);

		exchangePage(X, Y, section, no);
		printChangeLines(ck);
		nvs = printNyquv(ck);

		// 4. Y <= Z
		Utils.logrst("Y <= Z", section, ++no);

		exchangeDevsBreak(Y, Z, section, no);
		printChangeLines(ck);
		nvs = printNyquv(ck);

		for (String[] xuids : X_33_uids) {
			String X_0 = xuids[0];
			ck[Z].change_log(1, C, synodes[Z], X_0, ck[Z].devm);
			ck[Z].psubs(0, xuids[1], -1, -1, Z, -1);
		}
//		ck[Z].change_log(0, C, synodes[Z], X_0, ck[Z].devm);
//		ck[Z].change_log(0, C, x, X_0, ck[Z].devm);
//		ck[Z].psubs(0, X_0_uids[1], -1, -1, Z, -1);
//
//		ck[Z].change_doclog(0, C, B_0);
//		ck[Z].change_doclog(0, C, B_0);
//		ck[Z].psubs(0, B_0_uids[1], -1, -1, Z, -1);
//		
//		ck[Y].change_doclog(0, C, X_0);
//		ck[Y].change_doclog(0, C, B_0);
//		ck[Y].psubs(0, X_0_uids[1], -1, -1, Z, -1);
//		ck[Y].psubs(0, B_0_uids[1], -1, -1, Z, -1);

		assertI(ck, nvs);
		assertnv(nvs_[X], nvs[X], 0, 0, 0);
		assertnv(nvs_[Y], nvs[Y], 0, 1, 1);
		// 0, 0, 1 => 1, 2, 2
		assertnv(nvs_[Z], nvs[Z], 1, 2, 1);
		
		// 4. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeDevsBreak(X, Y, section, no);
		ck[X].change_devlog(0);
		ck[X].change_devlog(0);

		assertEquals(ck[X].devs(), ck[Y].devs());
		assertEquals(ck[Z].devs(), ck[Y].devs());
	}

	private void restart(int y2) {
		// TODO Auto-generated method stub
		
	}

	private void exchangePage(int y2, int z2, int section, int no) {
		// TODO Auto-generated method stub
		
	}

	private String[][] insertDevices(int cnt, int nx) throws TransException, SQLException {
		DBSyntableBuilder trb = ck[nx].synb;
		SyntityMeta m = ck[nx].devm;
		String synoder = trb.syndomx.synode;

		SyncUser usr = new SyncUser(synoder, synoder, "doc owner@" + synoder, "dev client of " + nx);

		String [][] dev_uids = new String[cnt][];

		for (int ci = 0; ci < cnt; ci++) {
			String devid = f("d-%s.%s", nx, ci);
			T_Device device = new T_Device(ck[nx].synb.syndomx.synconn, ura,
							devid, f("device %s[%s]", nx, ci));

			String[] pid_chid = trb
				.insertEntity(ck[nx].synb.syndomx, m, device, devid);

			dev_uids[ci] = new String[] {pid_chid[0], pid_chid[1],
					SynChangeMeta.uids(synoder, pid_chid[0])};
		}
		
		ck[nx].sessionUsr = usr; 
		return dev_uids;
	}

	String servnid; 
	DBSyntableBuilder ctb;
	DBSyntableBuilder stb;
	ExessionPersist cp;
	ExessionPersist sp;
	SyndomContext srvx;
	
	void exchangeDevsBreak(int srv, int cli, int test, int subno)
			throws TransException, SQLException, IOException {
		ctb = ck[cli].synb;
		stb = ck[srv].synb;

		exchangeBreak(ssm,
				new T_DA_DevMeta(stb.basictx().connId()),
				new T_DA_DevMeta(ctb.basictx().connId()),
				stb, ctb,
				test, subno);
	}

	void exchangeBreak(SynSessionMeta ssm, SyntityMeta sphm, SyntityMeta cphm,
			DBSyntableBuilder stb, DBSyntableBuilder ctb, int test, int subno)
			throws TransException, SQLException, IOException {

		int no = 0;
		SyndomContext cltx = ctb.syndomx; 
		String clientnid = cltx.synode;

		srvx = stb.syndomx;
		servnid = srvx.synode;
		
		Utils.logrst(new String[] {clientnid, "initiate"}, test, subno, ++no);
		cp = new ExessionPersist(ctb, servnid);

		ExchangeBlock ini = ctb.initExchange(cp);
		Utils.logrst(f("%s initiate: changes: %d    entities: %d",
				clientnid, ini.totalChallenges, ini.enitities(cphm.tbl)), test, subno, no, 1);

		Utils.logrst(new String[] {servnid, "on initiate"}, test, subno, ++no);
		sp = new ExessionPersist(stb, clientnid, ini);

		ExchangeBlock rep = stb.onInit(sp, ini);
		Utils.logrst(f("%s on initiate: changes: %d",
				servnid, rep.totalChallenges),
				test, subno, no, 1);

		//
		chLoopBreak(rep, test, subno, ++no);

		Utils.logrst(new String[] {clientnid, "closing exchange"}, test, subno, ++no);
		ExchangeBlock req = ctb.closexchange(cp, rep);
		assertEquals(ready, cp.exstate());

		printChangeLines(ck);
		printNyquv(ck);
		printNyquv(ck, true);

		Utils.logrst(new String[] {servnid, "on closing exchange"}, test, subno, ++no);
		// FIXME what if the server doesn't agree?
		rep = stb.onclosexchange(sp, req);
		assertEquals(ready, sp.exstate());

		printChangeLines(ck);
		printNyquv(ck);
		printNyquv(ck, true);
	}

	void chLoopBreak(ExchangeBlock rep, int test, int subno, int step) throws SQLException, TransException {
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

	/*
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
		// exchange_break(ssm, ck[X].docm, ck[X].synb, ck[Y].docm, ck[Y].synb, section, no);

		ck[X].buf_change(1, C, ck[X].synb.syndomx.synode, xu[0], ck[X].docm);
		ck[X].buf_change(1, C, ck[Y].synb.syndomx.synode, yi[0], ck[X].docm);
		ck[X].psubs(2, xu[1], -1, -1, Z, W);
		ck[X].psubs(2, yi[1], -1, -1, Z, W);
		ck[Y].buf_change(1, C, ck[X].synb.syndomx.synode, xu[0], ck[Y].docm);
		ck[Y].buf_change(1, C, ck[Y].synb.syndomx.synode, yi[0], ck[Y].docm);
		ck[Y].psubs(2, xu[1], -1, -1, Z, W);
		ck[Y].psubs(2, yi[1], -1, -1, Z, W);
	}
	*/
	
}
