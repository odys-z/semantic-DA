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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.odysz.semantic.syn.DBSyntableTest.*;
import static io.odysz.semantic.syn.DBSyn2tableTest.zsu;
import static io.odysz.semantic.syn.DBSyn2tableTest.ura;
import static io.odysz.semantic.syn.DBSyn2tableTest.chpageSize;

import java.io.File;
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

	static String runtimepath;

	static SynodeMeta snm;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;
	static SynchangeBuffMeta xbm;
	static SynDocRefMeta rfm;
	static SynSessionMeta ssm;
	static PeersMeta prm;

//	static String[] synodes;
	static T_SynDomanager[] synodes;

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
		synodes = new T_SynDomanager[4];

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
			SyndomContext.incN0Stamp(conn, snm, DBSyntableTest.synodes[s]);

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
					zsu,
					conn, DBSyntableTest.synodes[s],
					s != W ? SynodeMode.peer : SynodeMode.leaf, chpageSize, phm, dvm,
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

	void testPageBreak(int section) throws Exception {

		synodes[X] = new T_SynDomanager(ck[X]);
		synodes[Y] = new T_SynDomanager(ck[Y]);
		synodes[Z] = new T_SynDomanager(ck[Z]);

		@SuppressWarnings("unchecked")
		HashMap<String, Nyquence>[] nvs = (HashMap<String, Nyquence>[]) new HashMap[] {
				synodes[X].loadNvstamp(ck[X].synb),
				synodes[Y].loadNvstamp(ck[Y].synb),
				synodes[Z].loadNvstamp(ck[Z].synb)};

		HashMap<String, Nyquence>[] nvs_ = Nyquence.clone(nvs);

		int no = 0;

		// 1 insert A
		Utils.logrst("insert X", section, ++no);
		String[][] X_33_uids = insertDevices(33, X);

		assertEquals(33, len(X_33_uids));

		for (String[] xuids : X_33_uids) {
			String X_0 = xuids[0];
			ck[X].change_log(1, C, synodes[X].synode, X_0, ck[X].devm);
			ck[X].psubs(ck[X].devm, 2, xuids[1], -1, Y, Z, -1);
		}

		// 2 insert B
		Utils.logrst("insert Y", section, ++no);
		String[][] B_15_uids = insertDevices(15, Y);
		
		printChangeLines(ck);
		printNyquv(ck);

		for (String[] yuids : B_15_uids) {
			String B_0 = yuids[0];

			ck[Y].change_log(1, C, synodes[Y].synode, B_0, ck[Y].devm);
			ck[Y].psubs(ck[Y].devm, 2, yuids[1], X, -1, Z, -1);

			ck[Y].change_devlog(1, C, B_0);
//			ck[Y].change_devlog(1, C, x, X_0);
			ck[Y].psubs(ck[Y].devm, 1, yuids[1], -1, -1, Z, -1);
			ck[Y].psubs_uid(ck[Y].devm, 1, yuids[2], -1, -1, Z, -1);
		}

		// 3. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeDevsBreak(X, Y, section, no);

		printChangeLines(ck);
		nvs = printNyquv(ck);

		ExchangeBlock rep = restart(X);
		assertNull(rep);

		exchangeDevsBreak(X, Y, section, no);
		printChangeLines(ck);
		nvs = printNyquv(ck);

		rep = restart(Y);
		assertNull(rep);

		exchangeDevsBreak(X, Y, section, no);
		printChangeLines(ck);
		nvs = printNyquv(ck);

		// 4. Y <= Z
		Utils.logrst("Y <= Z", section, ++no);

		exchangeDevsBreak(Y, Z, section, no);
		printChangeLines(ck);
		nvs = printNyquv(ck);

		ck[Z].change_devlog(0);
		assertEquals(33 + 15, ck[Z].devs());

//		assertI(ck, nvs);
//		assertnv(nvs_[X], nvs[X], 0, 0, 0);
//		assertnv(nvs_[Y], nvs[Y], 0, 1, 1);
//		// 0, 0, 1 => 1, 2, 2
//		assertnv(nvs_[Z], nvs[Z], 1, 2, 1);
		
		// 4. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeDevsBreak(X, Y, section, no);
		ck[X].change_devlog(0);
		ck[Y].change_devlog(0);

		assertEquals(ck[X].devs(), ck[Y].devs());
		assertEquals(ck[Z].devs(), ck[Y].devs());
	}

	private ExchangeBlock restart(int nx) throws Exception {
		T_SynDomanager domx = synodes[nx];
		domx.breakdown();
		synodes[nx] = T_SynDomanager.reboot(ck[nx]);
		return synodes[nx].resumeBreakpoint();
	}

	private String[][] insertDevices(int cnt, int nx) throws TransException, SQLException {
		T_SynDomanager dom = synodes[nx];
		DBSyntableBuilder trb = ck[nx].synb;
		SyntityMeta m = ck[nx].devm;
		String synoder = trb.syndomx.synode;

		SyncUser usr = new SyncUser(synoder, synoder, "doc owner@" + synoder, "dev client of " + nx);

		String [][] dev_uids = new String[cnt][];

		for (int ci = 0; ci < cnt; ci++) {
			String devid = f("d-%s.%s", dom.synode, ci);
			T_Device device = new T_Device(dom.synconn, ura,
							devid, f("device %s[%s]", dom.synode, ci));

			String[] pid_chid = trb
				.insertEntity(dom, m, device, devid);

			dev_uids[ci] = new String[] {pid_chid[0], pid_chid[1],
					SynChangeMeta.uids(synoder, pid_chid[0])};
		}
		
		ck[nx].sessionUsr = usr; 
		return dev_uids;
	}

	void exchangeDevsBreak(int srv, int cli, int test, int subno)
			throws Exception {
		exchangeBreak(synodes[srv], synodes[cli], test, subno);
	}

	void exchangeBreak(T_SynDomanager srv, T_SynDomanager cli, int test, int subno)
			throws Exception {
		int no = 0;
		
		Utils.logrst(new String[] {cli.synode, "initiate"}, test, subno, ++no);
		ExessionPersist cp = new ExessionPersist(cli.synb, srv.synode);

		ExchangeBlock ini = cli.xp(cp).synb.initExchange(cp);
		Utils.logrst(f("%s initiate: changes: %d    entities: %d",
				cli.synode, ini.totalChallenges, ini.enitities(cli.devm.tbl)), test, subno, no, 1);

		Utils.logrst(new String[] {srv.synode, "on initiate"}, test, subno, ++no);
		ExessionPersist sp = new ExessionPersist(srv.synb, cli.synode, ini);

		ExchangeBlock rep = srv.xp(sp).synb.onInit(sp, ini);
		Utils.logrst(f("%s on initiate: changes: %d",
				srv.synode, rep.totalChallenges),
				test, subno, no, 1);

		// chLoop_ok(rep, srv, cli, test, subno, ++no);
		chLoopBreak(rep, srv, cli, test, subno, ++no);

		Utils.logrst(new String[] {cli.synode, "closing exchange"}, test, subno, ++no);
		ExchangeBlock req = cli.synb.closexchange(cp, rep);
		assertEquals(ready, cp.exstate());

		printChangeLines(ck);
		printNyquv(ck);
		printNyquv(ck, true);

		Utils.logrst(new String[] {srv.synode, "on closing exchange"}, test, subno, ++no);
		rep = srv.synb.onclosexchange(sp, req);
		assertEquals(ready, sp.exstate());

		printChangeLines(ck);
		printNyquv(ck);
		printNyquv(ck, true);
	}

	void chLoopBreak(ExchangeBlock rep, T_SynDomanager srv, T_SynDomanager cli, int test, int subno, int step) throws Exception {
		int no = 0;
		
		if (rep != null) {
			Utils.logrst(new String[] {"exchange loops", srv.synode, "<=>", cli.synode},
				test, subno, step);
			
			cli.synb.onInit(cli.xp, rep); // client on init reply

			while (cli.xp.hasNextChpages(cli.synb)
				|| rep.act == init // force to go on the initiation respond
				|| rep.hasmore()) {
				
				int exseq = 0;
				// client
				Utils.logrst(new String[] {cli.synode, "exchange"}, test, subno, step, ++no);

				ExchangeBlock req = cli.xp.nextExchange(rep);
				Utils.logrst(f("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						cli.synode, req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, ++exseq);
				req.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
				
				req = restart(Y); // continue or close

				Utils.logrst(new String[] {srv.synode, "on resume"}, test, subno, step, no, ++exseq);
				rep = srv.xp.onRestore(req);

				req = cli.xp.nextExchange(rep);
				Utils.logrst(f("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						cli.synode, req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, ++exseq);
				req.print(System.out);

				// server
				Utils.logrst(new String[] {srv.synode, "on exchange"}, test, subno, step, no, ++exseq);
				rep = srv.xp.nextExchange(req);

				Utils.logrst(f("%s on exchange response    changes: %d    entities: %d    answers: %d",
						srv.synode, rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, step, no, ++exseq);
				rep.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
			}
		}
	}

	void chLoop_ok(ExchangeBlock rep, T_SynDomanager srv, T_SynDomanager cli, int test, int subno, int step) throws Exception {
		int no = 0;
		
		if (rep != null) {
			Utils.logrst(new String[] {"exchange loops", srv.synode, "<=>", cli.synode},
				test, subno, step);
			
			cli.synb.onInit(cli.xp, rep); // client on init reply

			while (cli.xp.hasNextChpages(cli.synb)
				|| rep.act == init // force to go on the initiation respond
				|| rep.hasmore()) {
				// client
				Utils.logrst(new String[] {cli.synode, "exchange"}, test, subno, step, ++no);

				ExchangeBlock req = cli.xp.nextExchange(rep);
				Utils.logrst(f("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						cli.synode, req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, 1);
				req.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
				
				// server
				Utils.logrst(new String[] {srv.synode, "on exchange"}, test, subno, step, ++no);
				rep = srv.xp.nextExchange(req);

				Utils.logrst(f("%s on exchange response    changes: %d    entities: %d    answers: %d",
						srv.synode, rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, step, no, 1);
				rep.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
			}
		}
	}
	
}
