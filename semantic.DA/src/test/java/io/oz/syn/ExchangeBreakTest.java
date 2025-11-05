package io.oz.syn;

import static io.odysz.common.LangExt._0;
import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.f;
import static io.odysz.common.LangExt.len;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.CRUD.C;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.oz.syn.DBSyn2tableTest.chpageSize;
import static io.oz.syn.DBSyn2tableTest.ura;
import static io.oz.syn.DBSyn2tableTest.zsu;
import static io.oz.syn.DBSyntableTest.*;
import static io.oz.syn.Docheck.ck;
import static io.oz.syn.Docheck.printChangeLines;
import static io.oz.syn.Docheck.printNyquv;
import static io.oz.syn.ExessionAct.init;
import static io.oz.syn.ExessionAct.ready;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

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
import io.odysz.semantic.util.DAHelper;
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
public class ExchangeBreakTest {
	static boolean breakExchange = true;
	
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
			SyndomContext.incN0Stamp(conn, snm, DBSyntableTest.synodes[s], zsu);

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
					// s != W ? SynodeMode.peer : SynodeMode.leaf,
					SynodeMode.peer,
					chpageSize, phm, dvm,
					Connects.getDebug(conn));
		}
		
		SyndomContext.forceExceptionStamp2n0  = true;
		
		assertEquals("syn.00", ck[0].connId());
	}

	@Test
	void testNoBreak() throws Exception {
		printNyquv(ck);
		int no = 0;
		testInit();
		testPageBreak(++no, false, seqs_X33_Y15);
	}

	@Test
	void testRequestLost() throws Exception {
		printNyquv(ck);
		int no = 0;
		testInit();
		testPageBreak(++no, true, seqs_X33_Y15_breaks);
	}

	@Test
	void testReplyLost() throws Exception{
		printNyquv(ck);
		int no = 0;
		testInit();
		testReplyBreak(++no);
	}

	void testReplyBreak(int section) throws Exception {

		synodes[X] = new T_SynDomanager(ck[X]);
		synodes[Y] = new T_SynDomanager(ck[Y]);
		synodes[Z] = new T_SynDomanager(ck[Z]);

		@SuppressWarnings({ "unchecked", "unused" })
		HashMap<String, Nyquence>[] nvs = (HashMap<String, Nyquence>[]) new HashMap[] {
				synodes[X].loadNvstamp(ck[X].synb),
				synodes[Y].loadNvstamp(ck[Y].synb),
				synodes[Z].loadNvstamp(ck[Z].synb)};

		int no = 0;

		// 1 insert A
		Utils.logrst("insert X", section, ++no);
		String[][] X_uids = insertDevices(16, X);
		assertEquals(16, len(X_uids));

		for (String[] xuids : X_uids) {
			String X_0 = xuids[0];
			ck[X].change_log(1, C, synodes[X].synode, X_0, ck[X].devm);
			ck[X].psubs(ck[X].devm, 2, xuids[1], -1, Y, Z, -1);
		}

		// 2 insert B
		Utils.logrst("insert Y", section, ++no);
		String[][] B_uids = insertDevices(49, Y);
		assertEquals(49, len(B_uids));
		
		printChangeLines(ck);
		printNyquv(ck);

		for (String[] yuids : B_uids) {
			String B_0 = yuids[0];

			ck[Y].change_log(1, C, synodes[Y].synode, B_0, ck[Y].devm);
			ck[Y].psubs(ck[Y].devm, 2, yuids[1], X, -1, Z, -1);

			ck[Y].change_devlog(1, C, B_0);
			ck[Y].psubs(ck[Y].devm, 1, yuids[1], -1, -1, Z, -1);
			ck[Y].psubs_uid(ck[Y].devm, 1, yuids[2], -1, -1, Z, -1);
		}

		// 3. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeReverseBreak(X, Y, section, no, seqs_bibroken);

		printChangeLines(ck);
		printNyquv(ck);

		ExchangeBlock rep;
		rep = restart_synssion(X, ck[Y].synode());
		assertNull(rep);

		// 4. Y <= Z
		Utils.logrst("Y <= Z", section, ++no);

		exchangeReverseBreak(Y, Z, section, no);
		printChangeLines(ck);
		printNyquv(ck);

		ck[Z].change_devlog(0);
		assertEquals(16 + 49, ck[Z].devs());

		// 4. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeReverseBreak(X, Y, section, no);

		ck[X].change_devlog(0);
		ck[Y].change_devlog(0);

		assertEquals(ck[X].devs(), ck[Y].devs());
		assertEquals(ck[Z].devs(), ck[Y].devs());
	}

	void exchangeReverseBreak(int srvx, int cli, int test, int subno, int[][][]... ex_seqs)
			throws Exception {
		T_SynDomanager srv = synodes[srvx];
		int no = 0;
		
		Utils.logrst(new String[] {synodes[cli].synode, "initiate"}, test, subno, ++no);
		ExessionPersist cp = new ExessionPersist(synodes[cli].synb, srv.synode);

		ExchangeBlock ini = synodes[cli].xp(cp).synb.initExchange(cp);
		Utils.logrst(f("%s initiate: changes: %d    entities: %d",
				synodes[cli].synode, ini.totalChallenges, ini.enitities(synodes[cli].devm.tbl)), test, subno, no, 1);

		Utils.logrst(new String[] {srv.synode, "on initiate"}, test, subno, ++no);
		ExessionPersist sp = new ExessionPersist(srv.synb, synodes[cli].synode, ini);

		ExchangeBlock rep = srv.xp(sp).synb.onInit(sp, ini);
		Utils.logrst(f("%s on initiate: changes: %d",
				srv.synode, rep.totalChallenges),
				test, subno, no, 1);

		chLoopBiBreak(rep, srvx, cli, test, subno, ++no, _0(ex_seqs));

		Utils.logrst(new String[] {synodes[cli].synode, "closing exchange"}, test, subno, ++no);
		ExchangeBlock req = synodes[cli].synb.closexchange(cp, rep);
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

	int chLoopBiBreak(ExchangeBlock rep, int srvx, int clix, int test, int subno, int step, int[][][] ex_seqs) throws Exception {
		T_SynDomanager srv = synodes[srvx];
		int no = 0;
		int round = 1;
		assertSeqs(++round, srvx, clix, ex_seqs);
		
		if (rep != null) {
			Utils.logrst(new String[] {"exchange loops", srv.synode, "<=>", synodes[clix].synode},
				test, subno, step);
			
			synodes[clix].synb.onInit(synodes[clix].xp, rep);
			assertSeqs(++round, srvx, clix, ex_seqs);

			while (synodes[clix].xp.hasNextChpages(synodes[clix].synb)
				|| rep.act == init // force to go on the initiation respond
				|| rep.hasmore()) {
				
				int exseq = 0;

				// client
				// Test both server replied then lost connection;
				Utils.logrst(new String[] {synodes[clix].synode, "exchange"}, test, subno, step, ++no);

				ExchangeBlock req = synodes[clix].xp.nextExchange(rep);
				assertSeqs(++round, srvx, clix, ex_seqs);

				Utils.logrst(f("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						synodes[clix].synode, req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, ++exseq);
				req.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);

				// server reply
				Utils.logrst(new String[] {srv.synode, "on exchange"}, test, subno, step, no, ++exseq);
				rep = srv.xp.nextExchange(req);
				assertSeqs(++round, srvx, clix, ex_seqs);

				Utils.logrst(f("%s on exchange response    changes: %d    entities: %d    answers: %d",
						srv.synode, rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, step, no, ++exseq);
				rep.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
			
				// Client failed after server has replied
				req = restart_synssion(clix, ck[srvx].synode()); // continue or close
				assertSeqs(++round, srvx, clix, ex_seqs);
				srv.xp.loadsession(ck[clix].synode()); // equivalent to re-login

				Utils.logrst(new String[] {srv.synode, "server on resume"}, test, subno, step, no, ++exseq);
				rep = srv.xp.onRestore(req);
				assertSeqs(++round, srvx, clix, ex_seqs);
			}
		}
		return round;
	}


	void testPageBreak(int section, boolean broken, int[][][] ex_seqs) throws Exception {

		synodes[X] = new T_SynDomanager(ck[X]);
		synodes[Y] = new T_SynDomanager(ck[Y]);
		synodes[Z] = new T_SynDomanager(ck[Z]);

		@SuppressWarnings("unchecked")
		HashMap<String, Nyquence>[] nvs = (HashMap<String, Nyquence>[]) new HashMap[] {
				synodes[X].loadNvstamp(ck[X].synb),
				synodes[Y].loadNvstamp(ck[Y].synb),
				synodes[Z].loadNvstamp(ck[Z].synb)};

		@SuppressWarnings("unused")
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
		exchangeDevsBreak(X, Y, section, no, broken, ex_seqs);

		printChangeLines(ck);
		nvs = printNyquv(ck);

		ExchangeBlock rep;
		if (broken) {
			rep = restart_synssion(X, ck[Y].synode());
			assertNull(rep);
		}

		// 4. Y <= Z
		Utils.logrst("Y <= Z", section, ++no);

		exchangeDevsBreak(Y, Z, section, no, broken);
		printChangeLines(ck);
		nvs = printNyquv(ck);

		ck[Z].change_devlog(0);
		assertEquals(33 + 15, ck[Z].devs());

		// 4. X <= Y
		Utils.logrst("X <= Y", section, ++no);
		exchangeDevsBreak(X, Y, section, no, broken);
		ck[X].change_devlog(0);
		ck[Y].change_devlog(0);

		assertEquals(ck[X].devs(), ck[Y].devs());
		assertEquals(ck[Z].devs(), ck[Y].devs());
	}

	private ExchangeBlock restart_synssion(int at, String peer) throws Exception {
		T_SynDomanager domx = synodes[at];
		domx.breakdown();
		synodes[at] = T_SynDomanager.reboot(ck[at]);
		return synodes[at].syssionPeer_exesrestore(peer);
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

	void exchangeDevsBreak(int srv, int cli, int test, int subno, boolean broken, int[][][]... ex_seqs)
			throws Exception {
		exchangeBreak(srv, cli, test, subno, broken, ex_seqs);
	}

	/**
	 * 
	 * @param srv
	 * @param cli int, the index of T_Domanager which will be replaced with a new instance while restarting.   
	 * @param test
	 * @param subno
	 * @param ex_seqs the synssion seqs, i. e. the sequence numbers used in exchange handshakes. Null for ignore verification.
	 * @throws Exception
	 */
	void exchangeBreak(int srvx, int cli, int test, int subno, boolean broken, int[][][]... ex_seqs)
			throws Exception {
		T_SynDomanager srv = synodes[srvx];
		int no = 0;
		
		Utils.logrst(new String[] {synodes[cli].synode, "initiate"}, test, subno, ++no);
		ExessionPersist cp = new ExessionPersist(synodes[cli].synb, srv.synode);

		ExchangeBlock ini = synodes[cli].xp(cp).synb.initExchange(cp);
		Utils.logrst(f("%s initiate: changes: %d    entities: %d",
				synodes[cli].synode, ini.totalChallenges, ini.enitities(synodes[cli].devm.tbl)), test, subno, no, 1);

		Utils.logrst(new String[] {srv.synode, "on initiate"}, test, subno, ++no);
		ExessionPersist sp = new ExessionPersist(srv.synb, synodes[cli].synode, ini);

		ExchangeBlock rep = srv.xp(sp).synb.onInit(sp, ini);
		Utils.logrst(f("%s on initiate: changes: %d",
				srv.synode, rep.totalChallenges),
				test, subno, no, 1);

		if (broken)
			chLoopBreak(rep, srvx, cli, test, subno, ++no, ex_seqs);
		else
			chLoop_ok(rep, srvx, cli, test, subno, ++no, ex_seqs);

		Utils.logrst(new String[] {synodes[cli].synode, "closing exchange"}, test, subno, ++no);
		ExchangeBlock req = synodes[cli].synb.closexchange(cp, rep);
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

	/**
	 * Emulate package lost from client to server, before the server has any chances to take actions.
	 * @param rep
	 * @param srvx
	 * @param clix
	 * @param test
	 * @param subno
	 * @param step
	 * @param ex_seqs
	 * @return exchange rounds
	 * @throws Exception
	 */
	int chLoopBreak(ExchangeBlock rep, int srvx, int clix, int test, int subno, int step, int[][][]... ex_seqs) throws Exception {
		T_SynDomanager srv = synodes[srvx];
		int no = 0;
		int round = 1;
		assertSeqs(++round, srvx, clix, _0(ex_seqs));
		
		if (rep != null) {
			Utils.logrst(new String[] {"exchange loops", srv.synode, "<=>", synodes[clix].synode},
				test, subno, step);
			
			synodes[clix].synb.onInit(synodes[clix].xp, rep); // client on init reply
			assertSeqs(++round, srvx, clix, _0(ex_seqs));

			while (synodes[clix].xp.hasNextChpages(synodes[clix].synb)
				|| rep.act == init // force to go on the initiation respond
				|| rep.hasmore()) {
				
				int exseq = 0;
				// client
				Utils.logrst(new String[] {synodes[clix].synode, "exchange"}, test, subno, step, ++no);

				ExchangeBlock req = synodes[clix].xp.nextExchange(rep);
				assertSeqs(++round, srvx, clix, _0(ex_seqs));

				Utils.logrst(f("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						synodes[clix].synode, req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, ++exseq);
				req.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
				
				// Client failed before server has replied
				req = restart_synssion(clix, ck[srvx].synode()); // continue or close
				assertSeqs(++round, srvx, clix, _0(ex_seqs));

				Utils.logrst(new String[] {srv.synode, "server on resume"}, test, subno, step, no, ++exseq);
				rep = srv.xp.onRestore(req);
				assertSeqs(++round, srvx, clix, _0(ex_seqs));

				req = synodes[clix].xp.nextExchange(rep);
				assertSeqs(++round, srvx, clix, _0(ex_seqs));
				Utils.logrst(f("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						synodes[clix].synode, req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, ++exseq);
				req.print(System.out);

				// server
				Utils.logrst(new String[] {srv.synode, "on exchange"}, test, subno, step, no, ++exseq);
				rep = srv.xp.nextExchange(req);
				assertSeqs(++round, srvx, clix, _0(ex_seqs));

				Utils.logrst(f("%s on exchange response    changes: %d    entities: %d    answers: %d",
						srv.synode, rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, step, no, ++exseq);
				rep.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
			}
		}
		return round;
	}

	static final int xtotal = 0, xch = 1, xans = 2, xexp = 3;
	/**
	 * The syssnion seqs for the first round synssion, after X inserted 33 devices, Y inserted 15 devices.
	 */
	static final int[][][] seqs_X33_Y15 = new int[][][] {
		// 0
	    //                    server                             |    client
		//                   total challenge answer exp-answer   |   total challenge answer exp-answer
		new int[][] {new int[]{0,     -1,       -1,     -1}, new int[]{0,      -1,      -1,     -1}},
		// 1
		//                                                       |    cli.init
		new int[][] {new int[]{0,     -1,       -1,     -1}, new int[]{0,      -1,      -1,     -1}},
		// 2
		//                    srv.oninit, 33 ex-buffs                    
		new int[][] {new int[]{33,    -1,       -1,     -1}, new int[]{0,      -1,      -1,     -1}},
		// 3
		//                                                 => 0 entities
		//                                                       |    cli.oninit, 15 ex-buffs
		new int[][] {new int[]{33,    -1,       -1,     -1}, new int[]{15,     -1,      -1,     -1}},
		// 4
		//                                                       |    => 15 entities [dropped, TODO optimize]
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{33,    -1,       -1,     -1}, new int[]{15,      0,      -1,      0}},
		// 5
		//                                                 <= 15 entities
		//                    srv.next-exchange
		new int[][] {new int[]{33,     0,        0,      0}, new int[]{15,      0,      -1,      0}},
		// 6
		//                                                 => 16 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{33,     0,        0,      0}, new int[]{15,      1,       0,     -1}},
		// 7
		//                                                 <=  0 entities
		//                    srv.next-exchange
		new int[][] {new int[]{33,     1,        1,      1}, new int[]{15,      1,       0,     -1}},
		// 8
		//                                                 => 16 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{33,     1,        1,      1}, new int[]{15,      2,       1,     -1}},
		//                                                 <=  0 entities
		//                    srv.next-exchange
		new int[][] {new int[]{33,     2,        2,      2}, new int[]{15,      2,       1,     -1}},
		//                                                 =>  1 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{33,     2,        2,      2}, new int[]{15,      3,       2,     -1}},
		//                                                 <=  0 entities
		//                    srv.next-exchange
		new int[][] {new int[]{33,     3,        3,     -1}, new int[]{15,      3,       2,     -1}},
		//                                                 =>  0 entities
		//                                                       |    cli.close
		new int[][] {new int[]{33,     3,        3,     -1}, new int[]{15,      4,       3,     -1}},
		//                    srv.close
		new int[][] {new int[]{33,     4,        4,     -1}, new int[]{15,      4,       3,     -1}}
	};

	static final int[][][] seqs_X33_Y15_breaks = new int[][][] {
		// 0
	    //                    server                             |    client
		//                   total challenge answer exp-answer   |   total challenge answer exp-answer
		new int[][] {new int[]{0,     -1,       -1,     -1}, new int[]{0,      -1,      -1,     -1}},

		// 1
		//                                                       |    cli.init
		new int[][] {new int[]{0,     -1,       -1,     -1}, new int[]{0,      -1,      -1,     -1}},
		// 2
		//                    srv.oninit, 33 ex-buffs                    
		new int[][] {new int[]{33,    -1,       -1,     -1}, new int[]{0,      -1,      -1,     -1}},
		// 3
		//                                                 => 0 entities
		//                                                       |    cli.oninit, 15 ex-buffs
		new int[][] {new int[]{33,    -1,       -1,     -1}, new int[]{15,     -1,      -1,     -1}},
		// 4
		//                                                       |    => 15 entities [dropped, TODO optimize]
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{33,    -1,       -1,     -1}, new int[]{15,      0,      -1,      0}},
		// 5
		//                                                       |    cli failed, reboot
		//                                                       |    cli.loadomx, cli.restore_synssion (and exchange again)
		new int[][] {new int[]{33,    -1,       -1,     -1}, new int[]{15,      0,      -1,      0}},
		
		// 6
		//                                                 <= 15 entities
		//                    srv.on-restore, exchange
		new int[][] {new int[]{33,     0,        0,     16}, new int[]{15,      0,      -1,      0}},

		// 7
		//                                                 => 16 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{33,     0,        0,     16}, new int[]{15,      1,       0,      0}},

		// 8
		//                                                 <=  0 entities
		//                    srv.next-exchange
		new int[][] {new int[]{33,     1,        1,     16}, new int[]{15,      1,       0,      0}},

		// 9
		// 
		//                                                 => 16 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{33,     1,        1,     16}, new int[]{15,      2,       1,      0}},

		// 10
		//                                                       |    cli failed, reboot
		//                                                       |    cli.loadomx, cli.restore_synssion (and exchange again)
		new int[][] {new int[]{33,     1,        1,     16}, new int[]{15,      2,       1,      0}},

		// 11
		//                                                 <=  0 entities
		//                    srv.on-restore, exchange
		new int[][] {new int[]{33,     2,        2,      1}, new int[]{15,      2,       1,      0}},

		// 
		//                                                 =>  1 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{33,     2,        2,      1}, new int[]{15,      3,       2,      0}},
		// 
		//                                                 <=  0 entities
		//                    srv.next-exchange
		new int[][] {new int[]{33,     3,        3,      0}, new int[]{15,      3,       2,      0}},
		//                                                 =>  0 entities
		//                                                       |    cli.close
		new int[][] {new int[]{33,     3,        3,      0}, new int[]{15,      4,       3,      0}},
		//                    srv.close
		new int[][] {new int[]{33,     4,        4,      0}, new int[]{15,      4,       3,      0}}
	
	};
	
	static final int[][][] seqs_bibroken = new int[][][] {
		// 0
	    //                    server                             |    client
		//                   total challenge answer exp-answer   |   total challenge answer exp-answer
		new int[][] {new int[]{0,     -1,       -1,      0}, new int[]{0,      -1,      -1,      0}},

		// 1
		//                                                       |    cli.init
		new int[][] {new int[]{0,     -1,       -1,      0}, new int[]{0,      -1,      -1,      0}},
		// 2
		//                    srv.oninit, 16 ex-buffs
		new int[][] {new int[]{16,    -1,       -1,      0}, new int[]{0,      -1,      -1,      0}},
		// 3
		//                                                 => 0 entities
		//                                                       |    cli.oninit, 15 ex-buffs
		//                                                       |    => 16 entities [dropped, TODO optimize]
		new int[][] {new int[]{16,    -1,       -1,      0}, new int[]{49,     -1,      -1,      0}},

		// 4 loop-0
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{16,    -1,       -1,      0}, new int[]{49,      0,      -1,     16}},
		// 5
		//                                                 <= 16 entities
		//                    srv.next-exchange
		new int[][] {new int[]{16,     0,        0,     16}, new int[]{49,      0,      -1,     16}},

		// 6
		//                                                 => 16 entities
		//                                                       |    cli failed, reboot
		//                                                       |    cli.loadomx, cli.restore_synssion (and exchange again)
		new int[][] {new int[]{16,     0,        0,     16}, new int[]{49,      0,      -1,     16}},

		// 7
		//                                                 <= 16 entities
		//                    srv.on-restore, exchange
		new int[][] {new int[]{16,     0,        0,     16}, new int[]{49,      0,      -1,     16}},

		// 8 loop-1
		//                                                 => 16 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{16,     0,        0,     16}, new int[]{49,      1,       0,     16}},

		// 9
		//                                                 <= 16 entities
		//                    srv.next-exchange
		new int[][] {new int[]{16,     1,        1,      0}, new int[]{49,      1,       0,     16}},

		// 10
		//                                                 =>  0 entities
		//                                                       |    cli failed, reboot
		//                                                       |    cli.loadomx, cli.restore_synssion (and exchange again)
		new int[][] {new int[]{16,     1,        1,      0}, new int[]{49,      1,       0,     16}},

		// 11
		//                                                 <= 16 entities
		//                    srv.on-restore, exchange
		new int[][] {new int[]{16,     1,        1,      0}, new int[]{49,      1,       0,     16}},

		// 12 loop-2
		//                                                 =>  0 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{16,     1,        1,      0}, new int[]{49,      2,       1,     16}},

		// 13
		//                                                 <= 16 entities
		//                    srv.next-exchange
		new int[][] {new int[]{16,     2,        2,      0}, new int[]{49,      2,       1,     16}},

		// 14
		//                                                 =>  0 entities
		//                                                       |    cli failed, reboot
		//                                                       |    cli.loadomx, cli.restore_synssion (and exchange again)
		new int[][] {new int[]{16,     2,        2,      0}, new int[]{49,      2,       1,     16}},

		// 15
		//                                                 <= 16 entities
		//                    srv.on-restore, exchange
		new int[][] {new int[]{16,     2,        2,      0}, new int[]{49,      2,       1,     16}},

		// 16 loop-3
		//                                                 =>  0 entities
		//                                                       |    cli.next-exchange
		new int[][] {new int[]{16,     2,        2,      0}, new int[]{49,      3,       2,      1}},

		// 13
		//                                                 <=  1 entities
		//                    srv.next-exchange
		new int[][] {new int[]{16,     3,        3,      0}, new int[]{49,      3,       2,      1}},

		// 14
		//                                                 =>  0 entities
		//                                                       |    cli failed, reboot
		//                                                       |    cli.loadomx, cli.restore_synssion (and exchange again)
		new int[][] {new int[]{16,     3,        3,      0}, new int[]{49,      3,       2,      1}},

		// 15
		//                                                 <=  1 entities
		//                    srv.on-restore, exchange
		new int[][] {new int[]{16,     3,        3,      0}, new int[]{49,      3,       2,      1}},

		// 16
		//                                                 =>  0 entities
		//                                                       |    cli.close
		new int[][] {new int[]{16,     3,        3,      0}, new int[]{49,      3,       3,     0}},

		// 17
		//                    srv.close
		new int[][] {new int[]{16,     4,        3,      0}, new int[]{49,      4,       3,      0}}
	};
	
	static void assertSeqs(int round, int sx, int cx, int[][][] ex_seqs) {
		if (isNull(ex_seqs)) return;
		ExessionPersist sp = synodes[sx].xp;
		ExessionPersist cp = synodes[cx].xp;
		int[] arrs = new int[] {sp.totalChallenges, sp.challengeSeq(), sp.answerSeq(), ex_seqs[round][sx][xexp]};
		int[] arrc = new int[] {cp.totalChallenges, cp.challengeSeq(), cp.answerSeq(), ex_seqs[round][cx][xexp]};

		Utils.logi("Round %s", round);
		Utils.logArr2d(ex_seqs[round], new int[][] {arrs, arrc});

		assertArrayEquals((ex_seqs)[round][sx], arrs);
		assertArrayEquals((ex_seqs)[round][cx], arrc);
	};
	
	/**
	 * @param rep
	 * @param srv
	 * @param cli
	 * @param test
	 * @param subno
	 * @param step
	 * @return round of seqs
	 * @throws Exception
	 */
	int chLoop_ok(ExchangeBlock rep, int srvx, int clix, int test, int subno, int step,
			int[][][]... ex_seqs) throws Exception {
		T_SynDomanager srv = synodes[srvx], cli = synodes[clix];
		int no = 0;
		int round = 1;
		assertSeqs(++round, srvx, clix, _0(ex_seqs));
		
		if (rep != null) {
			Utils.logrst(new String[] {"exchange loops", srv.synode, "<=>", cli.synode},
				test, subno, step);

			cli.synb.onInit(cli.xp, rep); // client on init reply
			assertSeqs(++round, srvx, clix, _0(ex_seqs));

			while (cli.xp.hasNextChpages(cli.synb)
				|| rep.act == init // force to go on the initiation respond
				|| rep.hasmore()) {
				// client
				Utils.logrst(new String[] {cli.synode, "exchange"}, test, subno, step, ++no);

				ExchangeBlock req = cli.xp.nextExchange(rep);
				assertSeqs(++round, srvx, clix, _0(ex_seqs));

				Utils.logrst(f("%s exchange challenge    changes: %d    entities: %d    answers: %d",
						cli.synode, req.totalChallenges, req.enitities(), req.answers()), test, subno, step, no, 1);
				req.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
				
				// server
				Utils.logrst(new String[] {srv.synode, "on exchange"}, test, subno, step, ++no);
				rep = srv.xp.nextExchange(req);
				assertSeqs(++round, srvx, clix, _0(ex_seqs));

				Utils.logrst(f("%s on exchange response    changes: %d    entities: %d    answers: %d",
						srv.synode, rep.totalChallenges, rep.enitities(), rep.answers()), test, subno, step, no, 1);
				rep.print(System.out);
				printChangeLines(ck);
				printNyquv(ck);
			}
		}
		return round;
	}
	
}
