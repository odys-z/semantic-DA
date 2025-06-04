package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.f;
import static io.odysz.common.LangExt.gt;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.anson.Anson;
import io.odysz.common.AssertImpl;
import io.odysz.common.Configs;
import io.odysz.common.Utils;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.AutoSeqMeta;
import io.odysz.semantic.meta.DocRef;
import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SemanticTableMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.syn.registry.Syntities;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**
 * Test 2 syntity tables.
 * 
 * See test/res/console-print.txt
 * 
 * @author Ody
 */
public class DBSyn2tableTest {
	public static final String[] conns;
	public static final String[] testers;
	public static final String logconn = "log";
	public static final String rtroot  = "src/test/res/";
	public static final String father  = "src/test/res/Sun Yet-sen.jpg";
	public static final String path_ukraine = "src/test/res/Ukraine.png";
	
	static final String zsu = "zsu";
	static final String ura = "ura";
	static final int chpageSize = 480;

	public static final int X = 0;
	public static final int Y = 1;
	public static final int Z = 2;
	public static final int W = 3;

	static String runtimepath;

	static SynodeMeta snm;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;
	static SynchangeBuffMeta xbm;
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
//			Connects.commit(conns[s], DATranscxt.dummyUser(),
//				"CREATE TABLE if not exists a_logs (\n"
//				+ "  logId text(20),\n"
//				+ "  funcId text(20),\n"
//				+ "  funcName text(50),\n"
//				+ "  oper text(20),\n"
//				+ "  logTime text(20),\n"
//				+ "  cnt int,\n"
//				+ "  txt text(4000),\n"
//				+ "  CONSTRAINT oz_logs_pk PRIMARY KEY (logId)\n"
//				+ ");" );
			
			AutoSeqMeta autom = new AutoSeqMeta(conns[s]);
			Connects.commit(conns[s], DATranscxt.dummyUser(), autom.ddlSqlite);
		}

		ck = new Docheck[4];
		synodes = new String[] { "X", "Y", "Z", "W" };
		chm = new SynChangeMeta();
		sbm = new SynSubsMeta(chm);
		xbm = new SynchangeBuffMeta(chm);
		ssm = new SynSessionMeta();
		prm = new PeersMeta();

		for (int s = 0; s < 3; s++) {
			String conn = conns[s];
			snm = new SynodeMeta(conn);
			T_DA_PhotoMeta phm = new T_DA_PhotoMeta(conn);//.replace();
			T_DA_DevMeta   dvm = new T_DA_DevMeta(conn);  // .replace();

			SemanticTableMeta.setupSqliTables(conn, true, snm, chm, sbm, xbm, prm, ssm, phm, dvm);
			phm.replace();
			dvm.replace();

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
			phm.replace();
			for (SyntityMeta m : syntities.metas.values())
				m.replace();

			Docheck.ck[s] = new Docheck(new AssertImpl(), s != W ? zsu : null, conn, synodes[s],
					s != DBSyn2tableTest.W ? SynodeMode.peer : SynodeMode.leaf, chpageSize, phm,
					new T_DA_DevMeta(conn),
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
		String[] X_d_uids = registerDevice(X);
		String X_d = X_d_uids[0];

		// syn_change.curd = C
		ck[X].change_log(1, C, synodes[X], X_0, ck[X].docm);
		ck[X].change_log(1, C, synodes[X], X_d, ck[X].devm);

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
		assertEquals(ck[X].devs(), ck[Y].devs());
		
		// The following assertions are based on this, no extFilev2 replacing uri into external.
		// And refile() bypassed uri content at Y.
		assertFalse(DATranscxt.hasSemantics(ck[X].connId(), ck[X].docm.tbl, smtype.extFilev2));
		assertFalse(DATranscxt.hasSemantics(ck[Y].connId(), ck[Y].docm.tbl, smtype.extFilev2));

		String x_b64x0 = (String) DAHelper.getValstr(ck[X].b0, ck[X].connId(),
				ck[X].docm, ck[X].docm.uri, ck[X].docm.io_oz_synuid, X_0_uids[2]);
		logi(x_b64x0); // no extFilev2
		assertTrue(x_b64x0.startsWith("iVBORw0K"));

		DocRef y_refx0 = (DocRef) Anson.fromJson((String) DAHelper
				.getExprstr(ck[Y].b0, ck[Y].connId(), ck[Y].docm,
					Funcall.refile(new DocRef(ck[X].synb.syndomx.synode, ck[Y].docm)), "docref",
					ck[Y].docm.io_oz_synuid, X_0_uids[2]));
		assertNotNull(y_refx0);
		assertEquals("X", y_refx0.synode);
		assertEquals(X_0_uids[2], y_refx0.uids);
		assertEquals("h_photos", y_refx0.syntabl);
		assertTrue(gt(y_refx0.docId, B_0_uids[0]));
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
		DBSyntableBuilder ctb = ck[cli].synb;
		DBSyntableBuilder stb = ck[srv].synb;

		SyntityMeta sphm = new T_DA_PhotoMeta(stb.basictx().connId());
		SyntityMeta cphm = new T_DA_PhotoMeta(ctb.basictx().connId());
		
		exchange(ssm, sphm, cphm, stb, ctb, test, subno);
	}

	void exchangeSynodes(int srv, int cli, int test, int subno)
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
//		if (req.nv.containsKey(clientnid))
//			assertEquals(req.nv.get(clientnid).n + 1, SyndomContext.getNyquence(ctb).n);
		assertEquals(ready, cp.exstate());

		printChangeLines(ck);
		printNyquv(ck);
		printNyquv(ck, true);

		Utils.logrst(new String[] {servnid, "on closing exchange"}, test, subno, ++no);
		// FIXME what if the server doesn't agree?
		rep = stb.onclosexchange(sp, req);
//		if (req.nv.containsKey(clientnid))
//			assertEquals(rep.nv.get(clientnid).n + 1, SyndomContext.getNyquence(stb).n);
		assertEquals(ready, sp.exstate());

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
	String[] insertPhoto(int s) throws TransException, SQLException, IOException {
		DBSyntableBuilder trb = ck[s].synb;
		ExpDocTableMeta m = ck[s].docm;
		String synoder = trb.syndomx.synode;

		SyncUser usr = new SyncUser(synoder, synoder, "doc owner@" + synoder, "dev client of " + s);

		String[] pid_chid = trb.insertEntity(ck[s].synb.syndomx, m, new T_Photo(ck[s].synb.syndomx.synconn, ura)
				.create(path_ukraine)
				.device(usr.deviceId())
				.folder(usr.uid()));
		
		ck[s].sessionUsr = usr; 

		return new String[] {pid_chid[0], pid_chid[1],
					SynChangeMeta.uids(synoder, pid_chid[0])};
	}

	String[] registerDevice(int s) throws TransException, SQLException, IOException {
		DBSyntableBuilder trb = ck[s].synb;
		SyntityMeta devm = ck[s].devm;
		String synoder = trb.syndomx.synode;

		String devid = f("dev-%s", s);
		String[] did_chid = trb.insertEntity(ck[s].synb.syndomx, devm,
				new T_Device(ck[s].synb.syndomx.synconn, ura, devid, "test-" + devid), devid);
		
		return new String[] {did_chid[0], did_chid[1],
					SynChangeMeta.uids(synoder, did_chid[0])};
	}

	/**
	 * @param chgm
	 * @param s checker index
	 * @return [synuid, 1/0]
	 * @throws TransException
	 * @throws SQLException
	Object[] deletePhoto(int s) throws TransException, SQLException {
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
	 */
}
