package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.compoundVal;
import static io.odysz.common.LangExt.ifnull;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.is;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.repeat;
import static io.odysz.common.LangExt.strcenter;
import static io.odysz.transact.sql.parts.condition.Funcall.compound;
import static io.odysz.transact.sql.parts.condition.Funcall.concat;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.odysz.common.IAssert;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.condition.Predicate;
import io.odysz.transact.x.TransException;

/**
 * Checker of each Synode.
 * 
 * @author Ody
 */
public class Docheck {
	public static IAssert azert;

	public static final String org = "URA";

	public static Docheck[] ck = new Docheck[4];

	public ExpDocTableMeta docm;

	public final DBSyntableBuilder synb;

	final String domain;

	
	static SynChangeMeta chm = new SynChangeMeta();
	static SynSubsMeta sbm = new SynSubsMeta(chm);
	static SynchangeBuffMeta xbm = new SynchangeBuffMeta(chm);
	static SynSessionMeta ssm = new SynSessionMeta();
	static PeersMeta prm = new PeersMeta();

	public final DATranscxt b0;

	/** Count {@link #docm}'s table records. */
	public int docs() throws SQLException, TransException {
		return synb.entities(docm);
	}

	/** Count {@link #devm}'s table records. */
	public int devs() throws SQLException, TransException {
		return synb.entities(devm);
	}

	public String doclist(SyntityMeta entm) throws SQLException, TransException {
		AnResultset rs = synb.entitySynuids(entm).beforeFirst();
		String r = "";
		while (rs.next()) {
			r += " " + rs.getString(1);
		}
		return r.trim();
	}

	String connId() { return synb.basictx().connId(); }

	public Docheck(IAssert assertImpl, String domain, String conn,
			String synid, SynodeMode mod, ExpDocTableMeta m, SyntityMeta devm, boolean debugx)
			throws Exception {
		this(assertImpl, new SyndomContext(mod, domain, synid, conn, debugx), m, devm);
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
				nodes.add(ck[x].synb.syndomx.synode);
				cnt++;
			}
		}
		
		SyndomContext synx = synb.syndomx;

		AnResultset rs = (AnResultset) synb.select(synx.synm.tbl)
			.col(synx.synm.synoder)
			.distinct(true)
			.whereIn(synx.synm.synoder, nodes)
			.rs(synb.instancontxt(synb.basictx().connId(), synb.synrobot()))
			.rs(0);

		azert.equali(cnt, rs.getRowCount());
	}

	public Docheck(IAssert assertImpl, SyndomContext x, ExpDocTableMeta docm, SyntityMeta devm) throws Exception {
		synb = new DBSyntableBuilder(x); // .loadContext();

		x.loadNvstamp(synb);

		azert = assertImpl == null ? azert : assertImpl;

		this.docm = docm;
		this.devm = devm;
		this.domain = x.domain;
		this.tops = null;
		
		this.b0 = new DATranscxt(x.synconn);
	}
	
	/**
	 * Check doc count. 
	 * @param count
	 * @param synids
	 * @throws TransException
	 * @throws SQLException
	 */
	public void doc(int count, String... synids) throws TransException, SQLException {
		Query q = synb.select(docm.tbl).col(count(), "c");
		if (!isNull(synids))
			q.whereIn(docm.io_oz_synuid, synids);

		azert.equali(count, ((AnResultset) q
				.rs(synb.instancontxt(synb.syndomx.synconn, synb.synrobot()))
				.rs(0))
				.nxt()
				.getInt("c"));
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
		return buf_change(count, crud, synb.syndomx.synode, eid, entm);
	}

	public long buf_change_p(int count, String crud, String eid)
			throws TransException, SQLException {
		return buf_change(count, crud, synb.syndomx.synode, eid, docm);
	}

	public long change_doclog(int count, String crud, String eid) throws TransException, SQLException {
		return change_log(count, crud, synb.syndomx.synode, eid, docm);
	}

	public long change_doclog(int count, String crud, String synoder, String eid) throws TransException, SQLException {
		return change_log(count, crud, synoder, eid, docm);
	}

	// public long change_log(int count, String crud, String synoder, String eid, SyntityMeta entm)
	public long change_log(int count, String crud, String synoder, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		Query q = synb
				.select(synb.syndomx.chgm.tbl, "ch")
				.cols((Object[])synb.syndomx.chgm.insertCols())
				.whereEq(synb.syndomx.chgm.domain, domain)
				.whereEq(synb.syndomx.chgm.entbl, entm.tbl);
			if (synoder != null)
				q.whereEq(synb.syndomx.chgm.synoder, synoder);

			//
			if (eid != null)
				q.whereEq(synb.syndomx.chgm.uids, SynChangeMeta.uids(synoder, eid));

			AnResultset chg = (AnResultset) q
					.rs(synb.instancontxt())
					.rs(0);
			
			if (!chg.next() && count > 0)
				azert.fail(String.format("Expecting count == %d, but is actual 0", count));

			azert.equali(count, chg.getRowCount());
			if (count > 0) {
				azert.equals(crud, chg.getString(synb.syndomx.chgm.crud));
				azert.equals(entm.tbl, chg.getString(synb.syndomx.chgm.entbl));
				azert.equals(synoder, chg.getString(synb.syndomx.chgm.synoder));
				return chg.getLong(synb.syndomx.chgm.nyquence);
			}
			return 0;
	}

	public long change_log_uids(int count, String crud, String synoder, String uids, SyntityMeta entm)
			throws TransException, SQLException {
		SynChangeMeta chgm = synb.syndomx.chgm;

		Query q = synb
				.select(chgm.tbl, "ch")
				.cols((Object[])chgm.insertCols())
				.whereEq(chgm.domain, domain)
				.whereEq(chgm.entbl, entm.tbl);
			if (synoder != null)
				q.whereEq(synb.syndomx.chgm.synoder, synoder);

			// 
			if (uids != null)
				q.whereEq(chgm.uids, uids);

			AnResultset chg = (AnResultset) q
					.rs(synb.instancontxt())
					.rs(0);
			
			if (!chg.next() && count > 0)
				azert.fail(String.format("Expecting count == %d, but is actual 0", count));

			azert.equali(count, chg.getRowCount());
			if (count > 0) {
				azert.equals(crud, chg.getString(chgm.crud));
				azert.equals(entm.tbl, chg.getString(chgm.entbl));
				azert.equals(synoder, chg.getString(chgm.synoder));
				return chg.getLong(chgm.nyquence);
			}
			return 0;
	}


	public long buf_change(int count, String crud, String synoder, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		SynChangeMeta chgm = synb.syndomx.chgm;
		SynchangeBuffMeta exbm = synb.syndomx.exbm;

		Query q = synb
			.select(chgm.tbl, "ch")
			.je_(exbm.tbl, "xb", chgm.pk, exbm.changeId, chgm.synoder, exbm.peer)
			.cols((Object[])chgm.insertCols())
			.whereEq(chgm.domain, domain)
			.whereEq(chgm.entbl, entm.tbl);
		if (synoder != null)
			q.whereEq(chgm.synoder, synoder);
		if (eid != null)
			q.whereEq(chgm.uids, SynChangeMeta.uids(synoder, eid));

		AnResultset chg = (AnResultset) q
				.rs(synb.instancontxt())
				.rs(0);
		
		if (!chg.next() && count > 0)
			azert.fail(String.format("Expecting count == %d, but is actual 0", count));

		azert.equali(count, chg.getRowCount());
		if (count > 0) {
			azert.equals(crud, chg.getString(chgm.crud));
			azert.equals(entm.tbl, chg.getString(chgm.entbl));
			azert.equals(synoder, chg.getString(chgm.synoder));
			return chg.getLong(chgm.nyquence);
		}
		return 0;
	}

	public long changelog(int count, String crud, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		return changelog(count, crud, synb.syndomx.synode, eid, entm);
	}

	public long changelog(int count, String crud, String synoder, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		SynChangeMeta chgm = synb.syndomx.chgm;

		Query q = synb
			.select(chgm.tbl, "ch")
			.cols((Object[])chgm.insertCols())
			.whereEq(chgm.domain, domain)
			.whereEq(chgm.entbl, entm.tbl);
		if (synoder != null)
			q.whereEq(chgm.synoder, synoder);
		if (eid != null)
			q.whereEq(chgm.uids, SynChangeMeta.uids(synoder, eid));

		AnResultset chg = (AnResultset) q
				.rs(synb.instancontxt())
				.rs(0);
		
		if (!chg.next() && count > 0)
			azert.fail(String.format("Expecting count == %d, but is actual 0", count));

		azert.equali(count, chg.getRowCount());
		if (count > 0) {
			azert.equals(crud, chg.getString(chgm.crud));
			azert.equals(entm.tbl, chg.getString(chgm.entbl));
			azert.equals(synoder, chg.getString(chgm.synoder));
			return chg.getLong(chgm.nyquence);
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
				toIds.add(ck[n].synb.syndomx.synode);
		subsCount(docm, subcount, chgid, toIds.toArray(new String[0]));
	}

	public void synsubs(int subcount, String uids, int ... sub) throws SQLException, TransException {
		ArrayList<String> toIds = new ArrayList<String>();
		SynSubsMeta subm = synb.syndomx.subm;
		SynChangeMeta chgm = synb.syndomx.chgm;

		for (int n : sub)
			if (n >= 0)
				toIds.add(ck[n].synb.syndomx.synode);

			int cnt = 0;
			AnResultset subs = (AnResultset) synb
					.select(chgm.tbl, "ch")
					.je_(subm.tbl, "sb", chgm.pk, subm.changeId)
					.cols_byAlias("sb", (Object[])subm.cols())
					.whereEq(chgm.uids, uids)
					.rs(synb.instancontxt())
					.rs(0);
					;

			subs.beforeFirst();
			while (subs.next()) {
				if (indexOf(toIds.toArray(new String[0]), subs.getString(subm.synodee)) >= 0)
					cnt++;
			}

			azert.equali(subcount, cnt);
			azert.equali(subcount, subs.getRowCount());
	}

	public void subsCount(SyntityMeta entm, int subcount, String chgId, String ... toIds)
			throws SQLException, TransException {
		if (isNull(toIds)) {
			AnResultset subs = subscribes(connId(), chgId, entm);
			azert.equali(subcount, subs.getRowCount());
		}
		else {
			int cnt = 0;
			AnResultset subs = subscribes(connId(), chgId, entm);
			subs.beforeFirst();
			while (subs.next()) {
				if (indexOf(toIds, subs.getString(synb.syndomx.subm.synodee)) >= 0)
					cnt++;
			}

			azert.equali(subcount, cnt);
			azert.equali(subcount, subs.getRowCount());
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
	private AnResultset subscribes(String connId, String chgId, SyntityMeta entm) 
		throws TransException, SQLException {
		SynSubsMeta subm = synb.syndomx.subm;

		Query q = synb.select(subm.tbl, "ch")
				.cols((Object[])subm.cols())
				;
		if (!isblank(chgId))
			q.whereEq(subm.changeId, chgId) ;

		return (AnResultset) q
				.rs(synb.instancontxt())
				.rs(0);
	}
	
	/**
	 * Verify if and only if one instance exists on this node.
	 * 
	 * @param synoder
	 * @param clientpath
	 */
	public void verifile(String synoder, String clientpath, ExpDocTableMeta docm) {
		synb.select(docm.tbl)
			.col(count(docm.pk), "c")
			.where(new Predicate(op.eq, compound(synb.syndomx.chgm.uids), compoundVal(synoder, clientpath)))
			;
	}

	@SuppressWarnings("unchecked")
	public static HashMap<String, Nyquence>[] printNyquv(Docheck[] ck, boolean... printSynode) throws SQLException, TransException {
		Utils.logi(Stream.of(ck)
				.filter(c -> c != null)
				.map(c -> { return c.synb.syndomx.synode;})
				.collect(Collectors.joining("    ", "      ", "")));
		
		final HashMap<?, ?>[] nv2 = new HashMap[ck.length];

		for (int cx = 0; cx < ck.length && ck[cx] instanceof Docheck; cx++) {
			DBSyntableBuilder t = ck[cx].synb;

			boolean top = Connects.getDebug(ck[cx].synb.syndomx.synconn);
			Connects.setDebug(ck[cx].synb.syndomx.synconn, false);

			try {
				HashMap<String, Nyquence> nyquvect = ck[cx].synb.syndomx.loadNvstamp(t); 

				nv2[cx] = Nyquence.clone(nyquvect);

				Utils.logi("%s [ %s ] { %s }",
					ck[cx].synb.syndomx.synode,
					Stream.of(ck)
					.filter(c -> c != null)
					.map((c) -> {
						String n = c.synb.syndomx.synode;
						return String.format("%3s",
							nyquvect.containsKey(n) ?
							nyquvect.get(n).n : "");
						})
					.collect(Collectors.joining(", ")),
					is(printSynode) ?
					ck[cx].doclist(ck[cx].synb.syndomx.synm) :
					ck[cx].doclist(ck[cx].docm));
			}
			finally { Connects.setDebug(ck[cx].synb.syndomx.synconn, top); }
		}

		return (HashMap<String, Nyquence>[]) nv2;
	}
	
	static String changeLine(AnResultset r) throws SQLException {
		String seq = r.getString(xbm.pagex, " ");

		return String.format("%1$1s %2$9s %3$9s %4$2s %5$2s [%6$4s]",
				r.getString(chm.crud),
				r.getString(chm.pk),
				r.getString(chm.uids),
				r.getString(chm.nyquence),
				r.getString(sbm.synodee),
				seq == null ? " " : seq
				);
	}
	
	public static void printChangeLines(Docheck[] ck)
			throws TransException, SQLException {

		HashMap<String, String[]> uidss = new HashMap<String, String[]>();

		for (int cx = 0; cx < ck.length && ck[cx] instanceof Docheck; cx++) {
			DBSyntableBuilder b = ck[cx].synb;
			SyndomContext x = ck[cx].synb.syndomx;
			boolean top = Connects.getDebug(x.synconn);
			Connects.setDebug(x.synconn, false);
			try {
				HashMap<String,String> idmap = ((AnResultset) b
					.select(chm.tbl, "ch")
					.cols("ch.*", sbm.synodee).col(concat(ifnull(xbm.peer, " "), "':'", xbm.pagex), xbm.pagex)
					// .je("ch", sbm.tbl, "sub", chm.entbl, sbm.entbl, chm.domain, sbm.domain, chm.uids, sbm.uids)
					.je_(sbm.tbl, "sub", chm.pk, sbm.changeId)
					.l_(xbm.tbl, "xb", chm.pk, xbm.changeId)
					.orderby(xbm.pagex, chm.entbl, chm.uids)
					// .rs(b.instancontxt(b.basictx().connId(), b.synrobot()))
					.rs(b.instancontxt(x.synconn, b.synrobot()))
					.rs(0))
					.<String>map(new String[] {chm.pk, sbm.synodee}, (r) -> changeLine(r));

				for(String cid : idmap.keySet()) {
					if (!uidss.containsKey(cid))
						uidss.put(cid, new String[ck.length]);

					uidss.get(cid)[cx] = idmap.get(cid);
				}
			}
			finally { 
				Connects.setDebug(x.synconn, top);
			}
		}
		
		Utils.logi(Stream.of(ck)
				.filter(c -> c != null)
				.map(c -> strcenter(c.synb.syndomx.synode, 36))
				.collect(Collectors.joining("|")));
		Utils.logi(Stream.of(ck)
				.filter(c -> c != null)
				.map(c -> repeat("-", 36))
				.collect(Collectors.joining("+")));

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
	
	/**
	 * Assert ai = bi, where ai, bi in nvs [a0, a1, ..., b0, b1, ...].
	 * @param nvs
	 */
	public static void assertnv(long... nvs) {
		if (nvs == null || nvs.length == 0 || nvs.length % 2 != 0)
			azert.fail("Invalid arguments to assert.");
		
		for (int i = 0; i < nvs.length/2; i++) {
			azert.equall(nvs[i], nvs[i + nvs.length/2],
				String.format("nv[%d] %d : %d", i, nvs[i], nvs[i + nvs.length/2]));
		}
	}

	public static void assertI(Docheck[] ck, HashMap<?, ?>[] nvs) throws SQLException, TransException {
		for (int i = 0; i < nvs.length; i++) {
			if (nvs[i] != null && nvs[i].size() > 0)
				azert.equall(ck[i].n0().n, ((Nyquence)nvs[i].get(ck[i].synb.syndomx.synode)).n);
			else break;
		}
	}
	
	public Nyquence n0() throws SQLException, TransException {
		// return synx.getNyquence(synb, synx.synconn, synx.synm, synx.synm.nyquence, synx.synm.synoder, synx.synode);
		return synb.syndomx.n0();
	}

	public static void assertnv(HashMap<String, Nyquence> nv0,
			HashMap<String, Nyquence> nv1, int ... delta) {
		if (nv0 == null || nv1 == null || nv0.size() != nv1.size() || nv1.size() != delta.length)
			azert.fail("Invalid arguments to assert.");
		
		for (int i = 0; i < nv0.size(); i++) {
			azert.equall(nv0.get(ck[i].synb.syndomx.synode).n + delta[i], nv1.get(ck[i].synb.syndomx.synode).n,
				String.format("nv[%d] %d : %d + %d",
						i, nv0.get(ck[i].synb.syndomx.synode).n,
						nv1.get(ck[i].synb.syndomx.synode).n,
						delta[i]));
		}
	}

	public long stamp() throws SQLException, TransException {
		boolean dbg = Connects.getDebug(synb.syndomx.synconn);
		try {
			// return DAHelper.getNstamp(synb).n;
			return synb.syndomx.stamp.n;
		}
		finally { Connects.setDebug(synb.syndomx.synconn, dbg); }
	}
	
	final boolean[] tops;

	public SyncUser sessionUsr;

	/** device meta */
	public final SyntityMeta devm;
	
	/**
	 * <p>Push connection's debug flags and return a checker instance.</p>
	 * Example:
	 * <pre>pushDebug()
	 * .assertl(
	 *  expect1, loadNyquvect(actual1), // logging suppressed
	 *  expect2, loadNyquvect(actual2), // logging suppressed
	 *  ...)
	 * .popDebug();                     // logging restored</pre>
	 * @throws Exception 
	*/
	public static void pushDebug() throws Exception {
		if (ck != null) {
			final boolean[] tops = new boolean[ck.length];
			for (int cx = 0; cx < ck.length; cx++) {
				if (ck[cx] != null) {
					tops[cx] = Connects.getDebug(ck[cx].connId());
					Connects.setDebug(ck[cx].connId(), tops[cx]);
				}
			}
		
			// return new Docheck(tops);
		}
		// return new Docheck(new boolean[0]);
	}

	public Docheck assertl(long ... n) {
		if (!isNull(n))
			for (int x = 0; x < n.length; x+=2)
				azert.equall(n[x], n[x+1]);
		return this;
	}

	public void popDebug() {
		if (tops != null) 
			for (int cx = 0; cx < ck.length; cx++) 
				if (ck[cx] != null)
				Connects.setDebug(ck[cx].connId(), tops[cx]);
	}
}