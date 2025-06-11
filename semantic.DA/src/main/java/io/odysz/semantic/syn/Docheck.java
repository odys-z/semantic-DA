package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.ifnull;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.is;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.len;
import static io.odysz.common.LangExt.repeat;
import static io.odysz.common.LangExt.strcenter;
import static io.odysz.transact.sql.parts.condition.Funcall.concat;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.transact.sql.parts.condition.Funcall.constr;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.odysz.anson.Anson;
import io.odysz.common.IAssert;
import io.odysz.common.Regex;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.DocRef;
import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.parts.condition.Funcall;
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
		try {
			return synb.pushDebug(false).entities(docm);
		}finally { synb.popDebug(); }
	}

	/** Count {@link #devm}'s table records. */
	public int devs() throws SQLException, TransException {
		try { return synb.pushDebug(false).entities(devm);
		} finally { synb.popDebug(); }
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
			String synid, SynodeMode mod, int chpageSize, ExpDocTableMeta m, SyntityMeta devm, boolean debugx)
			throws Exception {
		this(assertImpl, new SyndomContext(mod, chpageSize, domain, synid, conn, debugx), m, devm);
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

		synb.pushDebug(false);
		try {
			AnResultset rs = (AnResultset) synb.select(synx.synm.tbl)
				.col(synx.synm.synoder)
				.distinct(true)
				.whereIn(synx.synm.synoder, nodes)
				.rs(synb.instancontxt(synb.basictx().connId(), synb.synrobot()))
				.rs(0);

			azert.equali(cnt, rs.getRowCount());
		} finally { synb.popDebug(); }
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
	 * @param uids
	 * @throws TransException
	 * @throws SQLException
	 */
	public void doc(int count, String... uids) throws TransException, SQLException {
		Query q = synb.select(docm.tbl).col(count(), "c");
		if (!isNull(uids))
			q.whereIn(docm.io_oz_synuid, uids);

		synb.pushDebug(false);
		try {
			azert.equali(count, ((AnResultset) q
				.rs(synb.instancontxt(synb.syndomx.synconn, synb.synrobot()))
				.rs(0))
				.nxt()
				.getInt("c"));
		} finally { synb.popDebug(); }
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
	public void buf_change(int count, String crud, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		buf_change(count, crud, synb.syndomx.synode, eid, entm);
	}

	public void buf_change_p(int count, String crud, String eid)
			throws TransException, SQLException {
		buf_change(count, crud, synb.syndomx.synode, eid, docm);
	}

	public void change_doclog(int count, String crud, String eid) throws TransException, SQLException {
		change_log(count, crud, synb.syndomx.synode, eid, docm);
	}

	public void change_doclog(int count, String crud, String synoder, String eid) throws TransException, SQLException {
		change_log(count, crud, synoder, eid, docm);
	}

	/**
	 * Verify change log's count. {@code origin_eid} can only domain wide,
	 * composed with {@code synoder} for syn-uids.
	 * 
	 * @param count
	 * @param crud
	 * @param synoder
	 * @param origin_eid
	 * @param entm
	 * @throws TransException
	 * @throws SQLException
	 */
	public void change_log(int count, String crud, String synoder, String origin_eid, SyntityMeta entm)
			throws TransException, SQLException {
		Query q = synb
				.select(synb.syndomx.chgm.tbl, "ch")
				.cols((Object[])synb.syndomx.chgm.insertCols())
				.whereEq(synb.syndomx.chgm.domain, domain)
				.whereEq(synb.syndomx.chgm.entbl, entm.tbl);
		if (synoder != null)
			q.whereEq(synb.syndomx.chgm.synoder, synoder);

		//
		if (origin_eid != null)
			q.whereEq(synb.syndomx.chgm.uids, SynChangeMeta.uids(synoder, origin_eid));

		synb.pushDebug(false);
		AnResultset chg = (AnResultset) q
				.rs(synb.instancontxt())
				.rs(0);
		synb.popDebug();
		
		if (!chg.next() && count > 0)
			azert.fail(String.format("Expecting count == %d, but is actual 0", count));

		azert.equali(count, chg.getRowCount());
		if (count > 0) {
			azert.equals(crud, chg.getString(synb.syndomx.chgm.crud));
			azert.equals(entm.tbl, chg.getString(synb.syndomx.chgm.entbl));
			azert.equals(synoder, chg.getString(synb.syndomx.chgm.synoder));
		}
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

			synb.pushDebug(false);
			AnResultset chg = (AnResultset) q
					.rs(synb.instancontxt())
					.rs(0);
			synb.popDebug();
			
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
	 * Verify
	 * @param count
	 * @param crud
	 * @param synoder
	 * @param eid
	 * @param entm
	 * @throws TransException
	 * @throws SQLException
	 */
	public void buf_change(int count, String crud, String synoder, String eid, SyntityMeta entm)
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

		synb.pushDebug(false);
		AnResultset chg = (AnResultset) q
				.rs(synb.instancontxt())
				.rs(0);
		synb.popDebug();
		
		if (!chg.next() && count > 0)
			azert.fail(String.format("Expecting count == %d, but is actual 0", count));

		azert.equali(count, chg.getRowCount());
		if (count > 0) {
			azert.equals(crud, chg.getString(chgm.crud));
			azert.equals(entm.tbl, chg.getString(chgm.entbl));
			azert.equals(synoder, chg.getString(chgm.synoder));
		}
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

		synb.pushDebug(false);
		AnResultset chg = (AnResultset) q
				.rs(synb.instancontxt())
				.rs(0);
		synb.popDebug();
		
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
	
	
	public void psubs_uid(int subcount, String synuid, int ... sub) throws SQLException, TransException {
		ArrayList<String> toIds = new ArrayList<String>();
		for (int n : sub)
			if (n >= 0)
				toIds.add(ck[n].synb.syndomx.synode);

		String chgid = DAHelper.getValstr(synb, synb.syndomx.synconn, chm, chm.pk,
				chm.domain, constr(synb.syndomx.domain), chm.entbl, constr(this.docm.tbl),
				chm.uids, synuid);

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
			if (len(toIds) == 0)
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

	public void subsCount_uid(SyntityMeta entm, int subcount, String synuid, String ... toIds)
			throws SQLException, TransException {

		String chgId = DAHelper.getValstr(synb, synb.syndomx.synconn, chm, chm.pk,
				chm.domain, constr(synb.syndomx.domain), chm.entbl, constr(this.docm.tbl),
				chm.uids, synuid);


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
		

		synb.pushDebug(false);
		try {
		return (AnResultset) q
				.rs(synb.instancontxt())
				.rs(0);
		} finally { synb.popDebug();}
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
					.je_(sbm.tbl, "sub", chm.pk, sbm.changeId)
					.l_(xbm.tbl, "xb", chm.pk, xbm.changeId)
					.orderby(xbm.pagex, chm.entbl, chm.uids)
					// .rs(b.instancontxt(x.synconn, b.synrobot()))
					.rs(b.instancontxt())
					.rs(0))
					.<String>map(new String[] {chm.uids, sbm.synodee}, (r) -> changeLine(r));

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
	
	static String fomatChangeLine(AnResultset r) throws SQLException {
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
	
	public Nyquence n0() {
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
			return synb.syndomx.stamp.n;
		}
		finally { Connects.setDebug(synb.syndomx.synconn, dbg); }
	}
	
	final boolean[] tops;

	public SyncUser sessionUsr;

	/** device meta */
	public final SyntityMeta devm;

	public Docheck assertl(long ... n) {
		if (!isNull(n))
			for (int x = 0; x < n.length; x+=2)
				azert.equall(n[x], n[x+1]);
		return this;
	}
	
	/**
	 * Try load doc-ref envelope from docm.tbl, not syn_docref.
	 * @return refernces if it's envelope, other wise a null value in the list.
	 * @throws SQLException
	 * @throws TransException
	 */
	public Collection<DocRef> docRef() throws SQLException, TransException {
		try {
			return ((AnResultset)synb.pushDebug(false).select(docm.tbl)
				.col(docm.pk).col(docm.uri)
				.where(Predicate.eq(Funcall.subStr(docm.uri, 1, 8), "{\"type\":")) // TODO we need function predicate
				.rs(synb.instancontxt())
				.rs(0))
				.map(docm.pk, (rs) -> {
					String s = rs.getString(docm.uri);
					try {
						return Regex.startsEvelope(s) ? (DocRef)Anson.fromJson(s) : null; 
					} catch (Exception e) {
						Utils.warn("[Docheck.docRef()] Deserializing docref failed: %s", s);
						return null;
					} 
				}).values();
		} finally { synb.popDebug(); }
	}

	public String synode() {
		return synb.syndomx.synode;
	}
}