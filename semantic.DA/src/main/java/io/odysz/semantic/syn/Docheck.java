package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.compoundVal;
import static io.odysz.common.LangExt.ifnull;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.repeat;
import static io.odysz.common.LangExt.strcenter;
import static io.odysz.transact.sql.parts.condition.Funcall.compound;
import static io.odysz.transact.sql.parts.condition.Funcall.concat;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

// import static org.junit.jupiter.api.Assertions.azert.equals;
// import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.IUser;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.condition.Predicate;
import io.odysz.transact.x.TransException;

/**
 * Checker of each Synode.
 * 
 * TODO fail if found {@link io.odysz.semantic.DASemantics.smtype#synChange syn-change}
 * is configured.
 * 
 * @author Ody
 */
public class Docheck {
	static IAssert azert;

	public static final String org = "URA";

	public static Docheck[] ck = new Docheck[4];

	public ExpDocTableMeta docm;

	public final DBSyntableBuilder trb;

	final String domain;

	
	static SynChangeMeta chm = new SynChangeMeta();
	static SynSubsMeta sbm = new SynSubsMeta(chm);
	static SynchangeBuffMeta xbm = new SynchangeBuffMeta(chm);
	static SynSessionMeta ssm = new SynSessionMeta();
	static PeersMeta prm = new PeersMeta();

	public IUser robot() { return trb.synrobot(); }

	public int docs() throws SQLException, TransException {
		return trb.entities(docm);
	}

	String connId() { return trb.basictx().connId(); }

	public Docheck(IAssert assertImpl, String domain, String conn,
			String synid, SynodeMode mod, ExpDocTableMeta m)
			throws SQLException, TransException, ClassNotFoundException, IOException, SAXException {
		this(assertImpl, m, domain, conn, mod, synid, "rob-" + synid);
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

		AnResultset rs = (AnResultset) trb.select(trb.synm.tbl)
			.col(trb.synm.synoder)
			.distinct(true)
			.whereIn(trb.synm.synoder, nodes)
			.rs(trb.instancontxt(trb.basictx().connId(), trb.synrobot()))
			.rs(0);

		azert.equali(cnt, rs.getRowCount());
	}

	public Docheck(IAssert assertImpl, ExpDocTableMeta docm, String domain, String conn,
			SynodeMode mode, String synid, String usrid)
			throws SQLException, TransException, ClassNotFoundException, IOException, SAXException {
		trb = new DBSyntableBuilder(domain, conn, synid, mode)
				.loadNyquvect(conn);

		azert = assertImpl == null ? azert : assertImpl;

		this.docm = docm;
		this.domain = trb.domain();
	}

	public HashMap<String, Nyquence> cloneNv() {
		HashMap<String, Nyquence> nv = new HashMap<String, Nyquence>(4);
		for (String n : trb.nyquvect.keySet())
			nv.put(n, new Nyquence(trb.nyquvect.get(n).n));
		return nv;
	}

	public void doc(int count, String... synids) throws TransException, SQLException {
		Query q = trb.select(docm.tbl).col(count(), "c");
		if (!isNull(synids))
			q.whereIn(docm.synuid, synids);

		azert.equali(count, ((AnResultset) q
				.rs(trb.instancontxt(trb.synconn(), trb.synrobot()))
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
		return buf_change(count, crud, trb.synode(), eid, entm);
	}

	public long buf_change_p(int count, String crud, String eid)
			throws TransException, SQLException {
		return buf_change(count, crud, trb.synode(), eid, docm);
	}

	public long change_doclog(int count, String crud, String eid) throws TransException, SQLException {
		return change_log(count, crud, trb.synode(), eid, docm);
	}

	public long change_doclog(int count, String crud, String synoder, String eid) throws TransException, SQLException {
		return change_log(count, crud, synoder, eid, docm);
	}

	public long change_log(int count, String crud, String synoder, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		Query q = trb
				.select(trb.chgm.tbl, "ch")
				.cols((Object[])trb.chgm.insertCols())
				.whereEq(trb.chgm.domain, domain)
				.whereEq(trb.chgm.entbl, entm.tbl);
			if (synoder != null)
				q.whereEq(trb.chgm.synoder, synoder);
			if (eid != null)
				q.whereEq(trb.chgm.uids, SynChangeMeta.uids(synoder, eid));

			AnResultset chg = (AnResultset) q
					.rs(trb.instancontxt(connId(), robot()))
					.rs(0);
			
			if (!chg.next() && count > 0)
				azert.fail(String.format("Expecting count == %d, but is actual 0", count));

			azert.equali(count, chg.getRowCount());
			if (count > 0) {
				azert.equals(crud, chg.getString(trb.chgm.crud));
				azert.equals(entm.tbl, chg.getString(trb.chgm.entbl));
				azert.equals(synoder, chg.getString(trb.chgm.synoder));
				return chg.getLong(trb.chgm.nyquence);
			}
			return 0;
	}

	public long buf_change(int count, String crud, String synoder, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		Query q = trb
			.select(trb.chgm.tbl, "ch")
			.je_(trb.exbm.tbl, "xb", trb.chgm.pk, trb.exbm.changeId, trb.chgm.synoder, trb.exbm.peer)
			.cols((Object[])trb.chgm.insertCols())
			.whereEq(trb.chgm.domain, domain)
			.whereEq(trb.chgm.entbl, entm.tbl);
		if (synoder != null)
			q.whereEq(trb.chgm.synoder, synoder);
		if (eid != null)
			q.whereEq(trb.chgm.uids, SynChangeMeta.uids(synoder, eid));

		AnResultset chg = (AnResultset) q
				.rs(trb.instancontxt(connId(), robot()))
				.rs(0);
		
		if (!chg.next() && count > 0)
			azert.fail(String.format("Expecting count == %d, but is actual 0", count));

		azert.equali(count, chg.getRowCount());
		if (count > 0) {
			azert.equals(crud, chg.getString(trb.chgm.crud));
			azert.equals(entm.tbl, chg.getString(trb.chgm.entbl));
			azert.equals(synoder, chg.getString(trb.chgm.synoder));
			return chg.getLong(trb.chgm.nyquence);
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
			.select(trb.chgm.tbl, "ch")
			.cols((Object[])trb.chgm.insertCols())
			.whereEq(trb.chgm.domain, domain)
			.whereEq(trb.chgm.entbl, entm.tbl);
		if (synoder != null)
			q.whereEq(trb.chgm.synoder, synoder);
		if (eid != null)
			q.whereEq(trb.chgm.uids, SynChangeMeta.uids(synoder, eid));

		AnResultset chg = (AnResultset) q
				.rs(trb.instancontxt(connId(), robot()))
				.rs(0);
		
		if (!chg.next() && count > 0)
			azert.fail(String.format("Expecting count == %d, but is actual 0", count));

		azert.equali(count, chg.getRowCount());
		if (count > 0) {
			azert.equals(crud, chg.getString(trb.chgm.crud));
			azert.equals(entm.tbl, chg.getString(trb.chgm.entbl));
			azert.equals(synoder, chg.getString(trb.chgm.synoder));
			return chg.getLong(trb.chgm.nyquence);
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
		subsCount(docm, subcount, chgid, toIds.toArray(new String[0]));
	}

	public void synsubs(int subcount, String uids, int ... sub) throws SQLException, TransException {
		ArrayList<String> toIds = new ArrayList<String>();
		for (int n : sub)
			if (n >= 0)
				toIds.add(ck[n].trb.synode());

			int cnt = 0;
			AnResultset subs = (AnResultset) trb
					.select(trb.chgm.tbl, "ch")
					.je_(trb.subm.tbl, "sb", trb.chgm.pk, trb.subm.changeId)
					.cols_byAlias("sb", (Object[])trb.subm.cols())
					.whereEq(trb.chgm.uids, uids)
					.rs(trb.instancontxt(connId(), robot()))
					.rs(0);
					;

			subs.beforeFirst();
			while (subs.next()) {
				if (indexOf(toIds.toArray(new String[0]), subs.getString(trb.subm.synodee)) >= 0)
					cnt++;
			}

			azert.equali(subcount, cnt);
			azert.equali(subcount, subs.getRowCount());
	}

	public void subsCount(SyntityMeta entm, int subcount, String chgId, String ... toIds)
			throws SQLException, TransException {
		if (isNull(toIds)) {
			AnResultset subs = subscribes(connId(), chgId, entm, robot());
			azert.equali(subcount, subs.getRowCount());
		}
		else {
			int cnt = 0;
			AnResultset subs = subscribes(connId(), chgId, entm, robot());
			subs.beforeFirst();
			while (subs.next()) {
				if (indexOf(toIds, subs.getString(trb.subm.synodee)) >= 0)
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
	private AnResultset subscribes(String connId, String chgId, SyntityMeta entm, IUser robot) 
		throws TransException, SQLException {
		Query q = trb.select(trb.subm.tbl, "ch")
				.cols((Object[])trb.subm.cols())
				;
		if (!isblank(chgId))
			q.whereEq(trb.subm.changeId, chgId) ;

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
	public void verifile(String synoder, String clientpath, ExpDocTableMeta docm) {
		trb.select(docm.tbl)
			.col(count(docm.pk), "c")
			.where(new Predicate(op.eq, compound(trb.chgm.uids), compoundVal(synoder, clientpath)))
			;
	}
	@SuppressWarnings("unchecked")
	public static HashMap<String, Nyquence>[] printNyquv(Docheck[] ck) throws SQLException, TransException {
		Utils.logi(Stream.of(ck)
				.filter(c -> c != null)
				.map(c -> { return c.trb.synode();})
				.collect(Collectors.joining("    ", "      ", "")));
		
		final HashMap<?, ?>[] nv2 = new HashMap[ck.length];

		for (int cx = 0; cx < ck.length && ck[cx] instanceof Docheck; cx++) {
			DBSyntableBuilder t = ck[cx].trb;

			boolean dbg = Connects.getDebug(t.synconn());
			Connects.setDebug(t.synconn(), false);
			t.loadNyquvect(t.synconn());
			Connects.setDebug(t.synconn(), dbg);

			nv2[cx] = Nyquence.clone(t.nyquvect);

			Utils.logi(
				t.synode() + " [ " +
				Stream.of(ck)
				.filter(c -> c != null)
				.map((c) -> {
					String n = c.trb.synode();
					return String.format("%3s",
						t.nyquvect.containsKey(n) ?
						t.nyquvect.get(n).n : "");
					})
				.collect(Collectors.joining(", ")) +
				" ]");
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
			DBSyntableBuilder b = ck[cx].trb;
			HashMap<String,String> idmap = ((AnResultset) b
					.select(chm.tbl, "ch")
					.cols("ch.*", sbm.synodee).col(concat(ifnull(xbm.peer, " "), "':'", xbm.pagex), xbm.pagex)
					// .je("ch", sbm.tbl, "sub", chm.entbl, sbm.entbl, chm.domain, sbm.domain, chm.uids, sbm.uids)
					.je_(sbm.tbl, "sub", chm.pk, sbm.changeId)
					.l_(xbm.tbl, "xb", chm.pk, xbm.changeId)
					.orderby(xbm.pagex, chm.entbl, chm.uids)
					.rs(b.instancontxt(b.basictx().connId(), b.synrobot()))
					.rs(0))
					.<String>map(new String[] {chm.pk, sbm.synodee}, (r) -> changeLine(r));

			for(String cid : idmap.keySet()) {
				if (!uidss.containsKey(cid))
					uidss.put(cid, new String[ck.length]);

				uidss.get(cid)[cx] = idmap.get(cid);
			}
		}
		
		Utils.logi(Stream.of(ck)
				.filter(c -> c != null)
				.map(c -> strcenter(c.trb.synode(), 36))
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

	public static void assertI(Docheck[] ck, HashMap<?, ?>[] nvs) {
		for (int i = 0; i < nvs.length; i++) {
			if (nvs[i] != null && nvs[i].size() > 0)
				azert.equall(ck[i].trb.n0().n, ((Nyquence)nvs[i].get(ck[i].trb.synode())).n);
			else break;
		}
	}
	
	public static void assertnv(HashMap<String, Nyquence> nv0,
			HashMap<String, Nyquence> nv1, int ... delta) {
		if (nv0 == null || nv1 == null || nv0.size() != nv1.size() || nv1.size() != delta.length)
			azert.fail("Invalid arguments to assert.");
		
		for (int i = 0; i < nv0.size(); i++) {
			azert.equall(nv0.get(ck[i].trb.synode()).n + delta[i], nv1.get(ck[i].trb.synode()).n,
				String.format("nv[%d] %d : %d + %d",
						i, nv0.get(ck[i].trb.synode()).n,
						nv1.get(ck[i].trb.synode()).n,
						delta[i]));
		}
	}
}