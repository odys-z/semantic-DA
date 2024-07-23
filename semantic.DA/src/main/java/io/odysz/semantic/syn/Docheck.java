package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.compoundVal;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.transact.sql.parts.condition.Funcall.compound;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.xml.sax.SAXException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.semantic.meta.SynChangeMeta;
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
	public static final String org = "URA";

	public static Docheck[] ck = new Docheck[4];

	public ExpDocTableMeta docm;

	public final DBSyntableBuilder trb;

	final String domain;

	public IUser robot() { return trb.synrobot(); }

	public int docs() throws SQLException, TransException {
		return trb.entities(docm);
	}

	String connId() { return trb.basictx().connId(); }

	public Docheck(String domain, String conn, String synid, SynodeMode mod, ExpDocTableMeta m)
			throws SQLException, TransException, ClassNotFoundException, IOException, SAXException {
		this(m, domain, conn, mod, synid, "rob-" + synid);
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
		assertEquals(cnt, rs.getRowCount());
	}

	public Docheck(ExpDocTableMeta docm, String domain, String conn, SynodeMode mode,
			String synid, String usrid)
			throws SQLException, TransException, ClassNotFoundException, IOException, SAXException {
		trb = new DBSyntableBuilder(domain, conn, synid, mode)
				.loadNyquvect0(conn);

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

		assertEquals(count, ((AnResultset) q
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
				fail(String.format("Expecting count == %d, but is actual 0", count));

			assertEquals(count, chg.getRowCount());
			if (count > 0) {
				assertEquals(crud, chg.getString(trb.chgm.crud));
				assertEquals(entm.tbl, chg.getString(trb.chgm.entbl));
				assertEquals(synoder, chg.getString(trb.chgm.synoder));
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
			fail(String.format("Expecting count == %d, but is actual 0", count));

		assertEquals(count, chg.getRowCount());
		if (count > 0) {
			assertEquals(crud, chg.getString(trb.chgm.crud));
			assertEquals(entm.tbl, chg.getString(trb.chgm.entbl));
			assertEquals(synoder, chg.getString(trb.chgm.synoder));
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
			fail(String.format("Expecting count == %d, but is actual 0", count));

		assertEquals(count, chg.getRowCount());
		if (count > 0) {
			assertEquals(crud, chg.getString(trb.chgm.crud));
			assertEquals(entm.tbl, chg.getString(trb.chgm.entbl));
			assertEquals(synoder, chg.getString(trb.chgm.synoder));
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

		// subsCount(synm, subcount, chgId, toIds.toArray(new String[0]));
			int cnt = 0;
			// AnResultset subs = trb.subscribes(connId(), domain, uids, entm, robot());
			AnResultset subs = (AnResultset) trb
					.select(trb.chgm.tbl, "ch")
					.je_(trb.subm.tbl, "sb", trb.chgm.pk, trb.subm.changeId)
					.cols_byAlias("sb", trb.subm.cols())
					.whereEq(trb.chgm.uids, uids)
					.rs(trb.instancontxt(connId(), robot()))
					.rs(0);
					;

			subs.beforeFirst();
			while (subs.next()) {
				if (indexOf(toIds.toArray(new String[0]), subs.getString(trb.subm.synodee)) >= 0)
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
				if (indexOf(toIds, subs.getString(trb.subm.synodee)) >= 0)
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
}