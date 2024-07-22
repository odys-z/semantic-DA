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
import io.odysz.semantic.meta.SynodeMeta;
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

	public ExpDocTableMeta phm;
	public SynodeMeta synm;

	final DBSyntableBuilder trb;

	final String domain;

	public IUser robot() { return trb.synrobot(); }

	public int docs() throws SQLException, TransException {
		return trb.entities(phm);
	}

	String connId() { return trb.basictx().connId(); }

	public Docheck(int s, String domain)
			throws SQLException, TransException, ClassNotFoundException, IOException, SAXException {
		this(domain, DBSyntableTest.conns[s], s != DBSyntableTest.W ? SynodeMode.peer : SynodeMode.leaf,
				DBSyntableTest.synodes[s], "rob-" + s);
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
				nodes.add(DBSyntableTest.ck[x].trb.synode());
				cnt++;
			}
		}

		AnResultset rs = (AnResultset) trb.select(synm.tbl)
			.col(synm.synoder)
			.distinct(true)
			.whereIn(synm.synoder, nodes)
			.rs(trb.instancontxt(trb.basictx().connId(), trb.synrobot()))
			.rs(0);
		assertEquals(cnt, rs.getRowCount());
	}

	public Docheck(String domain, String conn, SynodeMode mode, String synid, String usrid)
			throws SQLException, TransException, ClassNotFoundException, IOException, SAXException {
		trb = new DBSyntableBuilder(domain, conn, synid, mode)
				.loadNyquvect0(conn);

		phm = new T_PhotoMeta(conn);
		this.domain = trb.domain();
	}

	public HashMap<String, Nyquence> cloneNv() {
		HashMap<String, Nyquence> nv = new HashMap<String, Nyquence>(4);
		for (String n : trb.nyquvect.keySet())
			nv.put(n, new Nyquence(trb.nyquvect.get(n).n));
		return nv;
	}

	public void doc(int count, String... synids) throws TransException, SQLException {
		Query q = trb.select(phm.tbl).col(count(), "c");
		if (!isNull(synids))
			q.whereIn(phm.synuid, synids);

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
		return buf_change(count, crud, trb.synode(), eid, phm);
	}

	public long change_photolog(int count, String crud, String eid) throws TransException, SQLException {
		return change_log(count, crud, trb.synode(), eid, phm);
	}

	public long change_photolog(int count, String crud, String synoder, String eid) throws TransException, SQLException {
		return change_log(count, crud, synoder, eid, phm);
	}

	public long change_log(int count, String crud, String synoder, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		Query q = trb
				.select(DBSyntableTest.chm.tbl, "ch")
				.cols((Object[])DBSyntableTest.chm.insertCols())
				.whereEq(DBSyntableTest.chm.domain, domain)
				.whereEq(DBSyntableTest.chm.entbl, entm.tbl);
			if (synoder != null)
				q.whereEq(DBSyntableTest.chm.synoder, synoder);
			if (eid != null)
				q.whereEq(DBSyntableTest.chm.uids, SynChangeMeta.uids(synoder, eid));

			AnResultset chg = (AnResultset) q
					.rs(trb.instancontxt(connId(), robot()))
					.rs(0);
			
			if (!chg.next() && count > 0)
				fail(String.format("Expecting count == %d, but is actual 0", count));

			assertEquals(count, chg.getRowCount());
			if (count > 0) {
				assertEquals(crud, chg.getString(DBSyntableTest.chm.crud));
				assertEquals(entm.tbl, chg.getString(DBSyntableTest.chm.entbl));
				assertEquals(synoder, chg.getString(DBSyntableTest.chm.synoder));
				return chg.getLong(DBSyntableTest.chm.nyquence);
			}
			return 0;
	}

	public long buf_change(int count, String crud, String synoder, String eid, SyntityMeta entm)
			throws TransException, SQLException {
		Query q = trb
			.select(DBSyntableTest.chm.tbl, "ch")
			.je_(DBSyntableTest.xbm.tbl, "xb", DBSyntableTest.chm.pk, DBSyntableTest.xbm.changeId, DBSyntableTest.chm.synoder, DBSyntableTest.xbm.peer)
			.cols((Object[])DBSyntableTest.chm.insertCols())
			.whereEq(DBSyntableTest.chm.domain, domain)
			.whereEq(DBSyntableTest.chm.entbl, entm.tbl);
		if (synoder != null)
			q.whereEq(DBSyntableTest.chm.synoder, synoder);
		if (eid != null)
			q.whereEq(DBSyntableTest.chm.uids, SynChangeMeta.uids(synoder, eid));

		AnResultset chg = (AnResultset) q
				.rs(trb.instancontxt(connId(), robot()))
				.rs(0);
		
		if (!chg.next() && count > 0)
			fail(String.format("Expecting count == %d, but is actual 0", count));

		assertEquals(count, chg.getRowCount());
		if (count > 0) {
			assertEquals(crud, chg.getString(DBSyntableTest.chm.crud));
			assertEquals(entm.tbl, chg.getString(DBSyntableTest.chm.entbl));
			assertEquals(synoder, chg.getString(DBSyntableTest.chm.synoder));
			return chg.getLong(DBSyntableTest.chm.nyquence);
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
			.select(DBSyntableTest.chm.tbl, "ch")
			.cols((Object[])DBSyntableTest.chm.insertCols())
			.whereEq(DBSyntableTest.chm.domain, domain)
			.whereEq(DBSyntableTest.chm.entbl, entm.tbl);
		if (synoder != null)
			q.whereEq(DBSyntableTest.chm.synoder, synoder);
		if (eid != null)
			q.whereEq(DBSyntableTest.chm.uids, SynChangeMeta.uids(synoder, eid));

		AnResultset chg = (AnResultset) q
				.rs(trb.instancontxt(connId(), robot()))
				.rs(0);
		
		if (!chg.next() && count > 0)
			fail(String.format("Expecting count == %d, but is actual 0", count));

		assertEquals(count, chg.getRowCount());
		if (count > 0) {
			assertEquals(crud, chg.getString(DBSyntableTest.chm.crud));
			assertEquals(entm.tbl, chg.getString(DBSyntableTest.chm.entbl));
			assertEquals(synoder, chg.getString(DBSyntableTest.chm.synoder));
			return chg.getLong(DBSyntableTest.chm.nyquence);
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
				toIds.add(DBSyntableTest.ck[n].trb.synode());
		subsCount(phm, subcount, chgid, toIds.toArray(new String[0]));
	}

	public void synsubs(int subcount, String uids, int ... sub) throws SQLException, TransException {
		ArrayList<String> toIds = new ArrayList<String>();
		for (int n : sub)
			if (n >= 0)
				toIds.add(DBSyntableTest.ck[n].trb.synode());

		// subsCount(synm, subcount, chgId, toIds.toArray(new String[0]));
			int cnt = 0;
			// AnResultset subs = trb.subscribes(connId(), domain, uids, entm, robot());
			AnResultset subs = (AnResultset) trb
					.select(DBSyntableTest.chm.tbl, "ch")
					.je_(DBSyntableTest.sbm.tbl, "sb", DBSyntableTest.chm.pk, DBSyntableTest.sbm.changeId)
					.cols_byAlias("sb", DBSyntableTest.sbm.cols())
					.whereEq(DBSyntableTest.chm.uids, uids)
					.rs(trb.instancontxt(connId(), robot()))
					.rs(0);
					;

			subs.beforeFirst();
			while (subs.next()) {
				if (indexOf(toIds.toArray(new String[0]), subs.getString(DBSyntableTest.sbm.synodee)) >= 0)
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
				if (indexOf(toIds, subs.getString(DBSyntableTest.sbm.synodee)) >= 0)
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
		Query q = trb.select(DBSyntableTest.sbm.tbl, "ch")
				.cols((Object[])DBSyntableTest.sbm.cols())
				;
		if (!isblank(chgId))
			q.whereEq(DBSyntableTest.sbm.changeId, chgId) ;

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
			.where(new Predicate(op.eq, compound(DBSyntableTest.chm.uids), compoundVal(synoder, clientpath)))
			;
	}
}