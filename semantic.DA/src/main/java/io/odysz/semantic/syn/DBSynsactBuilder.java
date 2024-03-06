package io.odysz.semantic.syn;

import static io.odysz.transact.sql.parts.condition.Funcall.add;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.removele;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.meta.NyquenceMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.x.TransException;

/**
 * Sql statement builder for {@link DBSyntext} for handling database synchronization. 
 * 
 * @author Ody
 *
 */
public class DBSynsactBuilder extends DATranscxt {
	public static class SynmanticsMap extends SemanticsMap {
		public SynmanticsMap(String conn) {
			super(conn);
		}

		@Override
		public DASemantics createSemantics(Transcxt trb, String tabl, String pk, boolean debug) {
			return new DBSynmantics(trb, tabl, pk, debug);
		}
	}

	protected SynodeMeta synm;
	protected NyquenceMeta nyqm;
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;

	protected String synode;
	/** Nyquence vector [{synode, n0}]*/
	protected HashMap<String, Nyquence> nyquect;
	protected Nyquence n0;
	public final IUser synrobot;

	public DBSynsactBuilder(String conn, String synodeId)
			throws SQLException, SAXException, IOException, TransException {
		this(conn, synodeId,
			new SynSubsMeta(conn),
			new SynChangeMeta(conn),
			new NyquenceMeta(conn));
	}
	
	public DBSynsactBuilder(String conn, String synodeId,
			SynSubsMeta subm, SynChangeMeta chgm, NyquenceMeta nyqm)
			throws SQLException, SAXException, IOException, TransException {
		super ( new DBSyntext(conn,
			    initConfigs(conn, loadSemantics(conn),
						(c) -> new SynmanticsMap(c)),
				(IUser) new SyncRobot("rob-" + synodeId, synodeId), runtimepath));

		this.synode = synodeId;
		this.synrobot = new SyncRobot("rob-" + synodeId, synodeId);

		this.subm = subm != null ? subm : new SynSubsMeta(conn);
		this.chgm = chgm != null ? chgm : new SynChangeMeta(conn);
		this.nyqm = nyqm != null ? nyqm : new NyquenceMeta(conn);
		
		AnResultset rs = ((AnResultset) select(nyqm.tbl)
				.rs(instancontxt(conn, synrobot))
				.rs(0))
				.nxt();
		
		this.nyquect = toNyquvect(rs);
		this.n0 = nyquect.get(synode);
	}
	
	/**
	 * Create a basic sync-builder, without semantics.
	 * 
	 * @param tsx
	 * @throws SemanticException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws SQLException 
	public DBSynsactBuilder(Transcxt tsx) throws SemanticException, SQLException, SAXException, IOException {
		super(tsx.basictx().connId());
		this.synrobot = new SyncRobot("rob-" + synodeId, synodeId);
	}
	 */


	@Override
	public ISemantext instancontxt(String conn, IUser usr) throws TransException {
		try {
			return new DBSyntext(conn,
				initConfigs(conn, loadSemantics(conn),
						(c) -> new SynmanticsMap(c)),
				usr, runtimepath);
		} catch (SAXException | IOException | SQLException e) {
			e.printStackTrace();
			throw new TransException(e.getMessage());
		}
	}

	/**
	 * Get DB record change's subscriptions.
	 * 
	 * @param conn
	 * @param uids
	 * @param entm
	 * @param robot
	 * @return results with count's field named as 'cnt'
	 * @throws TransException
	 * @throws SQLException
	 */
	public AnResultset subscripts(String conn, String uids, SyntityMeta entm, IUser robot)
			throws TransException, SQLException {
		return (AnResultset) select(subm.tbl, "ch")
				.cols(subm.cols())
				.col(count(subm.subs), "cnt")
				.whereEq(subm.entbl, entm.tbl)
				.whereEq(subm.uids, uids)
				.rs(instancontxt(conn, robot))
				.rs(0);
	}
	

	public Nyquence nyquence(String conn, String org, String synid, String entity)
			throws SQLException, TransException {
		return new Nyquence(((AnResultset) select(nyqm.tbl)
				.col(nyqm.nyquence, "n")
				.whereEq(nyqm.entbl, entity)
				.whereEq(nyqm.org(), org)
				.whereEq(nyqm.synode, synid)
				.rs(instancontxt(conn, dummy))
				.rs(0))
				.nxt()
				.getInt("n"));
	}

	/**
	 * nyquence += inc;<br>
	 * inc = 0;
	 * 
	 * @param conn
	 * @param synid
	 * @param entity
	 * @param usr
	 * @return affected row count
	 * @throws TransException
	 * @throws SQLException
	 */
	public int incNyquence(String conn, String synid, String entity, IUser usr)
			throws TransException, SQLException {
		return update(nyqm.tbl, usr)
			.nv(nyqm.nyquence, select(nyqm.tbl).col(add(nyqm.nyquence, nyqm.inc))) // FIXME what happens when overflow?
			.nv(nyqm.inc, 0)
			.whereEq(nyqm.entbl, entity)
			.whereEq(nyqm.org(), usr.orgId())
			.whereEq(nyqm.synode, synid)
			.u(instancontxt(conn, usr))
			.total()
			;
	}

	public void addSynode(String conn, Synode node, IUser robot)
			throws TransException, SQLException {
		node.insert(synm, insert(synm.tbl, robot))
			.ins(this.instancontxt(conn, robot));
	}

	public SynEntity loadEntity(String eid, String conn, IUser usr, SyntityMeta phm)
			throws TransException, SQLException {
		AnResultset ent = (AnResultset)select(phm.tbl, "ch")
				.whereEq(phm.pk, eid)
				.rs(instancontxt(conn, usr))
				.rs(0);

		AnResultset subs = (AnResultset)select(chgm.tbl, "ch")
				.je("ch", subm.tbl, "sb", chgm.uids, subm.uids, chgm.org, subm.org)
				.whereEq("ch", chgm.entbl, phm.tbl)
				.whereEq("sb", subm.entbl, phm.tbl)
				.whereEq(chgm.uids, synode + chgm.UIDsep + eid)
				.rs(instancontxt(conn, usr))
				.rs(0);

		SynEntity entA = new SynEntity(ent, phm, chgm, subm);
		return entA.format(subs);
	}

	public AnResultset entities(SyntityMeta phm, String connId, IUser usr)
			throws TransException, SQLException {
		return (AnResultset)select(phm.tbl, "ch")
				.rs(instancontxt(connId, usr))
				.rs(0);
	}

	/**
	 * Collect Nyquense vector.
	 * @param phm
	 * @param synode
	 * @param connId
	 * @param robot
	 * @return e.g. Nyguenses
	 */
	public AnResultset tobegin(SyntityMeta phm, String synode, String connId, IUser robot) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * FIXME not correct if
	 * 
	 * Check change differences,
	 * e.g, with dst.n, check is there any changes for dst, with dst.n. 
	 * 
	 * @return true if has changes
	 * @throws SQLException 
	 * @throws TransException 
	private boolean shouldExchange(SynChangeMeta chm, String conn, IUser robot)
			throws TransException, SQLException {
		AnResultset chs = ((AnResultset) select(chm.tbl, "ch")
				.col(String.format("count(%s)", chm.nyquence), "cnt")
				.rs(instancontxt(conn, robot))
				.rs(0)).nxt();
		
		return chs.getInt("cnt") > 0;
	}
	 */

	/**
	 * Client/slave initiate a change logs exchange
	 * @return change logs
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public AnResultset toexchange(IUser robot, String destnode)
			throws TransException, SQLException {

		AnResultset chs = (AnResultset)select(chgm.tbl, "ch")
				.whereEq(chgm.synoder, destnode)
				.rs(instancontxt(this.getSysConnId(), robot))
				.rs(0);

		return chs;
	}

	/**
	 * Server/hub handle a change-logs' exchange
	 * 
	 * @param srcnode source node id
	 * @param sn source Nyquence
	 * @param srchgs change logs
	 * @param localbuf [in/out] local commitment buffer 
	 * @return change logs to reply
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public ChangeLogs mergeChange(String srcnode, Nyquence sn, AnResultset srchgs, IUser robot, List<ChangeLogs> localbuf)
			throws TransException, SQLException {
		
		if (isNull(srchgs)) return null;

		srchgs.beforeFirst();
		// long sn0 = srchgs.hasnext() ? srchgs.getLong(chgm.nyquence) : 0;
		long sn1 = srchgs.getLongAtRow(chgm.nyquence, srchgs.getRowCount() - 1);
		
		// select order by n, s
		AnResultset dchgs = (AnResultset) select(chgm.tbl)
				.rs(instancontxt(this.basictx.connId(), robot))
				.rs(0);
		
		ChangeLogs localog = new ChangeLogs(chgm);
		ChangeLogs remolog = new ChangeLogs(chgm);
		
		long srcn1 = nyquect.get(srcnode).n;
		// in case unfinished previous synchronizing
		// i.e. B haven't got last acknowledge from A since buffer is not empty 
		while (localbuf.size() > 0) {
			if (Nyquence.compareNyq(srchgs.getLong(chgm.nyquence), srcn1) > 0)
				commitUntil(localbuf, srcnode, srcn1);
			// i.e. B has operation haven't committed yet since A failed acknowledge
			else
				ignoreUntil(localbuf, srcnode, srcn1);
		}
		srchgs.beforeFirst();

		boolean hasmore = dchgs.next() && srchgs.next();
		while (hasmore) {
			// src - dst
			int diff = compare(srchgs, dchgs);
			// int diff = Nyquence.compareNyq(srchgs.getLong(chgm.nyquence), dchgs.getLong(chgm.nyquence));
			if (diff == 0) {
				// changes propagated to both sides through 3rd parties.
				if (indexOf(srchgs.getString(chgm.subs), this.synode) >= 0) {
					// insertion propagation
					remolog.remove_sub(srchgs, this.synode); // e.g. "I A A:0 1 B"
				}
				// e.g. I A A:0 1 C/D
			}
			else if (diff < -1) {
				// delete propagation
				remolog.remove(srchgs);
			}
			else if (diff < 0) {
				// e.g. I A A:0 1 is missing at B
				localog.append(srchgs);
				hasmore = srchgs.next();
			}
			else if (diff <= 1) {
				// e.g. I B B:0 1 is missing at A
				remolog.append(dchgs);
				hasmore = dchgs.next();
			}
			else // diff == 2
				localog.remove_sub(dchgs, this.synode);

			hasmore = srchgs.next() && dchgs.next();
		}

		while(srchgs.hasnext()) {
			int diff = compare(srchgs, this.n0);
			if (diff >= 0)
				throw new SemanticException("Shouldn't be here");
			else if (diff == -1)
				localog.append(srchgs);
			else // diff == -2
				remolog.remove(srchgs);

			srchgs.next();
		}

		while(dchgs.hasnext()) {
			remolog.append(dchgs);

			int diff = compare(sn1, dchgs);
			if (diff == 0)
				break;
			else if (diff < 0)
				throw new SemanticException("Shouldn't be here");
			else if (diff == -1)
				remolog.append(srchgs);
			else // diff == -2
				remolog.remove(srchgs);

			localog.append(srchgs);
			dchgs.next();
		}
		
		// FIXME what happen if local committed and remote lost response?
		// commit(localog);
		localbuf.add(localog);

		return remolog;
	}

	int compare(long n, AnResultset chgs) throws SQLException {
		long dn = chgs.getLong(chgm.nyquence);
		int diff = Nyquence.compareNyq(n, dn);
		if (diff == 0)
			return 0;
		
		return (int) Math.min(2, Math.max(-2, (n - dn)));
	}

	int compare(AnResultset chgs, Nyquence n) throws SQLException {
		return - compare(n.n, chgs);
	}

	/**
	 * Compare source and destination record, using both current row.
	 * @param a initiator
	 * @param b acknowledger
	 * @return
	 * -2: ack to ini deletion propagation,
	 * -1: ack to ini appending,
	 * 0: needing to be merged (same record),
	 * 1: ini to ack appending
	 * 2: ini to ack deletion propagation
	 * @throws SQLException 
	 */
	int compare(AnResultset a, AnResultset b) throws SQLException {
		long sn = a.getLong(chgm.nyquence);
		return compare(sn, b);
	}

	/**
	 * 
	 * @param commitBuff
	 * @param srcnode
	 * @param srcn1
	 * @return current log index with current row index of first in-ignorable. If all ignored, return the size
	 * @throws SQLException
	 */
	int ignoreUntil(List<ChangeLogs> commitBuff, String srcnode, long srcn1) throws SQLException {
		// FIXME, binary search?
		for (int lx = 0; lx < commitBuff.size(); lx++) {
			ChangeLogs log = commitBuff.get(lx);
			AnResultset rs = log.rs();
			rs.beforeFirst();
			while (rs.next() && rs.getLong(0) > srcn1)
				;
			if (rs.hasnext()) 
				return lx;
		}
		return commitBuff.size();
	}

	DBSynsactBuilder commitUntil(List<ChangeLogs> commitBuff, String srcnode, long untilN0) throws SQLException, TransException {
		List<Statement<?>> stats = new ArrayList<Statement<?>>();
		for (ChangeLogs log : commitBuff) {
			AnResultset c = log.rs();
			while (c.next()) {
				if (Nyquence.compareNyq(c.getLong(chgm.nyquence), untilN0) <= 0)
					break;
				String crud = c.getString(chgm.crud);
				SyntityMeta entm = getEntityMeta(chgm.entbl);
				stats.add(eq(crud, CRUD.C)
					/* create an entity, and trigger change log
					? insert(chgm.tbl)
						.nv(chgm.crud, CRUD.C)
						.nv(chgm.synoder, c.getString(chgm.synoder))
						.nv(chgm.uids, c.getString(chgm.uids))
						.post(insert(c.getString(chgm.entbl))
							.cols("")
							.value(""))
					*/
					? insert(entm.tbl, synrobot)
						.cols(entm.insertCols())
						.values(entm.insertVal(log))
						.post(insert(subm.tbl)
								.cols(subm.insertCols())
								.values(subm.insertVals(log)))
					: eq(crud, CRUD.U) // remove subscribes
					? update(chgm.tbl)
						.nv(chgm.subs, removele(c.getString(chgm.synoder), c.getString(chgm.nyquence)))
						.whereEq(chgm.synoder, c.getString(chgm.synoder))
						.whereEq(chgm.uids, c.getString(chgm.uids))
					: delete(chgm.tbl) // backward change log deletion propagation
						.whereEq(chgm.synoder, c.getString(chgm.synoder))
						.whereEq(chgm.uids, c.getString(chgm.uids)));
			}
		}
		Utils.logi("[DBSynsactBuilder.commitUntil()]");
		Utils.warn("Needing a Semantext which won't trigger semantics handling ...");
		Utils.logi(stats);
		return this;
	}

	private SyntityMeta getEntityMeta(String entbl) {
		// TODO Auto-generated method stub
		return null;
	}

	public static HashMap<String, Nyquence> toNyquvect(AnResultset schgs) {
		return null;
	}

	/**
	 * Source node acknowledge destionation's response.
	 * @param resp
	 * @return acknowledge, answer to the destination node's commitment, and have be committed at source.
	 * @throws SQLException 
	 */
	public ChangeLogs ackExchange(ChangeLogs resp) throws SQLException {
		ChangeLogs logs = new ChangeLogs(chgm);
		if (resp != null && resp.rs().getRowCount() > 0) {
			AnResultset changes = resp.rs();
			while (changes.next()) {
				
			}
		}
		return logs;
	}

	public void onAck(ChangeLogs resp, List<ChangeLogs> commitbuf) {
		// TODO Auto-generated method stub
		
	}

	public Nyquence closexchange() {
		// TODO Auto-generated method stub
		return null;
	}

	public void onClosexchange(String synode2, Nyquence n) {
		// TODO Auto-generated method stub
		
	}
}
