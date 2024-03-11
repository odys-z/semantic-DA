package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

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
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.parts.Logic.op;
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
	// protected NyquenceMeta nyqm;
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;

	protected String synode() { return ((DBSyntext)this.basictx).synode; }

	/** Nyquence vector [{synode, n0}]*/
	protected HashMap<String, Nyquence> nyquvect;
	protected Nyquence n0;
	public IUser synrobot() { return ((DBSyntext) this.basictx).usr(); }

	private HashMap<String, SyntityMeta> entityRegists;

	public DBSynsactBuilder(String conn, String synodeId)
			throws SQLException, SAXException, IOException, TransException {
		this(conn, synodeId,
			new SynSubsMeta(conn),
			new SynChangeMeta(conn),
			new SynodeMeta(conn));
	}
	
	public DBSynsactBuilder(String conn, String synodeId,
			SynSubsMeta subm, SynChangeMeta chgm, SynodeMeta synm)
			throws SQLException, SAXException, IOException, TransException {

		super ( new DBSyntext(conn,
			    	initConfigs(conn, loadSemantics(conn), (c) -> new SynmanticsMap(c)),
			    	(IUser) new SyncRobot("rob-" + synodeId, synodeId)
			    	, runtimepath));

		// wire up local identity
		DBSyntext tx = (DBSyntext) this.basictx;
		tx.synode = synodeId;
		tx.domain = DAHelper.loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain);
		((SyncRobot)tx.usr()).orgId = DAHelper.loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain);

		// this.synrobot = ((DBSyntext)this.basictx).usr();
		// String uid = "rob-" + synodeId;
		// this.synrobot = new SyncRobot("rob-" + synodeId, synodeId)
				// .orgId(DAHelper.loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain))
				;

		this.subm = subm != null ? subm : new SynSubsMeta(conn);
		this.chgm = chgm != null ? chgm : new SynChangeMeta(conn);
		this.synm = synm != null ? synm : new SynodeMeta(conn);
	}
	
	DBSynsactBuilder loadNyquvect0(String conn) throws SQLException, TransException {
		AnResultset rs = ((AnResultset) select(synm.tbl)
				.cols(synm.pk, synm.nyquence)
				.rs(instancontxt(conn, synrobot()))
				.rs(0));
		
		nyquvect = new HashMap<String, Nyquence>(rs.getRowCount());
		while (rs.next()) {
			nyquvect.put(rs.getString(synm.synode), new Nyquence(rs.getLong(synm.nyquence)));
		}
		
		// loadNyquvect0(conn);
		this.n0 = nyquvect.get(synode());

		return this;
	}

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
				.whereEq(subm.uids, chgm.uids(synode(), uids))
				.rs(instancontxt(conn, robot))
				.rs(0);
	}
	
	public void addSynode(String conn, Synode node, IUser robot)
			throws TransException, SQLException {
		node.insert(synm, insert(synm.tbl, robot))
			.ins(this.instancontxt(conn, robot));
	}

	public AnResultset entities(SyntityMeta phm, String connId, IUser usr)
			throws TransException, SQLException {
		return (AnResultset)select(phm.tbl, "ch")
				.rs(instancontxt(connId, usr))
				.rs(0);
	}

	/**
	 * Collect Change logs.
	 * Logs are collected across all registered entity tables, in sequence of Nyquence.
	 * @param target
	 * @param connId
	 * @param robot
	 * @return changes for request to remote node
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public AnResultset iniExchange(String target, String connId, IUser robot) throws TransException, SQLException {
		// FIXME sort all entities' change log
		// FIXME init synode change first
		
		Nyquence dn = this.nyquvect.get(target);
		return (AnResultset) select(chgm.tbl, "ch")
			.je("ch", subm.tbl, "sb", chgm.subs, subm.synodee)
			.where(op.ge, chgm.nyquence, dn.n)
			.rs(instancontxt(connId, robot))
			.rs(0);
	}

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
		
		long srcn1 = nyquvect.get(srcnode).n;
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
				if (indexOf(srchgs.getString(chgm.subs), this.synode()) >= 0) {
					// insertion propagation
					remolog.remove_sub(srchgs, this.synode()); // e.g. "I A A:0 1 B"
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
				localog.remove_sub(dchgs, this.synode());

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

	DBSynsactBuilder commitUntil(List<ChangeLogs> commitBuff, String srcnode, long untilN0)
			throws SQLException, TransException {

		List<Statement<?>> stats = new ArrayList<Statement<?>>();
		for (ChangeLogs log : commitBuff) {
			AnResultset c = log.rs();
			while (c.next()) {
				if (Nyquence.compareNyq(c.getLong(chgm.nyquence), untilN0) <= 0)
					break;
				String crud = c.getString(chgm.crud);
				SyntityMeta entm = getEntityMeta(c.getString(chgm.entbl));
				stats.add(eq(crud, CRUD.C)
					// create an entity, and trigger change log
					? insert(entm.tbl, synrobot())
						.cols(entm.insertCols())
						.values(entm.insertVal(log))
						.post(insert(chgm.tbl) // TODO implement semantics handler
							.nv(chgm.crud, CRUD.C)
							.nv(chgm.synoder, c.getString(chgm.synoder))
							.nv(chgm.uids, c.getString(chgm.uids))
							.post(insert(subm.tbl)
								.cols(subm.insertCols())
								.select(subm.subs2change(synm, this, log))))
					: eq(crud, CRUD.U) // remove subscribes
					// remove subscribers
					? delete(subm.tbl, synrobot())
						.whereEq(subm.entbl, c.getString(chgm.entbl))
						.whereEq(subm.synodee, c.getString(chgm.synoder))
						.whereEq(subm.uids, c.getString(chgm.uids))
						.post(delete(chgm.tbl) // delete change log if no subscribers exist
							.whereEq(subm.entbl,   c.getString(chgm.entbl))
							.whereEq(chgm.synoder, c.getString(chgm.synoder))
							.whereEq(chgm.uids,    c.getString(chgm.uids))
							.where(op.notin, chgm.synoder,
								select(subm.tbl)
									.col(subm.synodee)
									.whereEq(subm.synodee, c.getString(chgm.synoder)))
									.whereEq(subm.uids,  c.getString(chgm.uids)))
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

	public SyntityMeta getEntityMeta(String entbl) throws SemanticException {
		if (entityRegists == null || !entityRegists.containsKey(entbl))
			throw new SemanticException("Register %s first.");
			
		return entityRegists.get(entbl);
	}
	
	public DBSynsactBuilder registerEntity(SyntityMeta m) {
		if (entityRegists == null)
			entityRegists = new HashMap<String, SyntityMeta>();
		entityRegists.put(m.tbl, m);
		return this;
	}

	/**
	 * Source node acknowledge destionation's response.
	 * @param resp
	 * @param srcnode 
	 * @return acknowledge, answer to the destination node's commitment, and have be committed at source.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	@SuppressWarnings("serial")
	public ChangeLogs ackExchange(ChangeLogs resp, String srcnode) throws SQLException, TransException {
		ChangeLogs logs = new ChangeLogs(chgm);
		if (resp != null && resp.rs().getRowCount() > 0) {
			commitUntil(new ArrayList<ChangeLogs>() {{add(logs);}}, srcnode, this.n0.n);
		}
		return logs;
	}

	/**
	 * Commit suspended statements as source node confirmed the merges.
	 * @param resp
	 * @param commitbuf
	 * @throws TransException 
	 * @throws SQLException 
	 */
	@SuppressWarnings("serial")
	public void onAck(ChangeLogs resp, List<ChangeLogs> commitbuf) throws SQLException, TransException {
		ChangeLogs logs = new ChangeLogs(chgm);
		if (resp != null && resp.rs().getRowCount() > 0) {
			commitUntil(new ArrayList<ChangeLogs>() {{add(logs);}}, this.synode(), logs.maxn.n);
		}
	}
}
