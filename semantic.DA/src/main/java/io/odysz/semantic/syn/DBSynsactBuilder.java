package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.semantic.syn.Nyquence.max;
import static io.odysz.semantic.util.DAHelper.loadRecNyquence;
import static io.odysz.semantic.util.DAHelper.loadRecString;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
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
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.condition.Funcall;
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
	protected Nyquence n0() { return nyquvect.get(synode()); }
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
		tx.domain = loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain);
		((SyncRobot)tx.usr()).orgId = loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain);

		// this.synrobot = ((DBSyntext)this.basictx).usr();
		// String uid = "rob-" + synodeId;
		// this.synrobot = new SyncRobot("rob-" + synodeId, synodeId)
				// .orgId(DAHelper.loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain))
				;

		this.subm = subm != null ? subm : new SynSubsMeta(conn);
		this.chgm = chgm != null ? chgm : new SynChangeMeta(conn);
		this.synm = synm != null ? synm : new SynodeMeta(conn);
		
		this.commitbuf = new ChangeLogs(chgm);
	}
	
	DBSynsactBuilder loadNyquvect0(String conn) throws SQLException, TransException {
		AnResultset rs = ((AnResultset) select(synm.tbl)
				.cols(synm.pk, synm.nyquence)
				.rs(instancontxt(conn, synrobot()))
				.rs(0));
		
		nyquvect = new HashMap<String, Nyquence>(rs.getRowCount());
		while (rs.next()) {
			nyquvect.put(rs.getString(synm.synoder), new Nyquence(rs.getLong(synm.nyquence)));
		}
		
		return this;
	}

	public DBSynsactBuilder incNyquence() throws TransException, SQLException {
		update(synm.tbl, synrobot())
			.nv(synm.nyquence, Funcall.add(synm.nyquence, 1))
			.whereEq(synm.pk, synode())
			.u(instancontxt(basictx.connId(), synrobot()));
		
		// nyquvect.get(synode()).inc();
		nyquvect.put(synode(), loadRecNyquence(this, basictx.connId(), synm, synode(), synm.nyquence));
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
	public AnResultset iniExchange(String target, String connId, IUser robot)
			throws TransException, SQLException {
		
		Nyquence dn = this.nyquvect.get(target);
		return (AnResultset) select(chgm.tbl, "ch")
			.je("ch", subm.tbl, "sb", chgm.entbl, subm.entbl, chgm.uids, subm.uids)
			.where(op.ge, chgm.nyquence, dn.n)
			.orderby(chgm.nyquence, chgm.synoder)
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
	 * @param srcn0 source Nyquence from remote client
	 * @param srchgs change logs
	 * @param localbuf [in/out] local commitment buffer 
	 * @return change logs to reply
	 * @throws SQLException 
	 * @throws TransException 
	 */
	ChangeLogs mergeChange(String srcnode, Nyquence srcn0, AnResultset srchgs,
			IUser robot, List<ChangeLogs> localbuf) throws TransException, SQLException {
		
		if (isNull(srchgs)) return null;

		srchgs.beforeFirst();
		long sn0 = srchgs.getLongAt(chgm.nyquence, srchgs.getRowCount() - 1);
		
		AnResultset dchgs = (AnResultset) select(chgm.tbl, "ch")
				.je("ch", subm.tbl, "sb", chgm.entbl, subm.entbl, chgm.uids, subm.uids)
				.where(op.ge, chgm.nyquence, nyquvect.get(srcnode).n)
				.orderby(chgm.nyquence)
				.orderby(chgm.synoder)
				.rs(instancontxt(this.basictx.connId(), robot))
				.rs(0);
		
		ChangeLogs localog = new ChangeLogs(chgm);
		ChangeLogs remolog = new ChangeLogs(chgm);
		
		long mysrcn0 = nyquvect.get(srcnode).n;

		// in case unfinished previous synchronizing
		// i.e. B haven't got last acknowledge from A since buffer is not empty 
		while (localbuf.size() > 0) {
			if (Nyquence.compareNyq(srchgs.getLong(chgm.nyquence), mysrcn0) > 0)
				commitTill(localbuf, srcnode, mysrcn0);
			// i.e. B has operation haven't committed yet since A failed acknowledge
			else // if (Nyquence.compareNyq(srchgs.getLong(chgm.nyquence), mysrcn0) < 0)
				ignoreUntil(localbuf, srcnode, mysrcn0);
		}
		srchgs.beforeFirst();

		long maxn = max(mysrcn0, sn0);
		
		while(srchgs.next()) {
			if (compare(srchgs.getLong(chgm.nyquence), mysrcn0) <= -2)
				remolog.remove(dchgs);

			maxn = max(srchgs.getLong(chgm.nyquence), maxn);

			if (!eq(srchgs.getString(chgm.uids), srchgs.getString(chgm.uids)))
				throw new SemanticException("Shouldn't be here");
			else // diff == -2
				remolog.remove(srchgs);

			srchgs.next();
		}

		while(dchgs.hasnext()) {
			maxn = max(dchgs.getLong(chgm.nyquence), maxn);

			remolog.append(dchgs);

			int diff = compare(sn0, dchgs.getLong(chgm.nyquence));
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

		maxn = max(srcn0.n, maxn);

		// FIXME what happen if local committed and remote lost response?
		// commit(localog);
		// localbuf.add(localog.maxn(maxn));
		// return remolog.maxn(maxn);
		return remolog;
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
	int compare(long srcn, long dstn) throws SQLException {
		// long dn = chgs.getLong(chgm.nyquence);
		int diff = Nyquence.compareNyq(srcn, dstn);
		if (diff == 0)
			return 0;
		
		return (int) Math.min(2, Math.max(-2, (srcn - dstn)));
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
			AnResultset rs = log.answers();
			rs.beforeFirst();
			while (rs.next() && rs.getLong(0) > srcn1)
				;
			if (rs.hasnext()) 
				return lx;
		}
		return commitBuff.size();
	}

	DBSynsactBuilder commitTill(List<ChangeLogs> commitBuff, String srcnode, long tillN)
			throws SQLException, TransException {

		List<Statement<?>> stats = new ArrayList<Statement<?>>();
		for (ChangeLogs log : commitBuff) {
			AnResultset c = log.answers();
			String entid = null;
			while (c.next()) {
				if (Nyquence.compareNyq(c.getLong(chgm.nyquence), tillN) > 0)
					break;

				String change = c.getString(ChangeLogs.ChangeFlag);
				SyntityMeta entm = getEntityMeta(c.getString(chgm.entbl));
				// SynEntity currentity = log.entity(entm.tbl, entid);

				stats.add(eq(change, CRUD.C)
					// create an entity, and trigger change log
					? !eq(entid, c.getString(chgm.entfk)) ?
						insert(entm.tbl, synrobot())
							.cols(entm.entCols())
							.value(entm.insertChallengeEnt(entid, log.entities.get(entm.tbl)))
							.post(insert(chgm.tbl) // TODO implement semantics handler
								.nv(chgm.crud, CRUD.C)
								.nv(chgm.org, c.getString(chgm.org))
								.nv(chgm.entbl, c.getString(chgm.entbl))
								.nv(chgm.synoder, c.getString(chgm.synoder))
								.nv(chgm.uids, c.getString(chgm.uids))
								.nv(chgm.entfk, c.getString(chgm.entfk))
								.post(insert(subm.tbl)
									.cols(subm.insertCols())
									.value(subm.insertSubVal(c))))
						: insert(subm.tbl)
							.cols(subm.insertCols())
							.value(subm.insertSubVal(c))

					// : eq(change, CRUD.U) ?// remove subscribes
					// remove subscribers & backward change logs's deletion propagation
					: delete(subm.tbl, synrobot())
						.whereEq(subm.entbl, entm.tbl)
						.whereEq(subm.synodee, c.getString(subm.synodee))
						.whereEq(subm.uids, c.getString(chgm.uids))
						.post(delete(chgm.tbl) // delete change log if no subscribers exist
							.whereEq(chgm.entbl, entm.tbl)
							.whereEq(chgm.org, c.getString(chgm.org))
							.whereEq(chgm.synoder, c.getString(chgm.synoder))
							.whereEq(chgm.uids,    c.getString(chgm.uids))
							.whereEq("0", (Query)select(subm.tbl)
								.col(count(subm.synodee))
								.whereEq(subm.org, c.getString(chgm.org))
								.whereEq(subm.entbl, entm.tbl)
								.where(op.ne, subm.synodee, constr(c.getString(subm.synodee)))
								.whereEq(subm.uids,  c.getString(chgm.uids)))));

					// backward change logs's deletion propagation
//					: delete(subm.tbl, synrobot())
//						.whereEq(subm.entbl, entm.tbl)
//						.whereEq(subm.synodee, c.getString(subm.synodee))
//						.whereEq(subm.uids, c.getString(chgm.uids))
//						.post(delete(chgm.tbl)
//							.whereEq(chgm.entbl, entm.tbl)
//							.whereEq(chgm.org, c.getString(chgm.org))
//							.whereEq(chgm.synoder, c.getString(chgm.synoder))
//							.whereEq(chgm.uids, c.getString(chgm.uids))));

				entid = c.getString(chgm.entfk);
			}
		}
		Utils.logi("[DBSynsactBuilder.commitUntil()] TODO update entities...");
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			s.commit(sqls, synrobot());
		Connects.commit(basictx.connId(), synrobot(), sqls);
		return this;
	}

	public SyntityMeta getEntityMeta(String entbl) throws SemanticException {
		if (entityRegists == null || !entityRegists.containsKey(entbl))
			throw new SemanticException("Register %s first.");
			
		return entityRegists.get(entbl);
	}
	
	public DBSynsactBuilder registerEntity(String conn, SyntityMeta m)
			throws SemanticException, TransException, SQLException {
		if (entityRegists == null)
			entityRegists = new HashMap<String, SyntityMeta>();
		entityRegists.put(m.tbl, (SyntityMeta) m.clone(Connects.getMeta(conn, m.tbl)));
		return this;
	}

	/**
	 * Client node acknowledge destionation's response (from server),
	 * i.e. check answers up to n = my-n0
	 * @param answer
	 * @param srvnode 
	 * @return acknowledge, answer to the destination node's commitment, and have be committed at source.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	@SuppressWarnings("serial")
	public ChangeLogs ackExchange(ChangeLogs answer, String srvnode) throws SQLException, TransException, IOException {
		ChangeLogs logs = new ChangeLogs(chgm)
				.setChangeCols(answer)
				.nyquvect(nyquvect);

		if (answer != null && answer.answers != null && answer.answers.size() > 0) {
			commitTill(new ArrayList<ChangeLogs>() {{add(answer);}}, srvnode, n0().n);
		}
		return logs;
	}

	/**
	 * Commit suspended statements as source node confirmed the merges.
	 * @param resp
	 * @throws TransException 
	 * @throws SQLException 
	 */
	@SuppressWarnings("serial")
	public void onAck(ChangeLogs resp) throws SQLException, TransException {
		ChangeLogs logs = new ChangeLogs(chgm)
				.nyquvect(nyquvect);

		if (resp != null && Nyquence.compareNyq(resp.nyquvect.get(synode()), nyquvect.get(synode())) <= 0) {
			commitTill(new ArrayList<ChangeLogs>() {{add(logs);}},
					this.synode(), nyquvect.get(synode()).n);
		}
	}

	/**
	 * this.n0++, this.n0 = max(n0, maxn)
	 * @param maxn
	 * @return n0
	 */
	public Nyquence incN0(long maxn) {
		n0().inc(maxn);
		return n0();
	}
	public Nyquence incN0(Nyquence n) {
		return incN0(n.n);
	}

	ChangeLogs commitbuf;
	/**
	 * Delete local changes where change.n <= rv[local] && change[sub].n < rv[sub].
	 * Clear statements buffer waiting for ack.
	 */
	public void clean() {
		// delete local changes where change.n <= rv[remote] && change.n < rv[change.sub]
		commitbuf.clear();
	}

	/** @deprecated not used
	public boolean exbegin(String synodee, HashMap<String, Nyquence> rv) throws SQLException, TransException {
		
		// select n in range (src.b, src.a = x]
		AnResultset chgs = ((AnResultset) select(chgm.tbl, "ch")
				.col(count(chgm.nyquence), "cnt")
				.where(op.gt, chgm.nyquence, nyquvect.get(synodee).n)
				.where(op.le, chgm.nyquence, n0().n) // not needed?
				.rs(instancontxt(basictx.connId(), synrobot()))
				.rs(0)).nxt();

		return chgs.getInt("cnt") > 0;	
	} */

	/**
	 * Find if there are change logs such that chg.n &ge; remote n, to be exchanged.
	 * @param <T>
	 * 
	 * @param target exchange target (server)
	 * @param nyv nyquevect from target 
	 * @return logs such that chg.n > nyv[target], i.e there are change logs to be exchanged.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public <T extends SynEntity> ChangeLogs diffrom(String target, SyntityMeta entm) throws TransException, SQLException {
		Nyquence dn = this.nyquvect.get(target);
		AnResultset challenge = (AnResultset) select(chgm.tbl, "ch")
			.je("ch", subm.tbl, "sb", chgm.entbl, subm.entbl, chgm.uids, subm.uids) // FIXME line 1:7 mismatched input '<EOF>' expecting '.'
			.cols("ch.*", subm.synodee)
			.where(op.gt, chgm.nyquence, dn.n)
			.orderby(chgm.entbl)
			.orderby(chgm.nyquence)
			.orderby(chgm.synoder)
			.orderby(subm.synodee)
			.rs(instancontxt(basictx.connId(), synrobot()))
			.rs(0);

		AnResultset entbls = (AnResultset) select(chgm.tbl, "ch")
			.col(chgm.entbl)
			.where(op.gt, chgm.nyquence, dn.n)
			.groupby(chgm.entbl)
			.rs(instancontxt(basictx.connId(), synrobot()))
			.rs(0);

		ChangeLogs diff = new ChangeLogs(chgm).challenge(challenge);
		while (entbls.next()) {
			String tbl = entbls.getString(chgm.entbl);

			AnResultset entities = ((AnResultset) select(tbl, "e")
				.je("e", chgm.tbl, "ch", "ch." + chgm.entbl, constr(tbl), entm.pk, chgm.entfk)
				.cols_byAlias("e", entm.entCols())
				.where(op.gt, chgm.nyquence, dn.n)
				.orderby(chgm.nyquence)
				.orderby(chgm.synoder)
				.rs(instancontxt(basictx.connId(), synrobot()))
				.rs(0))
				.index0(entm.pk);
				// .map(entm.pk, lambda);
			
			diff.entities(tbl, entities);
		}

		return diff;
	}
	
	public <T extends SynEntity> ChangeLogs onExhange(String from, HashMap<String, Nyquence> remotv,
			ChangeLogs req, SyntityMeta entm) throws SQLException, TransException {

		ChangeLogs resp = diffrom(from, entm);

		while (req != null && req.challenge != null && req.challenge.next()) {
			String subscribe = req.challenge.getString(subm.synodee);
			if (Nyquence.compareNyq(req.challenge.getLong(chgm.nyquence), nyquvect.get(subscribe).n) < 0)
				// knowledge about the sub from req is older than this node's knowledge 
				resp.remove_sub(req.challenge, subscribe);	
			else if (eq(subscribe, synode())) {
				resp.remove_sub(req.challenge, synode());	
				commitbuf.append(req.challenge);
			}
		}
		return resp;
	}
}
