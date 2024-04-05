package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.getn;
import static io.odysz.semantic.syn.Nyquence.maxn;
import static io.odysz.semantic.util.DAHelper.loadRecNyquence;
import static io.odysz.semantic.util.DAHelper.loadRecString;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.transact.sql.parts.condition.Funcall.concatstr;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.ExprPart;
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
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;

	protected String synode() { return ((DBSyntext)this.basictx).synode; }

	/** Nyquence vector [{synode, Nyquence}]*/
	protected HashMap<String, Nyquence> nyquvect;
	protected Nyquence n0() { return nyquvect.get(synode()); }
	protected DBSynsactBuilder n0(Nyquence nyq) {
		nyquvect.put(synode(), new Nyquence(nyq.n));
		return this;
	}

	public IUser synrobot() { return ((DBSyntext) this.basictx).usr(); }

	private HashMap<String, SyntityMeta> entityRegists;

	public DBSynsactBuilder(String conn, String synodeId)
			throws SQLException, SAXException, IOException, TransException {
		this(conn, synodeId,
			new SynSubsMeta(conn),
			new SynChangeMeta(conn),
			new SynodeMeta(conn, null));
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

		this.subm = subm != null ? subm : new SynSubsMeta(conn);
		this.subm.replace();
		this.chgm = chgm != null ? chgm : new SynChangeMeta(conn);
		this.chgm.replace();
		this.synm = synm != null ? synm : new SynodeMeta(conn, this);
		this.synm.replace().autopk(false);
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

	/**
	 * Inc my n0, then reload from DB.
	 * @return this
	 * @throws TransException
	 * @throws SQLException
	 */
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
	 * @return results with count's field named as 'cnt', see {@link SynSubsMeta#cols()}
	 * @throws TransException
	 * @throws SQLException
	 */
	public AnResultset subscribes(String conn, String org, String uids, SyntityMeta entm, IUser robot)
			throws TransException, SQLException {
		Query q = select(subm.tbl, "ch")
				.cols(subm.cols())
				.whereEq(subm.domain, org)
				.whereEq(subm.entbl, entm.tbl);

		if (uids != null)
			q.whereEq(subm.uids, uids);

		return (AnResultset) q.whereEq(subm.uids, uids)
				.rs(instancontxt(conn, robot))
				.rs(0);
	}

	/**
	 * 
	 * @param conn
	 * @param org
	 * @param uids
	 * @param entm
	 * @param robot
	 * @return see {@link #subscribes(String, String, String, SyntityMeta, IUser)
	 * @throws TransException
	 * @throws SQLException
	 */
	public AnResultset subscribes(String conn, String org, Funcall uids, SyntityMeta entm, IUser robot)
			throws TransException, SQLException {
		return (AnResultset) select(subm.tbl, "ch")
				.cols(subm.cols())
				.whereEq(subm.domain, org)
				.whereEq(subm.entbl, entm.tbl)
				.whereEq(subm.uids, uids)
				.rs(instancontxt(conn, robot))
				.rs(0);
	}

	public AnResultset entities(SyntityMeta phm, String connId, IUser usr)
			throws TransException, SQLException {
		return (AnResultset)select(phm.tbl, "ch")
				.rs(instancontxt(connId, usr))
				.rs(0);
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
		int diff = compareNyq(srcn, dstn);
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
			AnResultset rs = log.answers;
			rs.beforeFirst();
			while (rs.next() && rs.getLong(0) > srcn1)
				;
			if (rs.hasnext()) 
				return lx;
		}
		return commitBuff.size();
	}

	DBSynsactBuilder commitAnswers(ExchangeContext x, String srcnode, long tillN)
			throws SQLException, TransException {

		List<Statement<?>> stats = new ArrayList<Statement<?>>();

		AnResultset rply = x.answer.beforeFirst();
		rply.beforeFirst();
		String entid = null;
		while (rply.next()) {
			if (compareNyq(rply.getLong(chgm.nyquence), tillN) > 0)
				break;

			SyntityMeta entm = getEntityMeta(rply.getString(chgm.entbl));
			String change = rply.getString(ChangeLogs.ChangeFlag);
			HashMap<String, AnResultset> entbuf = x.mychallenge.entities;
			
			// current entity
			String entid1 = rply.getString(chgm.entfk);

			String rporg  = rply.getString(chgm.domain);
			String rpent  = rply.getString(chgm.entbl);
			String rpuids = rply.getString(chgm.uids);
			String rpnodr = rply.getString(chgm.synoder);
			String rpscrb = rply.getString(subm.synodee);

			if (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).indices0(entid1) < 0) {
				Utils.warn("[DBSynsactBuilder commitTill] Fatal error ignored: can't restore entity record answered from target node.\n"
						+ "entity name: %s\nsynode(answering): %s\nsynode(local): %s\nentity id(by answer): %s", entm.tbl, srcnode, synode(), entid1);
				continue;
			}
				
			stats.add(eq(change, CRUD.C)
				// create an entity, and trigger change log
				? !eq(entid, entid1)
					? insert(entm.tbl, synrobot())
						.cols(entm.entCols())
						.value(entm.insertChallengeEnt(entid1, entbuf.get(entm.tbl)))
						.post(insert(chgm.tbl)
							.nv(chgm.crud, CRUD.C)
							.nv(chgm.domain, rporg)
							.nv(chgm.entbl, rpent)
							.nv(chgm.synoder, rpnodr)
							.nv(chgm.uids, rpuids)
							.nv(chgm.entfk, entid1)
							.post(insert(subm.tbl)
								.cols(subm.insertCols())
								.value(subm.insertSubVal(rply))))
					: insert(subm.tbl)
						.cols(subm.insertCols())
						.value(subm.insertSubVal(rply))

				// remove subscribers & backward change logs's deletion propagation
				: delete(subm.tbl, synrobot())
					.whereEq(subm.entbl, entm.tbl)
					.whereEq(subm.synodee, rpscrb)
					.whereEq(subm.uids, rpuids)
					.post(del0subchange(entm, rporg, rpnodr, rpuids, synode())
					));
			entid = entid1;
		}

		Utils.logi("[DBSynsactBuilder.commitAnswers()] updating change logs without modifying entities...");
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			s.commit(sqls, synrobot());
		Connects.commit(basictx.connId(), synrobot(), sqls);
		
		x.answer = null;
		return this;
	}

	/**
	 * Generate delete statement when change logs don't have synodees. 
	 * @param entitymeta
	 * @param org
	 * @param synoder
	 * @param uids
	 * @param deliffnode delete the change-log iff the node, i.e. only the subscriber, exists.
	 * For answers, it's the node himself, for challenge, it's the source node.
	 * @return
	 * @throws TransException
	 */
	Statement<?> del0subchange(SyntityMeta entitymeta,
			String org, String synoder, String uids, String deliffnode) throws TransException {
		return delete(chgm.tbl) // delete change log if no subscribers exist
			.whereEq(chgm.entbl, entitymeta.tbl)
			.whereEq(chgm.domain, org)
			.whereEq(chgm.synoder, synoder)
			.whereEq(chgm.uids,    uids)
			.whereEq("0", (Query)select(subm.tbl)
				.col(count(subm.synodee))
				.whereEq(subm.domain, org)
				.whereEq(subm.entbl, entitymeta.tbl)
				.where(op.ne, subm.synodee, constr(deliffnode))
				.whereEq(subm.uids,  uids));
	}

	/*
	DBSynsactBuilder commitChallenges(ExchangeContext x, String srcnode, HashMap<String, Nyquence> srcnv, long tillN)
			throws SQLException, TransException {
		List<Statement<?>> stats = new ArrayList<Statement<?>>();

		AnResultset chal = x.onchanges.challenge.beforeFirst();
		String entid0  = null;
		String entid1  = null;

		String chorg   = null;
		String chentbl = null;
		String chuids0 = null;
		String chuids1 = null;
		String synodr0 = null;
		String synodr1 = null;
		String subsrb  = null;

		SyntityMeta entm = null; 
		String change    = null;

		HashSet<String> missings = null;

		while (chal.next()) {
			if (compareNyq(chal.getLong(chgm.nyquence), tillN) > 0)
				break;

			entm = getEntityMeta(chal.getString(chgm.entbl));
			// create / update / delete an entity
			change = chal.getString(chgm.crud);

			// current entity
			entid1 = chal.getString(chgm.entfk);

			HashMap<String, AnResultset> entbuf = x.onchanges.entities;
			if (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).indices0(entid1) < 0) {
				Utils.warn("[DBSynsactBuilder commitChallenges] Fatal error ignored: can't restore entity record answered from target node.\n"
						+ "entity name: %s\nsynode(answering): %s\nsynode(local): %s\nentity id(by challenge): %s",
						entm.tbl, srcnode, synode(), entid1);
				continue;
			}
			
			chorg = chal.getString(chgm.org);
			chentbl = chal.getString(chgm.entbl);
			chuids1= chal.getString(chgm.uids);
			synodr1 = chal.getString(chgm.synoder);
			subsrb = chal.getString(subm.synodee);

			if (!eq(entid0, entid1)) {
				appendMissings(stats, missings, chal);
				missings = new HashSet<String>(nyquvect.keySet());
				missings.removeAll(srcnv.keySet());
				missings.remove(synode());

				// next entity
				stats.add(eq(change, CRUD.C)
					? insert(entm.tbl, synrobot())
						.cols(entm.entCols())
						.value(entm.insertChallengeEnt(entid1, entbuf.get(entm.tbl)))
						.post(insert(chgm.tbl)
							.nv(chgm.crud, CRUD.C)
							.nv(chgm.org, chorg)
							.nv(chgm.entbl, chentbl)
							.nv(chgm.synoder, synodr1)
							.nv(chgm.uids, chuids1)
							.nv(chgm.nyquence, chal.getLong(chgm.nyquence))
							.nv(chgm.entfk, entm.autopk() ? new Resulving(entm.tbl, entm.pk) : constr(entid1))
							.post(eq(subsrb, synode()) ? null : insert(subm.tbl)
								.cols(subm.insertCols())
								.value(subm.insertSubVal(chal)))
						.post(eq(entid0, entid1) || chuids0 == null // clean change-logs without subscription, FIXME command order not correct
							? null
							: del0subchange(entm, chorg, synodr0, chuids0, synode())))

					: eq(change, CRUD.D)
					? delete(subm.tbl, synrobot())
						.whereEq(subm.synodee, subsrb)
						.whereEq(subm.entbl, chentbl)
						.whereEq(subm.org, chorg)
						.whereEq(subm.uids, chuids1)
						.post(entid0 == null ? null : delete(chgm.tbl)
							.whereEq(chgm.entbl, chentbl)
							.whereEq(chgm.org, chorg)
							.whereEq(chgm.synoder, synodr1)
							.whereEq(chgm.uids, chuids1)
							.post(delete(entm.tbl)
								.whereEq(entm.org(), chorg)
								.whereEq(entm.synoder, synodr1)
								.whereEq(entm.pk, chentbl)))

					: eq(change, CRUD.U)
					? update(entm.tbl, synrobot())
						.nvs(entm.updateChallengeEnt(entid1, entbuf.get(entm.tbl)))
						.whereEq(entm.synoder, synodr1)
						.whereEq(entm.org(), chorg)
						.whereEq(entm.pk, chal.getString(chgm.entfk))
					: null);
			}
			else
				// same entity
				stats.add(eq(change, CRUD.C)
					? eq(subsrb, synode())
						? null
						: insert(subm.tbl)
							.cols(subm.insertCols())
							.value(subm.insertSubVal(chal))
					: eq(change, CRUD.D)
					? delete(subm.tbl, synrobot())
						.whereEq(subm.synodee, subsrb)
						.whereEq(subm.entbl, chentbl)
						.whereEq(subm.org, chorg)
						.whereEq(subm.uids, chuids1)
					: null); // eq(change, CRUD.U | R)
			entid0  = entid1;
			chuids0 = chuids1;
			synodr0 = synodr1;
			
			missings.remove(subsrb);
		}

		if (entid1 != null && (eq(change, CRUD.U) || eq(change, CRUD.C))) {
			// delete operation for last change log, including creating change logs, for the only one, say, Z, the last synodee.
			stats.add(del0subchange(entm, chorg, synodr0, chuids1, synode()));
			appendMissings(stats, missings, chal);
		}

		Utils.logi("[DBSynsactBuilder.commitChallenges()] update entities...");
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			if (s != null)
				s.commit(sqls, synrobot());
		Connects.commit(basictx.connId(), synrobot(), sqls);
		
		x.onchanges = null;
		return this;
	}
	*/
	
	/**
	 * <p>Commit challenges buffered in context.</p>
	 * 
	 * Challenges must grouped by synodee, entity-table and domain (org). 
	 * 
	 * @param x
	 * @param srcnode
	 * @param srcnv
	 * @param tillN
	 * @return this
	 * @throws SQLException
	 * @throws TransException
	 */
	DBSynsactBuilder commitChallenges(ExchangeContext x, String srcnode, HashMap<String, Nyquence> srcnv, long tillN)
			throws SQLException, TransException {
		List<Statement<?>> stats = new ArrayList<Statement<?>>();

		AnResultset chal = x.onchanges.challenge.beforeFirst();

		HashSet<String> missings = new HashSet<String>(nyquvect.keySet());
		missings.removeAll(srcnv.keySet());
		missings.remove(synode());

		while (chal.next()) {
			// if (compareNyq(chal.getLong(chgm.nyquence), tillN) > 0) break;

			String change = chal.getString(chgm.crud);
			Nyquence chgnyq = getn(chal, chgm.nyquence);

			SyntityMeta entm = getEntityMeta(chal.getString(chgm.entbl));
			// create / update / delete an entity
			String entid  = chal.getString(chgm.entfk);
			String synodr = chal.getString(chgm.synoder);
			String chuids = chal.getString(chgm.uids);

			HashMap<String, AnResultset> entbuf = x.onchanges.entities;
			if (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).indices0(entid) < 0) {
				Utils.warn("[DBSynsactBuilder commitChallenges] Fatal error ignored: can't restore entity record answered from target node.\n"
						+ "entity name: %s\nsynode(answering): %s\nsynode(local): %s\nentity id(by challenge): %s",
						entm.tbl, srcnode, synode(), entid);
				continue;
			}
			
			String chorg = chal.getString(chgm.domain);
			String chentbl = chal.getString(chgm.entbl);
			
			ArrayList<Statement<?>> subscribeUC = new ArrayList<Statement<?>>();

			if (eq(change, CRUD.D)) {
				String subsrb = chal.getString(subm.synodee);
				stats.add(delete(subm.tbl, synrobot())
					.whereEq(subm.synodee, subsrb)
					.whereEq(subm.entbl, chentbl)
					.whereEq(subm.domain, chorg)
					.whereEq(subm.uids, chuids)
					.post(ofLastEntity(chal, entid, chentbl, chorg)
						? delete(chgm.tbl)
							.whereEq(chgm.entbl, chentbl)
							.whereEq(chgm.domain, chorg)
							.whereEq(chgm.synoder, synodr)
							.whereEq(chgm.uids, chuids)
							.post(delete(entm.tbl)
								.whereEq(entm.org(), chorg)
								.whereEq(entm.synoder, synodr)
								.whereEq(entm.pk, chentbl))
						: null));
			}
			else { // CRUD.C || CRUD.U
				boolean iamSynodee = false;

				while (chal.validx()) {
					String subsrb = chal.getString(subm.synodee);
					if (eq(subsrb, synode())) {
					/** conflict: Y try send Z a record that Z already got from X.
					 *        X           Y               Z
                     *             | I Y Y,W 4 Z -> 4 < Z.y, ignore |
					 *
      				 *		  X    Y    Z    W
					 *	X [   7,   5,   3,   4 ]
					 *	Y [   4,   6,   1,   4 ]
					 *	Z [   6,   5,   7,   4 ]   change[Z].n < Z.y, that is Z knows later than the log
					 */
						Nyquence my_srcn = nyquvect.get(srcnode);
						if (my_srcn != null && compareNyq(chgnyq, my_srcn) >= 0)
							// conflict & override
							iamSynodee = true;
					}
					else if (compareNyq(chgnyq, nyquvect.get(subsrb)) > 0
						// ref: _merge-older-version
						// knowledge about the sub from req is older than this node's knowledge 
						// see #onchanges ref: answer-to-remove
						// FIXME how to abstract into one method?
						&& !eq(subsrb, synode()))
						subscribeUC.add(insert(subm.tbl)
							.cols(subm.insertCols())
							.value(subm.insertSubVal(chal))); 
					
					if (ofLastEntity(chal, entid, chentbl, chorg))
						break;
					chal.next();
				}

				appendMissings(stats, missings, chal);

				if (iamSynodee || subscribeUC.size() > 0) {
					stats.add(eq(change, CRUD.C)
					? insert(entm.tbl, synrobot())
						.cols(entm.entCols())
						.value(entm.insertChallengeEnt(entid, entbuf.get(entm.tbl)))
						.post(subscribeUC.size() <= 0 ? null :
							insert(chgm.tbl)
							.nv(chgm.crud, CRUD.C).nv(chgm.domain, chorg)
							.nv(chgm.entbl, chentbl).nv(chgm.synoder, synodr).nv(chgm.uids, chuids)
							.nv(chgm.nyquence, chal.getLong(chgm.nyquence))
							.nv(chgm.entfk, entm.autopk() ? new Resulving(entm.tbl, entm.pk) : constr(entid))
							.post(subscribeUC)
							.post(del0subchange(entm, chorg, synodr, chuids, synode())))
					: eq(change, CRUD.U)
					? update(entm.tbl, synrobot())
						.nvs(entm.updateChallengeEnt(entid, entbuf.get(entm.tbl)))
						.whereEq(entm.synoder, synodr)
						.whereEq(entm.org(), chorg)
						.whereEq(entm.pk, entid)
						.post(subscribeUC.size() <= 0 ? null :
							insert(chgm.tbl)
							.nv(chgm.crud, CRUD.U).nv(chgm.domain, chorg)
							.nv(chgm.entbl, chentbl).nv(chgm.synoder, synodr).nv(chgm.uids, chuids)
							.nv(chgm.nyquence, chgnyq.n)
							// .nv(chgm.entfk, entm.autopk() ? new Resulving(entm.tbl, entm.pk) : constr(entid))
							.nv(chgm.entfk, constr(entid))
							.post(subscribeUC)
							.post(del0subchange(entm, chorg, synodr, chuids, synode())))
					: null);
				}

				subscribeUC = new ArrayList<Statement<?>>();
				iamSynodee  = false;
			}
		}

		Utils.logi("[DBSynsactBuilder.commitChallenges()] update entities...");
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			if (s != null)
				s.commit(sqls, synrobot());
		Connects.commit(basictx.connId(), synrobot(), sqls);
		
		x.onchanges = null;
		return this;
	}

	/**
	 * Is next row in {@code chlogs} a change log for different entity?
	 * @param chlogs
	 * @param curEid
	 * @param curEntbl
	 * @param curDomain
	 * @return true if next row is new enitity's change log.
	 * @throws SQLException 
	 */
	boolean ofLastEntity(AnResultset chlogs, String curEid, String curEntbl, String curDomain)
			throws SQLException {
		return !chlogs.hasnext() || !eq(curEid, chlogs.nextString(chgm.entfk))
			|| !eq(curEntbl, chlogs.nextString(chgm.entbl)) || !eq(curDomain, chlogs.nextString(chgm.domain));
	}
	
	boolean isAnotherEntity (AnResultset chlogs, String curEid, String curEntbl, String curDomain)
			throws SQLException {
		return !chlogs.hasprev() || !eq(curEid, chlogs.prevString(chgm.entfk))
			|| !eq(curEntbl, chlogs.prevString(chgm.entbl)) || !eq(curDomain, chlogs.prevString(chgm.domain));
	}

	/**
	 * Append inserting statement for missing subscribes, for the knowledge
	 * that the source node dosen't know.
	 * 
	 * @param stats
	 * @param missing missing knowledge of synodees that the source node doesn't know
	 * @param chlog
	 * @return {@code missing }
	 * @throws TransException
	 * @throws SQLException
	 */
	HashSet<String> appendMissings(List<Statement<?>> stats, HashSet<String> missing, AnResultset chlog)
			throws TransException, SQLException {
		if(missing != null && missing.size() > 0
			&& eq(chlog.getString(chgm.crud), CRUD.C)) {
			String domain = chlog.getString(chgm.domain);
			String entbl  = chlog.getString(chgm.entbl);
			String uids   = chlog.getString(chgm.uids);

			for (String sub : missing) 
				stats.add(insert(subm.tbl)
					.cols(subm.insertCols())
					.value(subm.insertSubVal(domain, entbl, sub, uids)));
		}
		return missing;
	}

	public SyntityMeta getEntityMeta(String entbl) throws SemanticException {
		if (entityRegists == null || !entityRegists.containsKey(entbl))
			throw new SemanticException("Register %s first.", entbl);
			
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
	 * this.n0++, this.n0 = max(n0, maxn)
	 * @param maxn
	 * @return n0
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public Nyquence incN0(long maxn) throws TransException, SQLException {
		n0().inc(maxn);
		DAHelper.updateField(this, basictx.connId(), synm, synode(),
				synm.nyquence, new ExprPart(n0().n), synrobot());
		return n0();
	}

	public Nyquence incN0(Nyquence n) throws TransException, SQLException {
		return incN0(n == null ? nyquvect.get(synode()).n : n.n);
	}

	/**
	 * Find if there are change logs such that chg.n &gt; myvect[remote].n, to be exchanged.
	 * @param cx 
	 * @param <T>
	 * @param target exchange target (server)
	 * @param nv nyquevect from target 
	 * @return logs such that chg.n > nyv[target], i.e there are change logs to be exchanged.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public <T extends SynEntity> ChangeLogs initExchange(ExchangeContext x, String target,
			HashMap<String, Nyquence> nv) throws TransException, SQLException {
		synyquvectWith(target, nv);
		Nyquence dn = this.nyquvect.get(target);
		ChangeLogs diff = new ChangeLogs(chgm); //.challenge(challenge);
		if (dn == null) {
			Utils.warn("ERROR [%s#%s]: Me, %s, don't have knowledge about %s.",
					this.getClass().getName(),
					new Object(){}.getClass().getEnclosingMethod().getName(),
					synode(), target);
			throw new SemanticException("%s#%s(), don't have knowledge about %s.",
					synode(), new Object(){}.getClass().getEnclosingMethod().getName(), target);
		}
		else {
			AnResultset challenge = (AnResultset) select(chgm.tbl, "ch")
				.je("ch", subm.tbl, "sb", chgm.entbl, subm.entbl, chgm.uids, subm.uids)
				.cols("ch.*", subm.synodee)
				// FIXME not op.lt, must implement a function to compare nyquence.
				.where(op.gt, chgm.nyquence, dn.n) // FIXME
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

			diff.challenge(challenge);
			while (entbls.next()) {
				String tbl = entbls.getString(chgm.entbl);
				SyntityMeta entm = entityRegists.get(tbl);

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
		
			x.initChallenge(target, diff);
		}

		return diff;
	}
	
	public <T extends SynEntity> ChangeLogs onExchange(ExchangeContext x, String from,
			HashMap<String, Nyquence> remotv, ChangeLogs req) throws SQLException, TransException {

		if (x.onchanges != null && x.onchanges.challenges() > 0)
			Utils.warn("There are challenges buffered to be commited: %s@%s", from, synode());;

		ChangeLogs myanswer = initExchange(x, from, req.nyquvect);

		x.buffChanges(req.challenge.colnames(), onchanges(myanswer, req), req.entities);
		return myanswer.nyquvect(nyquvect);
	}

	ArrayList<ArrayList<Object>> onchanges(ChangeLogs resp, ChangeLogs req) throws SQLException {
		ArrayList<ArrayList<Object>> changes = new ArrayList<ArrayList<Object>>();
		while (req != null && req.challenge != null && req.challenge.next()) {
			String subscribe = req.challenge.getString(subm.synodee);

			if (eq(subscribe, synode())) {
				resp.remove_sub(req.challenge, synode());	
				changes.add(req.challenge.getRowAt(req.challenge.getRow() - 1));
			}
			else {
				Nyquence subnyq = getn(req.challenge, chgm.nyquence);
				if (!nyquvect.containsKey(subscribe) // I don't have information of the subscriber
					&& eq(synm.tbl, req.challenge.getString(chgm.entbl)))
					changes.add(req.challenge.getRowAt(req.challenge.getRow() - 1));
				else if (!nyquvect.containsKey(subscribe))
						; // I have no idea
				else if (compareNyq(subnyq, nyquvect.get(subscribe)) <= 0) {
					// || compareNyq(req.challenge.getLong(chgm.nyquence), nyquvect.get(subscribe).n) < 0)

					// ref: _answer-to-remove
					// knowledge about the sub from req is older than this node's knowledge 
					// see #commitChallenges ref: merge-older-version
					// FIXME how to abstract into one method?
					resp.remove_sub(req.challenge, subscribe);	

				}
				else
					changes.add(req.challenge.getRowAt(req.challenge.getRow() - 1));
			}
		}

		return changes;
	}

	/**
	 * Client node acknowledge destionation's response (from server),
	 * i.e. check answers up to n = my-n0
	 * If there is no more challenges, increase my.n0.
	 * @param x exchange execution instance 
	 * @param answer
	 * @param sn 
	 * @param srcnv 
	 * @return acknowledge, answer to the destination node's commitment, and have be committed at source.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public ChangeLogs ackExchange(ExchangeContext x, ChangeLogs answer, String sn,
			HashMap<String, Nyquence> srcnv) throws SQLException, TransException, IOException {

		ChangeLogs myack = new ChangeLogs(chgm);
		if (answer.answers != null && answer.answers.getRowCount() > 0) {
			x.addAnswer(answer.answers);
			commitAnswers(x, sn, n0().n);
		}

		cleanStaleThan(answer.nyquvect, sn);

		x.buffChanges(answer.challenge.colnames(), onchanges(myack, answer), answer.entities);
		if (x.onchanges.challenges() > 0) {
			commitChallenges(x, sn, srcnv, nyquvect.get(synode()).n);
		}

		synyquvectWith(sn, answer.nyquvect);
		myack.nyquvect(Nyquence.clone(nyquvect));

		return myack;
	}

	void cleanStaleThan(HashMap<String, Nyquence> srcnv, String srcn)
			throws TransException, SQLException {
		for (String sn : srcnv.keySet()) {
			if (eq(sn, synode()) || eq(sn, srcn))
				continue;
			if (!nyquvect.containsKey(sn) || compareNyq(nyquvect.get(sn), srcnv.get(sn)) >= 0)
				continue;

			Query cl = (Query)select(chgm.tbl, "cl")
				.cols("cl.*").col(subm.synodee)
				.je2(subm.tbl, "sb", constr(sn), subm.synodee,
					chgm.domain, subm.domain, chgm.entbl, subm.entbl,
					chgm.uids, subm.uids)
				.where(op.ne, subm.synodee, constr(srcn));

			delete(subm.tbl, synrobot())
				.where(op.exists, null, with(cl)
					.select("cl")
					.whereEq(subm.tbl, subm.domain,   "cl", chgm.domain)
					.whereEq(subm.tbl, subm.entbl, "cl", chgm.entbl)
					.whereEq(subm.tbl, subm.uids,  "cl", chgm.uids))
				.post(delete(chgm.tbl)
					.where(op.notexists, null, with(cl)
						.select("cl")
						.where(op.ne, subm.synodee, constr(sn))
						.whereEq(chgm.tbl, chgm.domain,  "cl", chgm.domain)
						.whereEq(chgm.tbl, chgm.entbl,"cl", chgm.entbl)
						.whereEq(chgm.tbl, chgm.uids, "cl", chgm.uids)))
				.d(instancontxt(basictx.connId(), synrobot()));
		}
	}

	/**
	 * Commit buffered answer's changes as client node acknowledged the answers with {@code ack}.
	 * @param x exchange execution's instance
	 * @param ack answer to previous challenge
	 * @param target 
	 * @param srcnv 
	 * @param entm 
	 * @return nyquvect
	 * @throws TransException 
	 * @throws SQLException 
	 */
	public HashMap<String,Nyquence> onAck(ExchangeContext x, ChangeLogs ack, String target, HashMap<String, Nyquence> srcnv, SyntityMeta entm)
			throws SQLException, TransException {

		cleanStaleThan(ack.nyquvect, target);

		if (ack != null && compareNyq(ack.nyquvect.get(synode()), nyquvect.get(synode())) <= 0) {
			commitChallenges(x, target, srcnv, nyquvect.get(synode()).n);
		}

		if (ack.answers != null && ack.answers.getRowCount() > 0) {
			x.addAnswer(ack.answers);
			commitAnswers(x, target, n0().n);
		}
		synyquvectWith(target, ack.nyquvect);
		n0(maxn(ack.nyquvect, n0()));
		return nyquvect;
	}
	
	public HashMap<String, Nyquence> closexchange(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws TransException, SQLException {
		x.clear();
		HashMap<String, Nyquence> snapshot = Nyquence.clone(nyquvect);
		nv.get(sn).inc(n0());
		incN0(maxn(nv));
		return snapshot;
	}

	public HashMap<String, Nyquence> closeJoining(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws TransException, SQLException {
		return closexchange(x, sn, nv);
	}
	
	public void onclosexchange(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws SQLException, TransException {
		x.clear();
		synyquvectWith(sn, nv);
		nv.get(sn).inc();
		incN0(maxn(nv));
	}
	
	public void oncloseJoining(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws SQLException, TransException {
		onclosexchange(x, sn, nv);
	}

	/**
	 * Update / step my nyquvect with {@code nv}, using max(my.nyquvect, nv).
	 * If nv[sn] &lt; my_nv[sn], throw SemanticException: can't update my nyquence with early knowledge.
	 * 
	 * @param sn whose nyquence in {@code nv} is required to be newer.
	 * @param nv
	 * @return this
	 * @throws TransException
	 * @throws SQLException
	 */
	private DBSynsactBuilder synyquvectWith(String sn, HashMap<String, Nyquence> nv)
			throws TransException, SQLException {
		if (nv == null) return this;

		Update u = null;

		if (compareNyq(nv.get(sn), nyquvect.get(sn)) < 0)
			throw new SemanticException(
				"[DBSynsactBuilder.synyquvectWith()] Updating my (%s) nyquence with %s's value early than already knowns.",
				synode(), sn);

		for (String n : nv.keySet()) {
			if (!eq(n, sn) && nyquvect.containsKey(n)
				&& compareNyq(nv.get(n), nyquvect.get(n)) > 0)
				if (u == null)
					u = update(synm.tbl, synrobot())
						.nv(synm.nyquence, n0().n)
						.whereEq(synm.pk, n);
				else
					u.post(update(synm.tbl)
						.nv(synm.nyquence, nv.get(n).n)
						.whereEq(synm.pk, n));

			if (nyquvect.containsKey(n))
				nyquvect.get(n).n = maxn(nv.get(n).n, nyquvect.get(n).n);
			else nyquvect.put(n, new Nyquence(nv.get(n).n));
		}

		nyquvect.put(synode(), maxn(n0(), nv.get(sn)));
		if (u != null)
			u.u(instancontxt(basictx.connId(), synrobot()));

		return this;
	}
		

	/**
	 * A {@link SynodeMode#hub hub} node uses this to setup change logs for joining nodes.
	 * @param x 
	 * 
	 * @param childId
	 * @param robot
	 * @param org
	 * @param domain
	 * @return [applyid, "My-id,applyid" as uids]
	 * @throws TransException
	 * @throws SQLException
	 */
	public ChangeLogs addChild(ExchangeContext x, String childId, SynodeMode reqmode, IUser robot, String org, String domain)
			throws TransException, SQLException {
		Synode apply = new Synode(basictx.connId(), childId, org, domain);
		apply.insert(synm, n0(), insert(synm.tbl, robot))
			.post(insert(chgm.tbl, robot)
				.nv(chgm.entfk, apply.recId)
				.nv(chgm.entbl, synm.tbl)
				.nv(chgm.crud, CRUD.C)
				.nv(chgm.synoder, synode())
				.nv(chgm.uids, concatstr(synode(), chgm.UIDsep, apply.recId))
				.nv(chgm.nyquence, n0().n)
				.nv(chgm.domain, robot.orgId())
				.post(insert(subm.tbl)
					.cols(subm.entbl, subm.synodee, subm.uids, subm.domain)
					.select((Query)select(synm.tbl)
						.col(constr(synm.tbl))
						.col(synm.synoder)
						.col(concatstr(synode(), chgm.UIDsep, apply.recId))
						.col(constr(robot.orgId()))
						.where(op.ne, synm.synoder, constr(synode()))
						.where(op.ne, synm.synoder, constr(childId))
						.whereEq(synm.domain, domain))))
			.ins(instancontxt(basictx.connId(), robot));
		
		nyquvect.put(apply.recId, new Nyquence(apply.nyquence));

		ChangeLogs log = new ChangeLogs(chgm)
			.nyquvect(nyquvect)
			.synodes(reqmode == SynodeMode.child
				? ((AnResultset) select(synm.tbl, "syn")
					.whereIn(synm.synoder, childId, synode())
					.whereEq(synm.domain, domain)
					.whereEq(synm.org(), org)
					.rs(instancontxt(basictx.connId(), synrobot()))
					.rs(0))
				: null); // Following exchange is needed
		return log;
	}

	public ChangeLogs onJoining(SynodeMode reqmode, String joining, String domain, String org)
			throws TransException, SQLException {
		insert(synm.tbl, synrobot())
			.nv(synm.pk, joining)
			.nv(synm.nyquence, new ExprPart(n0().n))
			.nv(synm.mac,  "#"+joining)
			.nv(synm.org(), org)
			.nv(synm.domain, domain)
			.ins(instancontxt(basictx.connId(), synrobot()));

		if (reqmode == SynodeMode.hub)
			incNyquence();

		ChangeLogs log = new ChangeLogs(chgm)
			.nyquvect(Nyquence.clone(nyquvect))
			.synodes(reqmode == SynodeMode.child
				? ((AnResultset) select(synm.tbl, "syn")
					.whereEq(synm.synoder, synode())
					.whereEq(synm.domain, domain)
					.whereEq(synm.org(), org)
					.rs(instancontxt(basictx.connId(), synrobot()))
					.rs(0))
				: null); // Following exchange is needed
		return log;
	}

	public ChangeLogs initDomain(ExchangeContext x, String from, ChangeLogs domainstatus)
			throws SQLException, TransException {
		AnResultset ns = domainstatus.synodes.beforeFirst();
		nyquvect = new HashMap<String, Nyquence>(ns.getRowCount());
		while (ns.next()) {
			Synode n = new Synode(ns, synm);
			Nyquence mxn = maxn(domainstatus.nyquvect);
			n.insert(synm, mxn, insert(synm.tbl, synrobot()))
				.ins(instancontxt(basictx.connId(), synrobot()));

			nyquvect.put(n.recId, new Nyquence(mxn.n));
		}

		return new ChangeLogs(chgm).nyquvect(nyquvect);
	}
	
	/**
	 * Syntity table updating. 
	 * Synchronized entity table can only be updated with a pk condition.
	 * @param entm
	 * @param pid
	 * @param field
	 * @param nvs name-value pairs
	 * @return 
	 * @return
	 * @throws TransException
	 * @throws SQLException
	 */
	public DBSynsactBuilder updateEntity(String synoder, String pid, SyntityMeta entm, Object ... nvs)
			throws TransException, SQLException {
		update(entm.tbl, synrobot())
			.nvs((Object[])nvs)
			.whereEq(entm.pk, pid)
			.post(insert(chgm.tbl, synrobot())
				.nv(chgm.entfk, pid)
				.nv(chgm.entbl, entm.tbl)
				.nv(chgm.crud, CRUD.U)
				.nv(chgm.synoder, synode()) // U.synoder != uids[synoder]
				.nv(chgm.uids, concatstr(synoder, chgm.UIDsep, pid))
				.nv(chgm.nyquence, n0().n)
				.nv(chgm.domain, synrobot().orgId())
				.post(insert(subm.tbl)
					.cols(subm.entbl, subm.synodee, subm.uids, subm.domain)
					.select((Query)select(synm.tbl)
						.col(constr(entm.tbl))
						.col(synm.synoder)
						.col(concatstr(synode(), chgm.UIDsep, pid))
						.col(constr(synrobot().orgId()))
						.where(op.ne, synm.synoder, constr(synode()))
						.whereEq(synm.domain, synrobot().orgId()))))
			.u(instancontxt(basictx.connId(), synrobot()));
		return this;
	}
}
