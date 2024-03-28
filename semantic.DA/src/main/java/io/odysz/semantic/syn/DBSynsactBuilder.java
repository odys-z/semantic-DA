package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.maxn;
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
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
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

	/** Nyquence vector [{synode, n0}]*/
	protected HashMap<String, Nyquence> nyquvect;
	protected Nyquence n0() { return nyquvect.get(synode()); }
	public IUser synrobot() { return ((DBSyntext) this.basictx).usr(); }

	private HashMap<String, SyntityMeta> entityRegists;

	// private HashMap<String, AnResultset> entitybuf;

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
	public AnResultset subscribes(String conn, String org, String uids, SyntityMeta entm, IUser robot)
			throws TransException, SQLException {
		return (AnResultset) select(subm.tbl, "ch")
				.cols(subm.cols())
				.whereEq(subm.org, org)
				.whereEq(subm.entbl, entm.tbl)
				.whereEq(subm.uids, uids)
				.rs(instancontxt(conn, robot))
				.rs(0);
	}

	public AnResultset subscribes(String conn, String org, Funcall uids, SyntityMeta entm, IUser robot)
			throws TransException, SQLException {
		return (AnResultset) select(subm.tbl, "ch")
				.cols(subm.cols())
				.whereEq(subm.org, org)
				.whereEq(subm.entbl, entm.tbl)
				.whereEq(subm.uids, uids)
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

			String rporg  = rply.getString(chgm.org);
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
							.nv(chgm.org, rporg)
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
					.post(del0subchange(entm,
							rporg, rpnodr, rpuids, synode()) // delete change log if no subscribers exist
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
	 * Generate delete statement when deleting changes. 
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
			.whereEq(chgm.org, org)
			.whereEq(chgm.synoder, synoder)
			.whereEq(chgm.uids,    uids)
			.whereEq("0", (Query)select(subm.tbl)
				.col(count(subm.synodee))
				.whereEq(subm.org, org)
				.whereEq(subm.entbl, entitymeta.tbl)
				.where(op.ne, subm.synodee, constr(deliffnode))
				.whereEq(subm.uids,  uids));
	}

	DBSynsactBuilder commitChallenges(ExchangeContext x, String srcnode, long tillN)
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

		while (chal.next()) {
			if (compareNyq(chal.getLong(chgm.nyquence), tillN) > 0)
				break;

			entm = getEntityMeta(chal.getString(chgm.entbl));
			String change = chal.getString(chgm.crud);

			// current entity
			entid1 = chal.getString(chgm.entfk);

			HashMap<String, AnResultset> entbuf = x.onchanges.entities;
			if (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).indices0(entid1) < 0) {
				Utils.warn("[DBSynsactBuilder commitChallenges] Fatal error ignored: can't restore entity record answered from target node.\n"
						+ "entity name: %s\nsynode(answering): %s\nsynode(local): %s\nentity id(by challenge): %s", entm.tbl, srcnode, synode(), entid1);
				continue;
			}
			
			chorg = chal.getString(chgm.org);
			chentbl = chal.getString(chgm.entbl);
			chuids1= chal.getString(chgm.uids);
			synodr1 = chal.getString(chgm.synoder);
			subsrb = chal.getString(subm.synodee);

			if (!eq(entid0, entid1))
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
							.nv(chgm.entfk, new Resulving(entm.tbl, entm.pk))
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
			
			else // same entity
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
		}

		if (entid1 != null)
			// delete change log if no subscribers exist
			stats.add(del0subchange(entm, chorg, synodr0, chuids1, synode()));

		Utils.logi("[DBSynsactBuilder.commitChallenges()] update entities...");
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			s.commit(sqls, synrobot());
		Connects.commit(basictx.connId(), synrobot(), sqls);
		
		x.onchanges = null;
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
	 * this.n0++, this.n0 = max(n0, maxn)
	 * @param maxn
	 * @return n0
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public Nyquence incN0(long maxn) throws TransException, SQLException {
		// FIXME, sync db
		n0().inc(maxn);
		DAHelper.updateField(this, basictx.connId(), synm, synode(), synm.nyquence, String.valueOf(n0().n), synrobot());
		return n0();
	}
	public Nyquence incN0(Nyquence n) throws TransException, SQLException {
		return incN0(n.n);
	}

	/**
	 * Find if there are change logs such that chg.n &gt; myvect[remote].n, to be exchanged.
	 * @param cx 
	 * @param <T>
	 * @param target exchange target (server)
	 * @param nyv nyquevect from target 
	 * @return logs such that chg.n > nyv[target], i.e there are change logs to be exchanged.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public <T extends SynEntity> ChangeLogs initExchange(ExchangeContext cx, String target, SyntityMeta entm)
			throws TransException, SQLException {
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
		
		cx.initChallenge(target, diff);

		return diff;
	}
	
	public <T extends SynEntity> ChangeLogs onExchange(ExchangeContext x, String from, HashMap<String, Nyquence> remotv,
			ChangeLogs req, SyntityMeta entm) throws SQLException, TransException {

		if (x.onchanges != null && x.onchanges.challenges() > 0)
			Utils.warn("There are challenges buffered to be commited: %s@%s", from, synode());;

		ChangeLogs myanswer = initExchange(x, from, entm);

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
				if (compareNyq(req.challenge.getLong(chgm.nyquence), nyquvect.get(subscribe).n) < 0)
					// knowledge about the sub from req is older than this node's knowledge 
					resp.remove_sub(req.challenge, subscribe);	
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
	 * @return acknowledge, answer to the destination node's commitment, and have be committed at source.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public ChangeLogs ackExchange(ExchangeContext x, ChangeLogs answer, String sn)
			throws SQLException, TransException, IOException {

		ChangeLogs myack = new ChangeLogs(chgm);
		if (answer.answers != null && answer.answers.getRowCount() > 0) {
			x.addAnswer(answer.answers);
			commitAnswers(x, sn, n0().n);
		}

		cleanStaleThan(answer.nyquvect, sn);

		x.buffChanges(answer.challenge.colnames(), onchanges(myack, answer), answer.entities);
		if (x.onchanges.challenges() > 0) {
			commitChallenges(x, sn, nyquvect.get(synode()).n);
		}

		this.synyquvectWith(sn, answer.nyquvect);
		myack.nyquvect(Nyquence.clone(nyquvect));

		return myack;
	}

	void cleanStaleThan(HashMap<String, Nyquence> srcnv, String srcn)
			throws TransException, SQLException {
		for (String sn : srcnv.keySet()) {
			if (eq(sn, synode()) || eq(sn, srcn))
				continue;

			ExprPart snyq = new ExprPart(srcnv.get(sn).n);

			Query cl = (Query)select(chgm.tbl, "cl")
					.cols("cl.*").col(subm.synodee)
					.je2(subm.tbl, "sb", constr(sn), subm.synodee,
						chgm.org, subm.org, chgm.entbl, subm.entbl,
						chgm.uids, subm.uids)
					// NOTE: it's myself's n, not nv['z'].n because the change log was stamped with n0
					// FIXME not op.lt, must implement a function to compare nyquence.
					// FIXME
					to be continued
					.where(op.lt, chgm.nyquence, n0().n)
					.where(op.ne, subm.synodee, constr(sn));

			delete(subm.tbl, synrobot())
				.where(op.exists, null,
					with((Query)select(chgm.tbl, "cl")
						.cols("cl.*")
						.je2(subm.tbl, "sb", constr(sn), subm.synodee,
						  chgm.org, subm.org, chgm.entbl, subm.entbl,
						  chgm.uids, subm.uids)
						.where(op.lt, chgm.nyquence, n0().n)) // FIXME Nyq.compare
						.select("cl")
						.whereEq(subm.tbl + "." + subm.org, new ExprPart("cl." + chgm.org))
						.whereEq(subm.tbl + "." + subm.entbl, new ExprPart(subm.entbl))
						.whereEq(subm.tbl + "." + subm.uids, new ExprPart(subm.uids)))
				.post(delete(chgm.tbl)
					.where(op.exists, null,
						with(cl)
						.select("cl")
						.where(op.ne, subm.synodee, constr(sn))
						.whereEq(chgm.tbl + "." + chgm.org, new ExprPart("cl." + chgm.org))
						.whereEq(chgm.tbl + "." + chgm.entbl, new ExprPart(subm.entbl))
						.whereEq(chgm.tbl + "." + chgm.uids, new ExprPart(subm.uids))))
				.d(instancontxt(basictx.connId(), synrobot()));
		}
	}

	/**
	 * Commit buffered answer's changes as client node acknowledged the answers with {@code ack}.
	 * @param x exchange execution's instance
	 * @param ack answer to previous challenge
	 * @param target 
	 * @param entm 
	 * @throws TransException 
	 * @throws SQLException 
	 */
	public void onAck(ExchangeContext x, ChangeLogs ack, String target, SyntityMeta entm)
			throws SQLException, TransException {

		cleanStaleThan(ack.nyquvect, target);

		if (ack != null && compareNyq(ack.nyquvect.get(synode()), nyquvect.get(synode())) <= 0) {
			commitChallenges(x, target, nyquvect.get(synode()).n);
		}

		if (ack.answers != null && ack.answers.getRowCount() > 0) {
			x.addAnswer(ack.answers);
			commitAnswers(x, target, n0().n);
		}
		synyquvectWith(target, ack.nyquvect);
	}
	
	public HashMap<String, Nyquence> closexchange(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws TransException, SQLException {
		x.clear();
		HashMap<String, Nyquence> snapshot = Nyquence.clone(nyquvect);
		incN0(maxn(nv));
		return snapshot;
	}
	
	public void onclosechange(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws SQLException, TransException {
		x.clear();
		synyquvectWith(sn, nv);
		incN0(maxn(nv));
	}

	private DBSynsactBuilder synyquvectWith(String sn, HashMap<String, Nyquence> nv) throws TransException, SQLException {
		for (String n : nv.keySet()) {
			if (eq(n, sn) && compareNyq(nv.get(n), nyquvect.get(n)) < 0)
				// incN0(nv.get(n));
				throw new SemanticException("[DBSynsactBuilder.synyquvectWith()] Updating my (%s) nyquence with %s's value early than already knowns.",
						synode(), n);
			nyquvect.get(n).n = maxn(nv.get(n).n, nyquvect.get(n).n);
		}
		updateSynodes(nyquvect);
		return this;
	}
		
	private void updateSynodes(HashMap<String, Nyquence> nv) throws TransException, SQLException {
		Update u = update(synm.tbl, synrobot())
			.nv(synm.nyquence, n0().n)
			.whereEq(synm.pk, synode());
		
		for (String n : nv.keySet())
			if (!eq(n, synode()))
				u.post(update(synm.tbl)
					.nv(synm.nyquence, nv.get(n).n)
					.whereEq(synm.pk, n));
		
		u.u(instancontxt(basictx.connId(), synrobot()));
	}

}
