package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.semantic.syn.ExessionAct.*;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.getn;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.ExprPart.constVal;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import io.odysz.common.CheapMath;
import io.odysz.common.Radix64;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.QueryPage;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**
 * Persisting exchange session with remote node, using temporary tables.
 * This is a session context, and different from {@link DBSyntext} which
 * is used for handling local data integration and database semantics. 
 * 
 * @author Ody
 */
public class ExessionPersist {
	final SynChangeMeta chgm;
	final SynSubsMeta subm;
	final SynchangeBuffMeta exbm;

	final String peer;

	// public ArrayList<ArrayList<Object>> answerPage;
	public AnResultset answerPage;

	DBSyntableBuilder trb;

	/**
	 * Set nswers to the challenges, page {@link #challengeSeq}, with entities.
	 * 
	 * @param answer
	 * @param cols
	 * @return this
	 */
	ExessionPersist saveAnswer(AnResultset answer) {
		answerPage = answer;
		return this;
	}

	ExessionPersist commitAnswers(ExchangeBlock conf, String srcnode, long tillN0)
			throws SQLException, TransException {
		
		if (conf == null || conf.anspage == null || conf.anspage.getRowCount() <= 0)
			return this;
	
		List<Statement<?>> stats = new ArrayList<Statement<?>>();
	
		AnResultset rply = conf.anspage.beforeFirst();
		rply.beforeFirst();
		String entid = null;
		while (rply.next()) {
			if (compareNyq(rply.getLong(chgm.nyquence), tillN0) > 0)
				break;
	
			SyntityMeta entm = trb.getEntityMeta(rply.getString(chgm.entbl));
			String change = rply.getString(ChangeLogs.ChangeFlag);

			HashMap<String, AnResultset> entbuf = entities;
			
			// current entity
			String entid1 = rply.getString(chgm.entfk);
	
			String rporg  = rply.getString(chgm.domain);
			String rpent  = rply.getString(chgm.entbl);
			String rpuids = rply.getString(chgm.uids);
			String rpnodr = rply.getString(chgm.synoder);
			String rpscrb = rply.getString(subm.synodee);
			String rpcid  = rply.getString(chgm.pk);
	
			if (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).rowIndex0(entid1) < 0) {
				Utils.warn("[DBSynsactBuilder commitTill] Fatal error ignored: can't restore entity record answered from target node.\n"
						+ "entity name: %s\nsynode(answering): %s\nsynode(local): %s\nentity id(by answer): %s",
						entm.tbl, srcnode, trb.synode(), entid1);
				continue;
			}
				
			stats.add(eq(change, CRUD.C)
				// create an entity, and trigger change log
				? !eq(entid, entid1)
					? trb.insert(entm.tbl, trb.synrobot())
						.cols(entm.entCols())
						.value(entm.insertChallengeEnt(entid1, entbuf.get(entm.tbl)))
						.post(trb.insert(chgm.tbl)
							.nv(chgm.pk, rpcid)
							.nv(chgm.crud, CRUD.C)
							.nv(chgm.domain, rporg)
							.nv(chgm.entbl, rpent)
							.nv(chgm.synoder, rpnodr)
							.nv(chgm.uids, rpuids)
							.nv(chgm.entfk, entid1)
							.post(trb.insert(subm.tbl)
								.cols(subm.insertCols())
								.value(subm.insertSubVal(rply))))
					: trb.insert(subm.tbl)
						.cols(subm.insertCols())
						.value(subm.insertSubVal(rply))
	
				// remove subscribers & backward change logs's deletion propagation
				: trb.delete(subm.tbl, trb.synrobot())
					.whereEq(subm.changeId, rpcid)
					.whereEq(subm.synodee, rpscrb)
					.post(del0subchange(entm, rporg, rpnodr, rpuids, rpcid, rpscrb)
					));
			entid = entid1;
		}
	
		Utils.logi("[DBSynsactBuilder.commitAnswers()] updating change logs without modifying entities...");
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			s.commit(sqls, trb.synrobot());
		Connects.commit(trb.synconn(), trb.synrobot(), sqls);
		
		// x.answer = null;
		return this;
	}

	/**
	 * Save local changes according to challenges by the peer.
	 * @param changes 
	 * @param cols [name, [NAME, index]]
	 * @param srcnv 
	 * @param entites 
	 * @return this
	 * @throws TransException 
	 * @throws SQLException 
	 */
	ExessionPersist saveChanges(AnResultset changes, HashMap<String, Nyquence> srcnv,
			HashMap<String, AnResultset> entites) throws SQLException, TransException {

		List<Statement<?>> stats = new ArrayList<Statement<?>>();

		changes.beforeFirst();

		HashSet<String> missings = new HashSet<String>(trb.nyquvect.keySet());
		missings.removeAll(srcnv.keySet());
		missings.remove(trb.synode());

		while (changes.next()) {
			String change = changes.getString(chgm.crud);
			Nyquence chgnyq = getn(changes, chgm.nyquence);

			SyntityMeta entm = trb.getEntityMeta(changes.getString(chgm.entbl));
			// create / update / delete an entity
			String entid  = changes.getString(chgm.entfk);
			String synodr = changes.getString(chgm.synoder);
			String chuids = changes.getString(chgm.uids);
			String chgid  = changes.getString(chgm.pk);

			HashMap<String, AnResultset> entbuf = entites; // x.onchanges.entities;
			if (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).rowIndex0(entid) < 0) {
				Utils.warn("[DBSynsactBuilder commitChallenges] Fatal error ignored: can't restore entity record answered from target node.\n"
						+ "entity name: %s\nsynode(answering): %s\nsynode(local): %s\nentity id(by challenge): %s",
						entm.tbl, peer, trb.synode(), entid);
				continue;
			}
			
			String chorg = changes.getString(chgm.domain);
			String chentbl = changes.getString(chgm.entbl);
			
			// current entity's subscribes
			ArrayList<Statement<?>> subscribeUC = new ArrayList<Statement<?>>();

			if (eq(change, CRUD.D)) {
				String subsrb = changes.getString(subm.synodee);
				stats.add(trb.delete(subm.tbl, trb.synrobot())
					.whereEq(subm.synodee, subsrb)
					.whereEq(subm.changeId, chgid)
					.post(ofLastEntity(changes, entid, chentbl, chorg)
						? trb.delete(chgm.tbl)
							.whereEq(chgm.entbl, chentbl)
							.whereEq(chgm.domain, chorg)
							.whereEq(chgm.synoder, synodr)
							.whereEq(chgm.uids, chuids)
							.post(trb.delete(entm.tbl)
								.whereEq(entm.org(), chorg)
								.whereEq(entm.synoder, synodr)
								.whereEq(entm.pk, chentbl))
						: null));
			}
			else { // CRUD.C || CRUD.U
				boolean iamSynodee = false;

				while (changes.validx()) {
					String subsrb = changes.getString(subm.synodee);
					if (eq(subsrb, trb.synode())) {
					/** conflict: Y try send Z a record that Z already got from X.
					 *        X           Y               Z
                     *             | I Y Y,W 4 Z -> 4 < Z.y, ignore |
					 *
      				 *		  X    Y    Z    W
					 *	X [   7,   5,   3,   4 ]
					 *	Y [   4,   6,   1,   4 ]
					 *	Z [   6,   5,   7,   4 ]   change[Z].n < Z.y, that is Z knows later than the log
					 */
						Nyquence my_srcn = trb.nyquvect.get(peer);
						if (my_srcn != null && compareNyq(chgnyq, my_srcn) >= 0)
							// conflict & override
							iamSynodee = true;
					}
					else if (compareNyq(chgnyq, trb.nyquvect.get(peer)) > 0
						// ref: _merge-older-version
						// knowledge about the sub from req is older than this node's knowledge 
						// see #onchanges ref: answer-to-remove
						// FIXME how to abstract into one method?
						&& !eq(subsrb, trb.synode()))
						subscribeUC.add(trb.insert(subm.tbl)
							.cols(subm.insertCols())
							.value(subm.insertSubVal(changes))); 
					
					if (ofLastEntity(changes, entid, chentbl, chorg))
						break;
					changes.next();
				}

				appendMissings(stats, missings, changes);

				if (iamSynodee || subscribeUC.size() > 0) {
					stats.add(eq(change, CRUD.C)
					? trb.insert(entm.tbl, trb.synrobot())
						.cols(entm.entCols())
						.value(entm.insertChallengeEnt(entid, entbuf.get(entm.tbl)))
						.post(subscribeUC.size() <= 0 ? null :
							trb.insert(chgm.tbl)
							.nv(chgm.pk, chgid)
							.nv(chgm.crud, CRUD.C).nv(chgm.domain, chorg)
							.nv(chgm.entbl, chentbl).nv(chgm.synoder, synodr).nv(chgm.uids, chuids)
							.nv(chgm.nyquence, changes.getLong(chgm.nyquence))
							.nv(chgm.entfk, entm.autopk() ? new Resulving(entm.tbl, entm.pk) : constr(entid))
							.post(subscribeUC)
							.post(del0subchange(entm, chorg, synodr, chuids, chgid, trb.synode())))
					: eq(change, CRUD.U)
					? trb.update(entm.tbl, trb.synrobot())
						.nvs(entm.updateEntNvs(chgm, entid, entbuf.get(entm.tbl), changes))
						.whereEq(entm.synoder, synodr)
						.whereEq(entm.org(), chorg)
						.whereEq(entm.pk, entid)
						// FIXME there shouldn't be an UPSERT if the change-log is handled in orignal order?
//						.post(insert(entm.tbl, synrobot()) 
//							.cols(entm.entCols())
//							.select(select(null)
//									.cols(entm.insertSelectItems(chgm, entid, entbuf.get(entm.tbl), chal)))
//							.where(op.notexists, null,
//								select(entm.tbl)
//								.whereEq(entm.synoder, synodr)
//								.whereEq(entm.org(), entbuf.get(entm.tbl).getStringByIndex(entm.org(), entid))
//								.whereEq(entm.pk, entid)))
						.post(subscribeUC.size() <= 0
							? null : trb.insert(chgm.tbl)
							.nv(chgm.pk, chgid)
							.nv(chgm.crud, CRUD.U).nv(chgm.domain, chorg)
							.nv(chgm.entbl, chentbl).nv(chgm.synoder, synodr).nv(chgm.uids, chuids)
							.nv(chgm.nyquence, chgnyq.n)
							.nv(chgm.entfk, constr(entid))
							.post(subscribeUC)
							.post(del0subchange(entm, chorg, synodr, chuids, chgid, trb.synode())))
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
				s.commit(sqls, trb.synrobot());
		Connects.commit(trb.synconn(), trb.synrobot(), sqls);
		
//		x.onchanges = null;
		return this;
	}

	/**
	 * Create context at client side.
	 * @param tb 
	 * @param chgm
	 * @param localtb local transaction builder
	 * @param target
	 * @param exbm 
	 * @param subm 
	 * @param builder 
	 */
	public ExessionPersist(DBSyntableBuilder tb, SynChangeMeta chgm,
			SynSubsMeta subm, SynchangeBuffMeta exbm, String target) {
		if (tb != null && eq(tb.synode(), target))
			Utils.warn("Creating persisting context for local builder, i.e. peer(%s) = this.synode?", target);;

		this.trb  = tb; 
		this.exbm = exbm;
		this.peer = target;
		this.chgm = chgm;
		this.subm = subm;
		this.exstate = new ExessionAct(mode_client, ready);
		this.session = Radix64.toString((long) (Math.random() * Long.MAX_VALUE));
		this.chsize = 480;
	}

	/**
	 * Create context at server side.
	 * @param tb 
	 * @param session session id supplied by client
	 * @param chgm
	 * @param localtb
	 * @param target
	 */
	public ExessionPersist(DBSyntableBuilder tb, SynChangeMeta chgm,
			SynSubsMeta subm, SynchangeBuffMeta exbm, String peer, ExchangeBlock ini) {
		this.trb = tb;
		this.exbm = exbm;
		this.session = ini.session;
		this.peer = peer;
		this.chgm = chgm;
		this.subm = subm;
		this.exstate = new ExessionAct(mode_server, ready);
		this.chsize = 480;
	}

	public ExchangeBlock signup(String admin) {
		exstate.state = signup;
	
		return new ExchangeBlock(trb == null
				? null
				: trb.synode(), peer, session, exstate)
			.totalChallenges(1)
			.chpagesize(this.chsize)
			.seq(this);
	}


	/**
	 * Setup exchange buffer table.
	 * <pre>
	 * exstate.state = init;
	 * expAnswerSeq = 0;
	 * challengeSeq = 0;
	 * answerSeq = 0;
	 * </pre>
	 * @param i 
	 * @return 
	 * @return 
	 * @throws TransException 
	 * @throws SQLException 
	 * @throws SemanticException
	 */
	public ExchangeBlock init() throws TransException, SQLException {
		if (trb != null) {
			Nyquence dn = trb.nyquvect.get(peer);
			trb.insert(exbm.tbl, trb.synrobot())
				.cols(exbm.insertCols())
				.select(trb.select(chgm.tbl, "ch")
					.distinct()
					.je_(subm.tbl, "sb", chgm.pk, subm.changeId)
					.cols(exbm.selectCols(peer, -1))
					// FIXME not op.lt, must implement a function to compare nyquence.
					.where(op.gt, chgm.nyquence, dn.n) // FIXME
					.orderby(chgm.nyquence)
					.orderby(chgm.entbl)
					.orderby(chgm.synoder)
					.orderby(subm.synodee))
				.ins(trb.instancontxt(trb.synconn(), trb.synrobot()));
		}
		else 
			Utils.warn("[%s#%s()] Null transact builder. - null builder only for test",
				getClass().getName(),
				new Object(){}.getClass().getEnclosingMethod().getName());
		
		challengeSeq = -1;
		expAnswerSeq = challengeSeq;
		answerSeq = -1;

		totalChallenges = trb == null ? 0 : DAHelper.count(trb, trb.synconn(), exbm.tbl, exbm.peer, peer);
		
		exstate = new ExessionAct(mode_client, init);

		return new ExchangeBlock(trb == null ? null : trb.synode(), peer, session, exstate)
			.totalChallenges(totalChallenges)
			.chpagesize(this.chsize)
			.seq(challengeSeq, answerSeq);
	}

	public ExchangeBlock onInit(ExchangeBlock ini) throws TransException, SQLException {
		if (trb != null) {
			String conn = trb.basictx().connId();
			Nyquence dn = trb.nyquvect.get(peer);
			trb.insert(exbm.tbl, trb.synrobot())
				.cols(exbm.insertCols())
				.select(trb.select(chgm.tbl, "ch")
					.distinct()
					.je_(subm.tbl, "sb", chgm.pk, subm.changeId) // filter zero subscriber
					.cols(exbm.selectCols(peer, -1))
					// FIXME not op.lt, must implement a function to compare nyquence.
					.where(op.gt, chgm.nyquence, dn.n) // FIXME
					.orderby(chgm.entbl)
					.orderby(chgm.nyquence)
					.orderby(chgm.synoder)
					.orderby(subm.synodee))
				.ins(trb.instancontxt(conn, trb.synrobot()));
		}
		else 
			Utils.warn("[%s#%s()] Null transact builder. - null builder only for test",
				getClass().getName(),
				new Object(){}.getClass().getEnclosingMethod().getName());
		
		challengeSeq = -1;
		expAnswerSeq = ini.answerSeq;
		answerSeq = ini.challengeSeq;
	
		totalChallenges = trb == null ? 0 : DAHelper.count(trb, trb.synconn(), exbm.tbl, exbm.peer, peer);
		chsize = ini.chpagesize > 0 ? ini.chpagesize : -1;

		exstate = new ExessionAct(mode_server, init);

		return new ExchangeBlock(trb == null ? ini.peer : trb.synode(), peer, session, exstate)
				.totalChallenges(totalChallenges)
				.chpagesize(ini.chpagesize)
				.seq(challengeSeq, answerSeq);
	}

	public void clear() { }

	private String session;
	public String session() { return session; }

	private ExessionAct exstate;
	public int exstate() { return exstate.state; }
	public ExessionAct exstat() { return exstate; }
	
	public int totalChallenges;

	public int expAnswerSeq;
	/** Challenging sequence number, i. e. current page */
	public int challengeSeq;
	/** challenge page size */
	protected int chsize;

	public int answerSeq;

	/**
	 * Has another page in {@link SynchangeBuffMeta}.tbl to be send to.
	 * @param b synchronizing transaction builder
	 * @return true if has another page
	 * @throws SQLException
	 * @throws TransException
	 */
	public boolean hasNextChpages(DBSyntableBuilder b)
			throws SQLException, TransException {
		// return DAHelper.count(b, b.synconn(), exbm.tbl, exbm.peer, peer, exbm.seq, -1) > 0;
		
		int pages = pages();
		if (pages > 0 && challengeSeq + 1 < pages)
			return true;
		else
			return false;
	}

	public ExessionPersist expect(ExchangeBlock req) throws ExchangeException {
		if (req == null)
			return this;
		if (!eq(req.srcnode, this.peer) || !eq(session, req.session))
			throw new ExchangeException(ExessionAct.unexpected,
					"Session Id or peer mismatched [%s : %s vs %s : %s]",
					this.peer, session, req.srcnode, req.session);

		if (expAnswerSeq == 0 && req.answerSeq == -1 // first exchange
			|| expAnswerSeq == req.answerSeq)
			return this;

		throw new ExchangeException(ExessionAct.unexpected,
			// "exp-challenge %s : challenge %s, exp-answer %s : answer %s",
			"req challenge %s, exp-answer %s : answer %s",
			req.challengeSeq, expAnswerSeq, req.answerSeq);
	}
	
	public boolean nextChpage() throws TransException, SQLException {
		int pages = pages();
		if (challengeSeq < pages) {
			challengeSeq++;

			// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
			QueryPage page = (QueryPage) trb
				.selectPage(trb
					.select(exbm.tbl, "bf")
					.col(exbm.seq, "page")
					.col_ases("bf", exbm.changeId)
					.col("sub." + subm.synodee)
					.je_(chgm.tbl, "chg", exbm.changeId, chgm.pk, exbm.peer, Funcall.constr(peer))
					.je_(subm.tbl, "sub", "chg." + chgm.pk, subm.changeId)
					.whereEq(exbm.peer, peer))
				.page(challengeSeq, chsize)
				.col(exbm.changeId);

			trb.update(exbm.tbl, trb.synrobot())
				.nv(exbm.seq, challengeSeq)
				.whereIn(exbm.changeId, page)
				.u(trb.instancontxt(trb.synconn(), trb.synrobot()))
				;
		}

		return challengeSeq < pages;
	}

	/**
	 * Reset to last page
	 * @return this
	 */
	public boolean pageback() {
		if (challengeSeq < 0)
			return false;

		challengeSeq--;
		expAnswerSeq = challengeSeq;
		return challengeSeq < pages();
	}

	public ExchangeBlock exchange(String peer, ExchangeBlock rep)
			throws TransException, SQLException {
		if (exstate.state == restore && rep.act == exchange)
			; // exstate.state = exchange; // got answer
		else if (exstate.state != init && exstate.state != exchange)
			throw new ExchangeException(exchange,
				"Can't handle exchanging states from %s to %s.",
				ExessionAct.nameOf(exstate.state), ExessionAct.nameOf(exchange)); 

		if (rep != null)
			answerSeq = rep.challengeSeq;
		expAnswerSeq = challengeSeq < pages() ? challengeSeq : -1; 

		AnResultset rs = chpage();

		exstate.state = exchange;

		return new ExchangeBlock(trb == null
			? rep.peer
			: trb.synode(), peer, session, exstate)
				.chpage(rs, entities)
				.totalChallenges(totalChallenges)
				.chpagesize(this.chsize)
				.seq(this);
	}

	public ExchangeBlock onExchange(String peer, ExchangeBlock req)
			throws TransException, SQLException {

		if (exstate.state != init && exstate.state != exchange)
			throw new ExchangeException(exchange, "Can't handle exchanging state on state %s", exstate.state); 

		if (req != null)
			answerSeq = req.challengeSeq;
		// challengeSeq++;
		expAnswerSeq = challengeSeq < pages() ? challengeSeq : -1; 

		// AnResultset rs = chpage();

		exstate.state = exchange;

		return new ExchangeBlock(trb == null
			? req.peer
			: trb.synode(), peer, session, exstate)
				.chpage(chpage(), entities)
				.totalChallenges(totalChallenges)
				.chpagesize(this.chsize)
				.seq(this);
	}

	public ExchangeBlock closexchange(ExchangeBlock rep) throws ExchangeException {
		if (exstate.state != init && exstate.state != exchange)
			throw new ExchangeException(exchange, "Can't handle closing state on state %s", exstate.state); 

		try {
			expAnswerSeq = -1; 
			if (rep != null)
				answerSeq = rep.challengeSeq;
			else answerSeq = -1;
			challengeSeq = -1; 

			exstate.state = ready;

			return new ExchangeBlock(trb == null ? rep.peer : trb.synode(), peer, session, new ExessionAct(exstate.mode, close))
				.totalChallenges(totalChallenges)
				.chpagesize(this.chsize)
				.seq(this);
		} finally {
			try {
				trb.delete(exbm.tbl, trb.synrobot())
					.whereEq(exbm.peer, peer)
					.d(trb.instancontxt(trb.synconn(), trb.synrobot()));
			} catch (TransException | SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Retry last page
	 * @param server
	 * @return request message
	 */
	public ExchangeBlock retryLast(String server) {

		exstate.state = restore;

		return new ExchangeBlock(trb == null ? null : trb.synode(), server, session, exstate)
				.requirestore()
				.totalChallenges(totalChallenges)
				.chpagesize(this.chsize)
				.seq(this);
	}

	public ExchangeBlock onRetryLast(String client, ExchangeBlock req) throws ExchangeException {
		if (!eq(session, req.session))
			throw new ExchangeException(ExessionAct.unexpected,
				"[local-session, peer, req-session]:%s,%s,%s", session, client, req.session);

		exstate.state = restore;

		return new ExchangeBlock(trb == null ? req.peer : trb.synode(), client, session, exstate)
			.requirestore()
			.totalChallenges(totalChallenges)
			.chpagesize(this.chsize)
			.seq(this);
	}

	HashMap<String, AnResultset> entities;

	/**
	 * Get challenge page
	 * @return ch-page
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public AnResultset chpage() throws TransException, SQLException {
		// 
		if (trb == null) return null; // test

		Nyquence dn = trb.nyquvect.get(peer);

		AnResultset entbls = (AnResultset) trb.select(chgm.tbl, "ch")
				.je_(exbm.tbl, "bf", chgm.pk, exbm.changeId, "bf." + exbm.peer, constr(peer), constVal(challengeSeq), exbm.seq)
				.col(chgm.entbl)
				.where(op.gt, chgm.nyquence, dn.n) // FIXME
				.groupby(chgm.entbl)
				.rs(trb.instancontxt(trb.synconn(), trb.synrobot()))
				.rs(0);

		while (entbls.next()) {
			String tbl = entbls.getString(chgm.entbl);
			SyntityMeta entm = trb.getSyntityMeta(tbl);

			AnResultset entities = ((AnResultset) trb.select(tbl, "e")
				// .je("e", chgm.tbl, "ch", "ch." + chgm.entbl, constr(tbl), entm.pk, chgm.entfk)
				.je_(chgm.tbl, "ch", "ch." + chgm.entbl, constr(tbl), entm.pk, chgm.entfk)
				.je_(exbm.tbl, "bf", "ch." + chgm.pk, exbm.changeId, constr(peer), exbm.peer, constVal(challengeSeq), exbm.seq)
				.cols_byAlias("e", entm.entCols()).col("e." + entm.pk)
				.where(op.gt, chgm.nyquence, dn.n)
				.orderby(chgm.nyquence)
				.orderby(chgm.synoder)
				.rs(trb.instancontxt(trb.synconn(), trb.synrobot()))
				.rs(0))
				.index0(entm.pk);
			
			entities(tbl, entities);
		}
			
		return trb == null ? null : (AnResultset)trb
			.select(chgm.tbl, "ch")
			.cols(exbm.seq, "ch.*", "sb." + subm.synodee)
			.je_(exbm.tbl, "bf", chgm.pk, exbm.changeId, constr(peer), exbm.peer, constVal(challengeSeq), exbm.seq)
			.je_(subm.tbl, "sb", chgm.pk, subm.changeId)
			.rs(trb.instancontxt(trb.synconn(), trb.synrobot()))
			.rs(0);
	}

	public ExessionPersist entities(String tbl, AnResultset ents) {
		if (entities == null)
			entities = new HashMap<String, AnResultset>();
		entities.put(tbl, ents);
		return this;
	}

	public int pages() {
		return CheapMath.blocks(totalChallenges, chsize);
	}

	/**
	 * update syn_node set session = {session, challengeSeq, answers, ...}
	 * @return this
	 */
	public ExessionPersist persisession() {
		return this;
	}

	///////////////////////////////////////////////////////////////////////////////////
	ExessionPersist forcetest(int total, int... chsize) {
		totalChallenges = total;
		if (!isNull(chsize))
			this.chsize = chsize[0];
		return this;
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
				stats.add(trb.insert(subm.tbl)
					.cols(subm.insertCols())
					.value(subm.insertSubVal(domain, entbl, sub, uids)));
		}
		return missing;
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
			String org, String synoder, String uids, String changeId, String deliffnode)
				throws TransException {
		return trb.delete(chgm.tbl) // delete change log if no subscribers exist
			.whereEq(chgm.pk, changeId)
			.whereEq(chgm.entbl, entitymeta.tbl)
			.whereEq(chgm.domain, org)
			.whereEq(chgm.synoder, synoder)
			.whereEq("0", (Query)trb.select(subm.tbl)
				.col(count(subm.synodee))
				.whereEq(chgm.pk, changeId)
				.where(op.eq, chgm.pk, subm.changeId)
				.where(op.ne, subm.synodee, constr(deliffnode)))
			;
	}
}