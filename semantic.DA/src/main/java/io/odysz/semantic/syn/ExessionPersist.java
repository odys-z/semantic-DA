package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.semantic.syn.ExessionAct.*;

import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.common.CheapMath;
import io.odysz.common.Radix64;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.parts.Logic.op;
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

	public ArrayList<ArrayList<Object>> answerPage;

	DBSyntableBuilder trb;

	/**
	 * Append local changes according to challenges initiated by
	 * {@link DBSynsactBuilder#initExchange(ExchangePersist, String, io.odysz.semantic.meta.SyntityMeta) initExchange()}.
	 * @param changes 
	 */
	ExessionPersist addChangeBuf(ArrayList<ArrayList<Object>> changes) {
		return this;
	}

	/** Answers to my challenges, {@link #mychallenge}, with entities in it. */
	ExessionPersist saveAnswer(ArrayList<ArrayList<Object>> answer) {
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
	public ExessionPersist(DBSyntableBuilder tb, SynChangeMeta chgm, SynSubsMeta subm, SynchangeBuffMeta exbm, String target) {
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
	public ExessionPersist(DBSyntableBuilder tb, SynChangeMeta chgm, SynSubsMeta subm, SynchangeBuffMeta exbm, String peer, ExchangeBlock ini) {
		this.trb = tb;
		this.exbm = exbm;
		this.session = ini.session;
		this.peer = peer;
		this.chgm = chgm;
		this.subm = subm;
		this.exstate = new ExessionAct(mode_server, ready);
		this.chsize = 480;
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

	/**
	 * Buffering changes while responding to {@code challenges}.
	 * 
	 * @param mynv 
	 * @param chcols
	 * @param yourchallenges
	 * @param entities
	 * @throws SemanticException
	public void buffChanges(HashMap<String,Nyquence> mynv, HashMap<String, Object[]> chcols,
			ArrayList<ArrayList<Object>> yourchallenges, HashMap<String, AnResultset> entities)
			throws SemanticException {
		ArrayList<Insert> ins = new ArrayList<Insert>();
	}
	 */

	public void clear() throws SemanticException { }

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
	
	public boolean nextChpage() {
		int pages = pages();
		if (challengeSeq < pages)
			challengeSeq++;

		expAnswerSeq = challengeSeq;

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
				? rep.peer : trb.synode(), peer, session, exstate).chpage(rs)
				.totalChallenges(totalChallenges)
				.chpagesize(this.chsize)
				.seq(this);
	}

	public ExchangeBlock onExchange(String peer, ExchangeBlock req) throws TransException, SQLException {
		if (exstate.state != init && exstate.state != exchange)
			throw new ExchangeException(exchange, "Can't handle exchanging state on state %s", exstate.state); 

		if (req != null)
			answerSeq = req.challengeSeq;
		// challengeSeq++;
		expAnswerSeq = challengeSeq < pages() ? challengeSeq : -1; 

		AnResultset rs = chpage();

		exstate.state = exchange;

		return new ExchangeBlock(trb == null ? req.peer : trb.synode(), peer, session, exstate)
				.chpage(rs)
				.totalChallenges(totalChallenges)
				.chpagesize(this.chsize)
				.seq(this);
	}

	public ExchangeBlock closexchange(String server, ExchangeBlock rep) throws ExchangeException {
		if (exstate.state != init && exstate.state != exchange)
			throw new ExchangeException(exchange, "Can't handle closing state on state %s", exstate.state); 

		expAnswerSeq = -1; 
		if (rep != null)
			answerSeq = rep.challengeSeq;
		else answerSeq = -1;
		challengeSeq = -1; 

		exstate.state = ready;

		return new ExchangeBlock(trb == null ? rep.peer : trb.synode(), server, session, new ExessionAct(exstate.mode, close))
				.totalChallenges(totalChallenges)
				.chpagesize(this.chsize)
				.seq(this);
	}

	public ExchangeBlock onclose(String peer, ExchangeBlock req) throws ExchangeException {

		expAnswerSeq = -1; 
		challengeSeq = -1; 

		exstate.state = ready;
		return new ExchangeBlock(trb == null ? req.peer : trb.synode(), peer, session, exstate)
				.totalChallenges(totalChallenges)
				.chpagesize(this.chsize)
				.seq(this);
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

	/**
	 * Get challenge page
	 * @return ch-page
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public AnResultset chpage() throws TransException, SQLException {
		// 
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		if (trb != null) {
			Query bf = trb
				.select(exbm.tbl, "bf")
				.cols("chg.*", "sub." + subm.synodee)
				.je_(chgm.tbl, "chg", exbm.changeId, chgm.pk, exbm.peer, Funcall.constr(peer))
				.je_(subm.tbl, "sub", "chg." + chgm.pk, subm.changeId)
				.page(challengeSeq, chsize) // page() is not working in with clause
				.whereEq(exbm.peer, peer)
				;
			trb.update(exbm.tbl, trb.synrobot())
				.nv(exbm.seq, challengeSeq)
				.whereIn(exbm.changeId, trb.with(bf).select("bf").col(exbm.changeId))
				.u(trb.instancontxt(trb.synconn(), trb.synrobot()))
				;
			return trb == null ? null : (AnResultset)bf
				.rs(trb.instancontxt(trb.synconn(), trb.synrobot()))
				.rs(0);
		}
		return null;
	}

	public int pages() {
		return CheapMath.blocks(totalChallenges, chsize);
	}

	public ExessionPersist forcetest(int total, int... chsize) {
		totalChallenges = total;
		if (!isNull(chsize))
			this.chsize = chsize[0];
		return this;
	}
}
