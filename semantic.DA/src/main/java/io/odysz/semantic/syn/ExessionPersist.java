package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.semantic.syn.Exchanging.init;
import static io.odysz.semantic.syn.Exchanging.mode_client;
import static io.odysz.semantic.syn.Exchanging.mode_server;
import static io.odysz.semantic.syn.Exchanging.ready;

import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.common.Radix64;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
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
		
		exstate = new ExessionAct(mode_client, init);

		challengeSeq = -1;
		expAnswerSeq = challengeSeq;
		answerSeq = -1;
		
		return new ExchangeBlock(peer, session)
			.totalChanges(trb == null ? 0 : DAHelper.count(trb, trb.synconn(), exbm.tbl, exbm.peer, peer))
			.seq(challengeSeq, answerSeq);
	}

	public ExchangeBlock onInit(ExchangeBlock ini) throws TransException, SQLException {
		exstate = new ExessionAct(mode_server, init);
		
		String conn = trb.basictx().connId();
		if (trb != null) {
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
		expAnswerSeq = challengeSeq;
		answerSeq = ini.challengeSeq;
	
		return new ExchangeBlock(peer, session)
				.totalChanges(trb == null ? 0 : DAHelper.count(trb, trb.synconn(), exbm.tbl, exbm.peer, peer))
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

	public int expAnswerSeq;
	/** Challenging sequence number, i. e. current page */
	public int challengeSeq;
	/** challenge page size */
	protected final int chsize;

	public int answerSeq;

	/**
	 * Has pages to be send to in {@link SynchangeBuffMeta}.tbl.
	 * @param b synchronizing transaction builder
	 * @return
	 * @throws SQLException
	 * @throws TransException
	 */
	public boolean hasNextChpages(DBSyntableBuilder b)
			throws SQLException, TransException {
		return DAHelper.count(b, b.synconn(), exbm.tbl, exbm.peer, peer, exbm.seq, -1) > 0;
	}

//	public void nextBlock() {
//		challengeSeq++;
//		expAnswerSeq = challengeSeq;
//	}

	public ExessionPersist expect(String peer, ExchangeBlock req) throws ExchangeException {
		// if ( expChallengeId == req.challengeId && expAnswerSeq == req.answerId
		if ( req == null
		  || expAnswerSeq == req.answerSeq
		  && eq(peer, this.peer) && eq(session, req.session))
			return this;
		throw new ExchangeException(ExessionAct.unexpected,
			// "exp-challenge %s : challenge %s, exp-answer %s : answer %s",
			"req challenge %s, exp-answer %s : answer %s",
			req.challengeSeq, expAnswerSeq, req.answerSeq);
	}
	
	public ExessionPersist nextChpage() {
		challengeSeq++;
		expAnswerSeq = challengeSeq;
		return this;
	}

	public ExchangeBlock exchange(String peer, ExchangeBlock rep) throws TransException, SQLException {
		expect(peer, rep);

		if (rep != null)
			answerSeq = rep.challengeSeq;
		// challengeSeq++; 
		expAnswerSeq = challengeSeq; 

		AnResultset rs = chpage();

		return new ExchangeBlock(peer, session).chpage(rs).seq(this);
	}

	public ExchangeBlock onExchange(String peer, ExchangeBlock req) throws TransException, SQLException {
		expect(peer, req);

		if (req != null)
			answerSeq = req.challengeSeq;
		// challengeSeq++;
		expAnswerSeq = challengeSeq; 

		AnResultset rs = chpage();
		return new ExchangeBlock(peer, session).chpage(rs).seq(this);
	}

	public ExchangeBlock closexchange(String server, ExchangeBlock rep) throws ExchangeException {
		expect(peer, rep);

		expAnswerSeq = -1; 
		answerSeq = challengeSeq;
		challengeSeq = -1; 
		return new ExchangeBlock(server, session).seq(this);
	}

	public ExchangeBlock onclose(String peer, ExchangeBlock req) throws ExchangeException {
		expect(peer, req);

		expAnswerSeq = -1; 
		challengeSeq = -1; 
		return new ExchangeBlock(peer, session).seq(this);
	}

	public ExchangeBlock retry(String server) {
		return new ExchangeBlock(server, session)
				.requirestore()
				.seq(this);
	}

	public ExchangeBlock onRetry(String client, ExchangeBlock req) throws ExchangeException {
		if (!eq(session, req.session))
			throw new ExchangeException(ExessionAct.unexpected,
				"[local-session, peer, req-session]:%s,%s,%s", session, client, req.session);
		
		/*
		if ( answerSeq == req.answerId
		  && challengeSeq == req.challengeId + 1) {
			// can be restored
			answerSeq = req.challengeId;
			// challengeSeq = req.answerId; 
			// expChallengeId = challengeSeq + 1;
			expAnswerSeq = challengeSeq;

			answerPage = null;
			return new ExchangeBlock(client, session)
				.requirestore()
				.seq(this);
		}
		else {
			// something lost
//			answerId = req.answerId;
//			challengeId = req.challengeId; 
//			expChallengeId = challengeId + 1;
//			expAnswerId = challengeId;

			// ignore the failed page
			throw new ExchangeException(ExessionAct.unexpected, "FIXME: ingore the failed page");
		}
		*/

		answerSeq = req.challengeSeq;
		expAnswerSeq = challengeSeq;

		answerPage = loadAnswers(answerSeq);
		return new ExchangeBlock(client, session)
			.requirestore()
			.seq(this);
	}

	private ArrayList<ArrayList<Object>> loadAnswers(int answerSeq) {
		return null;
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
		return (AnResultset)trb
			.select(exbm.tbl, "bf")
			.cols("chg.*", "sub." + subm.synodee)
			.je_(chgm.tbl, "chg", exbm.changeId, chgm.pk, exbm.peer, Funcall.constr(peer))
			.je_(subm.tbl, "sub", "chg." + chgm.pk, subm.changeId)
			.page(challengeSeq, chsize)
			.whereEq(exbm.peer, peer)
			.rs(trb.instancontxt(trb.synconn(), trb.synrobot()))
			.rs(0);
	}
}
