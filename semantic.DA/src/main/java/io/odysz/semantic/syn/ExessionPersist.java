package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.semantic.syn.Exchanging.init;
import static io.odysz.semantic.syn.Exchanging.mode_client;
import static io.odysz.semantic.syn.Exchanging.mode_server;
import static io.odysz.semantic.syn.Exchanging.ready;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.common.Radix64;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
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

	String peer;

	public ArrayList<ArrayList<Object>> answerPage;
	// public AnResultset challengePage;

	/**
	 * My challenges initiated by
	 * {@link DBSynsactBuilder#initExchange(ExchangePersist, String, io.odysz.semantic.meta.SyntityMeta) initExchange()}.
	 * @param changes 
	 */
	ExessionPersist saveChallenges(ArrayList<ArrayList<Object>> changes) {
		return this;
	}

	/** Answers to my challenges, {@link #mychallenge}, with entities in it. */
	AnResultset saveAnswer(AnResultset answer) { return answer;}

	/**
	 * Create context at client side.
	 * @param chgm
	 * @param localtb local transaction builder
	 * @param target
	 * @param builder 
	 */
	public ExessionPersist(SynChangeMeta chgm, String target) {
		this.peer = target;
		this.chgm = chgm;
		this.exstate = new ExessionAct(mode_client, ready);
		this.session = Radix64.toString((long) (Math.random() * Long.MAX_VALUE));
	}

	/**
	 * Create context at server side.
	 * @param session session id supplied by client
	 * @param chgm
	 * @param localtb
	 * @param target
	 */
	public ExessionPersist(SynChangeMeta chgm, String peer, ExchangeBlock ini) {
		this.session = ini.session;
		this.peer = peer;
		this.chgm = chgm;
		this.exstate = new ExessionAct(mode_server, ready);
	}

	/**
	 * <pre>
	 * exstate.state = init;
	 * expChallengeId = -1;
	 * expAnswerId = -1;
	 * challengeId = 0;
	 * answerId = -1;
	 * </pre>
	 * @param target
	 * @param i 
	 * @return 
	 * @throws SemanticException
	 */
	public ExchangeBlock init(String target, int total) {
		// if (!isblank(this.peer)) throw new SemanticException("Contexts are mismatched: %s vs %s", this.peer, target);

		exstate = new ExessionAct(mode_client, init);
		// expChallengeId = 1;
		expAnswerSeq = 0;
		challengeSeq = 0;
		answerSeq = 0;
		
		return new ExchangeBlock(target, session)
				.allChanges(total)
				.seq(challengeSeq, answerSeq);
	}

	public ExchangeBlock onInit(String client, ExchangeBlock ini, int total) {
		exstate = new ExessionAct(mode_server, init);
		// expChallengeId = 1;
		challengeSeq = 1;
		expAnswerSeq = challengeSeq;
		answerSeq = 0;
	
		return new ExchangeBlock(client, session)
				.allChanges(total)
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
	 */
	public void buffChanges(HashMap<String,Nyquence> mynv, HashMap<String, Object[]> chcols,
			ArrayList<ArrayList<Object>> yourchallenges, HashMap<String, AnResultset> entities)
			throws SemanticException {
		ArrayList<Insert> ins = new ArrayList<Insert>();
	}

	public void clear() throws SemanticException {
	}

	private String session;
	public String session() { return session; }

	private ExessionAct exstate;
	public int exstate() { return exstate.state; }

	public int expAnswerSeq;
	public int challengeSeq;
	public int answerSeq;

	public boolean hasChallenges(DBSyntableBuilder b)
			throws SQLException, TransException {
		return DAHelper.count(b, b.synconn(), chgm.tbl, chgm.synoder, peer) > 0;
	}

	public void nextBlock() {
		challengeSeq++;
		expAnswerSeq = challengeSeq;
	}

	public ExessionPersist expect(String peer, ExchangeBlock req) throws ExchangeException {
		// if ( expChallengeId == req.challengeId && expAnswerSeq == req.answerId
		if ( expAnswerSeq == req.answerId
		  && eq(peer, this.peer) && eq(session, req.session))
			return this;
		throw new ExchangeException(ExessionAct.unexpected,
			// "exp-challenge %s : challenge %s, exp-answer %s : answer %s",
			"req challenge %s, exp-answer %s : answer %s",
			req.challengeId, expAnswerSeq, req.answerId);
	}

	public ExchangeBlock exchange(String peer, ExchangeBlock rep) throws ExchangeException {
		expect(peer, rep);

		answerSeq = rep.challengeId;
		challengeSeq++; 
		expAnswerSeq = challengeSeq; 
		return new ExchangeBlock(peer, session).seq(this);
	}

	public ExchangeBlock onExchange(String peer, ExchangeBlock req) throws ExchangeException {
		expect(peer, req);

		answerSeq = req.challengeId;
		challengeSeq++;
		expAnswerSeq = challengeSeq; 
		return new ExchangeBlock(peer, session).seq(this);
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

		answerSeq = req.challengeId;
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
	 */
	public AnResultset chpage() {
		return null;
	}

}
