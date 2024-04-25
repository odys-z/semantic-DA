package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.semantic.syn.Exchanging.*;

import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.common.Radix64;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;

/**
 * Persisting exchange session with remote node, using temporary tables.
 * This is different from {@link DBSyntext} which is used for
 * handling local data integration and database semantics. 
 * 
 * @author Ody
 */
public class ExessionPersisting {
	final SynChangeMeta chgm;

	String target;

	/**
	 * My challenges initiated by
	 * {@link DBSynsactBuilder#initExchange(ExchangePersist, String, io.odysz.semantic.meta.SyntityMeta) initExchange()}.
	 * @param diff 
	 */
	ChangeLogs saveChallenges(ChangeLogs diff) {
		return null;
	}

	/** Answers to my challenges, {@link #mychallenge}, with entities in it. */
	AnResultset saveAnswer(AnResultset answer) { return answer;}

	/**
	 * Create context at client side.
	 * @param chgm
	 * @param localtb local transaction builder
	 * @param target
	 */
	public ExessionPersisting(SynChangeMeta chgm, String target) {
		this.target = target;
		this.chgm = chgm;
		this.exstate = new Exchanging(mode_client);
		this.session = Radix64.toString((long) (Math.random() * Long.MAX_VALUE));
	}

	/**
	 * Create context at server side.
	 * @param session session id supplied by client
	 * @param chgm
	 * @param localtb
	 * @param target
	 */
	public ExessionPersisting(String session, SynChangeMeta chgm, String target) {
		this.target = target;
		this.chgm = chgm;
		this.exstate = new Exchanging(mode_server);
		this.session = session;
	}

	public void initChallenge(String target, ChangeLogs diff) throws SemanticException {
		if (!eq(this.target, target))
			throw new SemanticException("Contexts are mismatched: %s vs %s", this.target, target);

		saveChallenges(diff);
		exstate.initexchange();
	}

	/** Local (server) nyquences when accepted exchanging request, used for restore onAck step at server.*/
	public HashMap<String, Nyquence> exNyquvect;

	/**
	 * Buffering changes while responding to {@code challenges}.
	 * 
	 * @param myNyquvect 
	 * @param chcols
	 * @param yourchallenges
	 * @param entities
	 * @throws SemanticException
	 */
	public void buffChanges(HashMap<String,Nyquence> myNyquvect, HashMap<String, Object[]> chcols,
			ArrayList<ArrayList<Object>> yourchallenges, HashMap<String, AnResultset> entities)
			throws SemanticException {

//		onchanges = new ExchangeBlock(srcnode, null)
//				.challenge(new AnResultset(chcols).results(yourchallenges))
//				.entities(entities);

		exNyquvect = Nyquence.clone(myNyquvect);
	}

	public void clear() throws SemanticException {
		exstate.close();
	}

	private String session;
	public String session() { return session; }

	private Exchanging exstate;
	public int exstate() { return exstate.state; }

	public void can(int go) throws ExchangeException {
		exstate.can(go);
	}

	public int expChallengeID;
	public int expAnswerID;

	public int challengeId;
	public int answerId;
}
