package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;

import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantics.x.SemanticException;

/**
 * Handling exchange session with remote node.
 * This is different from {@link DBSyntext} which is used for
 * handling local data integration and database semantics. 
 * 
 * @author Ody
 */
public class ExchangeContext {

	/** Changes to be committed according to the other's challenges */
	ChangeLogs onchanges;

	/**
	 * My challenges initiated by
	 * {@link DBSynsactBuilder#initExchange(ExchangeContext, String, io.odysz.semantic.meta.SyntityMeta) initExchange()}.
	 */
	ChangeLogs mychallenge;

	/** Answers to my challenges, {@link #mychallenge}, with entities in it. */
	AnResultset answer;
	
	String target;

	final SynChangeMeta chgm;

	/**
	 * Current nyquence stamp for selecting challenges
	 * @deprecated
	 */
	public Nyquence nyqstep;

	public ExchangeContext(SynChangeMeta chgm, DBSynsactBuilder localtb, String target) {
		this.target = target;
		this.chgm = chgm;
	}

	public void initChallenge(String target, ChangeLogs diff) throws SemanticException {
		if (!eq(this.target, target))
			throw new SemanticException("Contexts are mismatched: %s vs %s", this.target, target);
		
		this.mychallenge = diff;
	}

	/**
	 * Buffering changes while responding to {@code challenges}.
	 * @param chcols
	 * @param yourchallenges
	 * @param entities
	 * @throws SemanticException
	 */
	public void buffChanges(HashMap<String, Object[]> chcols, ArrayList<ArrayList<Object>> yourchallenges,
			HashMap<String, AnResultset> entities) throws SemanticException {
		if (onchanges != null && onchanges.challenge != null && onchanges.challenge.size() > 0)
			throw new SemanticException("There is challenges already buffered for committing.");
		onchanges = new ChangeLogs(chgm)
				.challenge(new AnResultset(chcols).results(yourchallenges))
				.entities(entities);
	}

	public void addAnswer(AnResultset answer) throws SemanticException {
		if (mychallenge == null || mychallenge.challenge == null || mychallenge.challenge.size() == 0)
			throw new SemanticException("There is no challenge awaiting for any answer.");
		this.answer = answer;
	}

	public void clear() throws SemanticException {
		if (onchanges != null && onchanges.challenge != null && onchanges.challenge.size() > 0 || answer != null && answer.size() > 0)
			throw new SemanticException("There are suspending operations needed tobe handled before clearing exchange conctext.\nChallenges: %s, Answers: %s",
					onchanges == null || onchanges.challenge == null ? 0 : onchanges.challenge.size(),
					answer == null ? 0 : answer.size());
	}

}
