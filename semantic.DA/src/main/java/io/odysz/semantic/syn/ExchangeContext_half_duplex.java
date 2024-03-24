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
public class ExchangeContext_half_duplex {

	ChangeLogs challengebuf;

	AnResultset answer;
	
	String target;

	final SynChangeMeta chgm;

	public ExchangeContext_half_duplex(SynChangeMeta chgm, DBSynsactBuilder_half_duplex localtb, String target) {
		this.target = target;
		this.chgm = chgm;
	}

	public void initChallenge(String target, ChangeLogs diff) throws SemanticException {
		if (!eq(this.target, target))
			throw new SemanticException("Contexts are mismatched: %s vs %s", this.target, target);
		
		this.challengebuf = diff;
	}

	public void addCommit(HashMap<String, Object[]> chcols, ArrayList<ArrayList<Object>> changes,
			HashMap<String, AnResultset> entities) throws SemanticException {
		if (challengebuf != null)
			throw new SemanticException("There is challenges already buffered for committing.");
		challengebuf = new ChangeLogs(chgm)
				.challenge(new AnResultset(chcols).results(changes))
				.entities(entities);
	}

	public void addAnswer(AnResultset answer) throws SemanticException {
		if (challengebuf == null || challengebuf.challenge == null || challengebuf.challenge.size() == 0)
			throw new SemanticException("There is no challenge awaiting for any answer.");
		this.answer = answer;
	}

	public void clear() throws SemanticException {
		if (challengebuf != null && challengebuf.challenge != null && challengebuf.challenge.size() > 0 || answer != null && answer.size() > 0)
			throw new SemanticException("There are suspending operations needed tobe handled before clearing exchange conctext.\nChallenges: %s, Answers: %s",
					challengebuf == null || challengebuf.challenge == null ? 0 : challengebuf.challenge.size(),
					answer == null ? 0 : answer.size());
	}

}
