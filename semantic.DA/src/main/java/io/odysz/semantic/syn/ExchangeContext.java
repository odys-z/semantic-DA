package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isNull;

import java.sql.ResultSet;
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

	ChangeLogs commit;

	AnResultset answer;
	
	String target;

	// ChangeLogs challengeWithEnts;

	final SynChangeMeta chgm;

	public ExchangeContext(SynChangeMeta chgm, DBSynsactBuilder localtb, String target) {
		this.target = target;
		this.chgm = chgm;
	}

	public void initChallenge(String target, ChangeLogs diff) throws SemanticException {
		if (!eq(this.target, target))
			throw new SemanticException("Contexts are mismatched: %s vs %s", this.target, target);
		
		this.commit = diff;
	}

	public void addCommit(HashMap<String, Object[]> chcols, ArrayList<ArrayList<Object>> changes,
			HashMap<String, AnResultset> entities) throws SemanticException {
		if (commit != null)
			throw new SemanticException("There is challenge already buffered for committing.");
		commit = new ChangeLogs(chgm)
				.challenge(new AnResultset(chcols).results(changes))
				.entities(entities);
	}

	public void addAnswer(AnResultset answer) throws SemanticException {
		if (commit == null || commit.challenge == null || commit.challenge.size() == 0)
			throw new SemanticException("There is no challenge awaiting for any answer.");
		this.answer = answer;
	}

	public void clear() throws SemanticException {
		if (commit != null && commit.challenge.size() > 0 || answer != null && answer.size() > 0)
			throw new SemanticException("There are suspending operations needed tobe handled before clearing exchange conctext.");
	}

}
