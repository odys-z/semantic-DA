package io.odysz.semantics.x;

import io.odysz.semantic.syn.Exchanging;
import io.odysz.semantic.syn.ExessionAct;
import io.odysz.semantic.syn.ExessionPersist;

@SuppressWarnings("serial")
public class ExchangeException extends SemanticException {

	protected Exchanging exstep;
	public ExessionPersist persist;

	public ExchangeException(Exchanging expecting, ExessionPersist xp, String format, Object... args) {
		super(format, args);
		this.exstep = expecting;
		this.persist = xp; 
	}

	public ExchangeException(int expect, ExessionPersist xp, String format, Object... args) {
		super(format, args);
		this.exstep = new Exchanging(ExessionAct.mode_client);
		this.exstep.state = expect;
		this.persist = xp; 
	}

	public int requires() {
		return exstep.state;
	}
}
