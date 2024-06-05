package io.odysz.semantics.x;

import io.odysz.semantic.syn.Exchanging;

@SuppressWarnings("serial")
public class ExchangeException extends SemanticException {

	protected Exchanging exstep;
	// public ExessionPersist persist;

	public ExchangeException(Exchanging expecting, String format, Object... args) {
		super(format, args);
		this.exstep = expecting;
		// this.persist = xp; 
	}

	// public ExchangeException(int expect, ExessionPersist xp, String format, Object... args) {
	public ExchangeException(int expect, String format, Object... args) {
		super(format, args);
		this.exstep = new Exchanging(Exchanging.mode_client);
		this.exstep.state = expect;
		// this.persist = xp; 
	}

	public int requires() {
		return exstep.state;
	}
}
