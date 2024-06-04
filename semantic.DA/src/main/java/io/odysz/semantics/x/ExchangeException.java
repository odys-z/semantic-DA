package io.odysz.semantics.x;

import io.odysz.semantic.syn.Exchanging;

@SuppressWarnings("serial")
public class ExchangeException extends SemanticException {

	protected Exchanging exstep;

	public ExchangeException(Exchanging expecting, String format, Object... args) {
		super(format, args);
		this.exstep = expecting;
	}

	public ExchangeException(int state, String format, Object... args) {
		super(format, args);
		this.exstep = new Exchanging(Exchanging.mode_client);
		this.exstep.state = state;
	}

	public int requires() {
		return exstep.state;
	}
}
