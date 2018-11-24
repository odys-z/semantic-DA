package io.odysz.semantics.x;

import io.odysz.transact.x.TransException;

// waiting maven
// public class SemanticException extends TransException {
public class SemanticException extends Exception {
	private static final long serialVersionUID = 1L;

	public SemanticException(String format, Object... args) {
		super(format, args);
	}

}
