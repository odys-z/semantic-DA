package io.odysz.semantics.x;

import io.odysz.transact.x.TransException;

 public class SemanticException extends TransException {
	private static final long serialVersionUID = 1L;

	public SemanticException(String format, Object... args) {
		super(format, args);
	}

}
