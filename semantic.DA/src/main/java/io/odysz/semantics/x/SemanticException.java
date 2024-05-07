package io.odysz.semantics.x;

import io.odysz.semantics.SemanticObject;
import io.odysz.transact.x.TransException;

 public class SemanticException extends TransException {
	private static final long serialVersionUID = 1L;
	private SemanticObject ex;

	public SemanticException(String format, Object... args) {
		super(format, args);
	}

	/**
	 * Get the exception message object that can be serialized to json and send to client.
	 * @return ex object
	 */
	public SemanticObject ex() {
		if (ex == null)
			ex = new SemanticObject();
		return ex;
	}

	/**
	 * Set object for details of exception
	 * 
	 * @param ex
	 * @return this
	 */
	public SemanticException ex(SemanticObject ex) {
		this.ex = ex;
		return this;
	}

}
