package io.odysz.semantics.x;

import io.odysz.semantics.SemanticObject;
import io.odysz.transact.x.TransException;

 public class SemanticException extends TransException {
	private static final long serialVersionUID = 1L;
	private SemanticObject ex;

	public SemanticException(String format, Object... args) {
		super(format, args);
//		if (args != null && args.length > 0
//			&& args[args.length - 1] instanceof SemanticObject)
//			ex = (SemanticObject) args[args.length - 1];
	}

	/**Get the exception message object that can be serialized to json and send to client.
	 * @return
	 */
	public SemanticObject ex() {
		return ex;
	}

	public SemanticException ex(SemanticObject ex) {
		this.ex = ex;
		return this;
	}
	
//	public SemanticException put(String p, Object v) {
//		ex.put(p, v);
//		return this;
//	}
}
