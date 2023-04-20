package io.odysz.semantic;

import io.odysz.semantic.syn.T_DocTableMeta;
import io.odysz.semantics.x.SemanticException;

public class T_PhotoMeta extends T_DocTableMeta {

	static {
		sqlite = "h_photos.sqlite.ddl";
	}

	public final String uri;
		
	public T_PhotoMeta(String conn) throws SemanticException {
		super("h_photos", "pid", conn);
		
		uri = "uri";
	}

}
