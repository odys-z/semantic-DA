package io.odysz.semantic.syn;

import io.odysz.transact.x.TransException;

public class T_PhotoMeta extends T_DocTableMeta {

	static {
		sqlite = "h_photos.sqlite.ddl";
	}

	public final String uri;
		
	public T_PhotoMeta(String conn) throws TransException {
		super("h_photos", "pid", conn);
		
		uri = "uri";
	}

}
