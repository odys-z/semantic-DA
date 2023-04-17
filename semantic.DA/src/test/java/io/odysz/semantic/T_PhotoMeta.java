package io.odysz.semantic;

import io.odysz.semantic.docsync.T_DocTableMeta;

public class T_PhotoMeta extends T_DocTableMeta {

	static {
		sqlite = "h_photos.sqlite.ddl";
	}

	public final String uri;
		
	public T_PhotoMeta(String conn) {
		super("h_photos", "pid", conn);
		
		uri = "uri";
	}

}
