package io.odysz.semantic;

import io.odysz.semantic.meta.SyntityMeta;

public class T_PhotoMeta extends SyntityMeta {

	
	static {
		sqlite = "h_photos.sqlite.ddl";
	}

	public final String uri;
		
	public T_PhotoMeta(String... conn) {
		super("h_photos", "pid", conn);
		
		uri = "uri";
	}

}
