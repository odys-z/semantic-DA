package io.odysz.semantic.syn;

import static io.odysz.common.Utils.loadTxt;

import io.odysz.transact.x.TransException;

public class T_PhotoMeta extends T_DocTableMeta {

	public final String exif;
		
	public T_PhotoMeta(String conn) throws TransException {
		super("h_photos", "pid", "family", conn);
		
		exif = "exif";

		ddlSqlite = loadTxt(T_PhotoMeta.class, "h_photos.sqlite.ddl");
	}
	
	public String device() { return synoder; }

}
