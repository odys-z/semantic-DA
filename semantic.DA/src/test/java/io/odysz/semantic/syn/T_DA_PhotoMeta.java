package io.odysz.semantic.syn;

import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.transact.x.TransException;

public class T_DA_PhotoMeta extends ExpDocTableMeta {

	public final String exif;

	public T_DA_PhotoMeta(String conn) throws TransException {
		super("h_photos", "pid", "device", conn);
		ddlSqlite = loadSqlite(T_DA_PhotoMeta.class, "h_photos.sqlite.ddl");
		exif = "exif";
	}

}
