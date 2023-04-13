package io.odysz.semantic;

import java.io.IOException;
import java.net.URISyntaxException;

import io.odysz.common.Utils;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.SyntityMeta;

public class T_PhotoMeta extends SyntityMeta {

	public static String ddlSqlite;
	
	static {
		try {
			ddlSqlite = Utils.loadTxt(T_PhotoMeta.class, "h_photos.sqlite.ddl");
			if (Connects.flag_printSql > 0)
				Utils.logi(ddlSqlite);
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public final String uri;
		
	public T_PhotoMeta() {
		super("h_photos", "pid", null);
		
		uri = "uri";
	}

}
