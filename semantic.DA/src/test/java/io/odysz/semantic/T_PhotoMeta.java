package io.odysz.semantic;

import io.odysz.semantic.meta.SyntityMeta;

public class T_PhotoMeta extends SyntityMeta {

	public static String ddlSqlite =
			"drop table if exists h_potos;\n" +
			"create table h_photos (\n" +
			"	pid    varchar2(64) not null,\n" +
			"	pname  varchar2(12) not null,\n" +
			"	device varchar2(12) not null,\n" +
			"	clientpath varchar2(12) not null,\n" +
			"	uri        text," +
			"	size       number," +
			"	mime       text," +
			"	shareby    varchar2(12)," +
			"	sharedate  number," +
			"	synflag    char(1) not null,\n" +
			"	synyquist  number  not null\n" +
			");";
		
	public T_PhotoMeta() {
		super("h_photos", "pid", null);
	}

}
