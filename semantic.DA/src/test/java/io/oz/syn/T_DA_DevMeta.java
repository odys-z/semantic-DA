package io.oz.syn;

import static io.odysz.common.Utils.loadTxt;

import io.odysz.semantic.meta.SyntityMeta;

public class T_DA_DevMeta extends SyntityMeta {
	final String devname;
	final String mac;
	final String org;
	final String owner;
	final String cdate;

	public T_DA_DevMeta(String conn) {
		super("doc_devices", "synode0", "device", conn);
		this.devname = "devname";
		this.mac     = "mac";
		this.org     = "org";
		this.owner   = "owner";
		this.cdate   = "cdate";
		
		this.ddlSqlite = loadTxt(T_DA_DevMeta.class, "doc_devices.sqlite.ddl");
	}

}
