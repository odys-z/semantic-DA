package io.odysz.semantic;

import io.odysz.semantics.meta.TableMeta;

public class SynTableMeta extends TableMeta {

	public final String recTabl;
	public final String recId;
	public final String clientpath;
	public final String clientpath2;

	public SynTableMeta(String tbl, String... conn) {
		super(tbl, conn);
		
		recTabl = "tabl";
		recId = "recId";
		clientpath = "clientpath";
		clientpath2 = "clientpath2";
	}

}
