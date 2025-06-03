package io.odysz.semantic.meta;

public class SynDocRefMeta extends SemanticTableMeta {

	public final String syntabl;
	public final String fromPeer;
	public final String uids;
	public final int tried;

	public SynDocRefMeta(String... conn) {
		super("syn_docref", conn);
		ddlSqlite = loadSqlite(SynchangeBuffMeta.class, "syn_docref.sqlite.ddl");

		syntabl  = "syntabl";
		fromPeer = "fromPeer";
		uids     = "uids";
		tried    = 0;
	}
	

}
