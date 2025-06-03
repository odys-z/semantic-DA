package io.odysz.semantic.meta;

public class SynDocRefMeta extends SemanticTableMeta {

	public final String syntabl;
	public final String fromPeer;
	public final String io_oz_synuid;
	public final String tried;

	public SynDocRefMeta(String... conn) {
		super("syn_docref", conn);
		ddlSqlite = loadSqlite(SynchangeBuffMeta.class, "syn_docref.sqlite.ddl");

		syntabl      = "syntabl";
		fromPeer     = "fromPeer";
		io_oz_synuid = "uids";
		tried        = "tried";
	}
	

}
