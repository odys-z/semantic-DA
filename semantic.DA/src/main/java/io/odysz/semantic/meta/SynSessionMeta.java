package io.odysz.semantic.meta;

public class SynSessionMeta extends SemanticTableMeta {

	public final String peer;
	public final String chpage;
	public final String answerx;
	public final String expansx;
	public final String mode;
	public final String state;

	public SynSessionMeta(String... conn) {
		super("syn_sessions", conn);
		
		pk      = "peer";
		peer    = pk;
		chpage  = "chpage";
		answerx = "answerx";
		expansx = "expansx";
		mode    = "mode";
		state   = "state";

		ddlSqlite = loadSqlite(SynSessionMeta.class, "syn_sessions.sqlite.ddl");
	}
}
