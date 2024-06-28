package io.odysz.semantic.meta;

import io.odysz.common.Utils;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Update;

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

		ddlSqlite = Utils.loadTxt(PeersMeta.class, "syn_sessions.sqlite.ddl");
	}

	public Statement<?> insertSession(Insert ins, String peer) {
		return ins
			.nv(this.peer, peer)
			.nv(chpage,  -1)
			.nv(answerx, -1)
			.nv(expansx, -1);
	}

	public Update update(Update st) {
		return st;
	}

}
