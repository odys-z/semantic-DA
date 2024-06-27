package io.odysz.semantic.syn;

import io.odysz.semantic.meta.SemanticTableMeta;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Update;

public class SynSessionMeta extends SemanticTableMeta {

	public String peer;

	public SynSessionMeta(String... conn) {
		super("syn_session", conn);
	}

	public Statement<?> insert(Insert ins) {
		return ins;
	}

	public Update update(Update st) {
		return st;
	}

}
