package io.odysz.semantic.meta;

import io.odysz.semantics.meta.TableMeta;

/**
 *<a href="./syn_change.sqlite.ddl">syn_change DDL</a>
 *
 * @author odys-z@github.com
 *
 */
public class SynChangeMeta extends TableMeta {

	public final String entbl;
	public final String entfk;
	public final String clientpath;
	public final String clientpath2;
	public final String crud;
	public final String synoder;
	public final String subscribe;
	public final String nyquence;

	
	static {
		sqlite = "syn_change.sqlite.ddl";
	}

	public SynChangeMeta(String ... conn) {
		super("syn_change", conn);

		entbl = "tabl";
		entfk = "recId";
		clientpath = "clientpath";
		clientpath2 = "clientpath2";
		crud = "crud";
		synoder = "synoder";
		subscribe = "subs";
		nyquence = "nyquence";
	}

	public String[] cols() {
		return new String[] {pk, entbl, entfk, clientpath, clientpath2, synoder, crud};
	}

}
