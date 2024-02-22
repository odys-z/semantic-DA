package io.odysz.semantic.meta;

import io.odysz.common.Utils;
import io.odysz.semantics.meta.TableMeta;

/**
 *<a href="./syn_change.sqlite.ddl">syn_change DDL</a>
 *
 * @author odys-z@github.com
 *
 */
public class SynChangeMeta extends TableMeta {

	public final String org;
	public final String entbl;
	public final String entfk;
	/** device:fullpath */
	public final String uids;
	public final String crud;
	public final String synoder;
	public final String nyquence;

	static {
		 ddlSqlite = Utils.loadTxt(SynChangeMeta.class, "syn_change.sqlite.ddl");
	}

	public SynChangeMeta(String ... conn) {
		super("syn_change", conn);

		// pk    = "uids";
		org   = "org";
		entbl = "tabl";
		entfk = "entfk";
		crud = "cud";
		synoder = "synoder";
		uids = "uids";
		nyquence = "nyquence";
	}

	public String[] cols() {
		return new String[] {pk, entbl, entfk, synoder, crud};
	}

}
