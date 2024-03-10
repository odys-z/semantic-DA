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
	/** Separator in uids, ",", for separating fields of pk */
	public final String UIDsep;

	public final String org;
	public final String entbl;
	// public final String entfk;
	/** Format: device {@link #UIDsep} entity-id */
	public final String uids;
	public final String crud;
	public final String synoder;
	public final String nyquence;

	public final String subs;

	static {
	}

	public SynChangeMeta(String ... conn) {
		super("syn_change", conn);
		ddlSqlite = Utils.loadTxt(SynChangeMeta.class, "syn_change.sqlite.ddl");

		UIDsep = ",";
		// pk    = "uids";
		org   = "org";
		entbl = "tabl";
		// entfk = "entfk";
		crud = "crud";
		synoder = "synoder";
		uids = "uids";
		nyquence = "nyquence";
		
		subs = "sub";
	}

	public String[] cols() {
		return new String[] {pk, entbl, crud, synoder, uids};
	}

}
