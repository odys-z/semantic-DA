package io.odysz.semantic.meta;

import io.odysz.common.Utils;
import io.odysz.semantics.meta.Semantation;

/**
 *<a href="./syn_change.sqlite.ddl">syn_change DDL</a>
 *
 * @author odys-z@github.com
 */
public class SynChangeMeta extends SemanticTableMeta {
	/** Separator in uids, ",", for separating fields of pk */
	@Semantation (noDBExists = true)
	public final static String UIDsep = ",";

	public final String domain;
	public final String entbl;

	/**
	 * Entity fk, redundant for convenient, not for synchronizing
	 * 
	 * @deprected no such field since branch "try-mandatory-uid"
	 */
	@Semantation (noDBExists = true)
	public final String entfk;

	/** Format: device + {@link #UIDsep} + entity-id */
	public final String uids;
	public final String crud;
	public final String synoder;
	public final String nyquence;

	/** updated fields when updating an entity */
	public final String updcols;

	// public final String timestamp;

	public SynChangeMeta(String ... conn) {
		super("syn_change", conn);
		// UIDsep = ",";

		ddlSqlite = Utils.loadTxt(SynChangeMeta.class, "syn_change.sqlite.ddl");

		pk       = "cid";
		domain   = "domain";
		entbl    = "tabl";
		crud     = "crud";
		synoder  = "synoder";
		uids     = "uids";
		nyquence = "nyquence";
		updcols  = "updcols";
		// timestamp= "timestamp";

		entfk    = "entfk";
	}

	public String[] insertCols() {
		return new String[] {pk, entbl, crud, synoder, uids, nyquence, updcols};
	}

	/** compose function for uids */
	public static String uids(String synode, String entityId) {
		return synode + UIDsep + entityId; // Funcall.concatstr(synode, UIDsep, entityId);
	}
}
