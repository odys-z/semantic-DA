package io.odysz.semantic.meta;

import io.odysz.semantics.meta.Semantation;
import io.odysz.transact.sql.parts.AbsPart;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;

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
	public final String seq;

	/** updated fields when updating an entity */
	public final String updcols;

	public SynChangeMeta(String ... conn) {
		super("syn_change", conn);

		ddlSqlite = loadSqlite(SynChangeMeta.class, "syn_change.sqlite.ddl");

		pk       = "cid";
		domain   = "domain";
		entbl    = "tabl";
		crud     = "crud";
		synoder  = "synoder";
		uids     = "uids";
		nyquence = "nyquence";
		seq      = "seq";
		updcols  = "updcols";

		entfk    = "entfk";
	}

	public String[] insertCols() {
		return new String[] {pk, entbl, crud, synoder, uids, nyquence, updcols};
	}

	/**
	 * Composing function for uids
	 * @param synoder
	 * @param entityId
	 * @return "synoder + UIDsep + entityId"
	 */
	public static String uids(String synoder, String entityId) {
		return synoder + UIDsep + entityId; // Funcall.concatstr(synode, UIDsep, entityId);
	}

	/**
	 * 
	 * @param synode
	 * @param eid {@link Resulving} object or the return of {@link ExprPart#constr(String)}.
	 * @return
	 */
	public static AbsPart uids(String synode, ExprPart eid) {
		return Funcall.concatstr(synode, UIDsep, eid);
	}
}
