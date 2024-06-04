package io.odysz.semantic.meta;

import io.odysz.common.Utils;
import io.odysz.semantics.meta.Semantation;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;

/**
 * <a href="./syn_change.sqlite.ddl">syn_change DDL</a>
 *
 * @author odys-z@github.com
 *
 */
public class SynchangeBuffMeta extends SemanticTableMeta {
	/** Separator in uids, ",", for separating fields of pk */
	@Semantation (noDBExists = true)
	public final String UIDsep;

	public final String peer;
	public final String changeId;
//	public final String domain;
//	public final String entbl;
//	/** Entity fk, redundant for convenient, not for synchronizing */
//	public final String entfk;
//	/** Format: device {@link #UIDsep} entity-id */
//	public final String uids;
//	public final String crud;
//	public final String synoder;
//	public final String nyquence;
//
//	/** updated fields when updating an entity */
//	public final String updcols;

	public final String seq;

	final SynChangeMeta chm; 

	static {
	}

	public SynchangeBuffMeta(SynChangeMeta chm, String ... conn) {
		super("syn_exchange_buf", conn);
		UIDsep = ",";
		ddlSqlite = Utils.loadTxt(SynchangeBuffMeta.class, "syn_exchange_buf.sqlite.ddl");
		this.chm = chm;

		// pk      = "cid";
		changeId= "changeId";
		peer    = "peer";
//		domain  = "domain";
//		entbl   = "tabl";
//		entfk   = "entfk";
//		crud    = "crud";
//		synoder = "synoder";
//		uids    = "uids";
//		nyquence= "nyquence";
//		updcols = "updcols";
		seq     = "seq";
	}

//	public String[] cols() {
//		return new String[] {pk, entbl, crud, synoder, uids, nyquence, updcols};
//	}

	/** compose function for uids */
	public String uids(String synode, String entityId) {
		return synode + UIDsep + entityId; // Funcall.concatstr(synode, UIDsep, entityId);
	}

	public String[] insertCols() {
		// return new String[] {peer, changeId, domain, entbl, entfk, crud, synoder, uids, nyquence, updcols};
		return new String[] {peer, changeId, seq};
	}

	public Object[] selectCols(String peer, int seq) {
		return new Object[] {Funcall.constr(peer), chm.pk, new ExprPart(seq)};
//			chm.domain, chm.entbl, chm.entfk, chm.crud,
//			chm.synoder, chm.uids, chm.nyquence, chm.updcols};
	}

	/**
	 * ISSUE: why not merge with {@link SyntityMeta#replace()}?
	 * @return
	 * @throws SQLException
	 * @throws TransException
	public SynChangeTempMeta replace() throws SQLException, TransException {
		TableMeta mdb = Connects.getMeta(conn, tbl);
		if (!(mdb instanceof SyntityMeta))
			DBSynmantics.replaceMeta(tbl, this, conn);
		if (isNull(this.ftypes) && mdb.ftypes() != null)
			this.ftypes = mdb.ftypes();
		return this;
	}
	 */

}
