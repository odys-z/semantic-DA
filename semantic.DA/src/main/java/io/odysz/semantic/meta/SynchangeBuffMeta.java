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

	public final String pagex;

	final SynChangeMeta chm; 

	public SynchangeBuffMeta(SynChangeMeta chm, String ... conn) {
		super("syn_exchange_buf", conn);
		UIDsep = ",";
		ddlSqlite = Utils.loadTxt(SynchangeBuffMeta.class, "syn_exchange_buf.sqlite.ddl");
		this.chm = chm;

		changeId = "changeId";
		peer     = "peer";
		pagex    = "pagex";
	}

	/** compose function for uids */
	public String uids(String synode, String entityId) {
		return synode + UIDsep + entityId; // Funcall.concatstr(synode, UIDsep, entityId);
	}

	public String[] insertCols() {
		return new String[] {peer, changeId, pagex};
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
