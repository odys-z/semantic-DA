package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.isNull;

import java.sql.SQLException;

import io.odysz.common.Utils;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.syn.DBSynmantics;
import io.odysz.semantics.meta.Semantation;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.x.TransException;

/**
 *<a href="./syn_change.sqlite.ddl">syn_change DDL</a>
 *
 * @author odys-z@github.com
 *
 */
public class SynChangeMeta extends TableMeta {
	/** Separator in uids, ",", for separating fields of pk */
	@Semantation (noDBExists = true)
	public final String UIDsep;

	// public final String cid;
	public final String domain;
	public final String entbl;
	/** Entity fk, redundant for convenient, not for synchronizing */
	public final String entfk;
	/** Format: device {@link #UIDsep} entity-id */
	public final String uids;
	public final String crud;
	public final String synoder;
	public final String nyquence;

	/** updated fields when updating an entity */
	public final String updcols;

	static {
	}

	public SynChangeMeta(String ... conn) {
		super("syn_change", conn);
		UIDsep = ",";

		ddlSqlite = Utils.loadTxt(SynChangeMeta.class, "syn_change.sqlite.ddl");

		pk      = "cid";
		domain  = "domain";
		entbl   = "tabl";
		entfk   = "entfk";
		crud    = "crud";
		synoder = "synoder";
		uids    = "uids";
		nyquence= "nyquence";
		updcols = "updcols";
	}

	public String[] cols() {
		return new String[] {pk, entbl, crud, synoder, uids, nyquence, updcols};
	}

	/** compose function for uids */
	public String uids(String synode, String entityId) {
		return synode + UIDsep + entityId; // Funcall.concatstr(synode, UIDsep, entityId);
	}

	/**
	 * ISSUE: why not merge with {@link SyntityMeta#replace()}?
	 * @return
	 * @throws SQLException
	 * @throws TransException
	 */
	public SynChangeMeta replace() throws SQLException, TransException {
		TableMeta mdb = Connects.getMeta(conn, tbl);
		if (!(mdb instanceof SyntityMeta))
			DBSynmantics.replaceMeta(tbl, this, conn);
		if (isNull(this.ftypes) && mdb.ftypes() != null)
			this.ftypes = mdb.ftypes();
		return this;
	}

}
