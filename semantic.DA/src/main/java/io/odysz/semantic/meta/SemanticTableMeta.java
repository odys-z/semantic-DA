package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.eq;

import java.sql.SQLException;

import io.odysz.common.Utils;
import io.odysz.module.xtable.XMLTable.IMapValue;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.x.TransException;

public abstract class SemanticTableMeta extends TableMeta implements IMapValue {

	@Override
	public String mapKey() {
		return tbl;
	}

	public SemanticTableMeta(String tbl, String... conn) {
		super(tbl, conn); 
	}

	/**
	 * Explicitly call this after this meta with semantics is created,
	 * to replace auto found meta from database, which is managed by {@link Connects}.
	 * 
	 * @return this
	 * @throws TransException
	 * @throws SQLException 
	 */
	@SuppressWarnings("unchecked")
	public <T extends SemanticTableMeta> T replace() throws TransException, SQLException {
		TableMeta mdb = Connects.getMeta(conn, tbl);
		if (debug && mdb instanceof SemanticTableMeta
			&& (Connects.getDebug(conn) || !eq(mdb.getClass().getName(), getClass().getName())))
			Utils.warn( "[TableMeta.debug true] Replacing existing Semantic table meta with new meta. Old: %s, new %s",
						mdb.getClass().getName(), getClass().getName());

		DASemantics.replaceMeta(tbl, this, conn);
		if (isNull(this.ftypes) && mdb.ftypes() != null)
			this.ftypes = mdb.ftypes();
		return (T) this;
	}
	
	/**
	 * Commit Sqlite3 table ddls defined by m[i].ddlSqlite.
	 * 
	 * @param conn
	 * @param force_drop force dropping table before commit {@link TableMeta#ddlSqlite}.
	 * @param ms
	 * @throws SQLException
	 * @throws TransException
	 */
	public static void setupSqliTables(String conn, boolean force_drop, SemanticTableMeta ... ms) throws SQLException, TransException {
		if (ms != null)
		for (TableMeta m : ms)
			if (m.ddlSqlite != null) {
				if (force_drop) Connects.commit(conn, DATranscxt.dummyUser(), String.format("drop table if exists %s;", m.tbl));
				Connects.commit(conn, DATranscxt.dummyUser(), m.ddlSqlite);
			}
	}
}
