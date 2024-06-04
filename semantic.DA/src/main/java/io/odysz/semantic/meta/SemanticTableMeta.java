package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.isNull;

import java.sql.SQLException;

import io.odysz.common.Utils;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.syn.DBSynmantics;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.x.TransException;

public abstract class SemanticTableMeta extends TableMeta {

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
		if (mdb instanceof SemanticTableMeta)
			Utils.warn( "Replacing existing Semantic table meta with new meta. Old: %s, new %s",
						mdb.getClass().getName(), getClass().getName());

		DBSynmantics.replaceMeta(tbl, this, conn);
		if (isNull(this.ftypes) && mdb.ftypes() != null)
			this.ftypes = mdb.ftypes();
		return (T) this;
	}
}
