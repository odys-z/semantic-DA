package io.odysz.semantic.meta;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.syn.ChangeLogs;
import io.odysz.semantic.syn.DBSynmantics;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.x.TransException;

/**
 * Syncrhonizable entity table meta
 * 
 * @since 1.4.25
 * @author odys-z@github.com
 *
 */
public abstract class SyntityMeta extends TableMeta {

	/** exposed to subclass to change
	 * @see SyntityMeta#SyntityMeta */
	protected String org;
	public String org() { return org; }

	/** entity creator id used for identify globally (experimental) */
	public final String synoder;

	public final HashSet<String> uids;
	
	/**
	 * @param tbl
	 * @param pk
	 * @param org DB field of orgnization - {@link io.odysz.semantic.syn.DBSynmantics}
	 * uses this to filter data for synchronization.
	 * Could be changed in the future. (What is this if using DB synchronization, data filter?)
	 * @param conn
	 */
	@SuppressWarnings("serial")
	public SyntityMeta(String tbl, String pk, String org, String... conn) {
		super(tbl, conn);

		this.pk = pk;
		this.org = org;
		synoder = "device";
		uids = new HashSet<String>() { {add("clientpath");} };
	}

	/**
	 * Explicitly call this after this meta with semantics is created,
	 * to replace auto found meta from database.
	 * 
	 * @return this
	 * @throws TransException
	 * @throws SQLException 
	 */
	@SuppressWarnings("unchecked")
	public <T extends SyntityMeta> T replace() throws TransException, SQLException {
		TableMeta mdb = Connects.getMeta(conn, tbl);
		if (!(mdb instanceof SyntityMeta))
			DBSynmantics.replaceMeta(tbl, this, conn);
		return (T) this;
	}
	

	public HashSet<String> globalIds() { return uids; }

	public String[] insertCols() {
		return null;
	}

	public ArrayList<ArrayList<Object[]>> insertVal(ChangeLogs l) {
		return null;
	}

}
