package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.isNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.syn.DBSynmantics;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
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
	public String synoder;

	public final HashSet<String> uids;

	private HashMap<String, Integer> entCols;
	
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
		synoder = "synode";
		uids = new HashSet<String>() { {add("clientpath");} };
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
	public <T extends SyntityMeta> T replace() throws TransException, SQLException {
		TableMeta mdb = Connects.getMeta(conn, tbl);
		if (!(mdb instanceof SyntityMeta))
			DBSynmantics.replaceMeta(tbl, this, conn);
		if (isNull(this.ftypes) && mdb.ftypes() != null)
			this.ftypes = mdb.ftypes();
		return (T) this;
	}
	

	public HashSet<String> globalIds() { return uids; }

	/**
	 * Generate columns for inserting challenging entities.
	 * @return columns in order of rows' fields, values should be same order for insertion
	 * @throws SemanticException this instance is not initialized from db ({@link #ftypes} is empty).
	 */
	public String[] entCols() throws SemanticException {
		if (entCols == null)
			this.entCols = new HashMap<String, Integer>(ftypes.size());

		if (ftypes == null || ftypes.size() == 0) 
			throw new SemanticException("This table meta is not initialized with information from DB. Call clone() or replace() first.");

		String[] cols = new String[ftypes.size()];
		int cx = 0;
		for (String c : ftypes.keySet()) {
			this.entCols.put(c, cx);
			cols[cx] = c;
			cx++;
		}
		return cols;
	}

	/**
	 * Generate data for value clause of the Insert statement,
	 * using columns for filter out fields of entity table.
	 * Columns are provided by {@link #entCols()}.
	 * 
	 * @return values as arguments for calling Insert.value(), the row for the change log
	 * @throws SQLException 
	 * @throws SemanticException 
	 * @since 1.4.40
	 * FIXME extend Insert statement API to handle this data structure
	 */
	public ArrayList<Object[]> insertChallengeEnt(String pk, AnResultset challengents)
			throws SQLException, SemanticException {
		// TODO optimize Insert to handle this values faster
		String[] cols = entCols();
		ArrayList<Object[]> val = new ArrayList<Object[]> (entCols.size());
		ArrayList<Object> row = challengents.getRowAt(challengents.indices0(pk));

		for (int cx = 0; cx < row.size(); cx++) {
			val.add(new Object[] {cols[cx], row.get(cx)});
		}
		return val;
	}
}
