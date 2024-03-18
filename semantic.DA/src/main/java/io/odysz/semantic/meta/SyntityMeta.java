package io.odysz.semantic.meta;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import io.odysz.module.rs.AnResultset;
import io.odysz.module.rs.ChangeLogs;
import io.odysz.semantic.DA.Connects;
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

	/**
	 * Generate columns for inserting challenging entities.
	 * @return columns in order of rows' fields, values should be same order for insertion
	 */
	public String[] insChallengeEntCols() {
		if (entCols == null)
			this.entCols = new HashMap<String, Integer>(ftypes.size());

		String[] cols = new String[entCols.size()];
		int cx = 0;
		for (String c : ftypes.keySet()) {
			this.entCols.put(c, cx);
			cols[(int) cx - 1] = c;
			cx++;
		}
		return cols;
	}

	/**
	 * Filtering for columns to insert into entity table, with columns specified in
	 * the Insert statement by {@link #insChallengeEntCols()}.
	 * @param challenge 
	 * @return values as arguments for calling Insert.values
	 * @throws SQLException 
	 */
	public ArrayList<ArrayList<Object[]>> insertChallengeEnts(AnResultset challenge) throws SQLException {
		// TODO optimize Insert to handle this values faster
		if (entCols == null)
			insChallengeEntCols();

		if (challenge != null && challenge.getRowCount() > 0) {
			ArrayList<ArrayList<Object[]>> vals = new ArrayList<ArrayList<Object[]>>(challenge.getRowCount());
			challenge.beforeFirst();
			while (challenge.next()) {
				Object[][] colrow = new Object[entCols.size()][];
				for (String c : entCols.keySet()) {
					colrow[entCols.get(c)] = new Object[] {c, challenge.getObject(c)};
				}
				vals.add((ArrayList<Object[]>) Arrays.asList(colrow));
			}
			return vals;
		}
		return null;
	}



}
