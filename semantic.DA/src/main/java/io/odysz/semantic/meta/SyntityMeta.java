package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.f;
import static io.odysz.common.LangExt.isNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.syn.DBSyntableBuilder;
import io.odysz.semantic.syn.SyndomContext;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.x.TransException;

/**
 * Synchronizable entity table meta
 * 
 * @since 1.4.25
 * @author odys-z@github.com
 *
 */
public abstract class SyntityMeta extends SemanticTableMeta {

	public static final String err_requires_synuid(String tabl, String syn_uid, String conn) {
		return f("Tables to be synchronized must come with a fixed column named '%s.%s' [%s].",
				 tabl, syn_uid, conn);
	}

	/**
	 * Global syn-uid. Must be transparent to users.
	 * 
	 * (using this field in transactions will suppress semantics handling of smtyp.synchange)
	 */
	public final String io_oz_synuid;
	
	public String device;

	/** Entity's columns for generation global uid */
	public final ArrayList<String> uids;

	/**
	 * Entity columns figured out from entity type, 
	 * which are used to access database records.
	 */
	private HashMap<String, Integer> entCols;

	boolean autopk;
	public boolean autopk() { return autopk; }
	public SyntityMeta autopk(boolean ak) {
		this.autopk = ak;
		return this;
	}
	
	/**
	 * @param tbl
	 * @param pk
	 * @param devid synode-id field
	 * uses this to filter data for synchronization.
	 * Could be changed in the future. 
	 * @param conn
	 */
	public SyntityMeta(String tbl, String pk, String devid, String conn) {
		super(tbl, conn);

		this.autopk = true;
		this.pk = pk;
		// synoder = synodr;
		device = devid;
		io_oz_synuid = "io_oz_synuid";
		
		uids = new ArrayList<String>();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends SemanticTableMeta> T replace() throws TransException, SQLException {
		super.replace();
		if (isNull(ftypes) || !ftypes.containsKey(io_oz_synuid))
			throw new TransException(err_requires_synuid(tbl, io_oz_synuid, conn));
		return (T) this;
	}

	/**
	 * Generate columns for inserting challenging entities.
	 * 
	 * <h5>Note:</h5>
	 * <ul>
	 * <li>This method will ignore auto-key field</li>
	 * <li>The returned value is an arry of String[], but is created as an Object[] array.</li>
	 * </ul>
	 * 
	 * @return column names in order of rows' fields, values should be same order for insertion
	 * @throws SemanticException this instance is not initialized from db ({@link #ftypes} is empty).
	 * @since 1.4.40
	 */
	Object[] entCols() throws SemanticException {
		if (entCols == null)
			this.entCols = new HashMap<String, Integer>(ftypes.size());

		if (ftypes == null || ftypes.size() == 0)
			throw new SemanticException(
					"The table %s's meta is not initialized with information from DB. Call clone() or replace() first.",
					tbl);

		if (!ftypes.containsKey(io_oz_synuid)) 
			throw new SemanticException(err_requires_synuid(tbl, io_oz_synuid, conn));

		Object[] cols = new Object[autopk() ? ftypes.size() - 1 : ftypes.size()];
		int cx = 0;
		for (String c : ftypes.keySet()) {
			if (autopk() && eq(pk, c))
				continue;
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
	 * <h6>Note:</h6>
	 * This method will ignore auto-key field
	 * 
	 * @return values as arguments for calling Insert.value(), the row for the change log
	 * @throws SQLException 
	 * @throws SemanticException 
	 * @since 1.4.40
	 */
	public ArrayList<Object[]> insertChallengeEnt(String uids, AnResultset challents)
			throws SQLException, SemanticException {
		throw new SemanticException("sholdn't reach here");
//		Object[] cols = entCols();
//		ArrayList<Object[]> val = new ArrayList<Object[]> (entCols.size());
//		ArrayList<Object> row = challents.getRowAt(challents.rowIndex0(uids));
//
//		for (int cx = 0; cx < cols.length; cx++) {
//			if (autopk() && cols[cx] instanceof String && eq(this.pk, (String)cols[cx]))
//				continue;
//			val.add(new Object[] {cols[cx], row.get(cx)});
//		}
//		return val;
	}

	/**
	 * Generate set values for the Update statement which is used for updating current entity, e.g.
	 * <pre>update t set c = v1, ...</pre>
	 * before
	 * <pre>insert into t select 'item-a', 'item-b' where not exists select 1 from t where condition-avoiding-duplicate</pre>
	 * org
	 * @param entid
	 * @param entities
	 * @param challenges
	 * @return n-v pairs for argument of {@link io.odysz.transact.sql.Update#nvs(ArrayList)}
	 * @throws SQLException 
	 * @throws SemanticException 
	 */
	public ArrayList<Object[]> updateEntNvs(SynChangeMeta chgm, String entid,
			AnResultset entities, AnResultset challenges)
			throws SemanticException, SQLException {
		ArrayList<Object[]> row = new ArrayList<Object[]>();
		String[] updatingcols = challenges.getStrArray(chgm.updcols);
		for (String c : updatingcols)
			row.add(new Object[] {c, entities.getStringByIndex(c, entid)});
		return row;
	}

	/**
	 * 
	 * <p>Entity meta's query event handler, while synchronizing.</p>
	 * <p>{@link io.odysz.semantic.syn.ExessionPersist ExessionPersist}
	 * (or {@link io.odysz.semantic.syn.DBSyntableBuilder DBSyntableBuilder})
	 * use this for loading entities in a syn-exchang page.</p>
	 * <p>Note: call select.cols(...) first.</p>
	 * 
	 * A typical task finished here is to add an extFile() function object to the
	 * parameter {@code select}.
	 * <pre>  T_DA_PhotoMet entm = new T_DA_PhotoMet(conn);  // extends SyntityMeta
	 *  AnResultset entities = ((AnResultset) entm
	 *    .onselectSyntities(trb.select(tbl, "e").col("e.*"))
	 * </pre>
	 * 
	 * <h5> TO BE FIXED Notes 2025.5.22:</h5>
	 * No. The file content should be resolved / synchronized later for performance.
	 * No file should be load multiple times, as this is called while loading exchange
	 * pages, which is based upon semantics of change-log pages, not entities' page. 
	 * @param syndomx 
	 * 
	 * @see io.odysz.semantic.syn.ExessionPersist#chpage()
	 * @param select typically should already called {@link Query#cols(String...)}, etc. alrady.
	 * @return select
	 * @throws TransException, SQLException 
	 */
	public Query onselectSyntities(SyndomContext syndomx, Query select, DBSyntableBuilder synb)
			throws TransException, SQLException { return select; }
}
