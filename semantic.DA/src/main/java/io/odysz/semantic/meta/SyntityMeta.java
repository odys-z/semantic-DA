package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * Syncrhonizable entity table meta
 * 
 * @since 1.4.25
 * @author odys-z@github.com
 *
 */
public abstract class SyntityMeta extends SemanticTableMeta {

	public static final String err_requires_synuid =
			"Tables to be synchronized must come with a fixed column named '%s.%s'.";

	/**
	 * exposed to subclass to change
	 * @see SyntityMeta#SyntityMeta
	 */
	public final String domain;

	public final String synuid;
	
	/** Entity creator's id used for identify originators in domain (globally?) */
	public String synoder;

	/** Entity's columns for generation global uid */
	protected final HashSet<String> uids;

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
	 * @param domain DB field of orgnization - {@link io.odysz.semantic.syn.DBSynmantics}
	 * @param node synode-id field
	 * uses this to filter data for synchronization.
	 * Could be changed in the future. 
	 * @param conn
	 */
	public SyntityMeta(String tbl, String pk, String domain, String synodr, String conn) {
		super(tbl, conn);

		this.autopk = true;
		this.pk = pk;
		this.domain = domain;
		synoder = synodr;
		synuid = "io_oz_synuid";
		
		uids = new HashSet<String>() {
			static final long serialVersionUID = 1L;
			{ add(domain);
			  add(synoder);
			  add(pk);}
		};
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends SemanticTableMeta> T replace() throws TransException, SQLException {
		super.replace();
		if (isNull(ftypes) || !ftypes.containsKey(synuid))
			throw new TransException(err_requires_synuid, tbl, synuid);
		return (T) this;
	}

	public HashSet<String> globalIds() throws SemanticException {
		if (uids == null)
			throw new SemanticException("SyntityMeta.uids must initialized by subclasses. Uids is null.");
		return uids;
	}

	/**
	 * Generate columns for inserting challenging entities.
	 * 
	 * <h5>Note:</h5>
	 * <p>This method will ignore auto-key field</p>
	 * 
	 * @return columns in order of rows' fields, values should be same order for insertion
	 * @throws SemanticException this instance is not initialized from db ({@link #ftypes} is empty).
	 * @since 1.4.40
	 */
	public String[] entCols() throws SemanticException {
		if (entCols == null)
			this.entCols = new HashMap<String, Integer>(ftypes.size());

		if (ftypes == null || ftypes.size() == 0)
			throw new SemanticException("This table meta is not initialized with information from DB. Call clone() or replace() first.");

		if (!ftypes.containsKey(synuid)) 
			throw new SemanticException(err_requires_synuid, synuid);
					
		String[] cols = new String[autopk() ? ftypes.size() - 1 : ftypes.size()];
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
		// TODO optimize Insert to handle this values faster
		String[] cols = entCols();
		ArrayList<Object[]> val = new ArrayList<Object[]> (entCols.size());
		ArrayList<Object> row = challents.getRowAt(challents.rowIndex0(uids));

		for (int cx = 0; cx < cols.length; cx++) {
			if (autopk() && eq(this.pk, cols[cx]))
				continue;
			val.add(new Object[] {cols[cx], row.get(cx)});
		}
		return val;
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
	 * Generate select-items for select clause which is used for Insert, e.g.
	 * <pre>insert into t select 'item-a', 'item-b' where not exists select 1 from t where condition-avoiding-duplicate</pre>
	 * 
	 * @param entid
	 * @param entities
	 * @param changes
	 * @return select-items
	 * @throws SQLException 
	 */
	public abstract Object[] insertSelectItems(SynChangeMeta chgm, String entid,
			AnResultset entities, AnResultset changes) throws TransException, SQLException;
}
