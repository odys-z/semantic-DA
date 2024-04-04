package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.len;
import static io.odysz.common.Utils.loadTxt;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.syn.DBSynmantics;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.x.TransException;

/**
 * <a href="./syn_subscribe.sqlite.ddl">syn_sbuscribe DDL</a>
 *
 * @author Ody
 *
 */
public class SynSubsMeta extends TableMeta {

	public final String domain;
	// public final String subs;
	public final String entbl;
	public final String uids;
	public final String synodee;
	private String[] subcols;

	static {
	}

	public SynSubsMeta(String ... conn) {
		super("syn_subscribe", conn);
		ddlSqlite = loadTxt(SynSubsMeta.class, "syn_subscribe.sqlite.ddl");

		domain = "domain";
		entbl  = "tabl";
		synodee= "synodee";
		uids   = "uids";
	}

	public String[] cols() {
		return new String[] {domain, entbl, synodee, uids};
	}

	/**
	 * Generate values for parameter of Insert.values();
	 * 
	 * @param subs row index not the same when return
	 * @param skips ignored synodes
	 * @return values
	 */
	public ArrayList<ArrayList<Object[]>> insubVals(AnResultset subs, Set<String> skips) {
		
		ArrayList<ArrayList<Object[]>> v = new ArrayList<ArrayList<Object[]>>(subs.getRowCount() - len(skips));

		return v;
	}

	/**
	 * @return [org, entbl, synodee, uids]
	 */
	public String[] insertCols() {
		if (this.subcols == null)
			this.subcols = new String[] { domain, entbl, synodee, uids };
		return subcols;
	}

	/**
	 * 
	 * @param chlogs
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<Object[]> insertSubVal(AnResultset chlogs) throws SQLException {
		String[] cols = insertCols();
		ArrayList<Object[]> val = new ArrayList<Object[]> (cols.length);

		for (int cx = 0; cx < cols.length; cx++) {
			val.add(new Object[] {cols[cx], chlogs.getString(cols[cx])});
		}
		return val;
	}

	@SuppressWarnings("serial")
	public ArrayList<Object[]> insertSubVal(String org, String entbl, String synodee, String uds) throws SQLException {
		return new ArrayList<Object[]>() {
			{add(new Object[] {subcols[0], org});}
			{add(new Object[] {subcols[1], entbl});}
			{add(new Object[] {subcols[2], synodee});}
			{add(new Object[] {subcols[3], uids});}
		};
	}

	/**
	 * ISSUE: why not merge with {@link SyntityMeta#replace()}?
	 * @return
	 * @throws SQLException
	 * @throws TransException
	 */
	public SynSubsMeta replace() throws SQLException, TransException {
		TableMeta mdb = Connects.getMeta(conn, tbl);
		if (!(mdb instanceof SyntityMeta))
			DBSynmantics.replaceMeta(tbl, this, conn);
		if (isNull(this.ftypes) && mdb.ftypes() != null)
			this.ftypes = mdb.ftypes();
		return this;
	}
}
