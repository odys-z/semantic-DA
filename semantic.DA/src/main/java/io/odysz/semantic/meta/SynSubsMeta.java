package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.len;
import static io.odysz.common.Utils.loadTxt;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.syn.ChangeLogs;
import io.odysz.semantic.syn.DBSynsactBuilder;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.sql.Query;

/**
 * <a href="./syn_subscribe.sqlite.ddl">syn_sbuscribe DDL</a>
 *
 * @author Ody
 *
 */
public class SynSubsMeta extends TableMeta {

	public final String org;
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

		org = "org";
		entbl = "tabl";
		synodee = "synodee";
		uids = "uids";
		// subs = "synodee";
	}

	public String[] cols() {
		return new String[] {org, entbl, synodee, uids};
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
			this.subcols = new String[] { org, entbl, synodee, uids };
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

	/**
	 * Select clause for inserting subscribers, into {@link #tbl}.
	 * @param synm
	 * @param tb
	 * @param log
	 * @return select clause
	public Query subs2change(SynodeMeta synm, DBSynsactBuilder tb, ChangeLogs log) {
		return tb
			.select(synm.tbl, "sn")
			.whereEq("", "");
	}
	 */

}
