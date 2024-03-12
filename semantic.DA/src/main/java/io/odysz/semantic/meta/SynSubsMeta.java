package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.len;
import static io.odysz.common.Utils.loadTxt;

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

	public String[] insertCols() {
		// TODO Auto-generated method stub
		return null;
	}

	public Query subs2change(SynodeMeta synm, DBSynsactBuilder tb, ChangeLogs log) {
		// TODO how about caching in memory for current subscribers?
		return tb.with(true, org, entbl, null)
				.select(synm.tbl, "sn")
			;
	}

}
