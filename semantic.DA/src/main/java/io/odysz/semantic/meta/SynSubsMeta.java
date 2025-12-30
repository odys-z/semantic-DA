package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.len;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;

import io.odysz.module.rs.AnResultset;
import io.odysz.transact.sql.parts.Resulving;

/**
 * <a href="./syn_subscribe.sqlite.ddl">syn_sbuscribe DDL</a>
 *
 * @author Ody
 *
 */
public class SynSubsMeta extends SemanticTableMeta {

	final SynChangeMeta chgm;

	public final String changeId;
	public final String synodee;
	private String[] subcols;

	public SynSubsMeta(SynChangeMeta chgm, String ... conn) {
		super("syn_subscribe", conn);

		changeId= "changeId";
		synodee = "synodee";
		this.chgm = chgm;

		ddlSqlite = loadSqlite(SynSubsMeta.class, "syn_subscribe.sqlite.ddl");
	}

	public String[] cols() {
		return new String[] {changeId, synodee};
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
	 * @return [changeId, synodee]
	 * 
	 * FIXME TODO add "domain"
	 */
	public String[] insertingCols() {
		if (this.subcols == null)
			this.subcols = new String[] {changeId, synodee};
		return subcols;
	}

	/**
	 * Add a subscribing synodee's record according to current row of {@code chlogs}.
	 * @param chlogs
	 * @return val list, the row
	 * @throws SQLException
	 */
	public ArrayList<Object[]> insertSubVal(AnResultset chlogs, Resulving chgId) throws SQLException {
		String[] cols = insertingCols();
		ArrayList<Object[]> val = new ArrayList<Object[]> (cols.length);

		val.add(new Object[] {cols[0], chgId});
		val.add(new Object[] {cols[1], chlogs.getString(synodee)});

		return val;
	}

	public ArrayList<Object[]> insertSubVal(String org, String entbl, String synodee, String uds) throws SQLException {
		return new ArrayList<Object[]>() {
			private static final long serialVersionUID = 1L;
			{add(new Object[] {changeId, new Resulving(chgm.tbl, chgm.pk)});}
			{add(new Object[] {subcols[2], synodee});}
		};
	}
}
