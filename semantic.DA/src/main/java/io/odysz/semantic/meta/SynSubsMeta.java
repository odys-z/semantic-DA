package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.len;

import java.util.ArrayList;
import java.util.Set;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantics.meta.TableMeta;

/**
 * <a href="./syn_subscribe.sqlite.ddl">syn_sbuscribe DDL</a>
 *
 * @author Ody
 *
 */
public class SynSubsMeta extends TableMeta {

	public final String subs;
	public final String entbl;
	public final String entId;
	public final String dre;

	static {
		sqlite = "syn_subscribe.sqlite.ddl";
	}

	public SynSubsMeta(String ... conn) {
		super("syn_subscribe", conn);
		this.subs = "synodee";
		this.entbl = "tabl";
		this.entId = "recId";
		this.dre = "dre";
	}

	public String[] cols() {
		return new String[] {subs, dre};
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

}
