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

	public final String org;
	public final String subs;
	public final String entbl;
	public final String uids;
	public final String synoder;

	static {
		sqlite = "syn_subscribe.sqlite.ddl";
	}

	public SynSubsMeta(String ... conn) {
		super("syn_subscribe", conn);
		org = "org";
		subs = "synodee";
		entbl = "tabl";
		uids = "uids";
		synoder = "";
	}

	public String[] cols() {
		return new String[] {subs, uids};
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
