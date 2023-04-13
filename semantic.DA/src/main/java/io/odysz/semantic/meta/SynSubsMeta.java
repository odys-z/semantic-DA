package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.len;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Set;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.Connects;
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

	public static String ddlSqlite;
	static {
		try {
			ddlSqlite = Utils.loadTxt("syn_subscribe.sqlite.ddl");
			if (Connects.flag_printSql > 0)
				Utils.logi(ddlSqlite);
		} catch (IOException | URISyntaxException | ClassNotFoundException e) {
			e.printStackTrace();
		}
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
