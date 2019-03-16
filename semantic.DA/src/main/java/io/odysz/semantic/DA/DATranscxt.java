package io.odysz.semantic.DA;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DASemantics;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Transcxt;

/**Statement manager that providing statements with overridden callback methods.
 * @author odys-z@github.com
 *
 */
public class DATranscxt extends Transcxt {

	private static HashMap<String, HashMap<String, DASemantics>> smtConfigs;
	private static Transcxt staticInstance = new DATranscxt(statiCtx);

	@Override
	public Query select(String tabl, String... alias) {
		Query q = super.select(tabl, alias);
		q.doneOp(sqls -> Connects.select(sqls.get(0)));
		return q;
	}
	
	public Insert insert(String tabl, IUser usr) {
		Insert i = super.insert(tabl);
		i.doneOp(sqls -> Connects.commit(usr, sqls));
		return i;
	}

	public DATranscxt(ISemantext semantext) {
		super(semantext);
	}

	public static HashMap<String, DASemantics> initConfigs(String connId, String filepath) throws SAXException, IOException {
//		HashMap<String, DASemantics> ss = new HashMap<String, DASemantics>();
		LinkedHashMap<String, XMLTable> xtabs = XMLDataFactoryEx.getXtables(
				new Log4jWrapper("").setDebugMode(false), filepath, new IXMLStruct() {
						@Override public String rootTag() { return "semantics"; }
						@Override public String tableTag() { return "t"; }
						@Override public String recordTag() { return "s"; }});

		XMLTable conn = xtabs.get("semantics");
		conn.beforeFirst();
		while (conn.next()) {
			String tabl = conn.getString("tabl");
			String pk = conn.getString("pk");
			String smtc = conn.getString("smtc");
			String args = conn.getString("args");
			try {
				addSemantics(connId, tabl, pk, smtc, args);
			} catch (SemanticException e) {
				// some configuration error
				// continue
				Utils.warn(e.getMessage());
			}
		}
		return smtConfigs.get(connId);
	}

	public static void addSemantics(String connId, String tabl, String pk, String smtcs, String args) throws SemanticException {
		if (smtConfigs == null) {
			smtConfigs = new HashMap<String, HashMap<String, DASemantics>>();
		}
		HashMap<String, DASemantics> ss = smtConfigs.get(connId);
		if (ss == null) {
			ss = new HashMap<String, DASemantics>();
			smtConfigs.put(connId, ss);
		}

		DASemantics s = ss.get(tabl);
		if (s == null) {
			s = new DASemantics(staticInstance, tabl, pk);
			ss.put(tabl, s);
		}
		s.addHandler(smtcs, args);
	}
}
