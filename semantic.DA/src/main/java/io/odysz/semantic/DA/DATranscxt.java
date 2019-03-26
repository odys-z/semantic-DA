package io.odysz.semantic.DA;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.xml.sax.SAXException;

import io.odysz.common.LangExt;
import io.odysz.common.Utils;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DASemantext;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;

/**Statement manager that providing statements with overridden callback methods.
 * @author odys-z@github.com
 *
 */
public class DATranscxt extends Transcxt {

	protected static HashMap<String, HashMap<String, DASemantics>> smtConfigs;
	public static HashMap<String, DASemantics> smtCfonfigs(String conn) {
		return smtConfigs.get(conn);
	}

	private static Transcxt staticInstance = new DATranscxt(basictx);

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

	public Update update(String tabl, IUser usr) {
		Update u = super.update(tabl);
		u.doneOp(sqls -> Connects.commit(usr, sqls));
		return u;
	}

	public DATranscxt(ISemantext semantext) {
		super(semantext);
	}

	protected String basiconnId;
	public String basiconnId() { return basiconnId; }

	/**Create a transact builder with basic DASemantext instance. 
	 * It's a null configuration, so semantics can be resolved, but can be used to do basic sql operation.
	 * @param conn connection Id
	 */
	public DATranscxt(String conn) {
		super(new DASemantext(conn, null, null));
		this.basiconnId = conn;
	}

	public static HashMap<String, DASemantics> initConfigs(String connId, String filepath) throws SAXException, IOException {
//		HashMap<String, DASemantics> ss = new HashMap<String, DASemantics>();
		LinkedHashMap<String, XMLTable> xtabs = XMLDataFactoryEx.getXtables(
				new Log4jWrapper("").setDebugMode(false), filepath, new IXMLStruct() {
						@Override public String rootTag() { return "semantics"; }
						@Override public String tableTag() { return "t"; }
						@Override public String recordTag() { return "s"; }});

		XMLTable conn = xtabs.get("semantics");
		
		return initConfigs(connId, conn);
	}
	
	public static HashMap<String, DASemantics> initConfigs(String conn, XMLTable xcfg) throws SAXException, IOException {
		xcfg.beforeFirst();
		if (smtConfigs == null)
			smtConfigs = new HashMap<String, HashMap<String, DASemantics>>();
		while (xcfg.next()) {
			String tabl = xcfg.getString("tabl");
			String pk = xcfg.getString("pk");
			String smtc = xcfg.getString("smtc");
			String args = xcfg.getString("args");
			try {
				addSemantics(conn, tabl, pk, smtc, args);
			} catch (SemanticException e) {
				// some configuration error
				// continue
				Utils.warn(e.getMessage());
			}
		}
		return smtConfigs.get(conn);
	}

		
	public static void addSemantics(String connId, String tabl, String pk, String smtcs, String args) throws SemanticException {
		smtype sm = smtype.parse(smtcs);
		addSemantics(connId, tabl, pk, sm, args);
	}

	public static void addSemantics(String connId, String tabl, String pk, smtype sm, String args) throws SemanticException {
		addSemantics(connId, tabl, pk, sm, LangExt.split(args, ","));
	}

	public static void addSemantics(String conn, String tabl, String pk, smtype sm, String[] args) throws SemanticException {
		if (smtConfigs == null) {
			smtConfigs = new HashMap<String, HashMap<String, DASemantics>>();
		}
		HashMap<String, DASemantics> ss = smtConfigs.get(conn);
		if (ss == null) {
			ss = new HashMap<String, DASemantics>();
			smtConfigs.put(conn, ss);
		}

		DASemantics s = ss.get(tabl);
		if (s == null) {
			s = new DASemantics(staticInstance, tabl, pk);
			ss.put(tabl, s);
		}
		s.addHandler(sm, tabl, pk, args);
	}
}
