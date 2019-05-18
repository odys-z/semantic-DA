package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.xml.sax.SAXException;

import io.odysz.common.LangExt;
import io.odysz.common.Utils;
import io.odysz.module.rs.SResultset;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;

/**Statement manager that providing statements with overridden callback methods.<br>
 * This manager can handling semantics configured in xml. See {@link #initConfigs(String, String)}. <br>
 * Every sql building needing semantics handling must use a context instance created by {@link #instancontxt(IUser)}.
 * @author odys-z@github.com
 */
public class DATranscxt extends Transcxt {
	protected static HashMap<String, HashMap<String, TableMeta>> metas;
	public static HashMap<String, TableMeta> meta(String connId) throws SemanticException {
		if (metas == null)
			throw new SemanticException("DATranscxt need db metas to work. Set metas first."); 
		return metas.get(connId);
	}

	protected static HashMap<String, HashMap<String, DASemantics>> smtConfigs;
	public static HashMap<String,HashMap<String,DASemantics>> smtConfigs() { return smtConfigs; }
	public static HashMap<String, DASemantics> smtCfonfigs(String conn) {
		return smtConfigs.get(conn);
	}

	/**{@link DATranscxt} use a basic context (without semantics handler) for basic sql building.<b>
	 * Every context used for {@link DASemantics} handling must use this to create a new context instance.
	 * @see io.odysz.transact.sql.Transcxt#instancontxt(io.odysz.semantics.IUser)
	 */
	@Override
	public ISemantext instancontxt(IUser usr) {
		if (basictx == null)
			return null;
		else
			try {
				return new DASemantext(basiconnId, smtConfigs.get(basiconnId), metas.get(basiconnId), usr);
			} catch (SemanticException e) {
				e.printStackTrace(); // meta is null? shouldn't happen because this instance is already created
				return null;
			}
	}

	@Override
	public Query select(String tabl, String... alias) {
		Query q = super.select(tabl, alias);
		q.doneOp((sctx, sqls) -> {
			if (q.page() < 0 || q.size() <= 0) {
				SResultset rs = Connects.select(sctx.connId(), sqls.get(0));
				rs.total(rs.getRowCount());
				return new SemanticObject().rs(rs, rs.total());
			}
			else {
				SResultset total = Connects.select(sctx.connId(),
					((DASemantext) sctx).totalSql(sqls.get(0)));
				total.beforeFirst().next();
				int t = total.getInt(1);

				SResultset rs = Connects.select(sctx.connId(),
					((DASemantext) sctx).pageSql(sqls.get(0), q.page(), q.size()));
				rs.total(t);
				return new SemanticObject().rs(rs, t);
			}
		});
		return q;
	}

	public Insert insert(String tabl, IUser usr) {
		Insert i = super.insert(tabl);
		i.doneOp((sctx, sqls) -> {
			int[] r = Connects.commit(sctx.connId(), usr, sqls);
			return new SemanticObject().addInts("inserted", r).put("resulved", sctx.resulves());
		});
		return i;
	}

	public Update update(String tabl, IUser usr) {
		Update u = super.update(tabl);
		u.doneOp((sctx, sqls) -> {
			int[] r = Connects.commit(sctx.connId(), usr, sqls);
			return new SemanticObject().addInts("updated", r).put("resulved", sctx.resulves());
		});
		return u;
	}

	public Delete delete(String tabl, IUser usr) {
		Delete d = super.delete(tabl);
		d.doneOp((sctx, sqls) -> {
			int[] r = Connects.commit(sctx.connId(), usr, sqls);
			return new SemanticObject().addInts("deleted", r).put("resulved", sctx.resulves());
		});
		return d;
	}

	protected String basiconnId;
	public String basiconnId() { return basiconnId; }

	/**Create a transact builder with basic DASemantext instance. 
	 * It's a null configuration, so semantics can not been resolved, but can be used to do basic sql operation.
	 * When creating DATranscxt, db metas can not be null.
	 * 
	 * @param conn connection Id
	 * @param meta
	 * @throws SemanticException meta is null
	 */
	public DATranscxt(String conn, HashMap<String, TableMeta> meta) throws SemanticException {
		super(new DASemantext(conn, smtConfigs == null ? null : smtConfigs.get(conn),
				meta, null));
		this.basiconnId = conn;
		if (metas == null)
			metas = new HashMap<String, HashMap<String, TableMeta>>();
		if (meta != null)
			metas.put(conn, meta);
	}

	public DATranscxt(String conn) throws SemanticException {
		super(new DASemantext(conn, smtConfigs == null ? null : smtConfigs.get(conn),
				meta(conn), null));
		this.basiconnId = conn;
	}

	/**Load semantics configuration from filepath.
	 * This method also initialize table meta by calling {@link Connects}.
	 * @param connId
	 * @param filepath path to semantics.xml 
	 * @return configurations
	 * @throws SAXException
	 * @throws IOException
	 * @throws SemanticException 
	 * @throws SQLException 
	 */
	public static HashMap<String, DASemantics> initConfigs(String connId, String filepath)
			throws SAXException, IOException, SemanticException, SQLException {

		Utils.logi("Loading database metas...");
		if (metas == null)
			metas = new HashMap<String, HashMap<String, TableMeta>>();
		if (!metas.containsKey(connId))
			metas.put(connId, Connects.loadMeta(connId));

		Utils.logi("Loading Semantics:\n%s", filepath);
		LinkedHashMap<String, XMLTable> xtabs = XMLDataFactoryEx.getXtables(
				new Log4jWrapper("").setDebugMode(false), filepath, new IXMLStruct() {
						@Override public String rootTag() { return "semantics"; }
						@Override public String tableTag() { return "t"; }
						@Override public String recordTag() { return "s"; }});

		XMLTable conn = xtabs.get("semantics");
		
		return initConfigs(connId, conn);
	}
	
	public static HashMap<String, DASemantics> initConfigs(String conn, XMLTable xcfg)
			throws SAXException, IOException {
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

	public static void addSemantics(String connId, String tabl, String pk,
			String smtcs, String args) throws SemanticException {
		smtype sm = smtype.parse(smtcs);
		addSemantics(connId, tabl, pk, sm, args);
	}

	public static void addSemantics(String connId, String tabl, String pk,
			smtype sm, String args) throws SemanticException {
		addSemantics(connId, tabl, pk, sm, LangExt.split(args, ","));
	}

	public static void addSemantics(String conn, String tabl, String pk,
			smtype sm, String[] args) throws SemanticException {
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
			// s = new DASemantics(staticInstance, tabl, pk);
			s = new DASemantics(getBasicTrans(conn), tabl, pk);
			ss.put(tabl, s);
		}
		s.addHandler(sm, tabl, pk, args);
	}


	//////////// basic transact builders for each connection ////////////
	private static HashMap<String, Transcxt> basicTrxes;

	/**Get a basic transact builder (without semantics handling)
	 * @param conn
	 * @return the basice transact builder
	 * @throws SemanticException meta is null (must already reported error somewhere else)
	 */
	private static Transcxt getBasicTrans(String conn) throws SemanticException {
		if (basicTrxes == null)
			basicTrxes = new HashMap<String, Transcxt>();
		
		if (!basicTrxes.containsKey(conn)) {
			DATranscxt tx = new DATranscxt(conn, metas.get(conn));
			basicTrxes.put(conn, tx);
		}
		
		return basicTrxes.get(conn);
	}
}
