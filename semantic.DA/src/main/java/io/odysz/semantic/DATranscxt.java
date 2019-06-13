package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.io.FilenameUtils;
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
 * <p>Those statements are the starting points to build a sql transact for querying, updating, etc.<br>
 * For how to use the created statements, see the testing class:
 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/java/io/odysz/semantic/DASemantextTest.java'>
 * DASemantextTest</a>.</p>
 * This manager can handling semantics configured in xml. See {@link #loadSemantics(String, String)}. <br>
 * Every sql building needing semantics handling must use a context instance created by {@link #instancontxt(IUser)}.
 * @author odys-z@github.com
 */
public class DATranscxt extends Transcxt {
	static String cfgroot = ""; 
	/** configuration's root */
	public static void configRoot(String cfgRoot, String runtimeRoot) {
		cfgroot = cfgRoot;
		runtimepath = runtimeRoot;
	}

	static String runtimepath = "";
//	/**Get meta of connId
//	 * @param connId
//	 * @return
//	 * @throws SemanticException
//	 * @throws SQLException
//	 */
//	public static HashMap<String, TableMeta> meta(String connId)
//			throws SemanticException, SQLException {
////		if (metas == null)
////			throw new SemanticException("DATranscxt need db metas to work. Set metas first."); 
//		return Connects.getMeta(connId);
//	}
	
	public TableMeta tableMeta(String t) throws SemanticException, SQLException {
		for (String cnn : Connects.connIds()) {
			HashMap<String, TableMeta> metas = Connects.getMeta(cnn);
			if (metas != null && metas.containsKey(t))
				return metas.get(t);
		}
		return null;

	}

	/**[conn, [table, DASemantics] */
	protected static HashMap<String, HashMap<String, DASemantics>> smtConfigs;

	/**Create a new semantext instance with the static resources.<br>
	 * {@link DATranscxt} use a basic context (without semantics handler) for basic sql building.<br>
	 * Every context used for {@link DASemantics} handling must use this to create a new context instance.
	 * @see ISemantext 
	 * @see io.odysz.transact.sql.Transcxt#instancontxt(io.odysz.semantics.IUser)
	 */
	@Override
	public ISemantext instancontxt(IUser usr) {
		if (basictx == null)
			return null;
		else
			try {
				// return new DASemantext(basiconnId, smtConfigs.get(basiconnId), meta(basiconnId), usr);
				return new DASemantext(basiconnId, getSmtcs(basiconnId),
						Connects.getMeta(basiconnId), usr, runtimepath);
			} catch (SemanticException | SQLException | SAXException | IOException e) {
				// meta is null? shouldn't happen because this instance is already created
				e.printStackTrace();
				return null;
			}
	}

	/**Create a select statement.
	 * <p>This statement is the starting points to build a sql transact for querying.<br>
	 * For how to use the created statements, see the testing class:
	 * <a href='https://github.com/odys-z/semantic-transact/blob/master/semantic.transact/src/test/java/io/odysz/transact/sql/TestTransc.java'>
	 * DASemantextTest</a>.</p>
	 * @see io.odysz.transact.sql.Transcxt#select(java.lang.String, java.lang.String[])
	 */
	@Override
	public Query select(String tabl, String... alias) {
		Query q = super.select(tabl, alias);
		q.doneOp((sctx, sqls) -> {
			if (q.page() < 0 || q.size() <= 0) {
				SResultset rs = Connects.select(sctx.connId(), sqls.get(0));
				rs.total(rs.getRowCount());
				sctx.onSelected(rs);
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

				sctx.onSelected(rs);
				return new SemanticObject().rs(rs, t);
			}
		});
		return q;
	}

	/**Create an insert statement.
	 * <p>Those statements are the starting points to build a sql transact for querying, updating, etc.<br>
	 * For how to use the created statements, see the testing class:
	 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/java/io/odysz/semantic/DASemantextTest.java'>
	 * DASemantextTest</a>.</p>
	 * @param tabl
	 * @param usr
	 * @return the starting statement
	 */
	public Insert insert(String tabl, IUser usr) {
		Insert i = super.insert(tabl);
		i.doneOp((sctx, sqls) -> {
			int[] r = Connects.commit(sctx.connId(), usr, sqls);
			
			// In semantic.DA 1.0, only deletingl external files here
			sctx.onCommitted(sctx);

			return new SemanticObject().addInts("inserted", r).put("resulved", sctx.resulves());
		});
		return i;
	}

	/**Create an update statement.
	 * <p>Those statements are the starting points to build a sql transact for querying, updating, etc.<br>
	 * For how to use the created statements, see the testing class:
	 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/java/io/odysz/semantic/DASemantextTest.java'>
	 * DASemantextTest</a>.</p>
	 * @param tabl
	 * @param usr
	 * @return the starting statement
	 */
	public Update update(String tabl, IUser usr) {
		Update u = super.update(tabl);
		u.doneOp((sctx, sqls) -> {
			int[] r = Connects.commit(sctx.connId(), usr, sqls);
			
			// In semantic.DA 1.0, only deletingl external files here
			sctx.onCommitted(sctx);

			return new SemanticObject().addInts("updated", r).put("resulved", sctx.resulves());
		});
		return u;
	}

	/**Create an update statement.
	 * <p>Those statements are the starting points to build a sql transact for querying, updating, etc.<br>
	 * For how to use the created statements, see the testing class:
	 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/java/io/odysz/semantic/DASemantextTest.java'>
	 * DASemantextTest</a>.</p>
	 * @param tabl
	 * @param usr
	 * @return the starting statement
	 */
	public Delete delete(String tabl, IUser usr) {
		Delete d = super.delete(tabl);
		d.doneOp((sctx, sqls) -> {
			int[] r = Connects.commit(sctx.connId(), usr, sqls);
			
			// In semantic.DA 1.0, only deletingl external files here
			// FIXME if this post operation always happend, this method should been called as an interface,
			// with default implementation been alwasy called by semantic.transact, and overridden by semantic.DA.
			sctx.onCommitted(sctx);

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
	 * @throws SQLException 
	 * @throws IOException load semantics configuration failed
	 * @throws SAXException load semantics configuration failed
	 * @throws SemanticException 
	 */
	public DATranscxt(String conn) throws SQLException, SAXException, IOException, SemanticException {
		// super(new DASemantext(conn, smtConfigs == null ? null : smtConfigs.get(conn),
		super(new DASemantext(conn, getSmtcs(conn),
				Connects.getMeta(conn), null, runtimepath));
		this.basiconnId = conn;
	}

	private static HashMap<String, DASemantics> getSmtcs(String conn)
			throws SAXException, IOException, SQLException, SemanticException {
		if (smtConfigs == null)
			smtConfigs = new HashMap<String, HashMap<String, DASemantics>>();
		if (!smtConfigs.containsKey(conn)) {
			String fpath = Connects.getSmtcs(conn);
			if (LangExt.isblank(fpath, "\\."))
				throw new SemanticException("Trying to find semantics of conn %s, but the configuration path is empty.\n" +
							"No 'smtcs' configured in connexts.xml?", conn);
			fpath = FilenameUtils.concat(cfgroot, fpath);
			smtConfigs.put(conn, loadSemantics(conn, fpath));
		}
		return smtConfigs.get(conn);
	}

	/**Load semantics configuration from filepath.
	 * This method also initialize table meta by calling {@link Connects}.
	 * @param connId
	 * @param full path to semantics.xml (path and name) 
	 * @return configurations
	 * @throws SAXException
	 * @throws IOException
	 * @throws SQLException 
	 * @throws SemanticException 
	 */
	public static HashMap<String, DASemantics> loadSemantics(String connId, String cfgpath)
			throws SAXException, IOException, SQLException, SemanticException {

//		Utils.logi("Loading database metas...");
//		if (metas == null)
//			metas = new HashMap<String, HashMap<String, TableMeta>>();
//		if (!metas.containsKey(connId))
//			metas.put(connId, Connects.getMeta(connId));

		Utils.logi("Loading Semantics:\n\t%s", cfgpath);
		LinkedHashMap<String, XMLTable> xtabs = XMLDataFactoryEx.getXtables(
				new Log4jWrapper("").setDebugMode(false), cfgpath, new IXMLStruct() {
						@Override public String rootTag() { return "semantics"; }
						@Override public String tableTag() { return "t"; }
						@Override public String recordTag() { return "s"; }});

		XMLTable conn = xtabs.get("semantics");
		
		return initConfigs(connId, conn);
	}
	
	protected static HashMap<String, DASemantics> initConfigs(String conn, XMLTable xcfg)
			throws SAXException, IOException, SQLException, SemanticException {
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

	public static boolean hasSemantics(String conn, String tabl, smtype sm) {
		if (smtConfigs == null || !smtConfigs.containsKey(conn)
				|| !smtConfigs.get(conn).containsKey(tabl))
			return false;
		DASemantics s = smtConfigs.get(conn).get(tabl);
		return s != null && s.has(sm);
	}

	public static void addSemantics(String connId, String tabl, String pk,
			String smtcs, String args) throws SQLException, SAXException, IOException, SemanticException {
		smtype sm = smtype.parse(smtcs);
		addSemantics(connId, tabl, pk, sm, args);
	}

	public static void addSemantics(String connId, String tabl, String pk,
			smtype sm, String args) throws SQLException, SAXException, IOException, SemanticException {
		addSemantics(connId, tabl, pk, sm, LangExt.split(args, ","));
	}

	public static void addSemantics(String conn, String tabl, String pk,
			smtype sm, String[] args) throws SQLException, SAXException, IOException, SemanticException {
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
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws SemanticException 
	 */
	private static Transcxt getBasicTrans(String conn)
			throws SQLException, SAXException, IOException, SemanticException {
		if (basicTrxes == null)
			basicTrxes = new HashMap<String, Transcxt>();
		
		if (!basicTrxes.containsKey(conn)) {
			DATranscxt tx = new DATranscxt(conn);
			basicTrxes.put(conn, tx);
		}
		
		return basicTrxes.get(conn);
	}
}
