package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DASemantics.SemanticHandler;
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
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

import static io.odysz.common.LangExt.*;

/**
 * A {@link io.odysz.transact.sql.Statement Statement} builder that can providing statements with overridden callback methods.<br>
 * 
 * <p>Those statements are the starting points to build a sql transact for querying, updating, etc.<br>
 * For how to use the created statements, see the testing class:
 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/java/io/odysz/semantic/DASemantextTest.java'>
 * DASemantextTest</a>.</p>
 * This manager can handling semantics configured in xml. See {@link #loadSemantics(String, String)}. <br>
 * Every sql building needing semantics handling must use a context instance created by
 * {@link DATranscxt#instancontxt(String, IUser)}.
 * @author odys-z@github.com
 *
 */
public class DATranscxt extends Transcxt {
	protected static String cfgroot = ""; 
	protected static String runtimepath = "";

	/** configuration's root
	 * @param cfgRoot
	 * @param runtimeRoot absolute path to current dir (test) or container root (web app)
	 */
	public static void configRoot(String cfgRoot, String runtimeRoot) {
		cfgroot = cfgRoot;
		runtimepath = runtimeRoot;
		
		Utils.logi("Configuration root path: %s", cfgRoot);
		Utils.logi("Runtime root path: %s", runtimeRoot);
	}

	protected static IUser dummy;

	@Override
	public TableMeta tableMeta(String conn, String tabl) throws SemanticException {
		try {
			HashMap<String, TableMeta> metas = Connects.getMeta(conn);
			if (metas != null && metas.containsKey(tabl))
				return metas.get(tabl);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new SemanticException(e.getMessage());
		}
		throw new SemanticException("Can't find table meta: %s : %s", conn, tabl);
	}

	/**[conn, [table, DASemantics]] */
	protected static HashMap<String, HashMap<String, DASemantics>> smtConfigs;

	/**
	 * <p>Create a new semantext instance with the static resources.</p>
	 * 
	 * {@link DATranscxt} use a basic context (without semantics handler) for basic sql building.<br>
	 * Every context used for {@link DASemantics} handling must use this to create a new context instance.
	 * @param connId connection id usually mapped with client function uri, like: Connects.uri2conn(req.uri())
	 * @param usr
	 * @see ISemantext 
	 * @return semantext
	 * @throws TransException 
	 */
	@Override
	public ISemantext instancontxt(String connId, IUser usr) throws TransException {
		try {
			return new DASemantext(connId, getSmtcs(connId),
				Connects.getMeta(connId), usr, runtimepath);
		} catch (SemanticException | SQLException | SAXException | IOException e) {
			// meta is null? shouldn't happen because this instance is already created
			e.printStackTrace();
			throw new TransException(e.getMessage());
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
				AnResultset rs = Connects.select(sctx.connId(), sqls.get(0));
				rs.total(rs.getRowCount());
				sctx.onSelected(rs);
				return new SemanticObject().rs(rs, rs.total());
			}
			else {
				AnResultset total = Connects.select(sctx.connId(),
					((DASemantext) sctx).totalSql(sqls.get(0)));
				total.beforeFirst().next();
				int t = total.getInt(1);

				AnResultset rs = Connects.select(sctx.connId(),
					((DASemantext) sctx).pageSql(sqls.get(0), (int)q.page(), (int)q.size()));
				rs.total(t);

				sctx.onSelected(rs);
				return new SemanticObject().rs(rs, t);
			}
		});
		return q;
	}

	/**Create an insert statement that will report affected rows as data entry "total".
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
			
			// Since v1.4.12, table stamps is handled here
			sctx.onCommitted(sctx, tabl);

			return new SemanticObject().addInts("total", r).put("resulved", sctx.resulves());
		});
		return i;
	}
	
	@SuppressWarnings("unchecked")
	public static String findResulved(SemanticObject rslt, String tabl, String pk) {
		return ((HashMap<String, String>) ((SemanticObject) rslt.get("resulved")).get(tabl)).get(pk);
	}

	/**Create an update statement that will report affected rows as data entry "total".
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
			
			// Since v1.4.12, moving external files & table stamps are handled here
			sctx.onCommitted(sctx, tabl);

			return new SemanticObject().addInts("total", r).put("resulved", sctx.resulves());
		});
		return u;
	}

	/**Create a delete statement that will report affected rows as data entry "total".
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
			
			// Since v1.4.12, deleting external files & table stamps are handled here
			sctx.onCommitted(sctx, tabl);

			return new SemanticObject().addInts("total", r).put("resulved", sctx.resulves());
		});
		return d;
	}

	public String getSysConnId() { return Connects.defltConn(); }
	public boolean getSysDebug() { return Connects.getDebug(Connects.defltConn()); }

	/**<p>Create a transact builder with basic DASemantext instance.</p>
	 * <p>If it's a null configuration, the semantics can not be used to resulving semantics between records,
	 * but can be used to do basic sql operation. (resulving is a special concept of semantic-*, see docs)</p>
	 * 
	 * When creating DATranscxt, db metas can not be null.
	 * 
	 * @param conn connection Id
	 * @throws SQLException 
	 * @throws IOException load semantics configuration failed
	 * @throws SAXException load semantics configuration failed
	 * @throws SemanticException 
	 */
	public DATranscxt(String conn) throws SQLException, SAXException, IOException, SemanticException {
		super(new DASemantext(conn, getSmtcs(conn),
				Connects.getMeta(conn), null, runtimepath));
	}
	
	public static boolean alreadyLoaded(String connId) {
		return smtConfigs != null && smtConfigs.containsKey(connId);
	}

	protected static HashMap<String, DASemantics> getSmtcs(String conn)
			throws SAXException, IOException, SQLException, SemanticException {
		if (smtConfigs == null)
			smtConfigs = new HashMap<String, HashMap<String, DASemantics>>();

		if (!smtConfigs.containsKey(conn)) {
			String fpath = Connects.getSmtcsPath(conn);
			if (isblank(fpath, "\\."))
				throw new SemanticException(
					"Trying to find semantics of conn %1$s, but the configuration path is empty.\n" +
					"No 'smtcs' configured in connects.xml for connection \"%1$s\"?\n" +
					"Looking in path: %2$s", conn, fpath);
			loadSemantics(conn, fpath, Connects.getDebug(conn));
		}
		return smtConfigs.get(conn);
	}

	/**Load semantics configuration from filepath.
	 * This method also initialize table meta by calling {@link Connects}.
	 * @param connId
	 * @param cfgpath full path to semantics.xml (path and name) 
	 * @param debug 
	 * @return configurations
	 * @throws SAXException
	 * @throws IOException
	 * @throws SQLException 
	 * @throws SemanticException 
	 */
	public static HashMap<String, DASemantics> loadSemantics(String connId, String cfgpath, boolean debug)
			throws SAXException, IOException, SQLException, SemanticException {
		Utils.logi("Loading Semantics (fullpath):\n\t%s", cfgpath);
		if (cfgpath == null) {
			Utils.warn("\nConnect's semantics configuration file can't be found:\n%s\n", connId);
			return null;
		}
		else {
			LinkedHashMap<String, XMLTable> xtabs = XMLDataFactoryEx.getXtables(
				new Log4jWrapper("").setDebugMode(false), cfgpath, new IXMLStruct() {
						@Override public String rootTag() { return "semantics"; }
						@Override public String tableTag() { return "t"; }
						@Override public String recordTag() { return "s"; }});

			XMLTable xconn = xtabs.get("semantics");
			
			return initConfigs(connId, xconn, debug);
		}
	}
	
	protected static HashMap<String, DASemantics> initConfigs(String conn, XMLTable xcfg, boolean debug)
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
				addSemantics(conn, tabl, pk, smtc, args, debug);
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
	
	public static SemanticHandler getHandler(String conn, String tabl, smtype sm) {
		if (smtConfigs == null || !smtConfigs.containsKey(conn)
				|| !smtConfigs.get(conn).containsKey(tabl))
			return null;
		DASemantics s = smtConfigs.get(conn).get(tabl);
		return s.handler(sm);
	}

	public static void addSemantics(String connId, String tabl, String pk,
			String smtcs, String args, boolean debug) throws SQLException, SAXException, IOException, SemanticException {
		smtype sm = smtype.parse(smtcs);
		addSemantics(connId, tabl, pk, sm, args, debug);
	}

	public static void addSemantics(String connId, String tabl, String pk,
			smtype sm, String args, boolean debug) throws SQLException, SAXException, IOException, SemanticException {
		addSemantics(connId, tabl, pk, sm, split(args, ","), debug);
	}

	public static void addSemantics(String conn, String tabl, String pk,
			smtype sm, String[] args, boolean debug) throws SQLException, SAXException, IOException, SemanticException {
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
			s = new DASemantics(getBasicTrans(conn), tabl, pk, debug);
			ss.put(tabl, s);
		}
		s.addHandler(sm, tabl, pk, args);
	}

	//////////// basic transact builders for each connection ////////////
	private static HashMap<String, Transcxt> basicTrxes;
	private static HashMap<String, String> keys;

	/**Get a basic transact builder (without semantics handling)
	 * @param conn
	 * @return the basic transact builder
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

	/**Set a key.
	 * @param name
	 * @param key
	 */
	public static void key(String name, String key) {
		if (keys == null)
			keys = new HashMap<String, String>();
		keys.put(name, key);
	}
	
	public static String key(String name) {
		return keys == null ? null : keys.get(name);
	}

	public Date now(String conn) throws TransException, SQLException {
		if (isblank(conn))
			conn = Connects.defltConn();
		
		AnResultset rs = (AnResultset) select("oz_autoseq", "t")
				.col(Funcall.now(), "n")
				.rs(instancontxt(conn, dummyUser()))
				.rs(0);

		rs.next();
		
		return rs.getDateTime("n");
	}
	
	public static IUser dummyUser() {
		if (dummy == null)
			dummy = new IUser() {
					@Override public TableMeta meta(String ... connId) { return null; }
					@Override public String uid() { return "dummy"; }
					@Override public IUser logAct(String funcName, String funcId) { return null; }
					@Override public IUser notify(Object note) throws TransException { return this; }
					@Override public List<Object> notifies() { return null; }
					@Override public long touchedMs() { return 0; }
					@Override public IUser sessionKey(String ssId) { return this; }
					@Override public String sessionKey() { return null; } };
		return dummy;
	}

	public boolean exists(String conn, String tbl, String id)
			throws TransException, SQLException {
		if (isblank(conn))
			conn = Connects.defltConn();
		AnResultset rs = (AnResultset) select("oz_autoseq", "t")
				.col(Funcall.count(), "c")
				.rs(instancontxt(conn, dummyUser()))
				.rs(0);

		rs.next();
		
		return rs.getInt("c") > 0;
	}
}
