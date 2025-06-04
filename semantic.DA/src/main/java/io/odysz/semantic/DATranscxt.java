package io.odysz.semantic;

import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.split;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
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
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.AbsPart;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**
 * A {@link io.odysz.transact.sql.Statement Statement} builder that can providing
 * statements handling callback methods.<br>
 * 
 * <p>Those statements are the starting points to build a sql transact for querying,
 * updating, etc.<br>
 * 
 * For how to use the created statements, see the testing class:
 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/java/io/odysz/semantic/DASemantextTest.java'>
 * DASemantextTest</a>.</p>
 * 
 * This manager can handling semantics configured in xml.
 * See {@link #loadSemanticsXml(String)}. <br>
 * 
 * Every sql building needing semantics handling must use a context instance
 * created by {@link DATranscxt#instancontxt(String, IUser)}.
 * 
 * @author odys-z@github.com
 */
@SuppressWarnings("deprecation")
public class DATranscxt extends Transcxt {
	/**
	 * <p>Callback for buiding a connection's semantics map, with map-key = table.</p>
	 * Example:<br>
	 * initConfigs(conn) -gt; new SynmanticsMap(conn);
	 * 
	 * @since 1.4.25
	 * @author odys-z@github.com
	 */

	@FunctionalInterface
	public interface SmapFactory<M extends SemanticsMap> {
		M ctor(String conn);
	}

	/**
	 * Semantics handler's map manager.
	 * 
	 * @since 1.4.25
	 * @author odys-z@github.com
	 */
	public static class SemanticsMap {

		public String conn;
		
		/** {table: semantics[handlers]} */
		public HashMap<String, DASemantics> ss;
		
		public SemanticsMap(String conn) {
			this.conn = conn;
			ss = new HashMap<String, DASemantics>();
		}
		
		public boolean containsKey(String tabl) {
			return ss != null && ss.containsKey(tabl);
		}

		public DASemantics get(String tabl) {
			return ss == null ? null : ss.get(tabl);
		}
		
		public List<SemanticHandler> get(smtype t) {
			List<SemanticHandler> handlers = new ArrayList<SemanticHandler>();
			if (ss != null)
			for (DASemantics s : ss.values())
				handlers.add(s.handler(t));
			return handlers;
		}

//		SemanticHandler parseHandler(Transcxt basicTrs, XMLTable x) {
//			return null;
//		}
		
		/**
		 * Note: trb is already created per the connection, i. e. connect id is known. 
		 * @param trb
		 * @param tabl
		 * @param pk
		 * @param debug
		 * @return
		 */
		public DASemantics createSemantics(Transcxt trb, String tabl, String pk, boolean debug) {
			return new DASemantics(trb, tabl, pk, debug);
		}
	}

	protected static String cfgroot = ""; 
	protected static String runtimepath = "";
	public static String runtimeRoot() { return runtimepath; }

	/**
	 * Configuration's root
	 * @since 1.4.25 will using EnvPath for this.
	 * @param cfgRoot
	 * @param absRuntimeRoot absolute path to current dir (test) or container root (web app)
	 */
	public static void configRoot(String cfgRoot, String absRuntimeRoot) {
		cfgroot = cfgRoot;
		runtimepath = absRuntimeRoot;
		
		Utils.logi("Configuration root path: %s", cfgRoot);
		Utils.logi("Runtime root path: %s", absRuntimeRoot);
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

	/** { conn: map{table: DASemantics[handlers]} } */
	protected static HashMap<String, SemanticsMap> smtMaps;

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
			return new DASemantext(connId,
				initConfigs(connId, loadSemanticsXml(connId),
						(c) -> new SemanticsMap(c)),
				usr, runtimepath);
		} catch (Exception e) {
			// meta is null? shouldn't happen because this instance is already created
			e.printStackTrace();
			throw new TransException(e.getMessage());
		}
	}

	/**
	 * Create a select statement.
	 * 
	 * <p>This statement is the starting points to build a sql transact for querying.<br>
	 * 
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

	/**
	 * Create an insert statement that will report affected rows as data entry "total".
	 * 
	 * <p>Those statements are the starting points to build a sql transact for querying, updating, etc.<br>
	 * For how to use the created statements, see the testing class:
	 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/java/io/odysz/semantic/DASemantextTest.java'>
	 * DASemantextTest</a>.</p>
	 * 
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

			return new SemanticObject()
					.addInts("total", r)
					.put("resulved", sctx.resulves());
		});
		return i;
	}
	
	@SuppressWarnings("unchecked")
	public static String findResulved(SemanticObject rslt, String tabl, String pk) {
		return ((HashMap<String, String>) ((SemanticObject) rslt.get("resulved")).get(tabl)).get(pk);
	}

	/**
	 * Create an update statement that will report affected rows as data entry "total".
	 * 
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

	/**
	 * <p>Create a transact builder with basic DASemantext instance.</p>
	 * 
	 * <p>If it's a null configuration, the semantics can not be used to resulving semantics between records,
	 * but can be used to do basic sql operation. (resulving is a special concept of semantic-*, see docs)</p>
	 * 
	 * When creating DATranscxt, db metas can not be null, and the first time creating globally
	 * will trigger the semantics loading.
	 * 
	 * @param conn connection Id
	 * @throws Exception 
	 */
	public DATranscxt(String conn) throws Exception {
		this(new DASemantext(conn,
				isblank(conn) ? null : initConfigs(conn, loadSemanticsXml(conn),
						(c) -> new SemanticsMap(c)),
				dummyUser(), runtimepath));
		if (isblank(conn))
			Utils.warnT(new Object() {},
				"Since v1.5.0, an empty connection ID won't trigger the semantics loading.");
	}
	
	/**
	 * Create a stub transaction helper without depending on a database connection,
	 * typically for initialization.
	 * @since 2.0.0
	 * @throws Exception
	 */
	public DATranscxt() throws Exception {
		this((String)null);
	}

	protected DATranscxt(DASemantext stxt) {
		super(stxt);
	}

	/**
	 * Load semantics configuration, x-table, from file path.
	 * This method also initialize table meta by calling {@link Connects}.
	 * 
	 * @param connId
	 * @return configurations
	 * @throws SAXException
	 * @throws IOException
	 * @throws SQLException 
	 * @throws SemanticException 
	 */
	public static XMLTable loadSemanticsXml(String connId)
			throws SAXException, IOException, SemanticException {

		String fpath = Connects.getSmtcsPath(connId);
		if (isblank(fpath, "\\."))
			throw new SemanticException(
				"Trying to find semantics of conn %1$s, but the configuration path is empty.\n" +
				"No 'smtcs' configured in connects.xml for connection \"%1$s\"?\n" +
				"Looking in path: %2$s", connId, fpath);
		
		Utils.logi("[%s] load semantics: %s", connId, fpath); // FIXME being parsing xml too many times

		LinkedHashMap<String, XMLTable> xtabs = XMLDataFactoryEx.getXtables(
			new Log4jWrapper("").setDebugMode(false), fpath, new IXMLStruct() {
					@Override public String rootTag() { return "semantics"; }
					@Override public String tableTag() { return "t"; }
					@Override public String recordTag() { return "s"; }});

		XMLTable xtbl = xtabs.get("semantics");
		if (xtbl == null)
			throw new SemanticException("Xml structure error (no semantics table) in\n%s", fpath);
		
		return xtbl;
	}
	
	/**
	 * Load {@link #smtMaps}.
	 * 
	 * @param <M> semantics map type
	 * @param <S> semantics
	 * @param conn
	 * @param xcfg
	 * @param smFactory
	 * @return map per {@code conn}
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static <M extends SemanticsMap, S extends DASemantics> M initConfigs(
			String conn, XMLTable xcfg, SmapFactory<M> smFactory)
			throws Exception {
		if (smtMaps == null)
			smtMaps = new HashMap<String, SemanticsMap>();
		if (!smtMaps.containsKey(conn))
			smtMaps.put(conn, smFactory.ctor(conn));
		else
			return (M) smtMaps.get(conn);

		Utils.logT(new Object() {}, "Loading semantics of connection %s", conn);
		xcfg.beforeFirst();

		Transcxt trb = getBasicTrans(conn);
		boolean debug = Connects.getDebug(conn);
		
		SemanticsMap s = smtMaps.get(conn); 
		xcfg.map(
			(XMLTable t) -> {
				String tabl = xcfg.getString("tabl");
				String pk   = xcfg.getString("pk");
				String smtc = xcfg.getString("smtc");
				String args = xcfg.getString("args");
				
				HashMap<String, DASemantics> m = s.ss;
				if (!m.containsKey(tabl))
					m.put(tabl, s.createSemantics(trb, tabl, pk, debug));

				S smtcs = (S) m.get(tabl);
				smtcs.addHandler(
					smtcs.parseHandler(trb, tabl, smtype.parse(smtc), pk, split(args)));

				// because the table is not come with pk = tabl, returned value is useless here.
				return null;
			});

		return (M) smtMaps.get(conn);
	}

	/**
	 * Call this only in case Semantics needing re-initialized, e. g. an Auto-key
	 * handler is loaded by previous tests and the seq number needs to be reset. 
	 */
	public static void clearSemanticsMaps() {
		smtMaps = null;
	}

	public static boolean hasSemantics(String conn, String tabl, smtype sm) {
		if (smtMaps == null || !smtMaps.containsKey(conn)
				|| !smtMaps.get(conn).ss.containsKey(tabl))
			return false;
		DASemantics s = smtMaps.get(conn).ss.get(tabl);
		return s != null && s.has(sm);
	}
	
	public static SemanticHandler getHandler(String conn, String tabl, smtype sm) {
		if (smtMaps == null || !smtMaps.containsKey(conn)
				|| !smtMaps.get(conn).ss.containsKey(tabl))
			return null;
		DASemantics s = smtMaps.get(conn).ss.get(tabl);
		return s.handler(sm);
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
	public static Transcxt getBasicTrans(String conn)
			throws SQLException, SAXException, IOException, SemanticException {
		if (basicTrxes == null)
			basicTrxes = new HashMap<String, Transcxt>();
		
		if (!basicTrxes.containsKey(conn)) {
			// DATranscxt tx = new DATranscxt(conn);
			DATranscxt tx = new DATranscxt(new DASemantext(conn, null, dummy, null));
			basicTrxes.put(conn, tx);
		}
		
		return basicTrxes.get(conn);
	}

	/**Set a key (configuration item).
	 * @param name e.g. connection's root-key is set here with name = "user-pswd".
	 * @param value
	 */
	public static void key(String name, String value) {
		if (keys == null)
			keys = new HashMap<String, String>();
		keys.put(name, value);
	}

	/**
	 * Set root key.
	 * @param k
	 * @since 1.5.14
	 */
	public static void rootkey(String k) {
		key("user-pswd", k);
	}
	
	/** Load a configuration item. 
	 * @param name configuration key, e.g. "user-pswd"
	 * @return the value
	 */
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

	@Override
	public AbsPart quotation(Object v, String conn, String tabl, String col) {
		if (v instanceof AbsPart)
			return (AbsPart) v;

		try {
			TableMeta mt = tableMeta(conn, tabl);
			return Statement.composeVal(v, mt, col);
		} catch (SemanticException e) {
			e.printStackTrace();
			return null;
		}
	}
}
