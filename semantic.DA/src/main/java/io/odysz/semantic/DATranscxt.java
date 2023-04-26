package io.odysz.semantic;

import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.split;

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
import io.odysz.semantic.DASemantics.ShAutoK;
import io.odysz.semantic.DASemantics.ShChkCntDel;
import io.odysz.semantic.DASemantics.ShChkPCInsert;
import io.odysz.semantic.DASemantics.ShDefltVal;
import io.odysz.semantic.DASemantics.ShDencrypt;
import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.ShFkInsCates;
import io.odysz.semantic.DASemantics.ShFkOnIns;
import io.odysz.semantic.DASemantics.ShFullpath;
import io.odysz.semantic.DASemantics.ShOperTime;
import io.odysz.semantic.DASemantics.ShPCDelAll;
import io.odysz.semantic.DASemantics.ShPCDelByCate;
import io.odysz.semantic.DASemantics.ShPostFk;
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
 * See {@link #loadSemantics(String, String)}. <br>
 * 
 * Every sql building needing semantics handling must use a context instance
 * created by {@link DATranscxt#instancontxt(String, IUser)}.
 * 
 * @author odys-z@github.com
 */
public class DATranscxt extends Transcxt {
	/**
	 * defualt example:<br>
	 * (c) -> new SemanticsMap(c)
	 * 
	 * <p>So, calling {@link DATranscxt#initConfigs(String, XMLTable, SMapFactory) can be:<pre>
	 * initConfigs(connId, (b, conn, pk, d) -> new SemanticsMap(b, conn, pk, d));</pre>
	 * </p>
	 * 
	 * @since 1.5.0
	 * @author odys-z@github.com
	 */

	@FunctionalInterface
	public interface SmtcFactory<S extends DASemantics> {
		S ctor(Transcxt trb, String conn, String pk, boolean debug);
	}

	/**
	 * 
	 * Semantics handler's map manager.
	 * 
	 * @since 1.5.0
	 * @author odys-z@github.com
	 */
	public static class SemanticsMap {

		String conn;
		
		protected HashMap<String, DASemantics> ss;
		
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

		SemanticHandler parseHandler(Transcxt basicTrs, XMLTable x) {
			return null;
		}

//		public void addSemantics(String conn, String tabl, smtype sm,
//				String pk, String args, boolean ... debug)
//				throws SemanticException, SQLException, SAXException, IOException {
//			if (ss == null) {
//				// ss = new HashMap<String, DASemantics>();
//				SemanticsMap smap = new SemanticsMap(conn);
//				smtConfigs.put(conn, smap);
//			}
//
//			DASemantics s = ss.get(tabl);
//			if (s == null) {
//				s = new DASemantics(getBasicTrans(conn), tabl, pk, debug);
//				ss.put(tabl, s);
//			}
//
//			s.addHandler(sm, tabl, pk, split(args, ","));
//		}

//		public static SemanticHandler parseHandler(Transcxt basicTsx, String tabl, smtype semantic,
//				String recId, String argstr, boolean ... debug)
//				throws SemanticException {
//			// checkParas(tabl, pk, args);
////			if (isDuplicate(tabl, semantic))
////				return;
//			SemanticHandler handler = null;
//
//			String[] args = split(argstr);
//
//			if (smtype.fullpath == semantic)
//				handler = new ShFullpath(basicTsx, tabl, recId, args);
//			else if (smtype.autoInc == semantic)
//				handler = new ShAutoK(basicTsx, tabl, recId, args);
//			else if (smtype.fkIns == semantic)
//				handler = new ShFkOnIns(basicTsx, tabl, recId, args);
//			else if (smtype.fkCateIns == semantic)
//				handler = new ShFkInsCates(basicTsx, tabl, recId, args);
//			else if (smtype.parentChildrenOnDel == semantic)
//				handler = new ShPCDelAll(basicTsx, tabl, recId, args);
//			else if (smtype.parentChildrenOnDelByCate == semantic)
//				handler = new ShPCDelByCate(basicTsx, tabl, recId, args);
//			else if (smtype.defltVal == semantic)
//				handler = new ShDefltVal(basicTsx, tabl, recId, args);
//			else if (smtype.dencrypt == semantic)
//				handler = new ShDencrypt(basicTsx, tabl, recId, args);
//			// else if (smtype.orclob == semantic)
//			// addClob(tabl, recId, argss);
//			else if (smtype.opTime == semantic)
//				handler = new ShOperTime(basicTsx, tabl, recId, args);
//			else if (smtype.checkSqlCountOnDel == semantic)
//				handler = new ShChkCntDel(basicTsx, tabl, recId, args);
//			else if (smtype.checkSqlCountOnInsert == semantic)
//				handler = new ShChkPCInsert(basicTsx, tabl, recId, args);
//			else if (smtype.postFk == semantic)
//				handler = new ShPostFk(basicTsx, tabl, recId, args);
//			else if (smtype.extFile == semantic)
//				// throw new SemanticException("Since 1.5.0, smtype.extFile is replaced by extFilev2!");
//				handler = new ShExtFilev2(basicTsx, tabl, recId, args);
//			else if (smtype.extFilev2 == semantic)
//				handler = new ShExtFilev2(basicTsx, tabl, recId, args);
//			else
//				throw new SemanticException("Cannot load configured semantics of key: %s", semantic);
//	
//			return handler;
//		}

//		public SemanticsMap map(HashMap<String, DASemantics> m) {
//			ss = m;
//			return this;
//		}
	}

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
	// protected static HashMap<String, HashMap<String, DASemantics>> smtConfigs;

	protected static HashMap<String, SemanticsMap> smtConfigs;

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
			return new DASemantext(connId, getSmtcs(connId), usr, runtimepath);
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
		this(new DASemantext(conn, getSmtcs(conn),
				dummyUser(), runtimepath));
	}
	
	protected DATranscxt(DASemantext stxt) {
		super(stxt);
	}

	public static boolean alreadyLoaded(String connId) {
		return smtConfigs != null && smtConfigs.containsKey(connId);
	}

	protected static SemanticsMap getSmtcs(String conn)
			throws SAXException, IOException, SQLException, SemanticException {
		if (smtConfigs == null)
			// smtConfigs = new HashMap<String, HashMap<String, DASemantics>>();
			smtConfigs = new HashMap<String, SemanticsMap>();

		if (!smtConfigs.containsKey(conn)) {
			initConfigs(conn, loadSemantics(conn),
				(trb, tbl, pk, ver) -> new DASemantics(trb, tbl, pk, ver));
		}
		return smtConfigs.get(conn);
	}

	/**
	 * Load semantics configuration from file path.
	 * This method also initialize table meta by calling {@link Connects}.
	 * 
	 * @param connId
	 * @param debug 
	 * @return configurations
	 * @throws SAXException
	 * @throws IOException
	 * @throws SQLException 
	 * @throws SemanticException 
	 */
	public static XMLTable loadSemantics(String connId) throws SAXException, IOException, SemanticException {

		String fpath = Connects.getSmtcsPath(connId);
		if (isblank(fpath, "\\."))
			throw new SemanticException(
				"Trying to find semantics of conn %1$s, but the configuration path is empty.\n" +
				"No 'smtcs' configured in connects.xml for connection \"%1$s\"?\n" +
				"Looking in path: %2$s", connId, fpath);
		Utils.logi("Loading Semantics (fullpath):\n\t%s", fpath);

		LinkedHashMap<String, XMLTable> xtabs = XMLDataFactoryEx.getXtables(
			new Log4jWrapper("").setDebugMode(false), fpath, new IXMLStruct() {
					@Override public String rootTag() { return "semantics"; }
					@Override public String tableTag() { return "t"; }
					@Override public String recordTag() { return "s"; }});

		XMLTable xconn = xtabs.get("semantics");
		if (xconn == null)
			throw new SemanticException("Xml structure error (no semantics table) in\n%s", fpath);
		
		// return initConfigs(connId, xconn, debug);
		return xconn;
	}
	
	/*
	private static SemanticsMap initConfigs(String conn, XMLTable xcfg)
			throws SAXException, IOException, SQLException, SemanticException {
		xcfg.beforeFirst();
		if (smtConfigs == null)
			smtConfigs = new HashMap<String, SemanticsMap>();

		Transcxt trb = null;
		boolean debug = Connects.getDebug(conn);
		
		HashMap<String, DASemantics> m = new HashMap<String, DASemantics>(); 

		xcfg.map(
			(XMLTable t) -> {
				String tabl = xcfg.getString("tabl");
				String pk = xcfg.getString("pk");
				String smtc = xcfg.getString("smtc");
				String args = xcfg.getString("args");
				
				// because the table is not come with pk = tabl, returned value is useless.
				if (!m.containsKey(tabl))
					m.put(tabl, new DASemantics(trb, tabl, pk, debug));
				m.get(tabl).addHandler(
					SemanticsMap.parseHandler(trb, tabl, smtype.parse(smtc), pk, args, debug));
				return null;
			});

		SemanticsMap semanticMap = new SemanticsMap(conn).map(m);
		smtConfigs.put(conn, semanticMap);
		return smtConfigs.get(conn);
	}
	*/
	
	@SuppressWarnings("unchecked")
	public static <M extends SemanticsMap, S extends DASemantics> M initConfigs(String conn,
			XMLTable xcfg, SmtcFactory<S> smFactory)
			throws SAXException, IOException, SQLException, SemanticException {
		xcfg.beforeFirst();
		if (smtConfigs == null)
			smtConfigs = (HashMap<String, SemanticsMap>) new HashMap<String, M>();

		Transcxt trb = getBasicTrans(conn);
		boolean debug = Connects.getDebug(conn);
		
		HashMap<String, S> m = new HashMap<String, S>(); 

		xcfg.map(
			(XMLTable t) -> {
				String tabl = xcfg.getString("tabl");
				String pk   = xcfg.getString("pk");
				String smtc = xcfg.getString("smtc");
				String args = xcfg.getString("args");
				
				// because the table is not come with pk = tabl, returned value is useless.
				if (!m.containsKey(tabl))
					// m.put(tabl, new DASemantics(trb, tabl, pk, debug));
					m.put(tabl, smFactory.ctor(trb, tabl, pk, debug));

				S smtcs = m.get(tabl);
				smtcs.addHandler(
					smtcs.parseHandler(trb, tabl, smtype.parse(smtc), pk, args, debug));
				return null;
			});

		smtConfigs.put(conn, S.semantics());
		return (M) smtConfigs.get(conn);
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

//	private static void addSemantics(String conn, String tabl,
//			String pk, String smtcs, String args, boolean debug)
//			throws SQLException, SAXException, IOException, SemanticException {
//		smtype sm = smtype.parse(smtcs);
//		if (smtConfigs == null) {
//			smtConfigs = new HashMap<String, SemanticsMap>();
//		}
//
//		SemanticsMap ss = smtConfigs.get(conn);
//		if (ss == null) {
//			ss = new SemanticsMap(conn);
//			smtConfigs.put(conn, ss);
//		}
//		ss.addSemantics(conn, tabl, sm, pk, args, debug);
//
////		DASemantics s = ss.get(tabl);
////		if (s == null) {
////			s = new DASemantics(getBasicTrans(conn), tabl, pk, debug);
////			ss.put(tabl, s);
////		}
////		s.addHandler(sm, tabl, pk, split(args, ","));
//	}
	
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
