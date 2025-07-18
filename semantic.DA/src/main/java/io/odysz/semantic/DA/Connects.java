package io.odysz.semantic.DA;

import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.f;
import static io.odysz.common.LangExt.len;

import java.io.File;
import java.io.IOException;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.NamingException;

import org.xml.sax.SAXException;

import io.odysz.common.dbtype;
import io.odysz.common.EnvPath;
import io.odysz.common.FilenameUtils;
import io.odysz.common.LangExt;
import io.odysz.common.Regex;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.module.xtable.ILogger;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactory;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.module.xtable.XMLTable.IMapValue;
import io.odysz.semantic.util.LogFlags;
import io.odysz.semantics.IUser;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * Connection configurations and DB table meta data manager.
 * 
 * @author odys-z@github.com
 */
@SuppressWarnings("deprecation")
public class Connects {
	/**Convert names like "sqlit" to {@link dbtype}.
	 * @param type
	 * @return db type
	 * @throws SemanticException
	 */
	public static dbtype parseDrvType(String type) throws SemanticException {
		if (type == null || type.trim().length() == 0)
			throw new SemanticException("Drived type not suppored: %s", type);
		type = type.trim().toLowerCase();
		if (type.equals("mysql")) 
			return dbtype.mysql;
		else if (type.equals("mssql2k") || type.equals("ms2k"))
			return dbtype.ms2k;
		else if (type.equals("oracle") || type.equals("orcl"))
			return dbtype.oracle;
		else if (type.startsWith("sqlit_queue") || type.startsWith("sqlit-queue"))
			return dbtype.sqlite_queue;
		else if (type.startsWith("sqlit") || type.startsWith("sqlite"))
			return dbtype.sqlite;
		else
			throw new SemanticException("Driver type not suppored yet: %s", type);
	}

	/** Connection (data sources) */
	private static HashMap<String, AbsConnect<? extends AbsConnect<?>>> srcs;
	public static Set<String> getAllConnIds() {
		return srcs == null ? null : srcs.keySet();
	}

	/** Component URI - connection mappings */
	private static LinkedHashMap<Regex, String> conn_uri;

	private static String defltConn;
	private static String workingDir;
	public static String defltConn() { return defltConn; }

	private static final int DmConn = 1;
	private static final int CpConn = 2;

	/**
	 * parse connects.xml, setup connections configured in table "drvmnger", for JDBC DriverManger,
	 * and "dbcp", for JDBC connection-pooled connection managed by container.
	 * 
	 * @param xmlDir configure file folder
	 */
	public static void init(String xmlDir) {
		Utils.logi("Initializing connects with path to %s", xmlDir);
		workingDir = xmlDir;
		if (srcs != null) return;
		srcs = new HashMap<String, AbsConnect<? extends AbsConnect<?>>>();
		try{
			ILogger logger = new Log4jWrapper("xtabl");
			srcs = loadConnects(srcs, "drvmnger", DmConn, logger, xmlDir);
			srcs = loadConnects(srcs, "dbcp",     CpConn, logger, xmlDir);

			conn_uri = loadConnUri("conn-uri", logger, xmlDir);
		
			if (srcs != null && srcs.size() > 0 && !srcs.containsKey(defltConn))
				throw new SQLException("Found connection configruations, but initialization has failed. DB source must be configured with a default source."); 

			if (LogFlags.Semantic.connects)
				Utils.logi("INFO - JDBC initialized using %s (%s) as default connection.",
					defltConn, srcs != null && srcs.size() > 0 ? srcs.get(defltConn).driverType() : "empty");
		}
		catch (Exception ex) {
			System.err.println("FATAL - Connection initializing failed! !!\n");
			ex.printStackTrace();
			return;
		}
	}
	
	static HashMap<String, AbsConnect<? extends AbsConnect<?>>> loadConnects(
			HashMap<String, AbsConnect<? extends AbsConnect<?>>> srcs,
			String tablId, int dmCp, ILogger logger, String xmlDir) throws SAXException {
		if (srcs == null)
			srcs = new HashMap<String, AbsConnect<? extends AbsConnect<?>>>();

		String absPath = FilenameUtils.concat(xmlDir, "connects.xml");
		Utils.logi(new File(absPath).getAbsolutePath());

		XMLTable conn = XMLDataFactory.getTable(logger , tablId, absPath, // xmlDir + "/connects.xml",
						new IXMLStruct() {
							@Override public String rootTag() { return "conns"; }
							@Override public String tableTag() { return "t"; }
							@Override public String recordTag() { return "c"; }});
		conn.beforeFirst();
			
		while (conn.next()) {
			try {
				// columns="type,id,isdef,conn,usr,pswd,dbg"
				dbtype type = parseDrvType(conn.getString("type"));
				String id = conn.getString("id");
				if (dmCp == DmConn)
					if (srcs.get(id) != null)
						;
					else
					srcs.put(id, AbsConnect.initDmConnect(xmlDir, type, id, conn.getString("src"),
						conn.getString("usr"), conn.getString("pswd"),
						conn.getBool("dbg", false), conn.getBool("log", false))
							.prop("smtcs", conn.getString("smtcs")));
				else
					srcs.put(id, AbsConnect.initPooledConnect(xmlDir, type, id, conn.getString("src"),
						conn.getString("usr"), conn.getString("pswd"),
						conn.getBool("dbg", false), conn.getBool("log", false))
							.prop("smtcs", conn.getString("smtcs")));

				if (conn.getBool("isdef", false)) {
					if (defltConn != null)
						Utils.warn("WARN - duplicate default ids found, the previous defined source been ignored: " + defltConn);
					defltConn = id;
				}
			} catch (Exception e) {
				Utils.warn("ERROR: Connection intiialization failed: %s. (default connection id can be null.)",
						conn.getString("type"));
				e.printStackTrace();
				continue;
			}
		}
		return srcs;
	}

	private static LinkedHashMap<Regex, String> loadConnUri(String tablId, ILogger logger, String xmlDir)
			throws SAXException, SemanticException {

		if (conn_uri == null)
			conn_uri = new LinkedHashMap<Regex, String>();

		String absPath = FilenameUtils.concat(xmlDir, "connects.xml");
		Utils.logi(new File(absPath).getAbsolutePath());

		XMLTable conn = XMLDataFactory.getTable(logger, tablId, absPath, //xmlDir + "/connects.xml",
						new IXMLStruct() {
							@Override public String rootTag() { return "conns"; }
							@Override public String tableTag() { return "t"; }
							@Override public String recordTag() { return "c"; }});
		
		if (conn == null)
			throw new SemanticException("Since v1.3.0, connects.xml/t[id='conn-uri' is necessary.");

		conn.beforeFirst();
			
		while (conn.next()) {
			try {
				String uriReg = conn.getString("uri");
				String connId = conn.getString("conn");

				conn_uri.put(new Regex(uriReg), connId);
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		return conn_uri;
	}

	public static void close() {
		if (srcs != null)
			for (AbsConnect<?> c : srcs.values())
				try {
					c.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
	}
	
	///////////////////////////////////// select ///////////////////////////////
	public static AnResultset select(String conn, String sql, int... flags) throws SQLException {
		// This is probably because of wrong configuration in connects.xml. 
		// if (flags != null && flags.length > 0 && flags[0] == flag_printSql )
			if (conn != null && !srcs.containsKey(conn))
				throw new SQLException("Can't find connection: " + conn);

		String connId = conn == null ? defltConn : conn;
		try {
			return srcs.get(connId)
				.select(sql, flags == null || flags.length <= 0 ? AbsConnect.flag_nothing : flags[0]);
		} catch (NamingException e) {
			throw new SQLException("Can't find connection, id=" + connId);
		}
	}

	public static AnResultset select(String sql, int... flags) throws SQLException {
		return select(null, sql, flags);
	}

	/**compose paged sql, e.g. for Oracle:<br>
	 * select * from (sql) t where rownum &gt; 0 and row num &lt; 14
	 * @param sql
	 * @param page
	 * @param size
	 * @return sql
	 * @throws TransException 
	 */
	public static String pagingSql(String conn, String sql, int page, int size)
			throws TransException {
		conn = conn == null ? defltConn : conn;
		dbtype driverType = srcs.get(conn).driverType();
		return pagingSql(driverType, sql, page, size);
	}

	public static String pagingSql(dbtype dt, String sql, int pageIx, int pgSize)
			throws TransException {
		if (pageIx < 0 || pgSize <= 0)
			return sql;
		long i1 = pageIx * pgSize;
		String r2 = String.valueOf(i1 + pgSize);
		String r1 = String.valueOf(i1);
		Stream<String> s;
		if (dt == dbtype.oracle)
			s = Stream.of("select * from (select t.*, rownum r_n_ from (", sql,
						") t order by rownum) t where r_n_ > ", r1, " and r_n_ <= ", r2);
		else if (dt == dbtype.ms2k)
			s = Stream.of("select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (", sql,
						") t) t where rownum > ", r1, " and rownum <= %s", r2);// FIXME "%s" is tested?
		else if (dt == dbtype.sqlite)
			// https://stackoverflow.com/a/51380906
			s = Stream.of("select * from (", sql, ") limit ", String.valueOf(pgSize), " offset ", r1);
		else // mysql
			s = Stream.of("select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (", sql,
						") t, (select @ic_num := 0) ic_t) t1 where rnum > ", r1, " and rnum <= ", r2);

		return s.collect(Collectors.joining(" "));
	}

	/////////////////////////////////// update /////////////////////////////
	/**
	 * Commit to default connection.
	 * @param usr
	 * @param sqls
	 * @param flags
	 * @return affected row count
	 * @throws SQLException
	 * @throws TransException
	 */
	public static int[] commit(IUser usr, ArrayList<String> sqls, int... flags) throws SQLException, TransException {
		try {
			return srcs.get(defltConn).commit(usr, sqls, flags.length > 0 ? flags[0] : AbsConnect.flag_nothing);
		} catch (NamingException e) {
			throw new TransException("Can't find connection, id=" + defltConn);
		}	
	}
	
	public static int[] commit(IUser usr, ArrayList<String> sqls, ArrayList<Clob> lobs, int... flags) throws SQLException {
		return srcs.get(defltConn).commit(usr, sqls, lobs, flags.length > 0 ? flags[0] : AbsConnect.flag_nothing);
	}

	@SuppressWarnings("serial")
	public static int[] commit(String conn, IUser usr, String sql, int... flags) throws SQLException, TransException {
		return commit(conn, usr, new ArrayList<String>() { {add(sql);} }, flags.length > 0 ? flags[0] : AbsConnect.flag_nothing);
	}
	
	public static int[] commit(String conn, IUser usr, ArrayList<String> sqls, int... flags)
			throws SQLException, TransException {

		if (srcs == null || !srcs.containsKey(conn))
			throw new SemanticException("Can't find connection %s.", conn);
		try {
			return srcs.get(conn).commit(usr, sqls, flags.length > 0 ? flags[0] : AbsConnect.flag_nothing);
		} catch (NamingException e) {
			throw new TransException("Can't find connection, id=" + defltConn);
		}
	}

	@SuppressWarnings("serial")
	public static int[] commit(IUser usr, final String sql) throws SQLException, TransException {
		return commit(usr, new ArrayList<String> () { {add(sql);} });
	}

	public static dbtype driverType(String conn) {
		conn = conn == null ? defltConn : conn;
		if (!srcs.containsKey(conn))
			throw new NullPointerException(f("Can't find datasourse: %s. Known sources: %s",
					conn, srcs.keySet().stream().collect(Collectors.joining(","))));
		return srcs.get(conn).driverType();
	}

	public static Set<String> connIds() {
		return srcs == null ? null : srcs.keySet();
	}

	/**
	 * <p>Build database tables' meta.</p>
	 * 
	 * @param conn
	 * @return metas
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws Exception 
	 */
	public static HashMap<String, TableMeta> loadMeta(String conn)
			throws SemanticException, SQLException {
		dbtype dt = driverType(conn);

		HashMap<String, TableMeta> metas = new HashMap<String, TableMeta>();

		if (dt == null)
			throw new SemanticException("Drived type not suppored: ", conn);
		if (dt == dbtype.mysql)
			metas = MetaBuilder.buildMysql(conn);
		else if (dt == dbtype.ms2k)
			metas = MetaBuilder.buildMs2k(conn);
		else if (dt == dbtype.oracle)
			metas = MetaBuilder.buildOrcl(conn);
		else if (dt == dbtype.sqlite || dt == dbtype.sqlite_queue)
			metas = MetaBuilder.buildSqlite(conn);
		else
			throw new SemanticException("Drived type not suppored: %s", dt.name());
		
		replaceSemantics(conn, metas);

		return metas;
	}

	/**
	 * Replace DB metas with classes defined in semantics.xml/t[id=metas]
	 * 
	 * @since 2.0.0
	 * @param metas
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws Exception 
	 */
	static void replaceSemantics(String connId, HashMap<String, TableMeta> metas) throws SemanticException {
		if (len(metas) > 0) {
			String fpath = Connects.getSmtcsPath(connId);
			LinkedHashMap<String, XMLTable> xtabs;
			try {
				xtabs = XMLDataFactoryEx.getXtables(
						new Log4jWrapper("").setDebugMode(false), fpath, new IXMLStruct() {
								@Override public String rootTag() { return "semantics"; }
								@Override public String tableTag() { return "t"; }
								@Override public String recordTag() { return "s"; }});
			} catch (SAXException | IOException e1) {
				e1.printStackTrace();
				throw new SemanticException(e1.getMessage());
			}

			XMLTable xmetas = xtabs.get("metas");

			if (xmetas != null) {
				HashMap<String, IMapValue> semetas;
				try {
					semetas = xmetas.map(
						(XMLTable t) -> {
							String tabl = xmetas.getString("tabl");
							String clzz = xmetas.getString("semanticlass");
							
							return (IMapValue) Class.forName(clzz)
									.getConstructor(String.class, String.class)
									.newInstance(tabl, connId);
						});
				} catch (Exception e) {
					e.printStackTrace();
					throw new SemanticException(e.getMessage());
				}

				for (String sm : semetas.keySet()) 
					metas.put(sm, (TableMeta) semetas.remove(sm));
			}
		}
	}
	
	protected static XMLTable loadMetaCfgs(String connId) throws SAXException, IOException, SemanticException {

		String fpath = Connects.getSmtcsPath(connId);
		if (isblank(fpath, "\\."))
			throw new SemanticException(
				"Trying to find semantics of conn %1$s, but the configuration path is empty.\n" +
				"No 'smtcs' configured in connects.xml for connection \"%1$s\"?\n" +
				"Looking in path: %2$s", connId, fpath);
		

		LinkedHashMap<String, XMLTable> xtabs = XMLDataFactoryEx.getXtables(
			new Log4jWrapper("").setDebugMode(false), fpath, new IXMLStruct() {
					@Override public String rootTag() { return "semantics"; }
					@Override public String tableTag() { return "t"; }
					@Override public String recordTag() { return "s"; }});

		XMLTable xconn = xtabs.get("semantics");
		if (xconn == null)
			throw new SemanticException("Xml structure error (no semantics table) in\n%s", fpath);
		
		return xconn;
	}

	protected static HashMap<String, HashMap<String, TableMeta>> metas;

	/**
	 * @since 1.5.17
	 * @param conn
	 * @return removed metas
	 */
	public static HashMap<String,TableMeta> clearMeta(String conn) {
		if (metas != null)
			return metas.remove(conn);
		else return null;
	}
	
	public static HashMap<String, TableMeta> getMeta(String connId)
			throws SemanticException, SQLException {
		if (metas == null)
			metas = new HashMap<String, HashMap<String, TableMeta>>(srcs.size());

		if (connId == null)
			connId = defltConn;

		if (!metas.containsKey(connId))
			metas.put(connId, loadMeta(connId));

		if (!metas.containsKey(connId)) {
			metas.put(connId, new HashMap<String, TableMeta>(0));
			throw new SemanticException("reached here?");
		}
		
		return metas.get(connId);
	}
	
	/**
	 * @since 1.4.25
	 * @param connId
	 * @param tbl
	 * @return table meta
	 * @throws SemanticException
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static TableMeta getMeta(String connId, String tbl)
			throws SemanticException, SQLException {
		return getMeta(connId).get(tbl);
	}

	/**
	 * Set table meta (providing a chance of extending table's semantics and keep a single copy for DB's meta).
	 * @since 1.4.25
	 * @param connId
	 * @param m
	 * @throws SemanticException
	 * @throws SQLException
	 * @throws IOException 
	 * @throws SAXException 
	 */
	public static void setMeta(String connId, TableMeta m) throws SemanticException, SQLException {
		if (m == null || !m.typesInited())
			throw new SemanticException(
				"Arg or arg's types are null, TableMeta is not loaded from DB? Call getMeta() and extend it.");
		TableMeta mdb = getMeta(connId, m.tbl);
		if (mdb == null)
			throw new SemanticException("Table %s deson't exists in connect %s.", m.tbl, connId);
		getMeta(connId).put(m.tbl, m);
	}

	/**
	 * Get the smtcs file path configured in connects.xml.
	 * 
	 * @param conn
	 * @return smtcs (e.g. semantics.xml)
	 */
	public static String getSmtcsPath(String conn) {
		if (conn == null)
			conn = defltConn;
		return FilenameUtils.concat(workingDir,
				srcs == null || !srcs.containsKey(conn) ? null
				: EnvPath.replaceEnv(srcs.get(conn).prop("smtcs")));
	}

	public static boolean getDebug(String conn) {
		if (conn == null)
			conn = defltConn;
		return srcs.get(conn).enableSystemout;
	}

	public static void setDebug(String conn, boolean debug) {
		if (conn == null)
			conn = defltConn;
		srcs.get(conn).enableSystemout = debug;
	}

	/**
	 * Mapping client function id to data connection according to configuration.
	 * 
	 * Uri pattern is matched with prefix of strings' in db config.xml.
	 * 
	 * @param uri
	 * @return
	 * @throws SemanticException uri is null
	 */
	public static String uri2conn(String uri) throws SemanticException {
		if (LangExt.isblank(uri))
			throw new SemanticException("[uri -> conn] Function's uri cannot be null! Which is used for connecting datasource.");
		for (Regex reg : conn_uri.keySet())
			if (reg.match(uri))
				return conn_uri.get(reg);
		return defltConn;
	}

	public static boolean isqlite(String conn) {
		return Connects.driverType(conn) == dbtype.sqlite
			|| Connects.driverType(conn) == dbtype.sqlite_queue;
	}

	public static AbsConnect<?> getConnect(String id) {
		return srcs.get(id); //.commit(null, CpConn);
	}
}
