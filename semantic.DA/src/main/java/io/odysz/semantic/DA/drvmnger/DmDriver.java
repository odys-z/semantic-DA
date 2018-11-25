package io.odysz.semantic.DA.drvmnger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.Lock;

import org.apache.commons.io.FilenameUtils;

import io.odysz.common.Regex;
import io.odysz.common.Utils;
import io.odysz.module.rs.ICResultset;
import io.odysz.module.xtable.ILogger;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactory;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.Connects.DriverType;
import io.odysz.semantic.DA.DatasetCfg;
import io.odysz.semantic.DA.DbLog;
import io.odysz.semantic.DA.IrSemantics;
import io.odysz.semantic.DA.Mappings;
import io.odysz.semantic.DA.OracleLob;
import io.odysz.semantics.meta.ColumnMeta;
import io.odysz.semantics.meta.DbMeta;
import io.odysz.semantics.meta.TableMeta;

/**This is the equivalent to DAO for driver-manager JDBC.<br>
 * Connection.xml is the configuration to change DA drivers.
 * @author ody
 */
public class DmDriver {
	private static HashMap<String, AbsConnect> srcs;

//	private static HashMap<String, HashMap<String, IrSemantics>>  metas;

	private static String defltConn;

	public static String defltConn() {return defltConn;}

	public static DriverType connType(String connId) {
		return srcs.get(connId).driverType();
	}

	public static DbMeta getDbMeta(String connId) {
		return srcs.get(connId).getSpec();
	}

	public static ICResultset select(String conn, String sql, int ... flags) throws SQLException {
		//return Mysql.select(sql);
		// return SqliteDriver.select(sql);
		if (conn == null) conn = defltConn;
		return srcs.get(conn).select(sql, flags != null && flags.length > 0 ? flags[0] : Connects.flag_nothing);
	}
	
	/**Use this to initialize connection without using servlet context for retrieving configured strings.<br>
	 * This is the typical scenario when running test from "main" thread.
	 * @param jdbc
	 * @param user
	 * @param psword
	 * @param dbg
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		// return Mysql.getConnection();
		return SqliteDriver.getConnection();
	}

	/**parse connection.xml/table id="drvmnger" pk="id" columns="type,id,isdef,conn,usr,pswd,dbg",
	 * initialize all connections.
	 * @param context
	 */
	public static void init(String xmlDir) {
		if (srcs != null) return;
		srcs = new HashMap<String, AbsConnect>();
		XMLTable conn = null;
		try{
			ILogger logger = new Log4jWrapper("xtabl");
			conn = XMLDataFactory.getTable(logger , "drvmnger", xmlDir + "/WEB-INF/connections.xml",
						new IXMLStruct() {
							@Override public String rootTag() { return "conns"; }
							@Override public String tableTag() { return "t"; }
							@Override public String recordTag() { return "c"; }});
			conn.beforeFirst();
			
			HashMap<String, HashMap<String, String>> orclMappings = null; 
			while (conn.next()) {
				try {
					// columns="type,id,isdef,conn,usr,pswd,dbg"
					String type = conn.getString("type");
					String id = conn.getString("id");
					if (type != null && type.trim().toLowerCase().equals("mysql")) {
						srcs.put(id, initMysqlDrv(conn.getString("src"),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false) ? Connects.flag_printSql : Connects.flag_nothing));
					}
					else if (type != null && type.trim().toLowerCase().equals("sqlite")) {
						srcs.put(id, initSqliteDrv(id, String.format("jdbc:sqlite:%s", FilenameUtils.concat(xmlDir, conn.getString("src"))),
//								conn.getString("src"),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false) ? Connects.flag_printSql : Connects.flag_nothing));
					}
					else if (type != null && type.trim().toLowerCase().equals("mssql2k")) {
						srcs.put(id, initMs2kDrv(conn.getString("src"),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false)));
					}
					else if (type != null && type.trim().toLowerCase().equals("oracle")) {
						// get name mapping config
						// String fullpath = getRealPath(conn.getString("nmap"));
						String fullpath = FilenameUtils.concat(xmlDir + "/", conn.getString("nmpa"));

						if (fullpath == null)
							throw new SQLException("Oracle connection need a nmap value, check connection.xml.");
						LinkedHashMap<String, XMLTable> xmappings = XMLDataFactoryEx.getXtables(new Log4jWrapper("nmap").setDebugMode(false), fullpath, Mappings.mapXStruct);
						if (xmappings == null | xmappings.size() == 0)
							throw new SQLException("Oracle connection need a nmap file, check " + fullpath);
						orclMappings = Mappings.convertMap(xmappings);

						// init with name mapping
						srcs.put(id, initOrclDrv(conn.getString("src"),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false), orclMappings));
					}
					else System.err.println(String.format("The configured DB type %s is not supported yet.", type));
					if (conn.getBool("isdef", false)) {
						if (defltConn != null)
							System.err.println("\nWARN - duplicate default id found, the previous defined source been ignored: " + defltConn);
						defltConn = id;
					}
				} catch (Exception e) {
					System.err.println(e.getMessage());
					continue;
				}
			}
		
			if (srcs != null && srcs.size() > 0 && !srcs.containsKey(defltConn))
				throw new SQLException("Failed initializing, db source must configured with a default source."); 

			conn.beforeFirst();
			DatasetCfg.init(conn, xmlDir, orclMappings);

			System.out.println(String.format("INFO - DmDriver initialized using %s (%s) as default datasource.",
					defltConn, srcs != null && srcs.size() > 0 ? srcs.get(defltConn).driverType() : "empty"));
		}
		catch (Exception ex) {
			System.err.println("\nFATAL - DmDriver initializing failed! !!\n");
			ex.printStackTrace();
			return;
		}
	}

	private static AbsConnect initMysqlDrv(String jdbc, String usr, String pswd, int dbg) throws SQLException {
		return MysqlDriver.initConnection(jdbc, usr, pswd, dbg);
	}

	/**<pre>
SELECT type, name, tbl_name FROM sqlite_master where type = 'table';

type  |name             |tbl_name         |
------|-----------------|-----------------|
table |ir_stub          |ir_stub          |
table |ir_stub2         |ir_stub2         |
table |ir_synctasks     |ir_synctasks     |
table |sqlite_sequence  |sqlite_sequence  |
table |sqlite_stat1     |sqlite_stat1     |
table |e_models         |e_models         |
table |e_nb_buffer      |e_nb_buffer      |
table |e_resport_data   |e_resport_data   |
table |e_resport_params |e_resport_params |
table |e_tcp_heart      |e_tcp_heart      |

PRAGMA table_info(ir_synctasks);

cid |name       |type     |notnull |dflt_value |pk |
----|-----------|---------|--------|-----------|---|
0   |syncId     |TEXT(50) |0       |           |1  |
1   |recId      |TEXT(50) |0       |           |0  |
2   |pushStamp  |INTEGER  |0       |           |0  |
3   |pullStamp  |INTEGER  |0       |           |0  |
4   |intervalss |INTEGER  |1       |-1         |0  |
5   |pushCols   |TEXT     |0       |           |0  |
6   |pgSize     |INTEGER  |1       |200        |0  |
7   |dir        |INTEGER  |0       |2          |0  |</pre>
	 * @param jdbc
	 * @param usr
	 * @param pswd
	 * @param dbg
	 * @return
	 * @throws SQLException
	 */
	private static AbsConnect initSqliteDrv(String connId, String jdbc, String usr, String pswd, int dbg) throws SQLException {

		DbMeta spec = new DbMeta();
		Regex regex = new Regex("(\\w+)");
//		DbSchema schema = spec.addDefaultSchema();

		SqliteDriver drv = SqliteDriver.initConnection(jdbc, usr, pswd, dbg);
		ICResultset rs = drv.select("SELECT type, name, tbl_name FROM sqlite_master where type = 'table'", Connects.flag_nothing);

		HashMap<String, HashMap<String, ColumnMeta>> tablCols = new HashMap<String, HashMap<String, ColumnMeta>>(rs.getRowCount());
		HashMap<String, TableMeta> tables = new HashMap<String, TableMeta>(rs.getRowCount());
		// also build locks and ir_autoseq initiation
//		HashMap<String, ReentrantLock> locks = new HashMap<String, ReentrantLock>(rs.getRowCount());
		
		rs.beforeFirst();
		while (rs.next()) {
			try {
				String tn = rs.getString("name");
				TableMeta tab = spec.addTable(tn);
				tables.put(tn, tab);
				tablCols.put(tn, buildColsSqlite(drv, connId, tab, regex));
			}
			catch (SQLException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				continue;
			}
		}
		
		drv = (SqliteDriver) drv.meta(spec, tables, tablCols, Connects.flag_printSql);

		drv.isSqlite(true);

		// drv.setLocks(locks);
//		drv.locks = locks;


		return drv;
	}

	/**1. construct columns of tab;<br>
	 * 2. create a Reentrant Lock for the table;<br>
	 * 3. insert a ir_autoseq record for the table if the record doesn't exists.<pre>
CREATE TABLE ir_autoseq (
  sid text(50),
  seq INTEGER,
  remarks text(200),
  CONSTRAINT ir_autoseq_pk PRIMARY KEY (sid)
);</pre>
	 * @param drv
	 * @param srcName
	 * @param tab
	 * @param regex
	 * @param locks
	 * @return
	 * @throws SQLException
	 */
	private static HashMap<String, ColumnMeta> buildColsSqlite(SqliteDriver drv, String srcName,
			TableMeta tab, Regex regex) throws SQLException {
		ICResultset rs = drv.select("PRAGMA table_info(" + tab.getName() + ")", Connects.flag_nothing);
		HashMap<String, ColumnMeta> cols = new HashMap<String, ColumnMeta>(rs.getRowCount());
		rs.beforeFirst();
		while (rs.next()) {
			/*
cid |name        |type     |notnull |dflt_value |pk |
----|------------|---------|--------|-----------|---|
0   |domainId    |text(40) |1       |           |1  |
1   |parentId    |text(40) |0       |           |0  |
2   |domainName  |text(50) |0       |           |0  |
3   |domainValue |text(50) |0       |           |0  |
4   |sort        |text(11) |0       |           |0  |
5   |others      |text(20) |0       |           |0  |
6   |fullpath    |text(80) |0       |           |0  |
7   |stamp       |text(20) |0       |           |0  |
			 */
			String tlen= rs.getString("type");
			if (tlen == null || tlen.trim().length() == 0) {
				System.err.println(String.format("Table meta ignored: tabl, name, type, notnull(%s, %s, %s, %s)",
						tab.getName(), rs.getString(2), rs.getString(3), rs.getString(4)));
				continue;
			}
			ArrayList<String> typeLen = regex.findGroups(tlen);
			int len = 0;
			try { len = Integer.valueOf(typeLen.get(1)); } catch (Exception e) {}
			ColumnMeta col = tab.addColumn(rs.getString("name"), typeLen.get(0), len == 0 ? null : len);
			cols.put(rs.getString("name"), col);
			
			if (rs.getBoolean("pk")) {
				// FIXME where to do this?
				// locks.put(tab.getName(), new ReentrantLock());
				
				// if doesn't exists an auto seq, insert one
				/*
CREATE TABLE ir_autoseq (
  sid text(50),
  seq INTEGER,
  remarks text(200),
  CONSTRAINT ir_autoseq_pk PRIMARY KEY (sid)
);				 */
				String sql = String.format("select seq from ir_autoseq where sid = '%s.%s'",
						tab.getName(), rs.getString("name"));
				ICResultset rseq = SqliteDriver.selectStatic(sql, Connects.flag_nothing);
				if (rseq.getRowCount() <= 0) {
					ArrayList<String> sqls = new ArrayList<String>(1);
					sqls.add(String.format("insert into ir_autoseq(sid, seq, remarks) values('%s.%s', 0, datetime('now'))",
						tab.getName(), rs.getString("name")));
					SqliteDriver.commitst(sqls, Connects.flag_nothing);
				}
			}
		}
		return cols;
	}

	private static AbsConnect initMs2kDrv(String string, String string2, String string3,
			boolean bool) throws SQLException {
		throw new SQLException("ms2k is not supported yet");
	}

	private static AbsConnect initOrclDrv(String string, String string2, String string3,
			boolean bool, HashMap<String, HashMap<String, String>> orclMappings) throws SQLException {
		throw new SQLException("orcale is not supported yet");
	}

	/**Commit with default connection
	 * @param log
	 * @param sqls
	 * @return 
	 * @throws SQLException 
	 */
	public static int[] commit(DbLog log, ArrayList<String> sqls, int flags) throws SQLException {
		return commit (defltConn, log, sqls, flags);
	}

	private static int[] commit(String connId, DbLog log, ArrayList<String> sqls, int flags) throws SQLException {
		int[] c = srcs.get(connId).commit(sqls, flags);
		if (log != null)
			log.log(sqls);
		else {
			System.err.println("Some db commitment not logged:");
			Utils.warn(sqls);
		}
		return c;
	}

	public static DriverType getConnType(String connId) {
		if (connId == null)
			return srcs.get(defltConn).driverType();
		else return srcs.get(connId).driverType();
	}

	public static ColumnMeta getColumn(String connId, String tabName, String expr) {
		if (connId == null)
			return srcs.get(defltConn).getColumn(tabName, expr);
		else return srcs.get(connId).getColumn(tabName, expr);
	}

	public static TableMeta getTable(String connId, String tabName) {
		if (connId == null)
			return srcs.get(defltConn).getTable(tabName);
		else return srcs.get(connId).getTable(tabName);
	}
	
	/**If oracle, to quoted upper case "FIELD"
	 * @param conn
	 * @param expr
	 * @return
	 */
	public static String formatFieldName(String conn, String expr) {
		return srcs.get(conn).formatFieldName(expr);
	}

	public static void reinstallSemantics(String conn, HashMap<String, IrSemantics> semantics) {
		srcs.get(conn).reinstallSemantics(semantics);
//		if (metas != null && metas.size() > 0) {
//			System.err.println("Clear and reinstall semantics ... ");
//			metas.clear();
//		} 
//		metas = semantics;
	}

	public static void reinstallSemantics(HashMap<String, HashMap<String, IrSemantics>> semantics) {
		if (semantics != null)
			for (String conn : semantics.keySet())
				reinstallSemantics(conn, semantics.get(conn));
	}

	public static IrSemantics getTableSemantics(String conn, String tabName) throws SQLException {
//		if (metas.containsKey(conn))
//			return metas.get(conn).get(tabName);
//		else return null;
		return srcs.get(conn).getTableSemantics(tabName);
	}

	public static int[] commit(DbLog log, ArrayList<String> sqls, ArrayList<OracleLob> lobs, int flags) throws SQLException {
		throw new SQLException(" TODO ...");
	}

	public static Lock getAutoseqLock(String conn, String target) throws SQLException {
		return srcs.get(conn).getAutoseqLock(target);
	}

	public static boolean isSqlite(String connId) {
		return srcs.get(connId).isSqlite();
	}

}
