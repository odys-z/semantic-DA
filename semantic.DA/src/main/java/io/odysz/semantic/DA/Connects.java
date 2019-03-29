package io.odysz.semantic.DA;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.xml.sax.SAXException;

import io.odysz.common.dbtype;
import io.odysz.common.Utils;
import io.odysz.module.rs.SResultset;
import io.odysz.module.xtable.ILogger;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactory;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.util.LogFlags;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;

public class Connects {
	// TODO: separate log switches from semantic flags like adding "''".
	/** nothing special for commit */
	public static final int flag_nothing = 0;
	public static final int flag_printSql = 1;
	public static final int flag_disableSql = 2;

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
		else if (type.startsWith("sqlit"))
			return dbtype.sqlite;
		else
			throw new SemanticException("Drived type not suppored: %s", type);
	}

	private static HashMap<String, AbsConnect<? extends AbsConnect<?>>> srcs;

	private static String defltConn;
	public static String defltConn() { return defltConn; }

	private static final int DmConn = 1;
	private static final int CpConn = 2;

	/**parse connects.xml, setup connections configured in table "drvmnger", for JDBC DriverManger,
	 * and "dbcp", for JDBC connection-pooled connection managed by container.
	 * @param xmlDir
	 */
	public static void init(String xmlDir) {
		if (srcs != null) return;
		srcs = new HashMap<String, AbsConnect<? extends AbsConnect<?>>>();
		try{
			ILogger logger = new Log4jWrapper("xtabl");
			srcs = loadConnects(srcs, "drvmnger", DmConn, logger, xmlDir);
			srcs = loadConnects(srcs, "dbcp", CpConn, logger, xmlDir);
		
			if (srcs != null && srcs.size() > 0 && !srcs.containsKey(defltConn))
				throw new SQLException("Failed initializing, db source must configured with a default source."); 

//			conn.beforeFirst();
//			DatasetCfg.init(conn, xmlDir, orclMappings);

			if (LogFlags.Semantic.connects)
				Utils.logi("INFO - JDBC initialized using %s (%s) as default connection.",
					defltConn, srcs != null && srcs.size() > 0 ? srcs.get(defltConn).driverType() : "empty");
		}
		catch (Exception ex) {
			System.err.println("\nFATAL - Connection initializing failed! !!\n");
			ex.printStackTrace();
			return;
		}
	}
	
	private static HashMap<String, AbsConnect<? extends AbsConnect<?>>> loadConnects(HashMap<String, AbsConnect<? extends AbsConnect<?>>> srcs,
			String tablId, int dmCp, ILogger logger, String xmlDir) throws SAXException {
		if (srcs == null)
			srcs = new HashMap<String, AbsConnect<? extends AbsConnect<?>>>();

		XMLTable conn = XMLDataFactory.getTable(logger , tablId, xmlDir + "/connects.xml",
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
					srcs.put(id, AbsConnect.initDmConnect(xmlDir, type, conn.getString("src"),
						conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false)));
				else
					srcs.put(id, AbsConnect.initPooledConnect(xmlDir, type, conn.getString("src"),
						conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false)));

				if (conn.getBool("isdef", false)) {
					if (defltConn != null)
						Utils.warn("WARN - duplicate default ids found, the previous defined source been ignored: " + defltConn);
					defltConn = id;
				}
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		return srcs;
	}

	/////////////////////////////// common helper /////////////////////////////
	/** If printSql is true or if asking enable, 
	 * then print sqls.
	 * @param asking
	 * @param flag
	 * @param sqls
	 */
	public static void printSql(boolean asking, int flag, ArrayList<String> sqls) {
		if ((flag & flag_printSql) == flag_printSql
			|| asking && (flag & flag_disableSql) != flag_disableSql)
			Utils.logi(sqls);
	}

	public static void printSql(boolean asking, int flag, String sql) {
		if ((flag & flag_printSql) == flag_printSql
			|| asking && (flag & flag_disableSql) != flag_disableSql)
			Utils.logi(sql);
	}

	///////////////////////////////////// select ///////////////////////////////
	public static SResultset select(String conn, String sql, int... flags) throws SQLException {
		// Print WARN? if conn is not null and srcs doesn't contains, it's probably because of wrong configuration in connects.xml. 
		if (flags != null && flags.length > 0 && flags[0] == flag_printSql )
			if (conn != null && !srcs.containsKey(conn))
				throw new SQLException("Can't find connection: " + conn);

		return srcs.get(conn == null ? defltConn : conn)
				.select(sql, flags == null || flags.length <= 0 ? flag_nothing : flags[0]);
	}

	public static SResultset select(String sql, int... flags) throws SQLException {
		return select(null, sql, flags);
	}

	/**compose paged sql, e.g. for Oracle: select * from (sql) t where rownum > 0 and row num < 14
	 * @param sql
	 * @param page
	 * @param size
	 * @return
	 * @throws SQLException 
	 */
	public static String pagingSql(String conn, String sql, int page, int size) throws SQLException {
		conn = conn == null ? defltConn : conn;
		dbtype driverType = srcs.get(conn).driverType();

		int r1 = page * size;
		int r2 = r1 + size;
		if (driverType == dbtype.mysql) {
			String s2 = String.format(
					"select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (%s) t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s",
					sql, r1, r2);
			return s2;
		}
		else if (driverType == dbtype.oracle)
			return String.format("select * from (select t.*, rownum r_n_ from (%s) t WHERE rownum <= %s  order by rownum) t where r_n_ > %s",
					sql, r2, r1);
		else if (driverType == dbtype.ms2k)
			return String.format("select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (%s) t) t where rownum >= 1 and rownum <= 2;" + 
					sql, r1, r2);
		else if (driverType == dbtype.sqlite)
			// DON'T COMMENT THIS OUT
			// Reaching here means your code has bugs
			// To stop paging from html, don't enable a html pager
			throw new SQLException("How to page in sqlite?");
		else return sql;
	}

	/////////////////////////////////// update /////////////////////////////
	public static int[] commit(IUser usr, ArrayList<String> sqls, int... flags) throws SQLException {
		return srcs.get(defltConn).commit(usr, sqls, flags.length > 0 ? flags[0] : flag_nothing);
	}
	
	public static int[] commit(IUser usr, ArrayList<String> sqls, ArrayList<Clob> lobs, int... flags) throws SQLException {
		return srcs.get(defltConn).commit(usr, sqls, lobs, flags.length > 0 ? flags[0] : flag_nothing);
	}

	public static int[] commit(String conn, IUser usr, ArrayList<String> sqls, int... flags) throws SQLException {
		return srcs.get(conn).commit(usr, sqls, flags.length > 0 ? flags[0] : flag_nothing);
	}

	@SuppressWarnings("serial")
	public static int[] commit(IUser usr, final String sql) throws SQLException {
		return commit(usr, new ArrayList<String> () { {add(sql);} });
	}

	public static dbtype driverType(String conn) {
		conn = conn == null ? defltConn : conn;
		return srcs.get(conn).driverType();
	}

//	public static void commitLog(String sql) {
//		// TODO Auto-generated method stub
//		
//	}


}
