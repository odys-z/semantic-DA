package io.odysz.semantic.DA.cp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

import org.xml.sax.SAXException;

import io.odysz.common.Regex;
import io.odysz.common.Utils;
import io.odysz.module.rs.ICResultset;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactory;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DA.Connects.DriverType;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.DatasetCfg;
import io.odysz.semantics.meta.ColumnMeta;
import io.odysz.semantics.meta.DbMeta;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantic.DA.DbLog;
import io.odysz.semantic.DA.IrSemantics;
import io.odysz.semantic.DA.IrSemantics.smtype;
import io.odysz.semantic.DA.Mappings;
import io.odysz.semantic.DA.OracleLob;

/**JDBC driver for connection pool mode.<br>
 * Use select() to query, use commit() to update.<br>
 * Troubleshooting:<br>
 * 1. org.apache.commons.dbcp.SQLNestedException: Cannot create PoolableConnectionFactory(The port number 1433/DBname is not valid.)<br>
 * The correct url: org.apache.commons.dbcp.SQLNestedException: Cannot create PoolableConnectionFactory(The port number 1433/DBname is not valid.)<br>
 * 2. Datasource name not found<br>
 * Check META-INF/context.xml and servers/host-config/servers.xml
 * @author ody
 *
 */
public class CpDriver {

	static String defltConn;
	static HashMap<String, CpSrc> srcs;
	static HashMap<String, HashMap<String, IrSemantics>> metas;
	
	public static String getDefltConnId() { return defltConn; }
	
	public static DriverType getConnType(String connId) {
		if (connId == null)
			return srcs.get(defltConn).driverType();
		else return srcs.get(connId).driverType();
	}
	
	public static DbMeta getDbMeta(String connId) {
		return srcs.get(connId).getSpec();
	}
	
	public static TableMeta getTable(String connId, String tabName) {
		if (connId == null)
			connId = defltConn;
		return srcs.get(connId).get(tabName);
	}
	
	public static ColumnMeta getColumn(String connId, String tabName, String expr) {
		if (connId == null)
			connId = defltConn;
		return srcs.get(connId).getColumn(tabName, expr);
	}

	/**If oracle, to quoted upper case "FIELD"
	 * @param conn
	 * @param expr
	 * @return
	 */
	public static String formatFieldName(String conn, String expr) {
		return srcs.get(conn).formatFieldName(expr);
	}
	
	/**compose paged sql, e.g. for Oracle: select * from (sql) t where rownum > 0 and row num < 14
	 * @param sql
	 * @param page
	 * @param size
	 * @return
	public static String pagingSql(String conn, String sql, int page, int size) {
		return srcs.get(conn).pageSql(sql, page, size);
	}
	 */
	
	public static String escapeValue(String v) {
		if (v != null) {
			v = v.replace("'", "''");
			v = v.replace("%", "%%");
		}
		return v;
	}

	public static boolean isKeywords(String conn, String expr) {
		return srcs.get(conn).isKeywords(expr);
	}
	
//	public static CustomSql formatNow(String conn) {
//		return srcs.get(conn).formatNow();
//	}

	public static ICResultset select(String connId, String sql, int flags) throws SQLException {
		if (connId == null)
			return srcs.get(defltConn).select(sql, flags);
		if (!srcs.containsKey(connId))
			throw new SQLException("Datasource not exist: " + connId);
		return srcs.get(connId).select(sql, flags);
	}
	
	public static ICResultset select(String sql, int flags) throws SQLException {
		return srcs.get(defltConn).select(sql, flags);
	}
	
	public static void readClob(String connId, ICResultset rs, String[] tabls) throws SQLException, IOException {
		srcs.get(connId).readClob(rs, tabls);
	}

	public static void init(String path) {
		if (srcs != null) return;
		srcs = new HashMap<String, CpSrc>();
		XMLTable conn = null;
		try{
//			conn = XAdaptorServlet.getXTable(context, "WEB-INF/connections.xml", "dbcp", null, new IXMLStruct() {
			conn = XMLDataFactory.getTable(new Log4jWrapper("dm"), "drvmnger", path + "/connections.xml",
						new IXMLStruct() {
							@Override public String rootTag() { return "conns"; }
							@Override public String tableTag() { return "t"; }
							@Override public String recordTag() { return "c"; }});
			conn.beforeFirst();
			
			HashMap<String, HashMap<String, String>> orclMappings = null; 
			while (conn.next()) {
				try {
					String type = conn.getString("type");
					String id = conn.getString("id");

					if (type != null && type.trim().toLowerCase().equals("mysql")) {
						srcs.put(id, initMysqlCp(conn.getString("src"), Connects.parseDrvType(conn.getString("type")),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false)));
					}
					else if (type != null && type.trim().toLowerCase().equals("mssql2k")) {
						srcs.put(id, initMs2k(conn.getString("src"), Connects.parseDrvType(conn.getString("type")), 
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false)));
					}
					else if (type != null && type.trim().toLowerCase().equals("oracle")) {
						// FIXME CpDriver don't care drive type anymore? (should be handled by semantic.transact?)
						// FIXME CpDriver don't care drive type anymore? (should be handled by semantic.transact?)
						// FIXME CpDriver don't care drive type anymore? (should be handled by semantic.transact?)
						// FIXME CpDriver don't care drive type anymore? (should be handled by semantic.transact?)
						// get name mapping config
						// String fullpath = HelperFactory.getRealPath(conn.getString("nmap"));
						String fullpath = conn.getString("nmap");
						if (fullpath == null)
							throw new SQLException("Oracle connection need a nmap value. Check connection.xml and map file.");
						if (orclMappings != null)
							throw new SQLException("io.ic.DA can only support one oracle source. This can be fixed if needed.");
						else {
							LinkedHashMap<String, XMLTable> xmappings = XMLDataFactoryEx.getXtables(new Log4jWrapper("nmap").setDebugMode(false), fullpath, Mappings.mapXStruct);
							orclMappings = Mappings.convertMap(xmappings);

							// init with name mapping
							srcs.put(id, initOrcl(conn.getString("src"), Connects.parseDrvType(conn.getString("type")),
								conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false), xmappings));
						}
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
			// Setup table semantics
//			reinstallSemantics(createSemanticsGarze());
			
			// FIXME this shouldn't happen in the future when DmDriver and CpDriver are merged.
//			if (DA.defltJdbc() == DA.jdbc_dbcp) {
				conn.beforeFirst();
				DatasetCfg.init(conn, path, orclMappings);
//			}

			System.out.println(String.format("INFO - DAO initialized using %s (%s) as default datasource.",
					// getDefltConnId(), srcs.get(getDefltConnId()).getDriverName()));
					defltConn, srcs != null && srcs.containsKey(defltConn) ? srcs.get(defltConn).driverType() : null));
		}
		catch (Exception ex) {
			System.err.println("\nFATAL - DAO initializing failed! !!\n");
			ex.printStackTrace();
			return;
		}
	}

	/**Semantics configuration are installed in this method
	 * - which can be called by ifire to install the other version.
	 * @throws SQLException
	 * @throws SAXException
	 */
	public static void reinstallSemantics(HashMap<String, HashMap<String, IrSemantics>> semantics) throws SQLException, SAXException {
		// FIXME let's do this with xml configuration.
		if (metas != null && metas.size() > 0) {
			System.err.println("Clear and reinstall semantics ... ");
			metas.clear();
		} 
		metas = semantics;
	}
	
	public static HashMap<String, HashMap<String, OracleLob>> getlobMeta(String connId) {
		return srcs.get(connId).getlobMeta();
	}

	public static void appendClobSemantics(HashMap<String, IrSemantics> semantics, String onLobOfConnId,
			HashMap<String, HashMap<String, OracleLob>> lobMeta) throws SAXException, SQLException {
		for (String btid : lobMeta.keySet()) {
			HashMap<String, OracleLob> tablobs = lobMeta.get(btid);
			ArrayList<HashMap<String, String>> casemaps = Mappings.getMappings4Tabl(
					srcs.get(onLobOfConnId).mappings(), btid); 
			if (casemaps == null || casemaps.size() < 1) continue; // why?
			if (tablobs != null)
				for (String bcol : tablobs.keySet()) {
					OracleLob lt = tablobs.get(bcol);
					IrSemantics smtc = semantics.get(btid);
					String idf = casemaps.get(0).get(lt.idField());
					String lobf = casemaps.get(0).get(lt.lobField());
					if (smtc == null) {
						smtc = new IrSemantics(smtype.orclClob, new String[] {btid, idf, lobf});
						semantics.put(btid, smtc);
					}
					else
						smtc.addSemantics(smtype.orclClob, new String[] { btid, idf, lobf});
				}
		}
	}

	/**Construct a pooled connection data source, kept in the DaSrc instance;<br>
	 * Query db meta info for sql builder, keep in the instance;<br>
	 * @param srcName
	 * @param diverType 
	 * @param usr
	 * @param pswd
	 * @param printSql
	 * @return
	 * @throws SQLException
	 * @throws SAXException 
	 */
	private static CpSrc initMysqlCp(String srcName, DriverType diverType, String usr, String pswd, boolean printSql) throws SQLException, SAXException {
		DbMeta spec = new DbMeta();
		/*RegTextHarness report:
		 * Enter your regex: (\w+)
		 * Enter input string to search: varchar(20)
		 * I found the text "varchar" starting at index 0 and ending at index 7.
		 * I found the text "20" starting at index 8 and ending at index 10.
		 * 
		 * Enter your regex: (\w+)
		 * Enter input string to search: varchar (20)
		 * I found the text "varchar" starting at index 0 and ending at index 7.
		 * I found the text "20" starting at index 9 and ending at index 11.
		 */
		Regex regex = new Regex("(\\w+)");
//		DbSchema schema = spec.addDefaultSchema();

		// ICResultset rs = DBDriver.select(DbSrc.mysql, "show tables");
		ICResultset rs = CpSrc.select(srcName, "show tables");
		HashMap<String, HashMap<String, ColumnMeta>> tablCols = new HashMap<String, HashMap<String, ColumnMeta>>(rs.getRowCount());
		HashMap<String, TableMeta> tables = new HashMap<String, TableMeta>(rs.getRowCount());
		rs.beforeFirst();
		while (rs.next()) {
			try {
				String tn = rs.getString(1);
				TableMeta tab = spec.addTable(tn);
				tables.put(tn, tab);
				tablCols.put(tn, buildColsMysql(srcName, tab, regex));
			}
			catch (SQLException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				continue;
			}
		}
		
		//tabless[DbSrc.mysql.getValue()] = tables;
		//tablsCols[DbSrc.mysql.getValue()] = tablCols;
		//return spec;
		return new CpSrc(srcName, diverType, null, spec, tables, tablCols, printSql);
	}

	private static HashMap<String, ColumnMeta> buildColsMysql(String srcName, TableMeta tab, Regex regex) throws SQLException {
		ICResultset rs = CpSrc.select(srcName, "show columns from " + tab.getName());
		HashMap<String, ColumnMeta> cols = new HashMap<String, ColumnMeta>(rs.getRowCount());
		rs.beforeFirst();
		while (rs.next()) {
			String tlen= rs.getString(2);
			ArrayList<String> typeLen = regex.findGroups(tlen);
			int len = 0;
			try { len = Integer.valueOf(typeLen.get(1)); } catch (Exception e) {}
			ColumnMeta col = tab.addColumn(rs.getString(1), typeLen.get(0), len == 0 ? null : len);
			cols.put(rs.getString(1), col);
		}
		return cols;
	}

	/**Troubleshooting:<br>
	 * 1. org.apache.commons.dbcp.SQLNestedException: Cannot create PoolableConnectionFactory(The port number 1433/DBname is not valid.)<br>
	 * The correct url: org.apache.commons.dbcp.SQLNestedException: Cannot create PoolableConnectionFactory(The port number 1433/DBname is not valid.)<br>
	 * 2. Name not found<br>
	 * Check META-INF/context.xml and servers/host-config/servers.xml
	 * @param srcName
	 * @param driverType 
	 * @param usr
	 * @param pswd
	 * @param printSql
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws SAXException 
	 */
	private static CpSrc initMs2k(String srcName, DriverType driverType, String usr, String pswd, boolean printSql) throws SQLException, ClassNotFoundException, SAXException {
		DbMeta spec = new DbMeta();
//		DbSchema schema = spec.addDefaultSchema();

		// https://stackoverflow.com/questions/175415/how-do-i-get-list-of-all-tables-in-a-database-using-tsql
		ICResultset rs = CpSrc.select(srcName, "SELECT s.name FROM sysobjects s WHERE s.xtype = 'U' or s.xtype = 'V'");
		HashMap<String, HashMap<String, ColumnMeta>> tablCols = new HashMap<String, HashMap<String, ColumnMeta>>(rs.getRowCount());
		HashMap<String, TableMeta> tables = new HashMap<String, TableMeta>(rs.getRowCount());
		rs.beforeFirst();
		while (rs.next()) {
			String tn = rs.getString(1);
			TableMeta tab = spec.addTable(tn);
			tables.put(tn, tab);
			tablCols.put(tn, buildColsMs2k(srcName, tab));
		}
		return new CpSrc(srcName, driverType, null, spec, tables, tablCols, printSql);
	}

	private static HashMap<String, ColumnMeta> buildColsMs2k(String srcName, TableMeta tab) throws SQLException {
		// https://stackoverflow.com/questions/2418527/sql-server-query-to-get-the-list-of-columns-in-a-table-along-with-data-types-no
		String sql = String.format("SELECT c.name, t.Name, c.max_length FROM sys.columns c " + 
			"INNER JOIN sys.types t ON c.user_type_id = t.user_type_id " +
			"LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
			"LEFT OUTER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
			"WHERE c.object_id = OBJECT_ID('%s')", tab.getName());
		ICResultset rs = CpSrc.select(srcName, sql);
		HashMap<String, ColumnMeta> cols = new HashMap<String, ColumnMeta>(rs.getRowCount());
		rs.beforeFirst();
		while (rs.next()) {
			int len = 0;
			try { len = rs.getInt(3); } catch (Exception e) {}
			ColumnMeta col = tab.addColumn(rs.getString(1), rs.getString(2), len == 0 ? null : len);
			cols.put(rs.getString(1), col);
		}
		return cols;
	}

	private static CpSrc initOrcl(String srcName, DriverType driverType, String usr, String pswd, boolean printSql,
			LinkedHashMap<String, XMLTable> maptables) throws SQLException, SAXException {
		DbMeta spec = new DbMeta();
		//DbSchema schema = spec.addDefaultSchema();

		// https://stackoverflow.com/questions/205736/get-list-of-all-tables-in-oracle
		// https://stackoverflow.com/questions/1953239/search-an-oracle-database-for-tables-with-specific-column-names
		/*
		ICResultset rs = DaSrc.select(srcName, "SELECT table_name, column_name, data_type, data_length, null as flag FROM cols " +
				"WHERE (substr(table_name, 0, 2) IN ('A_', 'B_', 'C_', 'D_', 'E_', 'F_', 'G_', 'K_', 'M_', 'V_', 'VW') " + 
				"OR table_name IN ('IR_AUTOSEQS', 'BAS_CHGCAP', 'BAS_COMFIRM', 'BAS_COMFIRM_RES', 'BAS_GROUP_CONTACT', " +
				"'BAS_HEALTHORG', 'BAS_HEALTHORG_RES', 'BAS_MATCAP', " +
				"'BAS_MATERIAL', 'BAS_MATERIALFIRM', 'BAS_ORG', 'BAS_PERSON', 'BAS_REPERTORY', 'BAS_RESCUE', " +
				"'BAS_SHELTER', 'BAS_TEAMEQUIP', 'BAS_TRANSFIRM', 'BAS_TRANSTOOL')) " +
				"and table_name NOT IN ('E_RISKTYPES', 'F_MEETINGROOM', 'F_TEL', 'E_TYPE_SIZES', 'A_MGCK', 'C_ALERTS', " +
				"'E_INDICS', 'C_INDICS', 'G_SECTIONTEMPLS', 'M_INOUTS', 'BAS_PLAN', 'D_DEDUCTDETAILS', 'F_FILES', 'F_MRUSE', " +
				"'D_SUBJECT', 'F_SHIFTSRECORD', 'F_LDRWORK', 'C_MODES', 'M_SUPPORTPLANDC', 'K_EXCER_EMERGS', 'E_RISKS', " +
				"'D_SUMMERY', 'E_VALUES', 'E_RISK_MARKS', 'A_RISKPOINTS', 'A_RISKMARKS', 'C_METAS', 'C_VALUES', 'C_MODE_INDIC', " +
				"'G_SECTIONS', 'D_OWNCONTACTS', 'F_LDRTRIP', 'D_DEDUCTIONS', 'G_PLANDC', 'K_EMERGASSERTS', 'F_RECEIVE', " +
				"'E_RISKLEVELS', 'D_OWNCOMMANDORG', 'G_RELATERES', 'F_PREPLANS', 'M_INTERFACES', 'F_PUB_IFS', 'BAS_EQUIPMENT', " +
				"'F_PLANRULE', 'C_LAYERS', 'C_DECISIONS', 'F_CALL') " +
				"ORDER BY table_name");
				*/
		ICResultset rs = CpSrc.select(srcName, "SELECT table_name, column_name, data_type, data_length, null as flag FROM cols " +
				"WHERE (substr(table_name, 0, 2) IN ('A_', 'B_', 'D_', 'F_', 'G_', 'K_', 'M_', 'V_', 'VW') " + 
				"OR table_name IN ('IR_AUTOSEQS', 'BAS_CHGCAP', 'BAS_COMFIRM', 'BAS_COMFIRM_RES', 'BAS_GROUP_CONTACT', " +
				"'BAS_HEALTHORG', 'BAS_HEALTHORG_RES', 'BAS_MATCAP', " +
				"'BAS_MATERIAL', 'BAS_MATERIALFIRM', 'BAS_ORG', 'BAS_PERSON', 'BAS_REPERTORY', 'BAS_RESCUE', " +
				"'BAS_SHELTER', 'BAS_TEAMEQUIP', 'BAS_TRANSFIRM', 'BAS_TRANSTOOL', 'BAS_UPLOADFILES','BAS_PUBLICSMS')) " +
				"and table_name NOT IN ('F_MEETINGROOM', 'F_TEL', 'A_MGCK', " +
				"'G_SECTIONTEMPLS', 'M_INOUTS', 'BAS_PLAN', 'D_DEDUCTDETAILS', 'F_FILES', 'F_MRUSE', " +
				"'D_SUBJECT', 'DISTRICTS','F_SHIFTSRECORD', 'F_LDRWORK', 'M_SUPPORTPLANDC', 'K_EXCER_EMERGS', " +
				"'D_SUMMERY', 'A_RISKPOINTS', 'A_RISKMARKS', " +
				"'G_SECTIONS', 'D_OWNCONTACTS', 'F_LDRTRIP', 'D_DEDUCTIONS', 'G_PLANDC', 'K_EMERGASSERTS', 'F_RECEIVE', " +
				"'D_OWNCOMMANDORG', 'G_RELATERES', 'F_PREPLANS', 'M_INTERFACES', 'F_PUB_IFS', 'BAS_EQUIPMENT', " +
				"'F_PLANRULE', 'F_CALL') " +
				"ORDER BY table_name");
		HashMap<String, TableMeta> tables = new HashMap<String, TableMeta>(rs.getRowCount());
		HashMap<String, HashMap<String, ColumnMeta>> tablCols = new HashMap<String, HashMap<String, ColumnMeta>>(rs.getRow());
		rs.beforeFirst();
		System.err.println("\nChecking oracle db mapping configuration (dc.xml) against taget DB - srcName = " + srcName);
		checkNames(maptables, rs);
		
		XMLTable mainxt = maptables.get("tabls");
		mainxt.beforeFirst();
		while (mainxt.next()) {
			String tablId = mainxt.getString("u");
			String bTabl = mainxt.getString("b");
			System.out.println("Mapping table " + bTabl);
			// TableMeta dbTab = schema.addTable(bTabl);
			TableMeta dbTab = spec.addTable(bTabl);
			tables.put(bTabl, dbTab);
			XMLTable xt = maptables.get(tablId);
			HashMap<String, ColumnMeta> cols = new HashMap<String, ColumnMeta>();
			xt.beforeFirst();
			while (xt.next()) {
				// f,b,tn,len,flag
				if (xt.getBool("flag", false)) {
					String b = xt.getString("b");
					String tn = xt.getString("tn");
					int len = xt.getInt("len", 0);
					cols.put(b, dbTab.addColumn(b, tn, len));
				}
				else
					System.err.println(String.format("WARN - ignoring db col: %s.%s", tablId, xt.getString("f")));
			}
			tablCols.put(bTabl, cols);
		}
		CpSrc src = new CpSrc(srcName, driverType, maptables, spec, tables, tablCols, printSql);
		return src;
	}

	/**
	 * @param log
	 * @param sqls
	 * @return an array of update counts, see {@link java.sql.Statement#executeBatch()}
	 * @throws SQLException
	 */
	public static int[] commit(DbLog log, ArrayList<String> sqls, int flags) throws SQLException {
		return commit(log, defltConn, sqls, flags);
	}

	public static int[] commit(DbLog log, ArrayList<String> sqls, ArrayList<OracleLob> lobs, int flags) throws SQLException {
		int[] ret = commit(log, defltConn, sqls, flags);
		srcs.get(defltConn).updateLobs(lobs);
		return ret;
	}

	public static int[] commit(DbLog log, String sql, int flags) throws SQLException {
		ArrayList<String> sqls = new ArrayList<String>(1);
		sqls.add(sql);
		return commit(log, defltConn, sqls, flags);
	}

	/**
	 * @param log
	 * @param connId
	 * @param sqls
	 * @return an array of update counts, see {@link java.sql.Statement#executeBatch()}
	 * @throws SQLException
	 */
	public static int[] commit(DbLog log, String connId, ArrayList<String> sqls, int flags) throws SQLException {
		if (connId == null)
			srcs.get(defltConn).commit(sqls, flags);
		if (!srcs.containsKey(connId))
			throw new SQLException("Datasource not exist: " + connId);
		int[] ret = srcs.get(connId).commit(sqls, flags);
		if (log != null)
			log.log(sqls);
		else {
			System.err.println("Some db commitment not logged:");
			// DA.printErr(sqls);
			Utils.warn(sqls);
		}
		return ret;
	}
	

	public static void commitLog(String log) {
		ArrayList<String> sqls = new ArrayList<String>(1);
		sqls.add(log);
		try {
			srcs.get(defltConn).commit(sqls, Connects.flag_nothing);
			if (isOracle(defltConn)) 
				srcs.get(defltConn).updateLobs(DbLog.formatLob(log));
		} catch (SQLException e) {
			System.err.println("Update db log failed: ");
			System.err.println(log.toString());
			for (String sql : sqls)
				System.err.println(sql);
		}
	}

	public static IrSemantics getTableSemantics(String conn, String tabName) {
		if (metas.containsKey(conn))
			return metas.get(conn).get(tabName);
		else return null;
	}

	static LinkedHashMap<String,XMLTable> checkNames(LinkedHashMap<String, XMLTable> maptables, ICResultset rs) throws SQLException, SAXException {
		HashMap<String, String> ku = new HashMap<String, String>(1);
		HashMap<String, String> kf = new HashMap<String, String>(1);
		XMLTable mainxtabl = maptables.get("tabls");
		HashSet<String> missingTabls = new HashSet<String>();
		rs.beforeFirst();
		while (rs.next()) {
			// SELECT table_name, column_name, data_type, data_length FROM cols 
			// tabls: u,b,flag
			// cols: f,b,tn,len,flag
			String dbTable = rs.getString("table_name");
			String dbCol = rs.getString("column_name");
			XMLTable xtab = maptables.get(dbTable);
			if (xtab != null) {
				ku.put("u", dbTable);
				try {
					ArrayList<String[]> tablRec = mainxtabl.findRecords(ku);
					if (tablRec != null && tablRec.size() > 0)
						mainxtabl.findRecords(ku).get(0)[2] = "true";
				} catch (Exception e) {System.err.println("Wrong rec in id=tabls: k=" + dbTable);}

				kf.put("f", dbCol);
				ArrayList<String[]> row = xtab.findRecords(kf);
				if (row != null && row.size() > 0) {
					row.get(0)[7] = "true"; // flag = merged
					rs.set("flag", "true");
				}
			}
			else {
				missingTabls.add(dbTable);
			}
		}
		
		// suggestions
		// 1 missing tables
		if (missingTabls.size() > 0) {
			System.err.println("The following tables exist in target db but not in xml:");
			System.err.println(missingTabls);
			for (String t : missingTabls)
				System.err.print("'" + t + "', ");
			System.err.println("");
		}

		boolean printWarn = true;
		for (String xkey : maptables.keySet()) {
			// 2 Main tables configured but not find in db (rs)
			if ("tabls".equals(xkey)) {
				XMLTable xt = maptables.get(xkey);
				xt.beforeFirst();
				while (xt.next()) {
					if (!xt.getBool("flag", false)) {
						if (printWarn) {
							System.err.println("Mapping tables found in mapping file but not in target DB.");
							printWarn = false;
						}
						System.err.println(String.format("\t<c><u>%s</u><b>%s</b></c>",
							xt.getString("u"), xt.getString("b")));
					}
				}
			}
			else {
				// 3 columns configured but not find in db (rs)
				printWarn = true;
				XMLTable xt = maptables.get(xkey);
				xt.beforeFirst();
				while (xt.next()) {
					if (!xt.getBool("flag", false)) {
						if (printWarn) {
							System.err.println("Fields exist in mapping file not found in DB, table = " + xkey);
							printWarn = false;
						}
						System.err.println(String.format("\t<c><f>%s</f><b>%s</b><tn>%s</tn><len>%s</len></c>",
								xt.getString("f"), xt.getString("b"), xt.getString("tn"), xt.getString("len")));
					}
				}
			}
		}

		// 3 Missing fields found in DB but not in mapping file
		String currTn = null;
		rs.beforeFirst();
		while (rs.next()) {
			if (!rs.getBoolean("flag")) {
				String tn = rs.getString("table_name");
				if (currTn == null || !currTn.equals(tn)) {
					System.err.println("Fields found in target DB not in mapping file for table = "
							+ tn);
					currTn = tn;
				}
				// System.err.println(rs.getString("table_name"));
				String b = rs.getString("column_name");
				System.err.println(String.format("\t<c><f>%s</f><b>%s</b><tn>%s</tn><len>%s</len></c>",
						b, b.toLowerCase(), rs.getString("data_type"), rs.getString("data_length")));
			}
		}
		return maptables;
	}

	/**Print sqls for debugging
	 * @param objs
	public static void print(List<?> objs) {
		if (objs != null)
			for (Object obj : objs)
				System.out.println(obj.toString());
	}
	 */

	public static boolean isOracle(String conn) {
		return srcs != null && srcs.containsKey(conn) && srcs.get(conn).isOracle();
	}
	
	public static boolean printSql(String conn) {
		if (conn == null) conn = CpDriver.defltConn;
		return srcs != null && srcs.containsKey(conn) && srcs.get(conn).printSql();
	}

	public static ArrayList<HashMap<String,String>> getMappings(String conn, String... tabls) throws SAXException {
		if (conn == null)
			conn = defltConn;
		CpSrc src = srcs.get(conn); 
		if (src != null)
			return Mappings.getMappings4Tabl(src.mappings(), tabls);
		return null;
	}

	public static String getTimestamp(String conn) throws SQLException {
		if (conn == null)
			conn = defltConn;
		CpSrc src = srcs.get(conn); 
		if (src != null)
			return src.getTimestamp();
		else throw new SQLException(String.format("Getting timestamp on conn %s failed", conn));
	}

	public static boolean isSqlite(String conn) {
		if (conn == null)
			conn = defltConn;
		return srcs.get(conn).driverType() == DriverType.sqlite;
	}
}
