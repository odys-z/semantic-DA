package io.odysz.semantic.DA;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.io.FilenameUtils;

import io.odysz.common.Utils;
import io.odysz.module.rs.ICResultset;
import io.odysz.module.xtable.ILogger;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactory;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DA.drvmnger.Msql2kDriver;
import io.odysz.semantic.DA.drvmnger.MysqlDriver;
import io.odysz.semantic.DA.drvmnger.OracleDriver;
import io.odysz.semantic.DA.drvmnger.SqliteDriver;
import io.odysz.semantic.util.LogFlags;
import io.odysz.semantics.x.SemanticException;

public class Connects {
	public enum DriverType {mysql(0), ms2k(1), oracle(2), sqlite(3), postGIS(4);
		private final int value;
    	private DriverType(int value) { this.value = value; }
    	public int getValue() { return value; }
	}

	// TODO: separate log witches from semantic flags like adding "''".
	/** no special for commit */
	public static final int flag_nothing = 0;
	public static final int flag_printSql = 1;
	public static final int flag_disableSql = 2;

	public static DriverType parseDrvType(String type) throws SemanticException {
		if (type == null || type.trim().length() == 0)
			throw new SemanticException("Drived type not suppored: %s", type);
		type = type.trim().toLowerCase();
		if (type.equals("mysql")) 
			return DriverType.mysql;
		else if (type.equals("mssql2k") || type.equals("ms2k"))
			return DriverType.ms2k;
		else if (type.equals("oracle") || type.equals("orcl"))
			return DriverType.oracle;
		else if (type.startsWith("sqlit"))
			return DriverType.sqlite;
		else
			throw new SemanticException("Drived type not suppored: %s", type);
	}

	private static HashMap<String, AbsConnect> srcs;

	private static String defltConn;
	public static String defltConn() { return defltConn; }

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
			conn = XMLDataFactory.getTable(logger , "drvmnger", xmlDir + "/connects.xml",
						new IXMLStruct() {
							@Override public String rootTag() { return "conns"; }
							@Override public String tableTag() { return "t"; }
							@Override public String recordTag() { return "c"; }});
			conn.beforeFirst();
			
			HashMap<String, HashMap<String, String>> orclMappings = null; 
			while (conn.next()) {
				try {
					// columns="type,id,isdef,conn,usr,pswd,dbg"
					DriverType type = parseDrvType(conn.getString("type"));
					String id = conn.getString("id");
					if (type != null && type == DriverType.mysql) {
						srcs.put(id, MysqlDriver.initConnection(conn.getString("src"),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false) ? Connects.flag_printSql : Connects.flag_nothing));
					}
					else if (type != null && type == DriverType.sqlite) {
						srcs.put(id, SqliteDriver.initConnection(String.format("jdbc:sqlite:%s", FilenameUtils.concat(xmlDir, conn.getString("src"))),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false) ? Connects.flag_printSql : Connects.flag_nothing));
					}
					else if (type != null && type == DriverType.ms2k) {
						srcs.put(id, Msql2kDriver.initConnection(conn.getString("src"),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false) ? Connects.flag_printSql : Connects.flag_nothing));
					}
					else if (type != null && type == DriverType.oracle) {
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
						srcs.put(id, OracleDriver.initConnection(conn.getString("src"),
							conn.getString("usr"), conn.getString("pswd"), conn.getBool("dbg", false) ? Connects.flag_printSql : Connects.flag_nothing));
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

			if (LogFlags.Semantic.connects)
				Utils.logi("INFO - JDBC initialized using %s (%s) as default connection.",
					defltConn, srcs != null && srcs.size() > 0 ? srcs.get(defltConn).driverType() : "empty");
		}
		catch (Exception ex) {
			System.err.println("\nFATAL - DmDriver initializing failed! !!\n");
			ex.printStackTrace();
			return;
		}
	}

	public static void installSemantics(HashMap<String, HashMap<String, IrSemantics>> semantics) {
		if (semantics != null)
			for (String conn : semantics.keySet())
				srcs.get(conn).reinstallSemantics(semantics.get(conn));
		
		if (LogFlags.Semantic.config) {
			Utils.logi("Semanitcs installed: ");
			Utils.logkeys(semantics);
		}
	}
	
	////////////////////////////////////////////// common helper //////////////
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

	public static String genId(String defltConn2, String string, String string2, Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	public static ICResultset select(String sql, int flagNothing) {
		// TODO Auto-generated method stub
		return null;
	}

	public static DriverType getConnType(String conn) {
		// TODO Auto-generated method stub
		return null;
	}

	public static DriverType dirverType(String conn) {
		// TODO Auto-generated method stub
		return null;
	}


}
