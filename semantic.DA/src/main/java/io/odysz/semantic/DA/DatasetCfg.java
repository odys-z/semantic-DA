package io.odysz.semantic.DA;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.xtable.ILogger;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;

/**Configured dataset.xml manager and mapping helper.<br>
 * Design Memo: Separating getSql() and mapRs() will separate DA driver dependency - won't care using CP data source or DB manager.
 * @author ody
 */
public class DatasetCfg {
	public static final int drv_mysql = 0;
	public static final int drv_orcl = 1;
	public static final int drv_ms2k = 2;
	public static final int drv_sqlit = 3;
	public static final int drv_unknow = 4;

	protected static ILogger log;
	protected static final String tag = "DataSet";
	protected static final String cfgFile = "dataset.xml";
	protected static final String deftId = "ds";
	protected static HashMap<String, Dataset> dss;
	protected static HashMap<String, String> conn_driver;

	public static void init(XMLTable connections, String path,
			HashMap<String, HashMap<String, String>> orclMappings) throws Exception {
		log = new Log4jWrapper("");
		dss = new HashMap<String, Dataset>();
		conn_driver = parseDrivers(connections);

		load(conn_driver, dss, path, orclMappings);
	}

	/**Get conn-id and conn-type pairs.
	 * @param connections
	 * @return
	 * @throws SAXException
	 */
	private static HashMap<String, String> parseDrivers(XMLTable connections) throws SAXException {
		HashMap<String, String> con_drv = new HashMap<String, String>(connections.getRowCount());
		connections.beforeFirst();
		while (connections.next()) {
			con_drv.put(connections.getString("id"), connections.getString("type"));
		}
		return con_drv;
	}

	/**Load all dataset.xml into the argument cfgs.<br>
	 * When return, cfgs is loaded with dataset configurations like [id, mysql:sql, orcl:sql, ...].
	 * @param conn_drv [conn-id, driver]
	 * @param cfgs
	 * @param context
	 * @param orclMappings 
	 * @throws Exception
	 */
	protected static void load(HashMap<String, String> conn_drv, HashMap<String, Dataset> cfgs,
			String xmlPath, HashMap<String, HashMap<String, String>> orclMappings) throws Exception {
		String fullpath = FilenameUtils.concat(xmlPath + "/", cfgFile);
		log.d("D", "message file path: " + fullpath);

		File f = new File(fullpath);
		if (!f.exists() || !f.isFile()) {
			Utils.warn("WARN - Can't find dataset.xml, configuration ignored. Check %s", fullpath);
			return;
		}

		LinkedHashMap<String,XMLTable> xtabs = XMLDataFactoryEx.getXtables(
				new Log4jWrapper("DA").setDebugMode(false), fullpath, new IXMLStruct() {
					@Override public String rootTag() { return "dataset"; }
					@Override public String tableTag() { return "t"; } 
					@Override public String recordTag() { return "c"; }
				});
		XMLTable deft = xtabs.get("ds");
		
		if (deft != null) {
			try {
				deft.beforeFirst();
				Dataset ds = null;
				while (deft.next()) {
					String[] sqls = new String[4];
					sqls[drv_mysql] = deft.getString("mysql");
					sqls[drv_orcl] = deft.getString("orcl");
					sqls[drv_sqlit] = deft.getString("sqlit");
					sqls[drv_ms2k] = deft.getString("ms2k");

					// columns="id,tabls,cols,orcl,mysql,ms2k"
					ds = new Dataset(conn_drv, deft.getString("id"),
									deft.getString("tabls"), deft.getString("cols"),
									sqls, deft.getString("s-tree"), orclMappings);
					if (ds != null)
						cfgs.put(deft.getString("id"), ds);
				}
			} catch (SAXException e) {
				e.printStackTrace();
			}
		}
	}

	public static String getSql(String conn, String k, Object[] args) throws SQLException {
		return getSqlx(conn, k, args);
	}

	public static String getSqlx(String conn, String k, Object... args) throws SQLException {
		if (dss == null)
			throw new SQLException("FATAL - dataset not initialized...");
		if (k == null || !dss.containsKey(k))
			throw new SQLException(String.format("No dataset configuration found for k = %s", k));

		if (conn == null) conn = Connects.defltConn();
		String sql = dss.get(k).getSql(conn_driver.get(conn));
		if (args == null || args.length == 0)
			return sql;
		else return String.format(sql, (Object[])args);
	}
	
	public static String getStree(String conn, String k) throws SQLException {
		if (dss == null)
			throw new SQLException("FATAL - dataset not initialized...");
		if (k == null || !dss.containsKey(k))
			throw new SQLException(String.format("No dataset configuration found for k = %s", k));
		if (conn == null) conn = Connects.defltConn();
		return dss.get(k).stree();
	}

//	public static ICResultset mapRs(String conn, String k, ICResultset rs) {
//		if (conn == null || k == null)
//			return rs;
//		return dss.get(k).map(conn, rs);
//	}

	
	/**POJO dataset element as configured in dataset.xml.<br>
	 * (oracle mapping information alse initialized according to mapping file and the "cols" tag.)*/
	static class Dataset {
		String k;
		/**[connId, list[mappings]], where mappings = map[upper-case-col, bump-case-col]*/
		HashMap<String, ArrayList<HashMap<String, String>>> mappings;
		HashMap<String, String> colsExt;
		String[] sqls;
		/** If the result set can be used to construct a tree, a tree semantics configuration is needed.
		 * "stree" is used to configure a tree semantics configuration.
		 */
		String stree;

		/**Create a dataset, with mapping prepared according with mapping file.
		 * @param conn_drv
		 * @param k
		 * @param tabls
		 * @param cols
		 * @param sqls
		 * @param stree
		 * @param orclMappings mappings from mapping the file.
		 * @throws SAXException 
		 */
		public Dataset(HashMap<String, String> conn_drv, String k, String tabls, String cols,
				String[] sqls, String stree, HashMap<String, HashMap<String, String>> orclMappings) throws SAXException {
			this.k = k;

			if (tabls != null) {
				String[] ts = tabls.split(",");
				if (ts != null && ts.length > 0)
					for (String connId : conn_drv.keySet()) {
						if (this.mappings == null)
							this.mappings = new HashMap<String, ArrayList<HashMap<String, String>>>();

						// 2018.09.15 mappings not only used by CPC Dao, also by drvMnger drivers.
						// So moving mappings to the common module.
						// That leads to take mappings a arguments for Dataset, not asking from Dao. 
						// this.mappings.put(connId, Dao.getMappings(connId, ts));
						this.mappings.put(connId, Mappings.getMappings4Tabl(orclMappings, ts));
					}
			}
			colsExt = cols == null ? null : upper_bumpCase(cols.split(","));
			this.sqls = sqls;
			
			this.stree = stree;
		}

//		private ArrayList<HashMap<String, String>> getMappings(LinkedHashMap<String, XMLTable> orclMappings,
//				String[] ts) {
//			// TODO Auto-generated method stub
//			return null;
//		}

//		public ICResultset map(String conn, ICResultset rs) {
//			return JsonHelper.mapRsCols(colsExt, mappings.get(conn), rs);
//		}

		/**
		 * @param driver drv_orcl, drv_ms2k, drv_sqlit, drv_mysql(default)
		 * @return
		 */
		public String getSql(String driver) {
			if (driver == null)
				return null;
			driver = driver.toLowerCase();
			if ("orcl".equals(driver) || "oracle".equals(driver))
				return sqls[drv_orcl] == null ? sqls[drv_mysql] : sqls[drv_orcl];
			else if ("ms2k".equals(driver))
				return sqls[drv_ms2k] == null ? sqls[drv_mysql] : sqls[drv_ms2k];
			else if (driver.startsWith("sqlit"))
				return sqls[drv_sqlit] == null ? sqls[drv_mysql] : sqls[drv_sqlit];
			else 
				return sqls[drv_mysql];
		}
		
		public String stree() { return stree; }

		private HashMap<String, String> upper_bumpCase(String[] bumps) {
			if (bumps == null) return null;
			HashMap<String, String> colMaps = new HashMap<String, String>();
			for (String bump : bumps) {
				if (bump == null) continue;
				colMaps.put(bump.toUpperCase(), bump);
			}
			return colMaps;
		}
	}
}
