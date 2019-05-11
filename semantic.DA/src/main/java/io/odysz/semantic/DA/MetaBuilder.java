package io.odysz.semantic.DA;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import io.odysz.common.Regex;
import io.odysz.module.rs.SResultset;
import io.odysz.semantics.meta.TableMeta;


public class MetaBuilder {
	private static HashSet<String> ignorTabls;
	/**Build mysql table metas
	 * @param conn
	 * @return table metas for the conn.
	 * @throws SQLException
	 * @throws SAXException
	 */
	public static HashMap<String, TableMeta> buildMysql(String conn) throws SQLException {
		SResultset rs = Connects.select(conn, "show tables");
		HashMap<String, TableMeta> tablMeta = new HashMap<String, TableMeta>(rs.getRowCount());
		rs.beforeFirst();
		while (rs.next()) {
			try {
				String tn = rs.getString(1);
				if (ignorTabls == null || !ignorTabls.contains(tn)) {
					TableMeta table =  metaMysql(conn, tn);
					tablMeta.put(tn, table);
				}
			}
			catch (SQLException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				continue;
			}
		}
		return tablMeta;
	}

	static Regex regexMysqlCol = new Regex("(\\w+)");
	private static TableMeta metaMysql(String conn, String tabl) throws SQLException {
		SResultset rs = Connects.select(conn, "show columns from " + tabl);
		rs.beforeFirst();
		TableMeta tab = new TableMeta(tabl);
		while (rs.next()) {
			String tlen= rs.getString(2);
			ArrayList<String> typeLen = regexMysqlCol.findGroups(tlen);
			int len = 0;
			try { len = Integer.valueOf(typeLen.get(1)); }
			catch (Exception e) {}
			tab.col(rs.getString(1), typeLen.get(0), len == 0 ? null : len);
		}
		return tab;
	}

	public static HashMap<String,TableMeta> buildMs2k(String conn) throws SQLException {
		// https://stackoverflow.com/questions/175415/how-do-i-get-list-of-all-tables-in-a-database-using-tsql
		SResultset rs = Connects.select(conn, "SELECT s.name FROM sysobjects s WHERE s.xtype = 'U' or s.xtype = 'V'");
		HashMap<String, TableMeta> tablMeta = new HashMap<String, TableMeta>(rs.getRowCount());
		rs.beforeFirst();
		while (rs.next()) {
			String tn = rs.getString(1);
			if (ignorTabls == null || !ignorTabls.contains(tn)) {
				TableMeta table =  metaMs2k(conn, tn);
				tablMeta.put(tn, table);
			}
		}
		return tablMeta;
	}

	private static TableMeta metaMs2k(String srcName, String tabl) throws SQLException {
		// https://stackoverflow.com/questions/2418527/sql-server-query-to-get-the-list-of-columns-in-a-table-along-with-data-types-no
		String sql = String.format("SELECT c.name, t.Name, c.max_length FROM sys.columns c " + 
			"INNER JOIN sys.types t ON c.user_type_id = t.user_type_id " +
			"LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
			"LEFT OUTER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
			"WHERE c.object_id = OBJECT_ID('%s')", tabl);
		SResultset rs = Connects.select(srcName, sql);
		TableMeta tab = new TableMeta(tabl);
		rs.beforeFirst();
		while (rs.next()) {
			int len = 0;
			try { len = rs.getInt(3); } catch (Exception e) {}
			tab.col(rs.getString(1), rs.getString(2), len == 0 ? null : len);
		}
		return tab;
	}

	public static HashMap<String,TableMeta> buildOrcl(String conn) throws SQLException {
		// https://stackoverflow.com/questions/205736/get-list-of-all-tables-in-oracle
		// https://stackoverflow.com/questions/1953239/search-an-oracle-database-for-tables-with-specific-column-names
		SResultset rs = Connects.select(conn, "SELECT table_name, column_name, data_type, data_length FROM cols");
		HashMap<String, TableMeta> tablMeta = new HashMap<String, TableMeta>(rs.getRowCount());
		rs.beforeFirst();
		
		while (rs.next()) {
			String tn = rs.getString(1);
			if (ignorTabls == null || !ignorTabls.contains(tn)) {
				TableMeta table = tablMeta.get(tn);
				if (table == null) {
					table = new TableMeta(rs.getString("table_name"));
					tablMeta.put(tn, table);
				}
				table.col(rs.getString(2), rs.getString(3), rs.getInt(4));
			}
		}
		return tablMeta;
	}

	public static HashMap<String, TableMeta> buildSqlite(String conn) throws SQLException {
		SResultset rs = Connects.select(conn, "select distinct tbl_name from sqlite_master  where type = 'table'");
		HashMap<String, TableMeta> tablMeta = new HashMap<String, TableMeta>(rs.getRowCount());
		rs.beforeFirst();
		
		while (rs.next()) {
			String tn = rs.getString(1);
			if (ignorTabls == null || !ignorTabls.contains(tn)) {
				TableMeta table =  metaSqlite(conn, tn);
				tablMeta.put(tn, table);
			}
		}
		return tablMeta;

	}

	private static TableMeta metaSqlite(String conn, String tabl) throws SQLException {
		// cid |name    |type |notnull |dflt_value |pk |
		// ----|--------|-----|--------|-----------|---|
		// 0   |aid     |TEXT |1       |           |1  |
		// 1   |remarka |TEXT |0       |           |0  |
		// 2   |afk     |TEXT |0       |           |0  |
		// 3   |testInt |INTEGER |0    |           |0  |
		String sql = String.format("pragma table_info(%s)", tabl);
		SResultset rs = Connects.select(conn, sql);
		TableMeta tab = new TableMeta(tabl);
		rs.beforeFirst();
		while (rs.next()) {
			tab.col(rs.getString("name"), rs.getString("type"), 0);
		}
		return tab;
	}
}
