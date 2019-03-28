package io.odysz.semantic.util;

import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.module.rs.SResultset;

public class SQLString {

	/**@deprecated not used?
	 * @param format
	 * @param objects
	 * @return
	public static String format(String format, Object ... objects) {
		Object[] copy = new Object[objects.length];
		for (int i = 0; i < objects.length; i++) {
			Object obj = objects[i];
			if (obj instanceof String) {
				String str = (String) obj;
				//copy[i] = str.replaceAll("'", "''");
				copy[i] = formatSql(str);
			}
			else copy[i] = obj;
		}
		return String.format(format, copy);
	}
	 */

	/**Get sql string "insert into [table]([f1], f[2], f[3], ...) values ([v1], [v2], fd_v[3], ...)"<br>
	 * Only varchar2 and date are parsable, tested on mysql.
	 * @param rs
	 * @param table
	 * @param fd_v
	 * @return
	 * @throws SQLException
	 */
	public static ArrayList<String> composeInserts(SResultset rs, String table) throws SQLException {
		if (rs == null) return null;
		
		String fields = null;
		for (int c = 1; c <= rs.getColCount(); c++) { // col-index start at 1
			if (fields == null)
				fields = rs.getColumnName(c);
			else fields += ", " + rs.getColumnName(c);
		}

		ArrayList<String> sqls = new ArrayList<String>(rs.getRowCount());
		rs.beforeFirst();

		while (rs.next()) {
			String values = null;
			for (int c = 1; c <= rs.getColCount(); c++) {
				String v = rs.getString(c);
				if (values == null)
					values = String.format("'%s'", v == null ? "" : v);
				else values += String.format(", '%s'", v == null ? "" : v);
			}

			String sql = String.format("insert into %s(%s) values(%s)",
				table, fields, values);
			sqls.add(sql);
		}
		return sqls;
	}

	public static String formatSql(String s) {
		if (s != null) {
			String s1 = s.replace("\n", "\\n");
			String s2 = s1.replace("\t", "\\t");
			String s3 = s2.replace("'", "''");
			return s3;
			// return res;
		}
		return "";
	}

	/** p = p.replace(".", "[^\t\n]{1}");<br>
		p = p.replace("*", "[^\t\n]*");<br>
		p = p.replace("-", "[-]");
	 * @param p
	 * @return
	 */
	public static String escapeRegex(String p) {
		String p1 = p.replace(".", "[^\t\n]{1}");
		p1 = p1.replace("*", "[^\t\n]*");
		p1 = p1.replace("-", "[-]");
		return p1;
	}

	/**Get sql string "update [table] set [f1] = [v1], f[2] = [v2], ... where pkv[0] = pkv[1] and pkv[2] = pkv[3] ...",<br>
	 * where fd_v are user specified field : value pairs.
	 * @param rs
	 * @param table
	 * @return
	 * @throws SQLException
	public static ArrayList<String> composeUpdates(ICResultset rs, String table, String... pkv)
			throws SQLException {
		if (rs == null) return null;
		
		String fields = null;
		for (int c = 0; c < rs.getColCount(); c++) {
			if (fields == null)
				fields = rs.getColumnName(c);
			else fields += ", " + rs.getColumnName(c);
		}

		ArrayList<String> sqls = new ArrayList<String>(rs.getRowCount());
		rs.beforeFirst();

		while (rs.next()) {
			String values = null;
			for (int c = 0; c < rs.getColCount(); c++) {
				if (values == null)
					values = rs.getString(c);
				else values += ", " + rs.getString(c);
			}

				
			String sql = String.format("insert into %s(%s) values(%s)",
				table, fields, values);
			sqls.add(sql);
		}
		return sqls;

	}
	 */
	
	/**<pre>mysql:
select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (%s) t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s

oracle:
select * from (select t.*, rownum r_n_ from (%s) t WHERE rownum <= %s  order by rownum) t where r_n_ > %s
...</pre>
	 * @param sql
	 * @param page
	 * @param size
	 * @return
	 */
	public String pageSql(String driverType, String sql, int page, int size) {
		int r1 = page * size;
		int r2 = r1 + size;
		if (driverType.equalsIgnoreCase("mysql")) {
			String s2 = String.format(
					"select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (%s) t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s",
					sql, r1, r2);
			return s2;
		}
		else if (driverType.equalsIgnoreCase("orcl") || driverType.equalsIgnoreCase("oracle"))
			return String.format("select * from (select t.*, rownum r_n_ from (%s) t WHERE rownum <= %s  order by rownum) t where r_n_ > %s",
					sql, r2, r1);
//			return String.format("select * from (%s) t where rownum > %d and rownum <= %s",
//					sql, r1, r2);
		else if (driverType.equalsIgnoreCase("mssql2k"))
			return String.format("select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (%s) t) t where rownum >= 1 and rownum <= 2;" + 
					sql, r1, r2);
		else return sql;
	}


}
