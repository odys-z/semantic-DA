package io.odysz.semantic.DA.drvmnger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import io.odysz.common.Configs;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;

public class MysqlDriver extends AbsConnect {
	public static boolean printSql = true;
	static boolean inited = false;
	static String userName;
	static String pswd;
	static String connect;
	
	/**
	 * MUST CLOSE CONNECTION!
	 * @return
	 * @throws SQLException
	 */
	protected static Connection getConnection() throws SQLException {
		if (!inited) {
			String isTrue = Configs.getCfg("MySql.printSQL.enable");
			printSql = isTrue != null && "true".equals(isTrue.toLowerCase());
			
			connect = Configs.getCfg("com.ic.DA.MySql.connect");
			userName = Configs.getCfg("com.ic.DA.MySql.username");
			pswd = Configs.getCfg("com.ic.DA.MySql.password");
			// FIXME decipher pswd
			// pswd = Encrypt.DecryptPswdImpl(pswd);
			try {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new SQLException(e.getMessage());
			}
			inited = true;
		}

		Connection conn = DriverManager.getConnection(connect, userName, pswd);
		return conn;
	}
	
	/**Use this to init connection without using servlet context for retrieving configured strings.<br>
	 * This is the typical scenario when running test from "main" thread.
	 * @param conn
	 * @param user
	 * @param psword
	 * @param dbg 
	 * @return 
	 * @throws SQLException
	 */
	public static MysqlDriver initConnection(String conn, String user, String psword, int flags) throws SQLException {
		if (!inited) {
			printSql = (flags & Connects.flag_printSql) > 0;
			
			connect = conn;
			userName = user;
			pswd = psword;
			// FIXME decipher pswd
			// pswd = Encrypt.DecryptPswdImpl(pswd);
			try {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new SQLException(e.getMessage());
			}
			inited = true;
		}
		return new MysqlDriver();
	}
 
	/**Not used. Reserved for encrypting db password?
	 * @param pswd
	 * @param userName
	 * @return
	@SuppressWarnings("unused")
	private static String DecryptPswd(String pswd, String userName) {
		if (pswd == null) return ""; 
		BASE64Decoder decoder = new BASE64Decoder(); 
		try { 
			byte[] b = decoder.decodeBuffer(pswd);
			return new String(b);
		} catch (Exception e) {
			return "";
		}
	}
	 */
	

	public static SResultset selectStatic(String sql, int flags) throws SQLException {
		Connection conn = getConnection();
		Connects.printSql(printSql, flags, sql);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		SResultset icrs = new SResultset(rs);
		rs.close();
		stmt.close();
		conn.close();
		
		return icrs;
	}

	public SResultset select(String sql, int flags) throws SQLException {
		return selectStatic(sql, flags);
	}
	
	/**@deprecated
	 * @param sqls
	 * @throws SQLException
	 */
	static void executeBatch(ArrayList<String> sqls) throws SQLException {
		Connection conn = getConnection();
		excecuteBatch(conn, sqls);
		conn.close();
	}
	
	/**@deprecated
	 * @param conn
	 * @param sqls
	 * @throws SQLException
	 */
	static void excecuteBatch(Connection conn, ArrayList<String> sqls) throws SQLException {
		if (printSql)
			System.out.println(sqls);

		Statement stmt = null;
		try {
			if (conn != null) {
				stmt = conn.createStatement();
				try {
					// String logs = "";
					// boolean noMoreLogs = false;
					stmt = conn.createStatement(
							ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					conn.setAutoCommit(false);

					for (String sql : sqls) {
						stmt.addBatch(sql);
					}

					/* not need 
					for (String sql : sqls) {
						if (enableSystemout) System.out.println(sql + ";");
						stmt.addBatch(sql);
						if (!noMoreLogs && logs.length() + sql.length() + 1 + "...".length() <= 4000) {
							logs += sql.replaceAll("'", "''") + ";";
						}
						else if (!noMoreLogs && logs.length() + sql.length() + 1 + "...".length() > 4000) {
							noMoreLogs = true;
							if (logs.length() + "...".length() <= 4000) {
								logs += "...";
							}
						}
					}
					if (!"".equals(logs)) {
						String logsql = String.format("insert into frame_logs(operID, oper, operTime, funcName, sysID, remarks) values " +
								"('%s', '%s', to_date('%s', 'yyyy-MM-dd hh24:mi:ss'), '%s', '%s', '%s')", userid, username, operTime, funcName, sysID, logs);
						stmt.addBatch(logsql);
						if (enableSystemout) System.out.println(logsql);
					}*/
					stmt.executeBatch();
					conn.commit();
					if (printSql) System.out.println("mysql batch execute successfully.");
//					logOperations (logs, userid, operTime, funcName, remarks);
				} catch (Exception exx) {
					conn.rollback();
					exx.printStackTrace();
					throw new SQLException(exx);
				} finally {
//					conn.setAutoCommit(status);
				}
			} else {
				throw new SQLException("mysql batch execution failed");
			}
		} catch (SQLException ex) {
			throw ex;
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				stmt = null;
			}
		}
	}

	@Override
	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		Connects.printSql(printSql, flags, sqls);
		
		int[] ret;
		Connection conn = getConnection();

		Statement stmt = null;
		try {
			if (conn != null) {
				stmt = conn.createStatement();
				try {
					// String logs = "";
					// boolean noMoreLogs = false;
					stmt = conn.createStatement(
							ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					conn.setAutoCommit(false);

					for (String sql : sqls) {
						stmt.addBatch(sql);
					}

					ret = stmt.executeBatch();
					conn.commit();
					if (printSql) System.out.println("mysql batch execute successfully.");
				} catch (Exception exx) {
					conn.rollback();
					exx.printStackTrace();
					throw new SQLException(exx);
				} finally { }
			} else {
				throw new SQLException("mysql batch execution failed");
			}
		} catch (SQLException ex) {
			throw ex;
		} finally {
			try {
				conn.close();
				if (stmt != null)
					stmt.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				stmt = null;
			}
		}
		return ret;
	}
}
