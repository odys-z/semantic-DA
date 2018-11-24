package io.odysz.semantic.DA.drvmnger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.sqlite.JDBC;

import io.odysz.common.Configs;
import io.odysz.module.rs.ICResultset;
import io.odysz.semantic.DA.DA;

/**All instance using the same connection.
 * @author ody
 *
 */
public class SqliteDriver extends IrAbsDriver {
	public static boolean enableSystemout = true;
	static boolean inited = false;
	static String userName;
	static String pswd;
	static String jdbcUrl;

	static {
		try {
			// see answer of Eehol:
			// https://stackoverflow.com/questions/16725377/no-suitable-driver-found-sqlite
			DriverManager.registerDriver(new JDBC());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	SqliteDriver() {drvName = DA.DriverType.sqlite;}

//	public static void getConnection(String fileName) {
//		 
//        String url = "jdbc:sqlite:C:/sqlite/db/" + fileName;
// 
//        try (Connection conn = DriverManager.getConnection(url)) {
//            if (conn != null) {
//                DatabaseMetaData meta = conn.getMetaData();
//                System.out.println("The driver name is " + meta.getDriverName());
//                System.out.println("A new database has been created.");
//            }
// 
//        } catch (SQLException e) {
//            System.out.println(e.getMessage());
//        }
//    }
	/**This method is only for debug and test, use #{@link SqliteDriver#initConnection(String, String, String, int)} before any function call.
	 * MUST CLOSE CONNECTION!
	 * @return
	 * @throws SQLException
	 */
	protected static Connection getConnection() throws SQLException {
		if (!inited) {
			String isTrue = Configs.getCfg("sqlite.printSQL.enable");
			enableSystemout = isTrue != null && "true".equals(isTrue.toLowerCase());
			
			// Hard coded config: This method is only for debug.
			jdbcUrl = "jdbc:sqlite:/media/sdb/docs/prjs/works/RemoteServ/WebContent/WEB-INF/remote.db";
			userName = "remote";
			pswd = "remote";
			
			// FIXME decipher pswd
			// pswd = Encrypt.DecryptPswdImpl(pswd);
//			try {
//				Class.forName("org.sqlite.JDBC").newInstance();
//			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
//				e.printStackTrace();
//				throw new SQLException(e.getMessage());
//			}
		}
		Connection conn = DriverManager.getConnection(jdbcUrl, userName, pswd);
		inited = true;
		return conn;
	}
	
	/**Use this to init connection without using servlet context for retrieving configured strings.<br>
	 * This is the typical scenario when running test from "main" thread.
	 * @param jdbc
	 * @param user
	 * @param psword
	 * @param flags 
	 * @return 
	 * @throws SQLException
	 */
	public static SqliteDriver initConnection(String jdbc, String user, String psword, int flags) throws SQLException {
		if (!inited) {
			enableSystemout = (flags & DA.flag_printSql) > 0;
			
			jdbcUrl = jdbc;
			userName = user;
			pswd = psword;
//			// FIXME decipher pswd
//			// pswd = Encrypt.DecryptPswdImpl(pswd);
//			try {
//				Class.forName("org.sqlite.JDBC").newInstance();
//			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
//				e.printStackTrace();
//				throw new SQLException(e.getMessage());
//			}
			inited = true;
		}
		return new SqliteDriver();
	}
	
	static ICResultset selectStatic(String sql, int flag) throws SQLException {
		Connection conn = getConnection();
		DA.printSql(enableSystemout, flag, sql);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ICResultset icrs = new ICResultset(rs);
		rs.close();
		stmt.close();
		conn.close();
		
		return icrs;
	}

	ICResultset select(String sql, int flag) throws SQLException {
//		Connection conn = getConnection();
//		DA.printSql(enableSystemout, flag, sql);
//		Statement stmt = conn.createStatement();
//		ResultSet rs = stmt.executeQuery(sql);
//		ICResultset icrs = new ICResultset(rs);
//		rs.close();
//		stmt.close();
//		conn.close();
//		
//		return icrs;
		return selectStatic(sql, flag);
	}
	
//	public static void executeBatch(DbLog log, ArrayList<String> sqls) throws SQLException {
//		Connection conn = getConnection();
//		excecuteBatch(conn, log, sqls);
//		conn.close();
//	}
//	
//	/**@deprecated replaced by commit
//	 * @param conn
//	 * @param sqls
//	 * @throws SQLException
//	 */
//	public static void excecuteBatch(Connection conn, DbLog log, ArrayList<String> sqls) throws SQLException {
//		if (enableSystemout)
//			System.out.println(sqls);
//
//		Statement stmt = null;
//		try {
//			if (conn != null) {
//				stmt = conn.createStatement();
//				try {
//					// String logs = "";
//					// boolean noMoreLogs = false;
//					stmt = conn.createStatement(
//							ResultSet.TYPE_SCROLL_SENSITIVE,
//							ResultSet.CONCUR_UPDATABLE);
//					conn.setAutoCommit(false);
//
//					for (String sql : sqls) {
//						stmt.addBatch(sql);
//					}
//
//					/* not need 
//					for (String sql : sqls) {
//						if (enableSystemout) System.out.println(sql + ";");
//						stmt.addBatch(sql);
//						if (!noMoreLogs && logs.length() + sql.length() + 1 + "...".length() <= 4000) {
//							logs += sql.replaceAll("'", "''") + ";";
//						}
//						else if (!noMoreLogs && logs.length() + sql.length() + 1 + "...".length() > 4000) {
//							noMoreLogs = true;
//							if (logs.length() + "...".length() <= 4000) {
//								logs += "...";
//							}
//						}
//					}
//					if (!"".equals(logs)) {
//						String logsql = String.format("insert into frame_logs(operID, oper, operTime, funcName, sysID, remarks) values " +
//								"('%s', '%s', to_date('%s', 'yyyy-MM-dd hh24:mi:ss'), '%s', '%s', '%s')", userid, username, operTime, funcName, sysID, logs);
//						stmt.addBatch(logsql);
//						if (enableSystemout) System.out.println(logsql);
//					}*/
//
//					stmt.executeBatch();
//					conn.commit();
//
////					logOperations (logs, userid, operTime, funcName, remarks);
//				} catch (Exception exx) {
//					conn.rollback();
//					exx.printStackTrace();
//					throw new SQLException(exx);
//				} finally { }
//			} else {
//				throw new SQLException("sqlite batch execution failed");
//			}
//		} finally {
//			try {
//				if (stmt != null)
//					stmt.close();
//			} catch (Exception ex) {
//				ex.printStackTrace();
//			} finally {
//				stmt = null;
//			}
//		}
//	}
	int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		return commitst(sqls, flags);
	}

	static int[] commitst(ArrayList<String> sqls, int flags) throws SQLException {
		DA.printSql(enableSystemout, flags, sqls);;

		int[] ret;
		Statement stmt = null;
		try {
			Connection conn = getConnection();
			stmt = conn.createStatement();
			try {
				conn.setAutoCommit(false);
				stmt = conn.createStatement(
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY);

				for (String sql : sqls) {
					stmt.addBatch(sql);
				}

				ret = stmt.executeBatch();
				conn.commit();

			} catch (Exception exx) {
				conn.rollback();
				exx.printStackTrace();
				throw new SQLException(exx);
			}
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
		return ret;
	}

	void setLocks(HashMap<String, ReentrantLock> locks) {
		this.locks = locks;
	}
	

}
