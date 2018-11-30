package io.odysz.semantic.DA.drvmnger;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import io.odysz.common.Configs;
import io.odysz.common.JDBCType;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.DbLog;

public class MysqlDriver extends AbsConnect<MysqlDriver> {
	public static boolean printSql = true;
	static boolean inited = false;
	static String userName;
	static String pswd;
	static String connect;
	
	/**
	 * IMPORTANT: Caller must close connection!
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

	public MysqlDriver() {
		super(JDBCType.mysql);
	}

	public SResultset select(String sql, int flags) throws SQLException {
		return selectStatic(sql, flags);
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

	@Override
	public int[] commit(DbLog log, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException {
		throw new SQLException("For the author's knowledge, Mysql TEXT is enough for CLOB"
				+ " - and not planning supporting BLOB as this project is currently designed for supporting mainly JSON module over HTTP. "
				+ "You can contact the author for any suggestion.");
	}
}
