package io.odysz.semantic.DA.drvmnger;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.IUser;

public class MysqlDriver extends AbsConnect<MysqlDriver> {
	static boolean inited = false;
	static String userName;
	static String pswd;
	static String connect;
	private static Connection conn;
	
	/**
	 * IMPORTANT: Caller must close connection!
	 * @return connection
	 * @throws SQLException
	 */
	protected static Connection getConnection() throws SQLException {
		if (!inited) {
			throw new SQLException("connection must explicitly initialized first - call initConnection()");
		}

		if (conn == null)
			conn = DriverManager.getConnection(connect, userName, pswd);
		return conn;
	}
	
	/**Use this to init connection without using servlet context for retrieving configured strings.<br>
	 * This is the typical scenario when running test from "main" thread.
	 * 
	 * @param conn
	 * @param user
	 * @param psword
	 * @param log
	 * @param flags
	 * @return new MysqlDriver
	 * @throws SQLException
	 */
	public static MysqlDriver initConnection(String conn, String user, String psword, boolean log, int flags) throws SQLException {
		if (!inited) {
			
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
		MysqlDriver inst = new MysqlDriver(log);
		inst.enableSystemout = (flags & Connects.flag_printSql) > 0;
		return inst;
	}
 
	public static AnResultset selectStatic(String sql, int flags) throws SQLException {
		Connection conn = getConnection();
		Connects.printSql(false, flags, sql);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		AnResultset icrs = new AnResultset(rs);
		rs.close();
		stmt.close();
		conn.close();
		
		return icrs;
	}

	public MysqlDriver(boolean log) {
		super(dbtype.mysql, log);
	}

	public AnResultset select(String sql, int flags) throws SQLException {
		return selectStatic(sql, flags);
	}
	
	@Override
	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		Connects.printSql(enableSystemout, flags, sqls);
		
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
					if (enableSystemout) System.out.println("mysql batch execute successfully.");
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
	public int[] commit(IUser log, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException {
		throw new SQLException("For the author's knowledge, Mysql TEXT is enough for CLOB"
				+ " - and not planning supporting BLOB as this project is currently designed for supporting mainly JSON module over HTTP. "
				+ "You can contact the author for any suggestion.");
	}
}
