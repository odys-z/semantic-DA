package io.odysz.semantic.DA.drvmnger;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.sqlite.JDBC;

import io.odysz.common.dbtype;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.IUser;

/**All instance using the same connection.<br>
 * 
 * Sqlite connection.<br>
 * SqliteDriver using sigle connection to avoid error:<br>
 * see <a href='https://stackoverflow.com/questions/13891006/getting-sqlite-busy-database-file-is-locked-with-select-statements'>
 * StackOverflow: Getting [SQLITE_BUSY] database file is locked with select statements</a>
 * 
 * @author odys-z@github.com
 */
public class SqliteDriver extends AbsConnect<SqliteDriver> {
	public static boolean enableSystemout = true;
	static boolean inited = false;
	private static JDBC drv;

	static String userName;
	static String pswd;
	static String jdbcUrl;
	
	/**Sqlite connection.<br>
	 * SqliteDriver using sigle connection to avoid error:<br>
	 * org.sqlite.SQLiteException: [SQLITE_BUSY]  The database file is locked (database is locked)<br>
	 * see <a href='https://stackoverflow.com/questions/13891006/getting-sqlite-busy-database-file-is-locked-with-select-statements'>
	 * StackOverflow: Getting [SQLITE_BUSY] database file is locked with select statements</a>
	 */
	private static Connection conn;

	static {
		try {
			// see answer of Eehol:
			// https://stackoverflow.com/questions/16725377/no-suitable-driver-found-sqlite
			drv = new JDBC();
			DriverManager.registerDriver(new JDBC());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public SqliteDriver() {
		super(dbtype.sqlite);
		drvName = dbtype.sqlite;
		locks = new HashMap<String, ReentrantLock>();
	}
	
	@Override
	protected void close() throws SQLException {
		// This is not correct
		// https://stackoverflow.com/questions/31530700/static-finally-block-in-java
		if (conn != null)
			conn.close();
		DriverManager.deregisterDriver(drv);
	}

	/**This method is only for debug and test, use #{@link SqliteDriver#initConnection(String, String, String, int)} before any function call.
	 * MUST CLOSE CONNECTION!
	 * @return
	 * @throws SQLException
	 */
	protected static Connection getConnection() throws SQLException {
		if (!inited) {
//			String isTrue = Configs.getCfg("sqlite.printSQL.enable");
//			enableSystemout = isTrue != null && "true".equals(isTrue.toLowerCase());
//			
//			jdbcUrl = "jdbc:sqlite:/media/sdb/docs/prjs/works/RemoteServ/WebContent/WEB-INF/remote.db";
//			userName = "remote";
//			pswd = "remote";
//			
//			if (conn == null)
//				conn = DriverManager.getConnection(jdbcUrl, userName, pswd);
//			inited = true;
			throw new SQLException("Sqlite connection not initialized.");
		}
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
			enableSystemout = (flags & Connects.flag_printSql) > 0;
			
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

			if (conn == null)
				conn = DriverManager.getConnection(jdbcUrl, userName, pswd);

			inited = true;
		}
		return new SqliteDriver();
	}
	
	static SResultset selectStatic(String sql, int flag) throws SQLException {
		Connection conn = getConnection();
		Connects.printSql(enableSystemout, flag, sql);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		SResultset icrs = new SResultset(rs);
		rs.close();
		stmt.close();

		// What about performance?
		// https://stackoverflow.com/questions/31530700/static-finally-block-in-java
		// conn.close();
		
		return icrs;
	}

	public SResultset select(String sql, int flag) throws SQLException {
		return selectStatic(sql, flag);
	}
	
	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		return commitst(sqls, flags);
	}

	static int[] commitst(ArrayList<String> sqls, int flags) throws SQLException {
		Connects.printSql(enableSystemout, flags, sqls);;

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

	@Override
	public int[] commit(IUser usr, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException {
		throw new SQLException("To the author's knowledge, Sqlite do not supporting CLOB - TEXT is enough. You can contact the author.");
	}
}
