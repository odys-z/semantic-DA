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
import org.sqlite.SQLiteConfig;

import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantics.IUser;

import static io.odysz.common.LangExt.isblank;

/**All instance using the same connection.<br>
 * 
 * Sqlite connection.<br>
 * SqliteDriver using sigle connection to avoid error:<br>
 * see <a href='https://stackoverflow.com/questions/13891006/getting-sqlite-busy-database-file-is-locked-with-select-statements'>
 * StackOverflow: Getting [SQLITE_BUSY] database file is locked with select statements</a>
 * 
 * @author odys-z@github.com
 */
public class SqliteDriver2 extends AbsConnect<SqliteDriver2> {

	String userName;
	String pswd;
	String jdbcUrl;
	
	/**Sqlite connection.<br>
	 * SqliteDriver using sigle connection to avoid error:<br>
	 * org.sqlite.SQLiteException: [SQLITE_BUSY]  The database file is locked (database is locked)<br>
	 * see <a href='https://stackoverflow.com/questions/13891006/getting-sqlite-busy-database-file-is-locked-with-select-statements'>
	 * StackOverflow: Getting [SQLITE_BUSY] database file is locked with select statements</a>
	 */
	protected Connection conn;

	/**
	 * Loads (initializes) org.sqlite.JDBC, which registers its own driver instance in its static block.
	 * <p>Before 1.5.32, a second instance was registered here, while {@link #close()} tried to deregister
	 * yet another one, never registered, which was a no-op (DriverManager matches drivers by identity).</p>
	 * see answer of Eehol:
	 * https://stackoverflow.com/questions/16725377/no-suitable-driver-found-sqlite
	 */
	static {
		try {
			Class.forName(JDBC.class.getName());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Deregister the sqlite drivers registered by this class loader, e.g. when a webapp is undeployed.
	 * Not called by {@link #close()}, as the driver is shared by all sqlite connections.
	 * @since 1.5.32
	 */
	public static void deregisterDrivers() {
		java.util.Enumeration<java.sql.Driver> ds = DriverManager.getDrivers();
		while (ds.hasMoreElements()) {
			java.sql.Driver d = ds.nextElement();
			if (d instanceof JDBC && d.getClass().getClassLoader() == SqliteDriver2.class.getClassLoader())
				try { DriverManager.deregisterDriver(d); }
				catch (SQLException e) { e.printStackTrace(); }
		}
	}

	SqliteDriver2(String id, boolean log) {
		super(dbtype.sqlite, id, log);
		drvName = dbtype.sqlite;
		locks = new HashMap<String, ReentrantLock>();
	}
	
	@Override
	/**
	 * Close the connection, but not the driver, which other sqlite connections share.
	 * @see #deregisterDrivers()
	 */
	public void close() throws SQLException {
		if (conn != null && !conn.isClosed())
			conn.close();
	}

	/**
	 * MUST CLOSE CONNECTION!
	 * @return connection
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		return conn;
	}
	
	/**
	 * Get {@link SqliteDriver2} instance, with database connection got via {@link DriverManager}.
	 * 
	 * @param jdbc
	 * @param user
	 * @param psword
	 * @param log
	 * @param flags
	 * @return SqliteDriver2 instance
	 * @throws SQLException
	 */
	public static SqliteDriver2 initConnection(String id, String jdbc, String user, String psword, boolean log, int flags) throws SQLException {
		SqliteDriver2 inst = new SqliteDriver2(id, log);

		inst.enableSystemout = (flags & AbsConnect.flag_printSql) > 0;
		inst.jdbcUrl = jdbc;
		inst.userName = user;
		inst.pswd = psword;
		
		SQLiteConfig cfg = new SQLiteConfig();
		cfg.setEncoding(SQLiteConfig.Encoding.UTF8);
		inst.conn = DriverManager.getConnection(jdbc, cfg.toProperties());
		return inst;
	}
	
	AnResultset selectStatic(String sql, int flag) throws SQLException {
		Connection conn = getConnection();
		printSql(flag, sql);

		Statement stmt = conn.createStatement();

		// ResultSet rs = stmt.executeQuery(sql);
		try {
			if (stmt.execute(sql)) {
				ResultSet rs = stmt.getResultSet();
				AnResultset icrs = new AnResultset(rs);
				rs.close();
				return icrs;
			}
		} finally {
			stmt.close();
		}
		return null;
	}

	public AnResultset select(String sql, int flag) throws SQLException {
		return selectStatic(sql, flag);
	}

	/**Commit statement
	 * @param sqls
	 * @param flags
	 * @return The update counts in order of commands
	 * @throws SQLException
	 */
	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		return commitst(sqls, flags);
	}

	/**
	 * <h4>Commit statement</h4>
	 * 
	 * Change in 1.5.30: fix test error: <pre>
	 * [INFO] Running io.odysz.semantic.DASemantextTest
	 * org.sqlite.SQLiteException: [SQLITE_BUSY] The database file is locked (database is locked)
	 * at org.sqlite.core.DB.newSQLException(DB.java:1179)
	 * at org.sqlite.core.DB.newSQLException(DB.java:1190)
	 * at org.sqlite.core.DB.throwex(DB.java:1150)
	 * at org.sqlite.core.DB.exec(DB.java:198)
	 * at org.sqlite.SQLiteConnection.commit(SQLiteConnection.java:449)
	 * ...</pre>
        
	 * Explain: A connection was left in manual-commit mode, so one of its later reads kept a lock
	 * that never ended. A different connection then couldn’t write to the same file.
	 * 
	 * @param sqls
	 * @param flags
	 * @return The update counts in order of commands
	 * @throws SQLException
	 */
	int[] commitst(ArrayList<String> sqls, int flags) throws SQLException {
	    printSql(flags, sqls);

	    Connection conn = getConnection();
	    boolean pushed = conn.getAutoCommit();      // push
	    conn.setAutoCommit(false);

	    int[] ret;
	    try (Statement stmt = conn.createStatement(
	            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
	        for (String sql : sqls)
	            if (!isblank(sql)) stmt.addBatch(sql);
	        ret = stmt.executeBatch();

	        if (autoCommit) {
	            conn.commit();
	            conn.setAutoCommit(pushed);         // pop
	        }
	        else {
	            pendding_conn = conn;
	            pendding_autoCommit = pushed;       // pop deferred to submitAutoCommit()
	        }
	    } catch (Exception exx) {
	        conn.rollback();
	        conn.setAutoCommit(pushed);             // pop
	        throw new SQLException(exx);
	    }
	    return ret;
	}
//	int[] commitst(ArrayList<String> sqls, int flags) throws SQLException {
//		printSql(flags, sqls);;
//
//		int[] ret;
//		Statement stmt = null;
//		try {
//			Connection conn = getConnection();
//
//			stmt = conn.createStatement();
//			try {
//				conn.setAutoCommit(false);
//				stmt = conn.createStatement(
//						ResultSet.TYPE_FORWARD_ONLY,
//						ResultSet.CONCUR_READ_ONLY);
//
//				for (String sql : sqls) {
//					if (isblank(sql)) continue;
//					stmt.addBatch(sql);
//				}
//
//				ret = stmt.executeBatch();
//				if (autoCommit)
//					conn.commit(); // 2025.06.16
//				else
//					pendding_conn = conn;
//			} catch (Exception exx) {
//				conn.rollback();
//				exx.printStackTrace();
//				throw new SQLException(exx);
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
//		return ret;
//	}

	@Override
	public int[] commit(IUser usr, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException {
		throw new SQLException("To the author's knowledge, Sqlite does not support CLOB - TEXT is enough. You can contact the author.");
	}
}
