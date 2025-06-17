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
	private static JDBC drv;

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

	SqliteDriver2(String id, boolean log) {
		super(dbtype.sqlite, id, log);
		drvName = dbtype.sqlite;
		locks = new HashMap<String, ReentrantLock>();
	}
	
	@Override
	public void close() throws SQLException {
		// This is not correct
		// https://stackoverflow.com/questions/31530700/static-finally-block-in-java
		if (conn != null)
			conn.close();
		DriverManager.deregisterDriver(drv);
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
	 * Commit statement
	 * 
	 * @param sqls
	 * @param flags
	 * @return The update counts in order of commands
	 * @throws SQLException
	 */
	int[] commitst(ArrayList<String> sqls, int flags) throws SQLException {
		printSql(flags, sqls);;

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
					if (isblank(sql)) continue;
					stmt.addBatch(sql);
				}

				ret = stmt.executeBatch();
				if (autoCommit)
					conn.commit(); // 2025.06.16
				else
					pendding_conn = conn;
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
		throw new SQLException("To the author's knowledge, Sqlite does not support CLOB - TEXT is enough. You can contact the author.");
	}
}
