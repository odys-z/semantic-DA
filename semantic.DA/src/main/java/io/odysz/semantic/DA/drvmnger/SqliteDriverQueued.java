package io.odysz.semantic.DA.drvmnger;

import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.f;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import org.sqlite.SQLiteConfig;

import io.odysz.semantic.DA.AbsConnect;

public class SqliteDriverQueued extends SqliteDriver2 {
	static boolean test;
	static final int qulen = 64;

	private Thread worker;
	private ArrayBlockingQueue<StatementOnCall> qu;
	
	private boolean stop;
	
	Object lock;
	
	SqliteDriverQueued(String connid, boolean log) {
		super(connid, log);
		
		qu = test ? new T_ArrayBlockingQueue<StatementOnCall>(qulen)
				  : new ArrayBlockingQueue<StatementOnCall>(qulen);
		
		stop = false;
		
		lock = new Object();

		this.worker = new Thread(() -> {
			StatementOnCall stmt = null;
			while (!stop) {
			try {
				stmt = qu.take();
				int[] ret = stmt.statment.executeBatch();
				conn.commit(); // 2025.06.16, queued commitments cannot submit without auto-committing. 
				
				stmt.onCommit.ok(ret);
			} catch (InterruptedException e) {
				if (!stop) e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			finally {
				if (stmt != null) {
					synchronized(stmt.lock) {
						stmt.finished = true;
						stmt.lock.notifyAll();
					}
				}
				stmt = null;
			}
			}
		}, f("sqlite queued dirver %s[%s/%s]",
			test ? "T_ArrayBlockingQueue" : "ArrayBlockingQueue",
			qu.size(), qulen));
		this.worker.start();
	}

	/**
	 * Get {@link SqliteDriver2} instance, with database connection got via {@link DriverManager}.
	 * 
	 * @return SqliteDriver2 instance
	 * @throws SQLException
	 */
	public static SqliteDriverQueued initConnection(String connId, String jdbc,
			String user, String psword, boolean log, int flags) throws SQLException {
		SqliteDriverQueued inst = new SqliteDriverQueued(connId, log);

		inst.enableSystemout = (flags & AbsConnect.flag_printSql) > 0;
		inst.jdbcUrl = jdbc;
		inst.userName = user;
		inst.pswd = psword;
		
		SQLiteConfig cfg = new SQLiteConfig();
		cfg.setEncoding(SQLiteConfig.Encoding.UTF8);
		inst.conn = DriverManager.getConnection(jdbc, cfg.toProperties());
		inst.conn.setAutoCommit(false);
		return inst;
	}
	
	/**
	 * Commit statement
	 * 
	 * @since 2.0.0
	 * @param sqls
	 * @param flags
	 * @return The update counts in order of commands
	 * @throws SQLException
	 */
	@Override
	int[] commitst(ArrayList<String> sqls, int flags) throws SQLException {
		// Connects.printSql(enableSystemout, flags, sqls);;
		printSql(flags, sqls);;

		int[][] ret = new int[][] {new int[0]};

		Statement stmt = null;
		try {
			Connection conn = getConnection();

			stmt = conn.createStatement();
			try {
				stmt = conn.createStatement(
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY);

				for (String sql : sqls) {
					if (isblank(sql)) continue;
					stmt.addBatch(sql);
				}

				// TODO we need a better BlockingQueue
				StatementOnCall stmtcall = new StatementOnCall(stmt, (re) -> ret[0] = re);
				qu.put(stmtcall);
				synchronized(stmtcall.lock) {
					if (!stmtcall.finished)
						stmtcall.lock.wait();
				}
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
		return ret[0];
	}	
	
	@Override
	public void close() {
		stop = true;
		try {
			// qu.put(null);
			this.worker.join(500);
		} catch (InterruptedException e) { }
	}
}
