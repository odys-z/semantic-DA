package io.odysz.semantic.DA;

import java.io.File;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.naming.NamingException;

import io.odysz.anson.Anson;
import io.odysz.common.EnvPath;
import io.odysz.common.FilenameUtils;
import io.odysz.common.Utils;
import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.module.rs.AnResultset.ObjCreator;
import io.odysz.semantic.DA.cp.CpConnect;
import io.odysz.semantic.DA.drvmnger.Msql2kDriver;
import io.odysz.semantic.DA.drvmnger.MysqlDriver;
import io.odysz.semantic.DA.drvmnger.OracleDriver;
import io.odysz.semantic.DA.drvmnger.SqliteDriver2;
import io.odysz.semantic.DA.drvmnger.SqliteDriverQueued;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;

public abstract class AbsConnect<T extends AbsConnect<T>> {
	public static final int flag_nothing = 0;
	public static final int flag_printSql = 1;
	public static final int flag_disableSql = 2;

	public boolean enableSystemout = true;

	protected boolean log;
	protected dbtype drvName;
	public dbtype driverType() { return drvName; }

	protected String id;

	/**
	 * @param drvName
	 * @param log enable logging user action
	 */
	public AbsConnect (dbtype drvName, String id, boolean log) {
		this.drvName = drvName;
		this.id = id;
		this.log = log;
		this.autoCommit = true;
	}
	
	public static AbsConnect<?> initDmConnect(String xmlDir, dbtype type, String id, String jdbcUrl,
			String usr, String pswd, boolean printSql, boolean log) throws SQLException, SemanticException {
		if (type == dbtype.mysql) {
			return MysqlDriver.initConnection(id, jdbcUrl,
					usr, pswd, log, printSql ? flag_printSql : flag_nothing);
		}
		else if (type == dbtype.sqlite) {
			// Debug Notes:
			// Since Docker volume can not be mounted in tomcat webapps' sub-folder,
			// file path handling can be replaced with environment variables now.
			String absXml = new File(xmlDir).getAbsolutePath();
			Utils.logi("[%s]\nResolving sqlite db, xmlDir: %s,\n\tjdbcUrl: %s\n\t%s", id, xmlDir, jdbcUrl, absXml);

			String dbpath = FilenameUtils.concat(absXml, EnvPath.replaceEnv(jdbcUrl));
			Utils.logi("\tUsing sqlite db: %s", dbpath);
			
			File f = new File(dbpath);
			if (!f.exists())
				throw new SemanticException("Can't find DB file: %s [%s]", dbpath, f.getAbsolutePath());

			return SqliteDriver2.initConnection(id, String.format("jdbc:sqlite:%s", dbpath),
					usr, pswd, log, printSql ? flag_printSql : flag_nothing);
		}
		else if (type == dbtype.sqlite_queue) {
			String absXml = new File(xmlDir).getAbsolutePath();
			Utils.logi("[%s]\nResolving sqlite db, xmlDir: %s,\n\tjdbcUrl: %s\n\t%s", id, xmlDir, jdbcUrl, absXml);

			String dbpath = FilenameUtils.concat(absXml, EnvPath.replaceEnv(jdbcUrl));
			Utils.logi("\tUsing sqlite db (queued): %s", dbpath);
			
			File f = new File(dbpath);
			if (!f.exists())
				throw new SemanticException("Can't find DB file: %s", f.getAbsolutePath());

			return SqliteDriverQueued.initConnection(id, String.format("jdbc:sqlite:%s", dbpath),
					usr, pswd, log, printSql ? flag_printSql : flag_nothing);
		}
		else if (type == dbtype.ms2k) {
			return Msql2kDriver.initConnection(jdbcUrl,
				usr, pswd, log, printSql ? flag_printSql : flag_nothing);
		}
		else if (type == dbtype.oracle) {
			return OracleDriver.initConnection(id, jdbcUrl,
				usr, pswd, log, printSql ? flag_printSql : flag_nothing);
		}
		else
			throw new SemanticException("The configured DB type %s is not supported yet.", type);
	}

	public static AbsConnect<? extends AbsConnect<?>> initPooledConnect(String xmlDir, dbtype type,
			String id, String jdbcUrl, String usr, String pswd, boolean printSql, boolean log) {
		return new CpConnect(id, jdbcUrl, type, printSql, log);
	}
	
	public void close() throws SQLException {
		if (pendding_conn != null) pendding_conn.close();
	}

	public abstract AnResultset select(String sql, int flags) throws SQLException, NamingException ;

	/**
	 * <p>Interface for the future, not supported.
	 * An optimized version of {@link AnResultset#map(String, ObjCreator)}</p>
	 * 
	 * Execute a query and convert results to a map, in stream mode.
	 * 
	 * @param sql
	 * @param builder
	 * @param flags
	 * @return null (not implemented yet)
	 * @since 1.4.12
	 */
	<V extends Anson> Map<String, V> select(String sql, ObjCreator<V> builder, int flags) { return null; }
	
	/**
	 * Commit statements, sqls, in batch mode. if {@link #autoCommit} is false, need user code
	 * to commit the statements.
	 * See {@link io.odysz.semantic.DATranscxt#select(io.odysz.transact.sql.Query, String...)}
	 * and {@link io.odysz.transact.sql.Query}.
	 * @param sqls
	 * @param flags
	 * @return results
	 * @throws SQLException
	 * @throws NamingException
	 */
	protected abstract int[] commit(ArrayList<String> sqls, int flags) throws SQLException, NamingException;

	public final int[] commit(IUser usr, ArrayList<String> sqls, int flags) throws SQLException, NamingException {
		int[] c = commit(sqls, flags);
		if (usr != null) {
			try {
				if (log) {
					sqls = usr.dbLog(sqls);

					if (sqls != null)
						commit(null, sqls, flag_nothing);
				}
			}
			catch (Exception ex) {
				Utils.warn("Logging db failed with %s#dbLog(sqls).", usr.getClass().getName());
				ex.printStackTrace();
			}
		}
		else if (log) {
			Utils.warn("Some db commitment not logged because usr is null:", sqls);
		}
		return c;
	}

	public abstract int[] commit(IUser usr, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException;
	
	/**Lock table when generating auto Id.<br>
	 * [table, lock]
	 */
	protected HashMap<String, ReentrantLock> locks;

	private HashMap<String, String> props;

	public Lock getAutoseqLock(String target) throws SQLException {
		return locks.get(target);
	}

	public AbsConnect<? extends AbsConnect<?>> prop(String k, String v) {
		if (props == null)
			props = new HashMap<String, String>();
		props.put(k, v);
		return this;
	}

	public String prop(String k) {
		return props == null ? null : props.get(k);
	}

	/////////////////////////////// common helper /////////////////////////////
	/** If printSql is true or if asking enable, 
	 * then print sqls.
	 * @param flag
	 * @param sqls
	 */
	public void printSql(int flag, ArrayList<String> sqls) {
		if ((flag & flag_printSql) == flag_printSql
			|| enableSystemout && (flag & flag_disableSql) != flag_disableSql) {
			Utils.logi("[%s]", id);
			Utils.logi(sqls);
		}
	}

	public void printSql(int flag, String sql) {
		if ((flag & flag_printSql) == flag_printSql
			|| enableSystemout && (flag & flag_disableSql) != flag_disableSql) {
			Utils.logi("[%s]", id);
			Utils.logi(sql);
		}
	}

	/**
	 * Pending connection for automatic commit is false.
	 */
	protected Connection pendding_conn;

	/**
	 * Batch auto-commit controlled by Statements, not of {@link java.sql.Connection#setAutoCommit(boolean)}.
	 * used in in {@link #commit(ArrayList, int)} implementation in each driver.
	 * @see SqliteDriver2#commitst(ArrayList, int) 
	 * @since 1.5.18
	 */
	protected boolean autoCommit = true;
	/** @since 1.5.18 */
	protected boolean autoCommitStack;
	/** @since 1.5.18 */
	public AbsConnect<?> pushAutoCommit(boolean autc) throws SQLException {
		autoCommitStack = autoCommit;
		this.autoCommit = autc;
		return this;
	}

	/** @since 1.5.18 */
	public boolean popAutoCommit() throws SQLException {
		boolean pop = autoCommit;
		autoCommit = autoCommitStack;
		return pop;
	}

	/**
	 * For Sqlite drivers, it's critical to keep only one db connection, which is required for 
	 * writing in on thread.
	 * @throws SQLException
	 */
	public void submitAutoCommit() throws SQLException {
		if (pendding_conn != null) {
			try { pendding_conn.commit(); }
			finally { pendding_conn = null; }
		}
	}
}
