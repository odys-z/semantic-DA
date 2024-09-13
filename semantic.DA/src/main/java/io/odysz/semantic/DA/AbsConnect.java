package io.odysz.semantic.DA;

import java.io.File;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.naming.NamingException;

import org.apache.commons.io_odysz.FilenameUtils;

import io.odysz.anson.Anson;
import io.odysz.common.EnvPath;
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
	protected boolean log;
	public boolean enableSystemout = true;

	protected dbtype drvName;
	public dbtype driverType() { return drvName; }

	/**
	 * @param drvName
	 * @param log enable logging user action
	 */
	public AbsConnect (dbtype drvName, boolean log) {
		this.drvName = drvName;
		this.log = log;
	}
	
	public static AbsConnect<?> initDmConnect(String xmlDir, dbtype type, String jdbcUrl,
			String usr, String pswd, boolean printSql, boolean log) throws SQLException, SemanticException {
		if (type == dbtype.mysql) {
			return MysqlDriver.initConnection(jdbcUrl,
					usr, pswd, log, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == dbtype.sqlite) {
			// Since docker volume can not be mounted in tomcat webapps' sub-folder, file path handling can be replaced with environment variables now.
			Utils.logi("Resolving sqlite db, xmlDir: %s,\n\tjdbcUrl: %s", xmlDir, jdbcUrl);

			String dbpath = FilenameUtils.concat(xmlDir, EnvPath.replaceEnv(jdbcUrl));
			Utils.logi("\tUsing sqlite db: %s", dbpath);
			
			File f = new File(dbpath);
			if (!f.exists())
				throw new SemanticException("Can't find DB file: %s", f.getAbsolutePath());

			return SqliteDriver2.initConnection(String.format("jdbc:sqlite:%s", dbpath),
					usr, pswd, log, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == dbtype.sqlite_queue) {
			Utils.logi("Resolving sqlite db (pooled), xmlDir: %s,\n\tjdbcUrl: %s", xmlDir, jdbcUrl);

			String dbpath = FilenameUtils.concat(xmlDir, EnvPath.replaceEnv(jdbcUrl));
			Utils.logi("\tUsing sqlite db (pooled): %s", dbpath);
			
			File f = new File(dbpath);
			if (!f.exists())
				throw new SemanticException("Can't find DB file: %s", f.getAbsolutePath());

			return SqliteDriverQueued.initConnection(String.format("jdbc:sqlite:%s", dbpath),
					usr, pswd, log, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == dbtype.ms2k) {
			return Msql2kDriver.initConnection(jdbcUrl,
				usr, pswd, log, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == dbtype.oracle) {
			return OracleDriver.initConnection(jdbcUrl,
				usr, pswd, log, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else
			throw new SemanticException("The configured DB type %s is not supported yet.", type);
	}

	public static AbsConnect<? extends AbsConnect<?>> initPooledConnect(String xmlDir, dbtype type,
			String jdbcUrl, String usr, String pswd, boolean printSql, boolean log) {
		return new CpConnect(jdbcUrl, type, printSql, log);
	}
	
	protected void close() throws SQLException {}

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
	
	protected abstract int[] commit(ArrayList<String> sqls, int flags) throws SQLException, NamingException;

	public final int[] commit(IUser usr, ArrayList<String> sqls, int flags) throws SQLException, NamingException {
		int[] c = commit(sqls, flags);
		if (usr != null) {
			try {
				if (log) {
					sqls = usr.dbLog(sqls);

					if (sqls != null)
						commit(null, sqls, Connects.flag_nothing);
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
}
