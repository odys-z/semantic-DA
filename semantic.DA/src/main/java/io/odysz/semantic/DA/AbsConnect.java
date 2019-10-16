package io.odysz.semantic.DA;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io_odysz.FilenameUtils;

import io.odysz.common.Utils;
import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.cp.CpConnect;
import io.odysz.semantic.DA.drvmnger.Msql2kDriver;
import io.odysz.semantic.DA.drvmnger.MysqlDriver;
import io.odysz.semantic.DA.drvmnger.OracleDriver;
import io.odysz.semantic.DA.drvmnger.SqliteDriver2;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;

public abstract class AbsConnect<T extends AbsConnect<T>> {
	protected dbtype drvName;
	public dbtype driverType() { return drvName; }

	public AbsConnect (dbtype drvName) {
		this.drvName = drvName;
	}
	
	public static AbsConnect<?> initDmConnect(String xmlDir, dbtype type, String jdbcUrl,
			String usr, String pswd, boolean printSql) throws SQLException, SemanticException {
		if (type == dbtype.mysql) {
			return MysqlDriver.initConnection(jdbcUrl,
					usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == dbtype.sqlite) {
			return SqliteDriver2.initConnection(String.format("jdbc:sqlite:%s", FilenameUtils.concat(xmlDir, jdbcUrl)),
				usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == dbtype.ms2k) {
			return Msql2kDriver.initConnection(jdbcUrl,
				usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == dbtype.oracle) {
			return OracleDriver.initConnection(jdbcUrl,
				usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else
			throw new SemanticException("The configured DB type %s is not supported yet.", type);
	}

	public static AbsConnect<? extends AbsConnect<?>> initPooledConnect(String xmlDir, dbtype type,
			String jdbcUrl, String usr, String pswd, boolean printSql) {
		return new CpConnect(jdbcUrl, type, printSql);
	}
	
	protected void close() throws SQLException {}

	public abstract AnResultset select(String sql, int flags) throws SQLException ;

	protected abstract int[] commit(ArrayList<String> sqls, int flags) throws SQLException;

	public final int[] commit(IUser usr, ArrayList<String> sqls, int flags) throws SQLException {
		int[] c = commit(sqls, flags);
		if (usr != null) {
			try {
				sqls = usr.dbLog(sqls);
				if (sqls != null)
					commit(null, sqls, Connects.flag_nothing);
			}
			catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		else {
			Utils.warn("Some db commitment not logged:", sqls);
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
