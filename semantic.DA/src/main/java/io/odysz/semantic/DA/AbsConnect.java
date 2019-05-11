package io.odysz.semantic.DA;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FilenameUtils;

import io.odysz.common.dbtype;
import io.odysz.common.Utils;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.cp.CpConnect;
import io.odysz.semantic.DA.drvmnger.Msql2kDriver;
import io.odysz.semantic.DA.drvmnger.MysqlDriver;
import io.odysz.semantic.DA.drvmnger.OracleDriver;
import io.odysz.semantic.DA.drvmnger.SqliteDriver;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;

public abstract class AbsConnect<T extends AbsConnect<T>> {
	protected dbtype drvName;
	public dbtype driverType() { return drvName; }

	public AbsConnect (dbtype drvName) {
		this.drvName = drvName;
	}
//	private boolean _isOrcl = false;
//	private boolean _isSqlite = false;
//	public void isSqlite(boolean is) { _isSqlite = is; }
//	public boolean isSqlite() { return _isSqlite; }
	
	public static AbsConnect<?> initDmConnect(String xmlDir, dbtype type, String jdbcUrl,
			String usr, String pswd, boolean printSql) throws SQLException, SemanticException {
		if (type == dbtype.mysql) {
			return MysqlDriver.initConnection(jdbcUrl,
					usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == dbtype.sqlite) {
			return SqliteDriver.initConnection(String.format("jdbc:sqlite:%s", FilenameUtils.concat(xmlDir, jdbcUrl)),
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
		/*
		if (type == JDBCType.mysql) {
			return MysqlDriver.initPooledConnect(jdbcUrl,
					usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == JDBCType.sqlite) {
			return SqliteDriver.initPooledConnect(String.format("jdbc:sqlite:%s", FilenameUtils.concat(xmlDir, jdbcUrl)),
				usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == JDBCType.ms2k) {
			return Msql2kDriver.initPooledConnect(jdbcUrl,
				usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else if (type == JDBCType.oracle) {
			return OracleDriver.initPooledConnect(jdbcUrl,
				usr, pswd, printSql ? Connects.flag_printSql : Connects.flag_nothing);
		}
		else
			throw new SemanticException("The configured DB type %s is not supported yet.", type);
			*/
		return new CpConnect(jdbcUrl, type, printSql);
	}
	
	protected void close() throws SQLException {}

//	HashMap<String, Semantics>  metas;

	public abstract SResultset select(String sql, int flags) throws SQLException ;

	protected abstract int[] commit(ArrayList<String> sqls, int flags) throws SQLException;

	public final int[] commit(IUser usr, ArrayList<String> sqls, int flags) throws SQLException {
		int[] c = commit(sqls, flags);
		if (usr != null) {
			sqls = usr.dbLog(sqls);
			if (sqls != null)
				commit(null, sqls, Connects.flag_nothing);
		}
		else {
			Utils.warn("Some db commitment not logged:", sqls);
		}
		return c;
	}

	public abstract int[] commit(IUser usr, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException;
	
//	private DbMeta spec;
//	public DbMeta getSpec() { return spec; }

//	private HashMap<String, TableMeta> tables;
//	private HashMap<String, HashMap<String, ColumnMeta>> tablCols;	

//	@SuppressWarnings("unchecked")
//	public AbsConnect<T> meta(DbMeta spec, HashMap<String, TableMeta> tables,
//			HashMap<String, HashMap<String, ColumnMeta>> tablCols, int flagPrintsql) {
//		this.spec = spec;
//		this.tables = tables;
//		this.tablCols = tablCols;
//		return (T) this;
//	}

//	public ColumnMeta getColumn(String tabName, String colName) {
//		if(tablCols.containsKey(tabName))
//			return tablCols.get(tabName).get(colName);
//		else
//			return null;
//	}
//
//	public TableMeta getTable(String tablname) {
//		return tables.get(tablname);
//}
	
//	public Semantics getTableSemantics(String tabName) throws SQLException {
//		if (metas == null) throw new SQLException ("not initialized ");
//		return metas.get(tabName);
//	}
//
//	public void reinstallSemantics(HashMap<String, Semantics> semantics) {
//		if (metas != null && metas.size() > 0) {
//			System.err.println("Clear and reinstall semantics of " + drvName);
//			metas.clear();
//		} 
//		metas = semantics;
//	}

	/**Lock table when generating auto Id.<br>
	 * [table, lock]
	 */
	protected HashMap<String, ReentrantLock> locks;

	public Lock getAutoseqLock(String target) throws SQLException {
//		if (!_isSqlite)
//			throw new SQLException ("?? Why needing a lock for " + drvName);
		return locks.get(target);
	}
}
