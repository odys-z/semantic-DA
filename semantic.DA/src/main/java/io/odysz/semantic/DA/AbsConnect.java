package io.odysz.semantic.DA;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.odysz.module.rs.SResultset;
import io.odysz.common.JDBCType;
import io.odysz.semantic.Semantics;
import io.odysz.semantic.DA.cp.CpSrc;
import io.odysz.semantics.meta.ColumnMeta;
import io.odysz.semantics.meta.DbMeta;
import io.odysz.semantics.meta.TableMeta;

public abstract class AbsConnect {
	protected JDBCType drvName;
	public JDBCType driverType() { return drvName; }

	private boolean _isOrcl = false;
	private boolean _isSqlite = false;
	public void isSqlite(boolean is) { _isSqlite = is; }
	public boolean isSqlite() { return _isSqlite; }

	HashMap<String, Semantics>  metas;

	public abstract SResultset select(String sql, int flags) throws SQLException ;

	public abstract int[] commit(ArrayList<String> sqls, int flags) throws SQLException;

	public String formatFieldName(String expr) {
		if (_isOrcl  && CpSrc.orclKeywords.contains(expr.trim()))
			return String.format("\"%s\"", expr.trim().toUpperCase());
		return expr;
	}

	private DbMeta spec;
	public DbMeta getSpec() { return spec; }

	private HashMap<String, TableMeta> tables;
	private HashMap<String, HashMap<String, ColumnMeta>> tablCols;	

	public AbsConnect meta(DbMeta spec, HashMap<String, TableMeta> tables,
			HashMap<String, HashMap<String, ColumnMeta>> tablCols, int flagPrintsql) {
		this.spec = spec;
		this.tables = tables;
		this.tablCols = tablCols;
		return this;
	}

	public ColumnMeta getColumn(String tabName, String colName) {
		if(tablCols.containsKey(tabName))
			return tablCols.get(tabName).get(colName);
		else
			return null;
	}

	public TableMeta getTable(String tablname) {
		return tables.get(tablname);
	}
	
	public Semantics getTableSemantics(String tabName) throws SQLException {
		if (metas == null) throw new SQLException ("not initialized ");
		return metas.get(tabName);
	}

	public void reinstallSemantics(HashMap<String, Semantics> semantics) {
		if (metas != null && metas.size() > 0) {
			System.err.println("Clear and reinstall semantics of " + drvName);
			metas.clear();
		} 
		metas = semantics;
	}

	protected HashMap<String, ReentrantLock> locks;

	public Lock getAutoseqLock(String target) throws SQLException {
		if (!_isSqlite)
			throw new SQLException ("?? Why needing a lock for " + drvName);
		return locks.get(target);
	}
}
