package io.ic.frame.DA.drvmnger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.odysz.semantic.DA.DA.DriverType;
import io.odysz.semantic.DA.ICResultset;
import io.odysz.semantic.DA.IrSemantics;
import io.odysz.semantic.DA.cp.CpSrc;

public abstract class IrAbsDriver {
	protected DriverType drvName;
	public DriverType driverName() { return drvName; }

	private boolean _isOrcl = false;
	private boolean _isSqlite = false;
	public void isSqlite(boolean is) { _isSqlite = is; }
	public boolean isSqlite() { return _isSqlite; }

	HashMap<String, IrSemantics>  metas;

	abstract ICResultset select(String sql, int flags) throws SQLException ;

	abstract int[] commit(ArrayList<String> sqls, int flags) throws SQLException;

	public String formatFieldName(String expr) {
		if (_isOrcl  && CpSrc.orclKeywords.contains(expr.trim()))
			return String.format("\"%s\"", expr.trim().toUpperCase());
		return expr;
	}

	private DbSpec spec;
	public DbSpec getSpec() { return spec; }

	private HashMap<String, DbTable> tables;
	private HashMap<String, HashMap<String, DbColumn>> tablCols;	

	public IrAbsDriver meta(DbSpec spec, HashMap<String, DbTable> tables,
			HashMap<String, HashMap<String, DbColumn>> tablCols, int flagPrintsql) {
		this.spec = spec;
		this.tables = tables;
		this.tablCols = tablCols;
		return this;
	}

	public DbColumn getColumn(String tabName, String colName) {
		if(tablCols.containsKey(tabName))
			return tablCols.get(tabName).get(colName);
		else
			return null;
	}

	public DbTable getTable(String tablname) {
		return tables.get(tablname);
	}
	
	public IrSemantics getTableSemantics(String tabName) throws SQLException {
		if (metas == null) throw new SQLException ("not initialized ");
		return metas.get(tabName);
	}

	public void reinstallSemantics(HashMap<String, IrSemantics> semantics) {
		if (metas != null && metas.size() > 0) {
			System.err.println("Clear and reinstall semantics of " + drvName);
			metas.clear();
		} 
		metas = semantics;
	}

	HashMap<String, ReentrantLock> locks;

	public Lock getAutoseqLock(String target) throws SQLException {
		if (!_isSqlite)
			throw new SQLException ("?? Why needing a lock for " + drvName);
		return locks.get(target);
	}
}
