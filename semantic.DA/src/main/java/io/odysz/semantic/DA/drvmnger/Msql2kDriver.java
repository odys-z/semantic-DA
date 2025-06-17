package io.odysz.semantic.DA.drvmnger;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantics.IUser;

public class Msql2kDriver extends AbsConnect<Msql2kDriver> {

	public Msql2kDriver(dbtype drvName, String id, boolean log) {
		super(drvName, id, log);
	}

	public static Msql2kDriver initConnection(String string, String string2, String string3, boolean log, int i) {
		return null;
	}

	@Override
	public AnResultset select(String sql, int flags) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int[] commit(IUser usr, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException {
		throw new SQLException("For the author's knowledge, MS 2000 seams do not supporting LOB - TEXT is enough. You can contact the author.");
	}
}
