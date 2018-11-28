package io.odysz.semantic.DA.drvmnger;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.DbLog;

public class Msql2kDriver extends AbsConnect<Msql2kDriver> {

	public static Msql2kDriver initConnection(String string, String string2, String string3, int i) {
		return null;
	}

	@Override
	public SResultset select(String sql, int flags) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

//	@Override
//	public int[] commit(DbLog log, ArrayList<String> sqls, int flags) throws SQLException {
//		// TODO Auto-generated method stub
//		return null;
//	}

	@Override
	public int[] commit(DbLog log, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException {
		throw new SQLException("For the author's knowledge, MS 2000 seams do not supporting LOB - TEXT is enough. You can contact the author.");
	}

	
}
