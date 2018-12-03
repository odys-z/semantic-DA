package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.odysz.common.JDBCType;
import io.odysz.common.Radix64;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.DATranscxt;
import io.odysz.semantic.DA.DbLogDumb;
import io.odysz.semantics.ISemantext;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Update;
import io.odysz.transact.x.TransException;

public class DASemantext implements ISemantext {

	/** main starting table */
	private String tabl0;
	private HashMap<Object, Object> autoVals;
	private Statement<?> callerStatement;
	private static DATranscxt rawst = new DATranscxt(null);

	public DASemantext(String tabl) {
		this.tabl0 = tabl;
	}

	/**When inserting, replace inserting values in 'AUTO' columns, e.g. generate auto PK for rec-id.
	 * @see io.odysz.semantics.ISemantext#onInsert(io.odysz.transact.sql.Insert, java.lang.String, java.util.List)
	 */
	@Override
	public ISemantext onInsert(Insert insert, String tabl, List<ArrayList<Object[]>> valuesNv) {
		callerStatement = insert;
		if (valuesNv != null)
			for (ArrayList<Object[]> value : valuesNv) {
				Map<String, Integer> cols = insert.getColumns();
				replaceAuto(value, tabl, cols);
				DASemantics s = DASemantics.get(tabl);
				if (s == null)
					continue;
//				if (s.is(smtype.autoPk)) {
//					String pk = s.autoPk();
//					String n = (String) value.get(cols.get(pk))[0];
//					if (n.equals(pk))
//						value.get(cols.get(pk))[1] = "TEST-" + DateFormat.format(new Date());
//				}
//				if (s.is(smtype.fullpath)) {
//					String n = s.getFullpathField();
//					String fp = s.genFullpath(value, cols);
//					Object[] nv = null;
//					if (!cols.containsKey(n)) {
//						// append fullpath nv
//						int c = cols.size();
//						nv = new Object[] {n, fp};
//						value.add(nv);
//						cols.put(n, c);
//					}
//					else {
//						nv = value.get(cols.get(n));
//						nv[1] = fp;
//					}
//				}
				s.onInsert(value, cols);
			}
		return this;
	}
	

	private void replaceAuto(ArrayList<Object[]> nvs, String tabl, Map<String, Integer> cols) {
		if (nvs != null) {
			for (String col : cols.keySet()) {
				Integer c = cols.get(col);
				Object[] nv = nvs.get(c);
				if (nv != null && nv.length > 1
					&& nv[1] instanceof String && "AUTO".equals(nv[1]))
					nv[1] = genId(tabl, col);
			}
		}
	}

	private String genId(String tabl, String col) {
		return tabl + col;
	}

	@Override
	public ISemantext onUpdate(Update update, String tabl, ArrayList<Object[]> nvs) {
		// TODO we need semantics here
		if (nvs != null)
			for (Object[] nv : nvs)
				if (nv != null && nv.length > 0 && "AUTO".equals(nv[1]))
					nv[1] = autoVals == null ? nv[1] : autoVals.get(nv[0]);
		return this;
	}


	@Override
	public ISemantext insert(Insert insert, String tabl) {
		return new DASemantext(tabl);
	}

	@Override
	public ISemantext update(Update update, String tabl) {
		return new DASemantext(tabl);
	}

	///////////////////////////////////////////////////////////////////////////
	// auto ID
	///////////////////////////////////////////////////////////////////////////
	/**Generate new Id with the help of db function f_incSeq(varchar idName)<br>
	 * Sql script for stored function:<br>
	 * Mysql:<pre>
create FUNCTION f_incSeq2 (seqId varchar(100), prefix varchar(4)) RETURNS int(11)
begin
	DECLARE seqName varchar(100);
	DECLARE cnt INT DEFAULT 0;
	
	if prefix = '' then set seqName = seqId;
	else set seqName = concat(seqId, '.', prefix);
	end if;
	
	select count(seq) into cnt from ir_autoSeqs where sid = seqName;

	if cnt = 0
	then
		insert into ir_autoSeqs(sid, seq, remarks) values (seqName, 0, now());
	end if;
	
	select seq into cnt from ir_autoSeqs where sid = seqName;
	update ir_autoSeqs set seq = cnt + 1 where sid = seqName;
	return cnt;
end;</pre>
	 * select f_incSeq2('%s.%s', '%s') newId<br>
	 * Oracle:<pre>
CREATE OR REPLACE FUNCTION GZDX_YJPT.f_incSeq2(seqId in varchar, prefix in varchar) RETURN integer
IS
	PRAGMA AUTONOMOUS_TRANSACTION;
	seqName varchar(100);
	cnt integer DEFAULT 0;
begin
	if prefix = '' then seqName := seqId;
	else seqName := concat(concat(seqId, '.'), prefix);
	end if;
	
	select count(seq) into cnt from ir_autoSeqs where sid = seqName;

	if cnt = 0
	then
		insert into ir_autoSeqs(sid, seq, remarks) values (seqName, 0, to_char(sysdate, 'MM-DD-YYYY HH24:MI:SS'));
		commit;
	end if;
	
	select seq into cnt from ir_autoSeqs where sid = seqName;
	update ir_autoSeqs set seq = cnt + 1, remarks = to_char(sysdate, 'MM-DD-YYYY HH24:MI:SS') where sid = seqName;
	commit;
	return cnt;
end;
	 </pre>
	 * select f_incSeq2('%s.%s', '%s') newId from dual
	 * @param connId
	 * @param target target table
	 * @param idField table id column (no multi-column id supported)
	 * @param jdbc using one of jdbc_dbcp or jdbc_drvmnger, defult jdbc_dbcp
	 * @return new Id (shortened in radix 64 by {@link com.infochange.frame.util.Radix64})
	 * @throws SQLException
	 * @throws TransException 
	 */
	public static String genId(String connId, String target, String idField, String subCate) throws SQLException, TransException {
		JDBCType dt = Connects.driverType(connId);
		if (dt == JDBCType.sqlite)
			return genSqliteId(connId, target, idField);

		if (subCate == null) subCate = "";
		String sql;
		if (dt == JDBCType.oracle)
			sql = String.format("select f_incSeq2('%s.%s', '%s') newId from dual", target, idField, subCate);
		else
			sql = String.format("select f_incSeq2('%s.%s', '%s') newId", target, idField, subCate);

		SResultset rs = null;
		rs = Connects.select(connId, sql);

		rs.beforeFirst().next();
		int newInt = rs.getInt("newId");
		
		if (subCate.equals(""))
			return Radix64.toString(newInt);
		else
			return String.format("%1$s_%2$6s", subCate, Radix64.toString(newInt));
	}
	
	static DbLogDumb dlog = new DbLogDumb();

	static String genSqliteId(String conn, String target, String idF) throws SQLException, TransException { 
		Lock lock;
		lock = getAutoseqLock(conn, target);

		// TODO insert initial value in ir_autoseq
		// 1. update ir_autoseq (seq) set seq = seq + 1 where sid = tabl.idf
		// 2. select seq from ir_autoseq where sid = tabl.id

		ArrayList<String> sqls = new ArrayList<String>();
		sqls.add(String.format("update ir_autoseq set seq = seq + 1 where sid = '%s.%s'",
					target, idF));
			
		String select = String.format("select seq from ir_autoseq where sid = '%s.%s'",
					target, idF);

		SResultset rs = null;
		
		// each table has a lock.
		// lock to prevent concurrency.
		lock.lock();
		try {
			// for efficiency
			Connects.commit(dlog, sqls, Connects.flag_nothing);

			// rs = Connects.select(conn, select, Connects.flag_nothing);
			rs = (SResultset) rawst.select("ir_autoseq").col("seq")
					.where("=", "sid", String.format("%s.%s", target, idF))
					.rs(null);
		} finally { lock.unlock();}
		rs.beforeFirst().next();

		return Radix64.toString(rs.getInt("seq"));
	}

	static Lock lockOflocks = new ReentrantLock();
	/** [conn-id, [table, lock]]] */
	private static HashMap<String, HashMap<String, Lock>> locks;
	private static Lock getAutoseqLock(String conn, String tabl) {
		lockOflocks.lock();
		Lock l = null;
		try {
			if (locks == null)
				locks = new HashMap<String, HashMap<String, Lock>>();
			if (!locks.containsKey(conn))
				locks.put(conn, new HashMap<String, Lock>());
			if (!locks.get(conn).containsKey(tabl))
				locks.get(conn).put(tabl, new ReentrantLock());
			l = locks.get(conn).get(tabl);
		} finally { lockOflocks.unlock(); }
		return l;
	}
	
}
