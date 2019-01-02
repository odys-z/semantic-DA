package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.xml.sax.SAXException;

import io.odysz.common.dbtype;
import io.odysz.common.Radix64;
import io.odysz.common.Utils;
import io.odysz.module.rs.SResultset;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.DATranscxt;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Update;
import io.odysz.transact.x.TransException;

public class DASemantext implements ISemantext {

	private HashMap<Object, Object> autoVals;
//	private Statement<?> callerStatement;
	private static DATranscxt rawst = new DATranscxt(null);

	private SemanticObject resolvedIds;

	private HashMap<String, DASemantics> ss;
	private IUser usr;
	private String connId;

	public DASemantext(String connId, String path) throws SemanticException, SAXException, IOException {
		this.connId = connId;
		if (path != null && ss == null)
			ss = init(path);
		else ss = null;
	}

	static HashMap<String,DASemantics> init(String filepath) throws SAXException, IOException {
		HashMap<String, DASemantics> ss = new HashMap<String, DASemantics>();
		LinkedHashMap<String,XMLTable> xtabs = XMLDataFactoryEx.getXtables(
				new Log4jWrapper("").setDebugMode(false), filepath, new IXMLStruct() {
						@Override public String rootTag() { return "semantics"; }
						@Override public String tableTag() { return "t"; }
						@Override public String recordTag() { return "s"; }});

		XMLTable conn = xtabs.get("semantics");
		conn.beforeFirst();
		while (conn.next()) {
			String tabl = conn.getString("tabl");
			DASemantics s = ss.get(tabl);
			if (s == null) {
				s = new DASemantics(tabl, conn.getString("pk"));
				ss.put(tabl, s);
			}
			try {s.addHandler(conn.getString("smtc"), conn.getString("args")); }
			catch (SemanticException e) {
				// some configuration error
				// continue
				Utils.warn(e.getMessage());
			}
		}
		return ss;
	}


	/**When inserting, replace inserting values in 'AUTO' columns, e.g. generate auto PK for rec-id.
	 * @see io.odysz.semantics.ISemantext#onInsert(io.odysz.transact.sql.Insert, java.lang.String, java.util.List)
	 */
	@Override
	public ISemantext onInsert(Insert insert, String tabl, List<ArrayList<Object[]>> valuesNv) {
//		callerStatement = insert;
		if (valuesNv != null)
			for (ArrayList<Object[]> value : valuesNv) {
				Map<String, Integer> cols = insert.getColumns();
				// replace AUTO
				try {
					replaceAuto(value, tabl, cols);
				} catch (SQLException | TransException e) {
					e.printStackTrace();
				}
				// handle semantics
				DASemantics s = ss.get(tabl);
				if (s != null)
					s.onInsert(value, cols, usr);
			}
		return this;
	}
	

	private void replaceAuto(ArrayList<Object[]> nvs, String tabl, Map<String, Integer> cols) throws SQLException, TransException {
		if (nvs != null) {
			for (String col : cols.keySet()) {
				Integer c = cols.get(col);
				Object[] nv = nvs.get(c);
				if (nv != null && nv.length > 1
					&& nv[1] instanceof String && "AUTO".equals(nv[1])) {
					nv[1] = genId(tabl, col);
					
					// set results
					if (resolvedIds == null)
						resolvedIds = new SemanticObject();
					resolvedIds.put(tabl, new SemanticObject());
					((SemanticObject) resolvedIds.get(tabl)).add("new-ids", nv[1]);
				}
			}
		}
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
	public ISemantext insert(Insert insert, String tabl, IUser... usr) {
		return clone(this, usr);
	}

	@Override
	public ISemantext update(Update update, String tabl, IUser... usr) {
		return clone(this, usr);
	}

	@Override
	public dbtype dbtype() {
		return Connects.driverType(connId);
	}

	private ISemantext clone(DASemantext srctx, IUser... usr) {
		DASemantext newInst;
		try {
			newInst = new DASemantext(connId, null);
			newInst.ss = srctx.ss;
			newInst.usr = usr != null && usr.length > 0 ? usr[0] : null;
			return newInst;
		} catch (IOException | SemanticException | SAXException e) {
			e.printStackTrace();
		}
		return this;
	}

	@Override
	public SemanticObject results() {
		return resolvedIds;
	}
	///////////////////////////////////////////////////////////////////////////
	// auto ID
	///////////////////////////////////////////////////////////////////////////
	public static String genId(String tabl, String col) throws SQLException, TransException {
		return genId(Connects.defltConn(), tabl, col, null);
	}

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
	 * <p>auto ID for sqlite is handled by {@link #genSqliteId(String, String, String)} - needing table initialization.</p> 
	 * @param connId
	 * @param target target table
	 * @param idField table id column (no multi-column id supported)
	 * @param jdbc using one of jdbc_dbcp or jdbc_drvmnger, defult jdbc_dbcp
	 * @return new Id (shortened in radix 64 by {@link com.infochange.frame.util.Radix64})
	 * @throws SQLException
	 * @throws TransException 
	 */
	public static String genId(String connId, String target, String idField, String subCate) throws SQLException, TransException {
		dbtype dt = Connects.driverType(connId);
		if (dt == dbtype.sqlite)
			return genSqliteId(connId, target, idField);

		if (subCate == null) subCate = "";
		String sql;
		if (dt == dbtype.oracle)
			sql = String.format("select f_incSeq2('%s.%s', '%s') newId from dual", target, idField, subCate);
		else
			sql = String.format("select f_incSeq2('%s.%s', '%s') newId", target, idField, subCate);

		SResultset rs = null;
		rs = Connects.select(connId, sql);

		rs.beforeFirst().next();
		int newInt = rs.getInt("newId");
		
		if (subCate == null || subCate.equals(""))
			return Radix64.toString(newInt);
		else
			return String.format("%1$s_%2$6s", subCate, Radix64.toString(newInt));
	}
	
	/**Generate auto id in sqlite.<br>
	 * All auto ids are recorded in oz_autoseq table.<br>
	 * See {@link DASemantextTest} for how to initialize oz_autoseq.
	 * @param conn
	 * @param target
	 * @param idF
	 * @return
	 * @throws SQLException
	 * @throws TransException
	 */
	static String genSqliteId(String conn, String target, String idF) throws SQLException, TransException { 
		Lock lock;
		lock = getAutoseqLock(conn, target);

		// 1. update ir_autoseq (seq) set seq = seq + 1 where sid = tabl.idf
		// 2. select seq from ir_autoseq where sid = tabl.id

		ArrayList<String> sqls = new ArrayList<String>();
		sqls.add(String.format("update oz_autoseq set seq = seq + 1 where sid = '%s.%s'",
					target, idF));
			
//		String select = String.format("select seq from oz_autoseq where sid = '%s.%s'", target, idF);

		SResultset rs = null;
		
		// each table has a lock.
		// lock to prevent concurrency.
		lock.lock();
		try {
			// for efficiency
			Connects.commit(null, sqls, Connects.flag_nothing);

			// rs = Connects.select(conn, select, Connects.flag_nothing);
			rs = (SResultset) rawst.select("oz_autoseq").col("seq")
					.where("=", "sid", String.format("'%s.%s'", target, idF))
					.rs();
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
