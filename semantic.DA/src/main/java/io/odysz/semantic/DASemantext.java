package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import io.odysz.common.Radix64;
import io.odysz.common.Regex;
import io.odysz.common.Utils;
import io.odysz.common.dbtype;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.x.TransException;

/**A basic semantic context for generating sql.
 * Handling semantics defined in xml file. See path of constructor.
 * @author odys-z@github.com
 */
public class DASemantext implements ISemantext {

	private SemanticObject autoVals;
	// private static DATranscxt rawst = new DATranscxt();
	private static Transcxt rawst;

	/**Semantic Configurations */
	private HashMap<String, DASemantics> ss;
	private IUser usr;
	private String connId;
	private Regex refReg;

	/**Initialize a context for semantics handling.
	 * This class handling semantics comes form path, usually an xml like test/res/semantics.xml.
	 */
	public DASemantext(String connId, HashMap<String, DASemantics> smtcfg, IUser usr) {
		this.connId = connId;
		ss = smtcfg;
		rawst = new Transcxt(null);
		
		refReg = new Regex(ISemantext.refPattern);
		
		this.usr = usr;
	}

	/**When inserting, replace inserting values in 'AUTO' columns, e.g. generate auto PK for rec-id.
	 * @see io.odysz.semantics.ISemantext#onInsert(io.odysz.transact.sql.Insert, java.lang.String, java.util.List)
	 */
	@Override
	public ISemantext onInsert(Insert insert, String tabl, List<ArrayList<Object[]>> valuesNv) {
		if (valuesNv != null && ss != null)
			for (ArrayList<Object[]> value : valuesNv) {
				Map<String, Integer> cols = insert.getColumns();
				DASemantics s = ss.get(tabl);
				if (s != null)
					s.onInsert(this, value, cols, usr);
			}
		return this;
	}

	@Override
	public ISemantext onUpdate(Update update, String tabl, ArrayList<Object[]> nvs) {
		if (nvs != null && ss != null)
			for (Object[] nv : nvs)
				if (nv != null && nv.length > 0 && "AUTO".equals(nv[1])) // FIXME bug: use ISemantic Regex.
					// resolve AUTO value
					nv[1] = autoVals != null && autoVals.has(tabl)
							? ((SemanticObject)autoVals.get(tabl)).get((String)nv[0])
							: nv[1];
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
		newInst = new DASemantext(connId, null, usr != null && usr.length > 0 ? usr[0] : null);
		newInst.ss = srctx.ss;
		newInst.usr = usr != null && usr.length > 0 ? usr[0] : null;
		return newInst;
	}

	/**Find resolved value in results.
	 * @param table
	 * @param col
	 * @return RESULt resoLVED VALue in tabl.col
	 */
	@Override
	public Object resulvedVal(String tabl, String col) {
		return ((SemanticObject) autoVals.get(tabl)).get(col);
	}
	
	@Override
	public Object resulvedVal(String ref) {
		if (autoVals == null) {
			Utils.warn("Value reference can not resolved: %s. autoVals is null", ref);
			return ref;
		}
		ArrayList<String> grps = refReg.findGroups(ref);
		if (grps == null || grps.size() != 2) {
			Utils.warn("Value reference can not resolved: %s. Pattern is incorrect.", ref);
			return ref;
		}
		String tabl = grps.get(0);
		return ((SemanticObject)autoVals.get(tabl)).get(grps.get(1));
	}

	///////////////////////////////////////////////////////////////////////////
	// auto ID
	///////////////////////////////////////////////////////////////////////////
	@Override
	public String genId(String tabl, String col) throws SQLException, TransException {
		String newv = genId(connId, tabl, col, null);

		if (autoVals == null)
			autoVals = new SemanticObject();
		SemanticObject tabl_ids = (SemanticObject) autoVals.get(tabl);
		if (tabl_ids == null) {
			tabl_ids = new SemanticObject();
			autoVals.put(tabl, tabl_ids);
		}
		tabl_ids.put(col, newv);
		return newv;
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
	 * @param subCate
	 * @return new Id (shortened in radix 64 by {@link com.infochange.frame.util.Radix64})
	 * @throws SQLException
	 * @throws TransException
	 */
	public String genId(String connId, String target, String idField, String subCate) throws SQLException, TransException {
		dbtype dt = Connects.driverType(connId);
		if (dt == dbtype.sqlite)
			return genSqliteId(connId, target, idField);

		if (subCate == null) subCate = "";
		String sql;
		if (dt == dbtype.oracle)
			sql = String.format("select f_incSeq2('%s.%s', '%s') newId from dual", target, idField, subCate);
		else
			// bugs here
			sql = String.format("select f_incSeq2('%s.%s', '%s') newId", target, idField, subCate);

		SResultset rs = null;
		rs = Connects.select(connId, sql);
		if (rs.getRowCount() <= 0)
			// throw new TransException("Can't find auot seq of %s, you may check where oz_autoseq.seq and table %s are existing?",
			throw new TransException("Can't find auot seq of %1$s.\nFor performantc reason, DASemantext assumes a record in oz_autoseq.seq (id='%1$s.%2$s') exists.\nMay be you would check where oz_autoseq.seq and table %2$s are existing?",
					idField, target);

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
	 * @return new Id
	 * @throws SQLException
	 * @throws TransException
	 */
	String genSqliteId(String conn, String target, String idF) throws SQLException, TransException { 
		Lock lock;
		lock = getAutoseqLock(conn, target);

		// 1. update ir_autoseq (seq) set seq = seq + 1 where sid = tabl.idf
		// 2. select seq from ir_autoseq where sid = tabl.id

		ArrayList<String> sqls = new ArrayList<String>();
		sqls.add(String.format("update oz_autoseq set seq = seq + 1 where sid = '%s.%s'",
					target, idF));
			
//		String select = String.format("select seq from oz_autoseq where sid = '%s.%s'", target, idF);
		SResultset rs = null;
		
		Query q = rawst.select("oz_autoseq").col("seq")
				.where("=", "sid", String.format("'%s.%s'", target, idF));

		// each table has a lock.
		// lock to prevent concurrency.
		lock.lock();
		try {
			// for efficiency
			Connects.commit(null, sqls, Connects.flag_nothing);

			// don't usr rs(), there is no postOp initialized in rawst, it's only for basice operation - genId() is a basic operation
			//.rs(rawst.basiContext());

			rs = Connects.select(conn, q.sql(rawst.basiContext()), Connects.flag_nothing);
		} finally { lock.unlock();}
		
		if (rs.getRowCount() <= 0)
			throw new TransException("Can't find auot seq of %1$s.\nFor performantc reason, DASemantext assumes a record in oz_autoseq.seq (id='%1$s.%2$s') exists.\nMay be you would check where oz_autoseq.seq and table %2$s are existing?",
					idF, target);
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

	@Override
	public Stream<String> pagingStream(Stream<String> s, int pageIx, int pgSize) throws TransException {
		dbtype dt = Connects.driverType(connId);
		return pagingStream(dt, s, pageIx, pgSize);
	}

	public static Stream<String> pagingStream(dbtype dt, Stream<String> s, int pageIx, int pgSize) throws TransException {
		int r1 = pageIx * pgSize;
		int r2 = r1 + pgSize;
		if (dt == dbtype.oracle)
			// "select * from (select t.*, rownum r_n_ from (%s) t WHERE rownum <= %s  order by rownum) t where r_n_ > %s"
			return Stream.concat(Stream.concat(
						Stream.of("select * from (select t.*, rownum r_n_ from ("), s),
						Stream.of(String.format(") t WHERE rownum <= %s  order by rownum) t where r_n_ > %s", r1, r2)));
		else if (dt == dbtype.ms2k)
			// "select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (%s) t) t where rownum >= %s and rownum <= %s"
			return Stream.concat(Stream.concat(
						Stream.of("select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from ("), s),
						Stream.of(String.format(") t) t where rownum >= %s and rownum <= %s", r1, r2)));
		else if (dt == dbtype.sqlite)
			throw new TransException("There is no easy way to support sqlite paging. Don't using server side paging for sqlite datasource."); 
		else // mysql
			// "select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (%s) t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s"
			return Stream.concat(Stream.concat(
						Stream.of("select * from (select t.*, @ic_num := @ic_num + 1 as rnum from ("), s),
						Stream.of(String.format(") t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s", r1, r2)));
	}

	@Override
	public ISemantext clone(IUser usr) {
		return new DASemantext(connId, ss, usr);
	}
}
