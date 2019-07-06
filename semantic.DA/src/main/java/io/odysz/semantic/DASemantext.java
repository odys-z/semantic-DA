package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FilenameUtils;

import io.odysz.common.Radix64;
import io.odysz.common.dbtype;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Statement.IPostOperat;
import io.odysz.transact.sql.Statement.IPostSelectOperat;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.AbsPart;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.x.TransException;

/**A basic semantic context for generating sql.
 * Handling semantics defined in xml file. See path of constructor.
 * 
 * <p>{@link #pageSql(String, int, int)} is an example that must handled by context, but not interested by semantic.jserv.
 * When composing SQL like select statement, if the results needing to be paged at server side,
 * the paging sql statement is different for different DB.
 * But semantic-transact don't care DB type or JDBC connection, so it's the context that will handling this.
 * See the {@link #pageSql(Stream, int, int)}.</p>
 * 
 * @author odys-z@github.com
 */
public class DASemantext implements ISemantext {

	private SemanticObject autoVals;
	private static Transcxt rawst;

	/**Semantic Configurations */
	private HashMap<String, DASemantics> ss;
	private HashMap<String, TableMeta> metas;

	private IUser usr;
	private String connId;

	private String basePath;
	private ArrayList<IPostOperat> onOks;
	private ArrayList<IPostSelectOperat> onSelecteds;
	
	/**for generating sqlite auto seq */
	private static IUser sqliteDumyUser;

	/**Initialize a context for semantics handling.
	 * This class handling semantics comes form path, usually an xml like test/res/semantics.xml.
	 * @param connId
	 * @param smtcfg semantic configs, usally load by {@link io.odysz.semantic.DATranscxt}.
	 * <p>sample code: </p>
	 * DATranscxt.initConfigs("inet", rootINF + "/semantics.xml");
	 * @param usr
	 * @param rtPath runtime root path
	 * @throws SemanticException metas is null 
	 */
	DASemantext(String connId, HashMap<String, DASemantics> smtcfg,
			HashMap<String, TableMeta> metas, IUser usr, String rtPath) throws SemanticException {
		basePath = rtPath;
		this.connId = connId;
		ss = smtcfg;
		if (metas == null)
			throw new SemanticException("DASemantext can not work without DB metas. connId: %s", connId);
		this.metas = metas;
		if (rawst == null) {
			rawst = new Transcxt(null);
		}
		
		this.usr = usr;
	}

	/**When inserting, process data row with configured semantics, like auto-pk, fk-ins, etc..
	 * @throws SemanticException 
	 * @see io.odysz.semantics.ISemantext#onInsert(io.odysz.transact.sql.Insert, java.lang.String, java.util.List)
	 */
	@Override
	public ISemantext onInsert(Insert insert, String tabl, List<ArrayList<Object[]>> rows) throws SemanticException {
		if (rows != null && ss != null)
			// second round
			for (ArrayList<Object[]> row : rows) {
				Map<String, Integer> cols = insert.getColumns();
				DASemantics s = ss.get(tabl);
				if (s == null)
					continue;
				s.onInsert(this, insert, row, cols, usr);
			}
		return this;
	}

	@Override
	public ISemantext onUpdate(Update update, String tabl, ArrayList<Object[]> nvs) throws SemanticException {
		if (nvs != null && ss != null) {
			Map<String, Integer> cols = update.getColumns();
			DASemantics s = ss.get(tabl);
			if (s != null)
				s.onUpdate(this, update, nvs, cols, usr);
		}
		return this;
	}

	@Override
	public ISemantext onDelete(Delete delete, String tabl, Condit whereCondt) throws TransException {
		if (ss != null) {
			DASemantics s = ss.get(tabl);
			if (s != null)
				s.onDelete(this, delete, whereCondt, usr);
		}
		return this;
	}

	@Override
	public ISemantext onPost(Statement<?> stmt, String tabl, ArrayList<Object[]> row, ArrayList<String> sqls) throws TransException {
		if (row != null && ss != null) {
			Map<String, Integer> cols = stmt.getColumns();
			if (cols == null)
				return this;
			DASemantics s = ss.get(tabl);
			if (s != null)
				s.onPost(this, stmt, row, cols, usr, sqls);
		}
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

	@Override
	public String connId() { return connId; }

	@Override
	public ISemantext clone(IUser usr) {
		try {
			return new DASemantext(connId, ss, metas, usr, basePath);
		} catch (SemanticException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}

	private ISemantext clone(DASemantext srctx, IUser... usr) {
		try {
			DASemantext newInst = new DASemantext(connId,
					srctx.ss, srctx.metas, usr != null && usr.length > 0 ? usr[0] : null, basePath);
			// newInst.ss = srctx.ss;
			// newInst.usr = usr != null && usr.length > 0 ? usr[0] : null;
			return newInst;
		} catch (SemanticException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}

	/**Find resolved value in results.
	 * @param table
	 * @param col
	 * @return RESULt resoLVED VALue in tabl.col, or null if not exists.
	 */
	@Override
	public Object resulvedVal(String tabl, String col) {
		return autoVals != null && autoVals.has(tabl) ?
				((SemanticObject) autoVals.get(tabl)).get(col)
				: null;
	}
	
	/**Get the resolved value in {@link #autoVals}
	 * a.k.a return value of {@link Update#doneOp(io.odysz.transact.sql.Statement.IPostOperat)}.
	 * @return {@link #autoVals}
	 * @see io.odysz.semantics.ISemantext#resulves()
	 */
	public SemanticObject resulves() {
		return autoVals;
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
			
		SResultset rs = null;
		
		Query q = rawst.select("oz_autoseq").col("seq")
				.where("=", "sid", String.format("'%s.%s'", target, idF));

		// dumy user for update oz_autoseq
		if (sqliteDumyUser == null)
			sqliteDumyUser = new IUser() {
				@Override public TableMeta meta() { return null; }
				@Override public String uid() { return "sqlite-dumy"; }
				@Override public IUser logAct(String funcName, String funcId) { return null; }
				@Override public String sessionKey() { return null; }
				@Override public IUser sessionKey(String skey) { return null; }
				@Override public IUser notify(Object note) throws TransException { return null; }
				@Override public List<Object> notifies() { return null; } };

		// each table has a lock.
		// lock to prevent concurrency.
		lock.lock();
		try {
			// for efficiency
			Connects.commit(conn, sqliteDumyUser, sqls, Connects.flag_nothing);

			rs = Connects.select(conn, q.sql(null), Connects.flag_nothing);
		} finally { lock.unlock();}
		
		if (rs.getRowCount() <= 0)
			throw new TransException("Can't find auot seq of %1$s.\n" +
					"For performance reason and difficulty of implementing sqlite stored process, DASemantext assumes a record in oz_autoseq.seq (id='%1$s.%2$s') exists.\n" +
					"May be you would check where oz_autoseq.seq and table %2$s are existing?",
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

	public String totalSql(String rawSql) throws TransException {
		return DASemantext.totalSql(Connects.driverType(connId()), rawSql);
	}

	public String pageSql(String rawSql, int page, int size) throws TransException {
		return DASemantext.pagingSql(Connects.driverType(connId()), rawSql, page, size);
	}	

	public static String pagingSql(dbtype dt, String sql, int pageIx, int pgSize) throws TransException {
		if (pageIx < 0 || pgSize <= 0)
			return sql;
		int i1 = pageIx * pgSize;
		String r2 = String.valueOf(i1 + pgSize);
		String r1 = String.valueOf(i1);
		Stream<String> s;
		if (dt == dbtype.oracle)
			// "select * from (select t.*, rownum r_n_ from (%s) t WHERE rownum <= %s  order by rownum) t where r_n_ > %s"
			s = Stream.of("select * from (select t.*, rownum r_n_ from (", sql,
						") t WHERE rownum <= ", r1, " order by rownum) t where r_n_ > ", r2);
		else if (dt == dbtype.ms2k)
			// "select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (%s) t) t where rownum >= %s and rownum <= %s"
			s = Stream.of("select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (", sql,
						") t) t where rownum >= ", r1, " and rownum <= %s", r2);
		else if (dt == dbtype.sqlite)
			throw new TransException("There is no easy way to support sqlite paging. Don't use server side paging for sqlite datasource."); 
		else // mysql
			// "select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (%s) t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s"
			s = Stream.of("select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (", sql,
						") t, (select @ic_num := 0) ic_t) t1 where rnum > ", r1, " and rnum <= ", r2);
	
		return s.collect(Collectors.joining(" "));
	}

	public static String totalSql(dbtype dt, String sql) throws TransException {
		return Stream.of("select count(*) as total from (", sql)
				.collect(Collectors.joining("", "", ") s_jt"));
	}

	public void clear() {
		autoVals = null;
	}
	
	@Override
	public TableMeta colType(String tabl) {
		return metas.get(tabl);
	}

	@Override
	public String relativpath(String... sub) throws TransException {
		return FilenameUtils.concat(".", sub);
	}

	@Override
	public String containerRoot() { return basePath; }

	@Override
	public void onCommitted(ISemantext ctx) throws TransException, SQLException {
		if (onOks != null)
			for (IPostOperat ok : onOks)
				// onOk handlers shoudn't using sqls, it's already committed
				ok.onCommitOk(ctx, null);
	}

	@Override
	public void addOnOkOperate(IPostOperat op) {
		if (onOks == null)
			onOks = new ArrayList<IPostOperat>();
		onOks.add(op);
	}

	@Override
	public void onSelected(Object resultset) throws SQLException, TransException {
		SResultset rs = (SResultset) resultset;
		if (rs != null && onSelecteds != null && onSelecteds.size() > 0) {
			rs.beforeFirst();
			while (rs.next())
				for (IPostSelectOperat op : onSelecteds)
					op.onSelected(this, rs.getRowCells(), rs.getColnames());
		}
	}

	@Override
	public void addOnSelectedHandler(IPostSelectOperat op) {
		if (onSelecteds == null)
			onSelecteds = new ArrayList<IPostSelectOperat>();
		onSelecteds.add(op);
	}

	@Override
	public AbsPart composeVal(Object v, String tabl, String col) {
		if (v instanceof AbsPart)
			return (AbsPart) v;

		TableMeta mt = colType(tabl);
		return Statement.composeVal(v, mt, tabl, col);
	}
}
