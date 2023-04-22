package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io_odysz.FilenameUtils;

import io.odysz.common.Configs;
import io.odysz.common.Configs.keys;
import io.odysz.common.Radix32;
import io.odysz.common.Radix64;
import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
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
import io.odysz.transact.sql.Statement.IPostOptn;
import io.odysz.transact.sql.Statement.IPostSelectOptn;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.AbsPart;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.x.TransException;

/**A basic semantic context for generating sql.
 * Handling semantics defined in runtime-root/semantics.xml file.
 *
 * <p>For example, {@link #pageSql(String, int, int)} is an example that must
 * handled by context, but not interested by semantic.jserv. When composing SQL
 * like select statement, if the results needing to be paged at server side,
 * the paging sql statement is different for different DB. But semantic-transact
 * don't care DB type or JDBC connection, so it's the context that will handling
 * this.
 * 
 * See the {@link Connects#pagingSql(dbtype, String, long, long)}.</p>
 *
 * @author odys-z@github.com
 */
public class DASemantext implements ISemantext {

	private SemanticObject autoVals;
	private static Transcxt rawst;

	/**Semantic Configurations */
	protected HashMap<String, DASemantics> ss;
	protected HashMap<String, TableMeta> metas;

	private IUser usr;
	protected String connId;

	protected String basePath;
	protected ArrayList<IPostOptn> onRowsOk;
	protected LinkedHashMap<String,IPostSelectOptn> onSelecteds;

	protected LinkedHashMap<String, IPostOptn> onTableOk;

	/**for generating sqlite auto seq */
	protected static IUser sqliteDumyUser;

	/**Initialize a context for semantics handling.
	 * This class handling semantics comes form path, usually an xml like test/res/semantics.xml.
	 * @param connId
	 * @param smtcfg semantic configs, usally load by {@link io.odysz.semantic.DATranscxt}.
	 * <p>sample code: </p>
	 * DATranscxt.initConfigs("inet", rootINF + "/semantics.xml");
	 * @param usr
	 * @param rtPath runtime root path, for docker layer, it's typically the volume folder. 
	 * @throws SemanticException metas is null
	 */
	protected DASemantext(String connId, HashMap<String, DASemantics> smtcfg,
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
	public ISemantext connId(String conn) {
		connId = conn;
		return this;
	}

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
			return newInst;
		} catch (SemanticException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}

	/**Find resolved value in results.
	 * @param tabl
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
	 * a.k.a return value of {@link Update#doneOp(io.odysz.transact.sql.Statement.IPostOptn)}.
	 * @return {@link #autoVals}
	 * @see io.odysz.semantics.ISemantext#resulves()
	 */
	public SemanticObject resulves() {
		return autoVals;
	}
	
	public ISemantext reset() {
		if (autoVals != null)
			autoVals.clear();
		// FIXME no this.clear()?
		return this;
	}

	///////////////////////////////////////////////////////////////////////////
	// auto ID
	///////////////////////////////////////////////////////////////////////////
	@Override
	public String genId(String tabl, String col) throws SQLException, TransException {
		String newv = genId(tabl, col, null);

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
	 * @param target target table
	 * @param idField table id column (no multi-column id supported)
	 * @param subCate
	 * @return new Id (shortened in radix 64 by {@link Radix64})
	 * @throws SQLException
	 * @throws TransException
	 */
	public static String genId(String target, String idField, String subCate) throws SQLException, TransException {
		// String connId = ""; 
		dbtype dt = Connects.driverType(null);
		if (dt == dbtype.sqlite)
			return genSqliteId(Connects.defltConn(), target, idField);

		if (subCate == null) subCate = "";
		String sql;
		if (dt == dbtype.oracle)
			sql = String.format("select \"oz_fIncSeq\"('%s.%s', '%s') newId from dual", target, idField, subCate);
		else // mysql, etc.
			sql = String.format("select oz_fIncSeq('%s.%s', '%s') newId", target, idField, subCate);

		AnResultset rs = null;
		// v1.3.0: user sys default conn to generate log-id
		rs = Connects.select(Connects.defltConn(), sql);
		if (rs.getRowCount() <= 0)
			// throw new TransException("Can't find auot seq of %s, you may check where oz_autoseq.seq and table %s are existing?",
			throw new TransException("Can't find auot seq of %1$s.\nFor performantc reason, DASemantext assumes a record in oz_autoseq.seq (id='%1$s.%2$s') exists.\nMay be you would check where oz_autoseq.seq and table %2$s are existing?",
					idField, target);

		rs.beforeFirst().next();
		long newInt = rs.getLong("newId");

		if (subCate == null || subCate.equals(""))
			// return Radix64.toString(newInt);
			return radix64_32(newInt);
		else
			return String.format("%1$s_%2$6s", subCate,
					// Radix64.toString(newInt));
					radix64_32(newInt));
	}

	public static int file_sys = 0; 
	/**Try generate a radix 64 string of v.
	 * String length is controlled by connfigs.xml/k=db-len, overriding default 8 for windows, or 6 for others.
	 * If configs.xml/k=filesys is "windows", then generate a radix 32 string.
	 * @param v
	 * @return radix 64/32
	 */
	public static String radix64_32(long v) {
		if (file_sys == 0)
			file_sys = "windows".equals(Configs.getCfg(keys.fileSys)) ? 1 : 2;
		if (file_sys == 2)
			return Radix64.toString(v, Configs.getInt(keys.idLen, 6));
		else
			return Radix32.toString(v, Configs.getInt(keys.idLen, 8));
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
	static String genSqliteId(String conn, String target, String idF) throws SQLException, TransException {
		Lock lock;
		lock = getAutoseqLock(conn, target);

		// 1. update ir_autoseq (seq) set seq = seq + 1 where sid = tabl.idf
		// 2. select seq from ir_autoseq where sid = tabl.id

		ArrayList<String> sqls = new ArrayList<String>();
		sqls.add(String.format("update oz_autoseq set seq = seq + 1 where sid = '%s.%s'",
					target, idF));

		AnResultset rs = null;

		Query q = rawst.select("oz_autoseq").col("seq")
				.where("=", "sid", String.format("'%s.%s'", target, idF));

		// dumy user for update oz_autoseq
		if (sqliteDumyUser == null)
			sqliteDumyUser = new IUser() {
				@Override public TableMeta meta(String ... connId) { return null; }
				@Override public String uid() { return "sqlite-dumy"; }
				@Override public IUser logAct(String funcName, String funcId) { return null; }
				@Override public IUser notify(Object note) throws TransException { return this; }
				@Override public List<Object> notifies() { return null; }
				@Override public long touchedMs() { return 0; }
				@Override public IUser sessionKey(String ssId) { return this; }
				@Override public String sessionKey() { return null; } };

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

		return radix64_32(rs.getLong("seq"));
					
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
		return Connects.pagingSql(connId, rawSql, page, size);
	}

	/**Wrap sql only for rows in a page, in stream mode.
	 * @param dt
	 * @param sql
	 * @param pageIx
	 * @param pgSize
	 * @return pagination wrapped sql
	 * @throws TransException
	public static String pagingSql(dbtype dt, String sql, long pageIx, long pgSize) throws TransException {
		if (pageIx < 0 || pgSize <= 0)
			return sql;
		long i1 = pageIx * pgSize;
		String r2 = String.valueOf(i1 + pgSize);
		String r1 = String.valueOf(i1);
		Stream<String> s;
		if (dt == dbtype.oracle)
			// "select * from (select t.*, rownum r_n_ from (%s) t WHERE rownum <= %s  order by rownum) t where r_n_ > %s"
			s = Stream.of("select * from (select t.*, rownum r_n_ from (", sql,
						") t order by rownum) t where r_n_ > ", r1, " and r_n_ <= ", r2);
		else if (dt == dbtype.ms2k)
			// "select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (%s) t) t where rownum >= %s and rownum <= %s"
			s = Stream.of("select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (", sql,
						") t) t where rownum > ", r1, " and rownum <= %s", r2);
						// v1.3.0 Sep.6 2021 ">=" to ">"
		else if (dt == dbtype.sqlite)
			// throw new TransException("There is no easy way to support sqlite paging. Don't use server side paging for sqlite datasource.");

			// https://stackoverflow.com/a/51380906
			s = Stream.of("select * from (", sql, ") limit ", String.valueOf(pgSize), " offset ", r1);
		else // mysql
			// "select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (%s) t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s"
			s = Stream.of("select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (", sql,
						") t, (select @ic_num := 0) ic_t) t1 where rnum > ", r1, " and rnum <= ", r2);

		return s.collect(Collectors.joining(" "));
	}
	@deprecated replaced by {@link Connects#pagingSql(string, String, int, int)}
	 */
	public static String pagingSql(dbtype dt, String sql, int pageIx, int pgSize) throws TransException {
		return Connects.pagingSql(dt, sql, pageIx, pgSize);
	}

	/**
	 * @param dt
	 * @param sql
	 * @return SQL: select count(*) as total from [sql]
	 * @throws TransException
	 */
	public static String totalSql(dbtype dt, String sql) throws TransException {
		return Stream.of("select count(*) as total from (", sql)
				.collect(Collectors.joining("", "", ") s_jt"));
	}

	/** @deprecated */
	public void clear() {
		autoVals = null;
		if (onTableOk != null)
			onTableOk.clear();
		if (onRowsOk != null)
			onRowsOk.clear();
		if (onSelecteds != null)
			onSelecteds.clear();
	}

	@Override
	public TableMeta tablType(String tabl) {
		return metas.get(tabl);
	}

	@Override
	public String relativpath(String... sub) throws TransException {
		return FilenameUtils.concat(".", sub);
	}

	@Override
	public String containerRoot() { return basePath; }

	@Override
	public void onCommitted(ISemantext ctx, String tabl) throws TransException, SQLException {
		if (onRowsOk != null)
			for (IPostOptn ok : onRowsOk)
				// onOk handlers shoudn't using sqls, it's already committed
				ok.onCommitOk(ctx, null);

		if (onTableOk != null && onTableOk.containsKey(tabl))
			onTableOk.get(tabl).onCommitOk(ctx, null);
	}

	@Override
	public void addOnRowsCommitted(IPostOptn op) {
		if (onRowsOk == null)
			onRowsOk = new ArrayList<IPostOptn>();
		onRowsOk.add(op);
	}
	
	@Override
	public void addOnTableCommitted(String tabl, IPostOptn op) {
		if (onTableOk == null)
			onTableOk = new LinkedHashMap<String, IPostOptn>();
		
		onTableOk.put(tabl, op);
	}

	@Override
	public IPostOptn onTableCommittedHandler(String tabl) {
		return onTableOk == null ? null : onTableOk.get(tabl);
	}

	@Override
	public boolean hasOnSelectedHandler(String name) {
		return onSelecteds != null && onSelecteds.containsKey(name);
	}

	@Override
	public void onSelected(Object resultset) throws SQLException, TransException {
		AnResultset rs = (AnResultset) resultset;
		if (rs != null && onSelecteds != null && onSelecteds.size() > 0) {
			rs.beforeFirst();
			while (rs.next())
				for (IPostSelectOptn op : onSelecteds.values())
					op.onSelected(this, rs.getRowCells(), rs.getColnames());
			rs.beforeFirst(); // makes caller less error prone
		}
	}

	@Override
	public void addOnSelectedHandler(String name, IPostSelectOptn op) {
		if (onSelecteds == null)
			onSelecteds = new LinkedHashMap<String, IPostSelectOptn>();
		onSelecteds.put(name, op);
	}

	@Override
	public AbsPart composeVal(Object v, String tabl, String col) {
		if (v instanceof AbsPart)
			return (AbsPart) v;

		TableMeta mt = tablType(tabl);
		return Statement.composeVal(v, mt, col);
	}

	@Override
	public TableMeta getTableMeta(String tbl) {
		return metas.get(tbl);
	}
}
