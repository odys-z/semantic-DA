package io.odysz.semantic.DA;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.xml.sax.SAXException;

import io.ic.frame.DA.cp.CpDriver;
import io.ic.frame.DA.drvmnger.DmDriver;
import io.ic.frame.xtable.XMLTable;
import io.ic.semantics.IrSingleton;

public class DA {
	public enum DriverType {deflt(99), mysql(0), ms2k(1), oracle(2), sqlite(3), postGIS(4), unknown(90);
		private final int value;
    	private DriverType(int value) { this.value = value; }
    	public int getValue() { return value; }
	};

	public static DriverType parseDrvType(String type) {
		if (type == null || type.trim().length() == 0)
			return DriverType.unknown;
		type = type.trim().toLowerCase();
		if (type.equals("mysql")) 
			return DriverType.mysql;
		else if (type.equals("mssql2k") || type.equals("ms2k"))
			return DriverType.ms2k;
		else if (type.equals("oracle") || type.equals("orcl"))
			return DriverType.oracle;
		else if (type.startsWith("sqlit"))
			return DriverType.sqlite;
		else
			return DriverType.unknown;
	}
	
	/** no special for commit */
	public static final int flag_nothing = 0;
	public static final int flag_printSql = 1;
	public static final int flag_disableSql = 2;

	/** If printSql is true or if asking enable, 
	 * then print sqls.
	 * @param printSql
	 * @param flag
	 * @param sqls
	 */
	public static void printSql(boolean printSql, int flag, ArrayList<String> sqls) {
		if ((flag & DA.flag_printSql) == DA.flag_printSql
			|| printSql && (flag & DA.flag_disableSql) != DA.flag_disableSql)
			print(sqls);
	}

	public static void printSql(boolean printSql, int flag, String sql) {
		if ((flag & DA.flag_printSql) == DA.flag_printSql
			|| printSql && (flag & DA.flag_disableSql) != DA.flag_disableSql)
			System.out.println(sql);
	}

	/////////////////////// stupid bridging /////////////////////////
	public static final int jdbc_dbcp = 1;
	public static final int jdbc_drvmnger = 2;
	static int defltJdbc = jdbc_dbcp;

	public static int defltJdbc() { return defltJdbc; }
	public static void defltJdbc(int jdbc) { defltJdbc = jdbc; }
	
	
	public static String getDefltConnId() {
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.getDefltConnId();
		else
			return DmDriver.defltConn();
	}

	public static DriverType getConnType(String connId) {
		return getConnType(defltJdbc, connId);
	}

	private static DriverType getConnType(int jdbc, String connId) {
		if (jdbc == jdbc_dbcp)
			return CpDriver.getConnType(connId);
		else
			return DmDriver.connType(connId);
	}

	public static DriverType dirverType(String conn) {
		return getConnType(defltJdbc, conn);
	}
	// //////////////////////// // metas // //////////////////////// //
	public static final String rootKey = "infochange-v2";

	static String defltConn;
//	static HashMap<String, DaSrc> srcs;
	static HashMap<String, HashMap<String, IrSemantics>> metas;
	
	public static ArrayList<HashMap<String, String>> getMappings(String connId, String... tabls) throws SAXException {
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.getMappings(connId, tabls);
		else
			return null;
	}

	public static DbSpec getDbSpec(String connId) {
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.getDbSpec(connId);
		else return DmDriver.getDbSpec(connId);
	}
	
	public static DbTable getTable(String connId, String tabName) {
		if (connId == null)
			connId = defltConn;
		if (defltJdbc == jdbc_dbcp)
//			return srcs.get(connId).get(tabName);
			return CpDriver.getTable(connId, tabName);
		else 
			return DmDriver.getTable(connId, tabName);
	}
	
	public static DbColumn getColumn(String connId, String tabName, String expr) {
		if (connId == null)
			connId = defltConn;
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.getColumn(connId, tabName, expr);
		else return DmDriver.getColumn(connId, tabName, expr);
	}

	/////////////////////////////////// semantics ////////////////////////////

	public static void init(ServletContext servletContext) {
		CpDriver.init(servletContext);

		String webRoot = servletContext.getRealPath("/");
		DmDriver.init(webRoot );
	}

	public static void reinstallSemantics(HashMap<String, HashMap<String, IrSemantics>> semantics) throws SQLException, SAXException {
		if (defltJdbc == jdbc_dbcp)
			CpDriver.reinstallSemantics(semantics);
		else
			DmDriver.reinstallSemantics(semantics);
		
		if (IrSingleton.debug) {
			System.out.println("Semanitcs installed: ");
			if (semantics != null)
				for (String mk : semantics.keySet())
					System.out.print(mk + ", ");
			System.out.println();
		}
	}

	public static IrSemantics getTableSemantics(String connId, String tabl) throws SQLException {
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.getTableSemantics(connId, tabl);
		else
			return DmDriver.getTableSemantics(connId, tabl);
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
	 * @param connId
	 * @param target target table
	 * @param idField table id column (no multi-column id supported)
	 * @param jdbc using one of jdbc_dbcp or jdbc_drvmnger, defult jdbc_dbcp
	 * @return new Id (shortened in radix 64 by {@link com.infochange.frame.util.Radix64})
	 * @throws SQLException
	 */
	public static String genId(String connId, String target, String idField, String subCate) throws SQLException {
		if (isSqlite(connId))
			return genSqliteId(connId, target, idField);

		if (subCate == null) subCate = "";
		String sql;
		if (isOracle(connId))
			sql = String.format("select f_incSeq2('%s.%s', '%s') newId from dual", target, idField, subCate);
		else
			sql = String.format("select f_incSeq2('%s.%s', '%s') newId", target, idField, subCate);

		ICResultset rs = null;
		if (defltJdbc == jdbc_drvmnger)
			rs = DmDriver.select(connId, sql);
		else rs = CpDriver.select(connId, sql, DA.flag_nothing);

		rs.beforeFirst().next();
		int newInt = rs.getInt("newId");
		
		if (subCate.equals(""))
			return Radix64.toString(newInt);
		else
			return String.format("%1$s_%2$6s", subCate, Radix64.toString(newInt));
	}
	
	static DbLogDumb dlog = new DbLogDumb();
	static String genSqliteId(String conn, String target, String idF) throws SQLException { 
		Lock lock;
		if (defltJdbc == jdbc_drvmnger)
			lock = DmDriver.getAutoseqLock(conn, target);
		else 
			// lock = CpDriver.getAutoseqLock(conn, target);
			throw new SQLException("Really needing an pooled sqlite and generating an auto sequence ID ?");

		// TODO insert initial value in ir_autoseq
		// 1. update ir_autoseq (seq) set seq = seq + 1 where sid = tabl.idf
		// 2. select seq from ir_autoseq where sid = tabl.id

		ArrayList<String> sqls = new ArrayList<String>();
		sqls.add(String.format("update ir_autoseq set seq = seq + 1 where sid = '%s.%s'",
					target, idF));
			
		String select = String.format("select seq from ir_autoseq where sid = '%s.%s'",
					target, idF);

		ICResultset rs = null;
		
		// each table has a lock.
		// lock to prevent concurrency.
		lock.lock();
		try {
			// for efficiency
			DmDriver.commit(dlog, sqls, flag_nothing);
			rs = DmDriver.select(conn, select, flag_nothing);
		} finally { lock.unlock();}
		rs.beforeFirst().next();

		return Radix64.toString(rs.getInt("seq"));
	}

	///////////////////////////////////// select ////////////////////////////
	public static ICResultset select(String conn, String sql, int... flags) throws SQLException {
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.select(conn, sql, flags != null && flags.length > 0 ? flags[0] : flag_nothing);
		else
			return DmDriver.select(conn, sql, flags != null && flags.length > 0 ? flags[0] : flag_nothing);
	}

	public static ICResultset select(String sql, int... flags) throws SQLException {
		return select(null, sql, flags);
	}

	/**compose paged sql, e.g. for Oracle: select * from (sql) t where rownum > 0 and row num < 14
	 * @param sql
	 * @param page
	 * @param size
	 * @return
	 * @throws SQLException 
	 */
	public static String pagingSql(String conn, String sql, int page, int size) throws SQLException {
		//return srcs.get(conn).pageSql(sql, page, size);
		DriverType driverType = null;
		if (defltJdbc == jdbc_dbcp)
			driverType = CpDriver.getConnType(conn);
		else
			driverType = DmDriver.getConnType(conn);

		int r1 = page * size;
		int r2 = r1 + size;
		if (driverType == DriverType.mysql) {
			String s2 = String.format(
					"select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (%s) t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s",
					sql, r1, r2);
			return s2;
		}
		else if (driverType == DriverType.oracle)
			return String.format("select * from (select t.*, rownum r_n_ from (%s) t WHERE rownum <= %s  order by rownum) t where r_n_ > %s",
					sql, r2, r1);
		else if (driverType == DriverType.ms2k)
			return String.format("select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (%s) t) t where rownum >= 1 and rownum <= 2;" + 
					sql, r1, r2);
		else if (driverType == DriverType.sqlite)
			// DON'T COMMENT THIS OUT
			// Reaching here means your code has bugs
			// To stop paging from html, don't enable a html pager
			throw new SQLException("How to page in sqlite?");
		else return sql;
	}
	
	/////////////////////////////////// update /////////////////////////////
	public static int[] commit(DbLog log, ArrayList<String> sqls, int... flags) throws SQLException {
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.commit(log, sqls, flags.length > 0 ? flags[0] : DA.flag_nothing);
		else return DmDriver.commit(log, sqls, flags.length > 0 ? flags[0] : DA.flag_nothing);
	}
	
	public static int[] commit(DbLog log, ArrayList<String> sqls, ArrayList<OracleLob> lobs, int... flags) throws SQLException {
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.commit(log, sqls, lobs, flags.length > 0 ? flags[0] : DA.flag_nothing);
		else return DmDriver.commit(log, sqls, lobs, flags.length > 0 ? flags[0] : DA.flag_nothing);
	}

	@SuppressWarnings("serial")
	public static int[] commit(DbLog dblog, String sql) throws SQLException {
		return commit(dblog, new ArrayList<String> () { {add(sql);} });
	}

	//////////////////////////////// helpers ////////////////////////////////

	public static CustomSql formatNow(String conn) throws SQLException {
		DriverType driverType = null;
		if (defltJdbc == jdbc_dbcp)
			driverType = CpDriver.getConnType(conn);
		else
			driverType = DmDriver.getConnType(conn);

		if (driverType == DriverType.mysql)
			return new CustomSql("now()");
		else if (driverType == DriverType.oracle)
			return new CustomSql("sysdate");
		else if (driverType == DriverType.ms2k)
			return new CustomSql("now()");
		else if (driverType == DriverType.sqlite)
			return new CustomSql("datetime('now')");
		else throw new SQLException("format Now(), TODO..");
	}

	public static String escapeValue(String v) {
		if (v != null) {
			v = v.replace("'", "''");
			v = v.replace("%", "%%");
		}
		return v;
	}

	public static void checkNames(LinkedHashMap<String, XMLTable> maptables, ICResultset rs) {
		// TODO Auto-generated method stub
	}

	/**Helper for error message printing.
	 * @param sqls
	 */
	public static void printErr(ArrayList<String> sqls) {
		if (sqls != null && sqls.size() > 0) {
			for (String sql : sqls) {
				System.err.println(sql);
			}
		}
	}

	public static void print(List<?> objs) {
		try {
			if (objs != null)
				for (Object obj : objs)
					System.out.println(obj.toString());
		} catch (Exception ex) {
			StackTraceElement[] x = ex.getStackTrace();
			System.err.println(String.format("DA.print(): Can't print. Error: %s. called by %s.%s()",
					ex.getMessage(), x[0].getClassName(), x[0].getMethodName()));
		}
	}

	////////////////////////////// sqlite special //////////////////////////
	private static boolean isSqlite(String connId) {
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.isSqlite(connId);
		else
			return DmDriver.isSqlite(connId);
	}

	////////////////////////////// oracle special //////////////////////////

	/**If oracle, to quoted upper case "FIELD"
	 * @param conn
	 * @param expr
	 * @return
	 */
	public static String formatFieldName(String conn, String expr) {
		//return srcs.get(conn).formatFieldName(expr);
		if (defltJdbc == jdbc_dbcp)
			return CpDriver.formatFieldName(conn, expr);
		else return DmDriver.formatFieldName(conn, expr);
	}
	
	public static boolean isKeywords(String connId, String col) {
		// TODO later when oracle is needed
		return false;
	}

	public static boolean isOracle(String connId) {
		// TODO later when oracle is needed
		return false;
	}

	public static void readClob(String connId, ICResultset rs, String[] tabls) {
		// TODO later when oracle is needed
	}

	public static Object getlobMeta(String defltConnId) {
		// TODO later when oracle is needed
		return null;
	}

	public static void appendClobSemantics(HashMap<String, IrSemantics> defltMetas, String defltConnId,
			Object getlobMeta) {
		// TODO later when oracle is needed
	}

}
