package io.ic.frame.DA.cp;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.xml.sax.SAXException;

import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;

import io.ic.frame.DA.DA;
import io.ic.frame.DA.DA.DriverType;
import io.ic.frame.DA.ICResultset;
import io.ic.frame.DA.Mappings;
import io.ic.frame.DA.OracleLob;
import io.ic.frame.xtable.XMLTable;
// Deprecated. Use java.sql.Clob interface for declaration instead of using concrete class oracle.sql.CLOB.
// see https://docs.oracle.com/database/121/JAJDB/oracle/sql/CLOB.html 
import oracle.sql.CLOB;

@SuppressWarnings("deprecation")
public class CpSrc {
	@SuppressWarnings("serial")
	public static final HashSet<String> orclKeywords = new HashSet<String>() {{add("level");}};

	
	/**Use this for querying database without help of sql builder (which need query meta data first with this method).
	 * @param src name that matches context.xml/Resource/name, like 'inet' etc.
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public static ICResultset select(String src, String sql) throws SQLException {
        Connection con = null;
        PreparedStatement pstmt;
        ICResultset icrs = null; 
        try {
        	InitialContext ctx = new InitialContext();
        	// DataSource ds = (DataSource)ctx.lookup("java:/comp/env/jdbc/frameDs");
        	DataSource ds = (DataSource)ctx.lookup("java:/comp/env/" + src);
        	// apache told us use 
            con = ds.getConnection();
            con.setAutoCommit(false);

        	System.out.println(con.getMetaData().getURL());
            System.out.println(sql);

            pstmt = con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            icrs = new ICResultset(rs);

            con.commit();
            pstmt.close();
        } catch (Exception e) {
        	System.err.println("ERROR - " + src);
        	System.err.println("      - " + sql);
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		} finally {
            if (con != null) con.close();
        }
        return icrs;
	}

	private boolean printSql;
	boolean printSql() { return printSql; }
	private String srcId;
	private DriverType driverType;
	private DbSpec spec;
	private HashMap<String, DbTable> tables;
	private HashMap<String, HashMap<String, DbColumn>> tablCols;
	private DataSource ds;
	/**[table-id(logic/bump name), [upper-case-col, bump-case-col]] */
	private HashMap<String, HashMap<String, String>> mappings;

	/**Managing connection and ds for mysql, oracle, ...
	 * @param srcId
	 * @param maptables 
	 * @param tables
	 * @param tablCols
	 * @param printSql
	 * @throws SAXException 
	 */
	CpSrc (String srcId, DriverType driverType, LinkedHashMap<String, XMLTable> mappings, DbSpec spec, HashMap<String, DbTable> tables,
			HashMap<String, HashMap<String, DbColumn>> tablCols, boolean printSql) throws SAXException {
		this.mappings = Mappings.convertMap(mappings);
		this.srcId = "java:/comp/env/" + srcId;
		this.driverType = driverType;
		if (DriverType.oracle == driverType) {
			_isOrcl = true;
			clobMeta = buildClobMeta(mappings);
		}
		this.spec = spec;
		this.tables = tables;
		this.tablCols = tablCols;
		this.printSql = printSql;
	}
	
	/**Scanning map file, collect any text field: [tabl, [field, lob]]
	 * @param mappings
	 * @return
	 * @throws SAXException
	 */
	private static HashMap<String, HashMap<String, OracleLob>> buildClobMeta(LinkedHashMap<String, XMLTable> mappings) throws SAXException {
		if (mappings == null) return null;
		HashMap<String, HashMap<String, OracleLob>> lobMetas = new HashMap<String, HashMap<String, OracleLob>>();
		XMLTable mainxt = mappings.get("tabls");
		mainxt.beforeFirst();
		while (mainxt.next()) {
			String tid = mainxt.getString("u");
			String logictid = mainxt.getString("b");
			String pk = null;
			XMLTable xt = mappings.get(tid);
			xt.beforeFirst();
			ArrayList<String> lobCols = null;
			while (xt.next()) {
				// 0; idfield, 1; lobfield
				if ("text".equals(xt.getString("tn").toLowerCase())) {
					if (lobCols == null)
						lobCols = new ArrayList<String>(1); 
					lobCols.add(xt.getString("f"));
				}
				else if ("PRI".equals(xt.getString("k"))) {
					pk = xt.getString("f");
				}
			}
			if (lobCols != null) {
				for (String lobCol : lobCols) {
					HashMap<String, OracleLob> tabMeta = lobMetas.get(tid);
					if (tabMeta == null) {
						tabMeta = new HashMap<String, OracleLob>(1);
						lobMetas.put(logictid, tabMeta);
					}
					OracleLob orclob = OracleLob.template(tid, pk, lobCol); 
					tabMeta.put(lobCol, orclob);
				}
			}
			xt.beforeFirst();
		}
		mainxt.beforeFirst();
		return lobMetas;
	}

	private boolean _isOrcl;
	
	public boolean isOracle() { return _isOrcl; }
	
	/** e.g. ["a_logs" ["TXT",  CLOB]]*/
	private HashMap<String, HashMap<String, OracleLob>> clobMeta;
	/** Get the CLOBs meta data - which is built while initialization.
	 * @return: [tabl, [field, lob]]*/
	public HashMap<String, HashMap<String, OracleLob>> getlobMeta() { return clobMeta; }
	
	private Connection getConnection () throws SQLException {
		if (ds == null) {
			InitialContext ctx;
			try {
				ctx = new InitialContext();
				ds = (DataSource)ctx.lookup(srcId);
			} catch (NamingException e) {
				throw new SQLException(e.getMessage());
			}
		}
        Connection conn = ds.getConnection();
        return conn;
	}

	DriverType driverName() {return driverType; }
	
	public DbSpec getSpec() { return spec; }

	public DbTable get(String tablname) {
		return get(DriverType.deflt, tablname);
	}

	public DbTable get(DriverType srcId, String tablname) {
		return tables.get(tablname);
	}

	public HashMap<String, DbColumn> getCols(String tabl) {
		return tablCols.get(tabl);
	}

	public DbColumn getColumn(String tabName, String colName) {
		if(tablCols.containsKey(tabName))
			return tablCols.get(tabName).get(colName);
		else{
			// System.err.println(String.format("%s.%s doesn't exists！", tabName, colName));
			return null;
		}
	}

	public String formatFieldName(String expr) {
		if (_isOrcl && orclKeywords.contains(expr.trim()))
			return String.format("\"%s\"", expr.trim().toUpperCase());
		return expr;
	}

//	public CustomSql formatNow() {
//		if (_isOrcl) return new CustomSql("sysdate"); 
//		else if ("")
//		else return new CustomSql("now()");
//	}

	public boolean isKeywords(String expr) {
		return _isOrcl && expr != null && orclKeywords.contains(expr.trim());
	}

	/**For {@link CpDriver} creating Meta data before Datasource is usable.
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @throws NamingException 
	 */
	ICResultset select(String sql, int flags) throws SQLException {
		Connection con = null;
        PreparedStatement pstmt;
        ICResultset icrs = null; 
        try {
        	// if (printSql) System.out.println(sql);
        	DA.printSql(printSql, flags, sql);

        	con = getConnection();
            con.setAutoCommit(false);
            pstmt = con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            icrs = new ICResultset(rs);

            con.commit();
            pstmt.close();
		} finally {
            if (con != null) con.close();
        }
        if (icrs != null) icrs.beforeFirst();
        return icrs;
	}

	public void readClob(ICResultset rs, String[] tabls) throws SQLException, IOException {
		if (tabls == null) return;
		for (String tabl : tabls) {
			readClob(rs, clobMeta.get(tabl));
		}
	}

	private void readClob(ICResultset rs, HashMap<String, OracleLob> tablobs) throws SQLException, IOException {
		if (rs == null || tablobs == null) return;
		rs.beforeFirst();
		while (rs.next()) {
			boolean foundClob = false;
			for (int ci = 1; ci <= rs.getColCount(); ci++) {
				Object obj = rs.getObject(ci);
				if (obj instanceof CLOB) {
					foundClob = true;
					// read
					OracleLob lob = tablobs.get(rs.getColumnName(ci));
					if (lob == null) {
						System.err.println("Can't find CLOB field: " + rs.getColumnName(ci));
						System.err.println("Table CLOBs: " + tablobs.keySet().toString());
						System.err.println("Tips:\n\tDon't use alais for a CLOB/text field. Can't handle this.");
						continue;
					}
					ICResultset lobrs = select(String.format("select %s, length(%s) from %s where %s = '%s'",
							lob.lobField(), lob.lobField(), lob.tabl(), lob.idField(), rs.getString(lob.idField())),
							DA.flag_nothing);

					lobrs.beforeFirst().next();
					int len = lobrs.getInt(2);
					CLOB clob = (CLOB) lobrs.getObject(1); 
					Reader chareader = clob.getCharacterStream();
					char [] charray = new char [len];
					@SuppressWarnings("unused")
					int bytes_read = chareader.read(charray, 0, len);
					//conn.close();
					rs.set(ci, String.valueOf(charray));
				}
			}
			if (!foundClob) break;
		}
		
		rs.beforeFirst();
	}

	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		DA.printSql(printSql, flags, sqls);

		int[] ret = null;
        Connection conn = null;
		Statement stmt = null;
		try {
            conn = getConnection();
			if (conn != null) {
				stmt = conn.createStatement();
				try {
					stmt = conn.createStatement(
							ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					conn.setAutoCommit(false);

					for (String sql : sqls) {
						stmt.addBatch(sql);
					}

					ret = stmt.executeBatch();
					conn.commit();
				} catch (Exception exx) {
					conn.rollback();
					exx.printStackTrace();
					throw new SQLException(exx);
				} finally { }
			} else {
				throw new SQLException("batch execution failed");
			}
//        } catch (NamingException e) {
//			e.printStackTrace();
//			throw new SQLException(e.getMessage());
		} catch (SQLException ex) {
			throw ex;
		} finally {
			try {
				if (conn != null) conn.close();
				if (stmt != null) stmt.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				conn = null;
				stmt = null;
			}
		}
		return ret;
	}
	
	//////////////////////////////// oracle the hateful ///////////////////////////////////////
	public void updateLobs(ArrayList<OracleLob> lobs) throws SQLException {
		if (!_isOrcl && lobs != null && lobs.size() > 0)
			throw new SQLException(" Why updating lobs to a non oracle db ? ");

		for (OracleLob lb : lobs) {
			try { updateClob(lb); }
			catch (Exception e) {
				e.printStackTrace();
				String msg = lb.lob() == null ? "" : lb.lob().toString();
				msg = msg.length() > 30 ? msg.substring(0, 30) : msg;
				System.err.println(String.format(
					"ERROR - ignoring clob updating error on %s.%s = '%s', lob = %s ...",
					lb.tabl(), lb.idField(), lb.recId(), msg));
			}
		}
	}

	private void updateClob(OracleLob lob) throws Exception {
		String blobField = lob.lobField();
	// private void insert_updateClob(String blobTable, String idField, String blobField, String recID, String v) throws Exception {
		Connection conn = getConnection();
		try{
			conn.setAutoCommit(false);
			
			Statement stmt=conn.createStatement();
//			if (insertSql != null) {
//				if (enableSystemout) System.out.println("insert sql: " + insertSql);
//				stmt.executeUpdate(insertSql);
//			}
			
			String sql = String.format("SELECT %s FROM %s WHERE %s = '%s' FOR UPDATE",
					blobField, lob.tabl(), lob.idField(), lob.recId());
			ResultSet rs=stmt.executeQuery(sql);
			
			if(rs.next()){
				// BLOB rsblob = (BLOB)rs.getBlob(1); rs.getClob(1);
				CLOB rslob = (CLOB)rs.getClob(1);
				if (rslob == null) {
					System.out.println("CLOB " + blobField + " is null. insert '...' first");
					// System.out.println("insert into myUploadTable(id, filedata) values('id.001', EMPTY_BLOB())");
					return;
				}
				Writer out = rslob.getCharacterOutputStream(); // .getBinaryOutputStream();
				
//				int size = rslob.getBufferSize();
//				byte[] buffer = new byte[size];
//				int len;
				//while((len = inStream.read(buffer)) != -1)
					// out.write(buffer,0,len);
				out.write((String)lob.lob());
				out.close();
				conn.commit();
				if (printSql) System.out.println("blob updated.");
			}
		}
		catch(Exception ex){
			conn.rollback();
			throw ex;
		}
		finally {conn.close();}
	}

	public static String truncatUtf8(String s, int maxBytes) {
		    int b = 0;
		    for (int i = 0; i < s.length(); i++) {
		        char c = s.charAt(i);

		        // ranges from http://en.wikipedia.org/wiki/UTF-8
		        int skip = 0;
		        int more;
		        if (c <= 0x007f) {
		            more = 1;
		        }
		        else if (c <= 0x07FF) {
		            more = 2;
		        } else if (c <= 0xd7ff) {
		            more = 3;
		        } else if (c <= 0xDFFF) {
		            // surrogate area, consume next char as well
		            more = 4;
		            skip = 1;
		        } else {
		            more = 3;
		        }

		        if (b + more > maxBytes) {
		            return s.substring(0, i);
		        }
		        b += more;
		        i += skip;
		    }
		    return s;
	}

//	public String pageSql(String sql, int page, int size) {
//		int r1 = page * size;
//		int r2 = r1 + size;
//		if (driverType.equalsIgnoreCase("mysql")) {
//			String s2 = String.format(
//					"select * from (select t.*, @ic_num := @ic_num + 1 as rnum from (%s) t, (select @ic_num := 0) ic_t) t1 where rnum > %s and rnum <= %s",
//					sql, r1, r2);
//			return s2;
//		}
//		else if (driverType.equalsIgnoreCase("orcl") || driverType.equalsIgnoreCase("oracle"))
//			return String.format("select * from (select t.*, rownum r_n_ from (%s) t WHERE rownum <= %s  order by rownum) t where r_n_ > %s",
//					sql, r2, r1);
////			return String.format("select * from (%s) t where rownum > %d and rownum <= %s",
////					sql, r1, r2);
//		else if (driverType.equalsIgnoreCase("mssql2k"))
//			return String.format("select * from (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, * from (%s) t) t where rownum >= 1 and rownum <= 2;" + 
//					sql, r1, r2);
//		else return sql;
//	}

	/**Only oracle has the mappings (inited by constructor).
	 * @return
	 */
	public HashMap<String,HashMap<String,String>> mappings() {
		return mappings;
	}

	public String getTimestamp() throws SQLException {
		String sql = null;
		if (driverType == DriverType.mysql) {
			sql = "select now()";
		}
		else if (driverType == DriverType.oracle)
			sql = "select sysDate";
		else if (driverType == DriverType.ms2k)
			sql = "select now()";
		else if (driverType == DriverType.sqlite)
			sql = "select DATETIME('now')";
		
		ICResultset rs = select(sql, DA.flag_nothing);
		if (rs.next())
			return rs.getString(1);
		else return null;
	}
}
