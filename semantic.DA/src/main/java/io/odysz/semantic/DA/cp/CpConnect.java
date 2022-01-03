package io.odysz.semantic.DA.cp;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.OracleLob;
import io.odysz.semantics.IUser;
// Deprecated. Use java.sql.Clob interface for declaration instead of using concrete class oracle.sql.CLOB.
// see https://docs.oracle.com/database/121/JAJDB/oracle/sql/CLOB.html 
//import oracle.sql.CLOB;


/**Pooled connection manager.
 * @author Ody
 *
 */
public class CpConnect extends AbsConnect<CpConnect> {
	/**Use this for querying database without help of sql builder (which need query meta data first with this method).
	 * @param src name that matches context.xml/Resource/name, like 'inet' etc.
	 * @param sql
	 * @return results
	 * @throws SQLException
	 */
	public static AnResultset select(String src, String sql) throws SQLException {
        Connection con = null;
        PreparedStatement pstmt;
        AnResultset icrs = null; 
        try {
        	InitialContext ctx = new InitialContext();
        	// DataSource ds = (DataSource)ctx.lookup("java:/comp/env/jdbc/frameDs");
        	DataSource ds = (DataSource)ctx.lookup("java:/comp/env/" + src);
        	// apache told us use 
            con = ds.getConnection();
            con.setAutoCommit(false);

        	// System.out.println(con.getMetaData().getURL());
            // System.out.println(sql);
            Utils.logi(con.getMetaData().getURL());
            Utils.logi(sql);

            pstmt = con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            icrs = new AnResultset(rs);

            con.commit();
            pstmt.close();
        } catch (Exception e) {
        	// System.err.println("ERROR - " + src);
        	// System.err.println("      - " + sql);
        	Utils.warn("ERROR - " + src);
        	Utils.warn("      - " + sql);
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
	private DataSource ds;
	/**[table-id(logic/bump name), [upper-case-col, bump-case-col]] */
	// private HashMap<String, HashMap<String, String>> mappings;

	/**Managing connection and ds for mysql, oracle, ...
	 * @param srcId
	 * @param driverType
	 * @param printSql
	 * @throws SAXException
	 */
	public CpConnect (String srcId, dbtype driverType, boolean printSql) {
		super(driverType);
		this.srcId = "java:/comp/env/" + srcId;
		this.printSql = printSql;
	}
	
	/** e.g. ["a_logs" ["TXT",  CLOB]]*/
	private HashMap<String, HashMap<String, OracleLob>> clobMeta;

	/** Get the CLOBs meta data - which is built while initialization.
	 * @return map[tabl, [field, lob]]*/
	public HashMap<String, HashMap<String, OracleLob>> getlobMeta() { return clobMeta; }
	
	/**Get Connection
	 * 
	 * <h4>Issue with Mysql 8.0.2:</h4>
	 * For mysql 8.0.0, the SSL connection is enabled by default. And got SSL connection exception:<pre>
	 javax.net.ssl.SSLHandshakeException: Remote host closed connection during handshake
	 Caused by: java.io.EOFException: SSL peer shut down incorrectly</pre>
	 * Probable caused by no certificate?<br>
	 * To solve this temporarily, for pooled connect, in server.xml (tomcat), set connectionProperties="useSSL=false",<pre>
	 &lt;Resource auth="Container" 
	   	connectionProperties="useUnicode=yes;characterEncoding=utf8;autoReconnect=true;autoReconnectForPools=true;useSSL=false;enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2"
  		driverClassName="com.mysql.cj.jdbc.Driver"
  		maxActive="10" maxIdle="3" maxWait="10000"
  		global="jdbc/db-name"
  		name="jdbc/db-name" password="..."
  		type="javax.sql.DataSource"
  		url="jdbc:mysql://host:3306/db-name"
  		username="..."/&gt;</pre>
	 * 
	 * See <a href='https://stackoverflow.com/a/51365287/7362888'>This</a>.
	 * 
	 * @return connection
	 * @throws SQLException database access error occurs while get connection. See {@link DataSource#getConnection()}.
	 * @throws NamingException lookup connection failed
	 */
	protected Connection getConnection () throws SQLException, NamingException {
		if (ds == null) {
			System.out.print(srcId);
			System.setProperty("https.protocols", "TLSv1 TLSv1.1 TLSv1.2 TLSv1.3");
			
			InitialContext ctx = new InitialContext();
			ds = (DataSource)ctx.lookup(srcId);
		}
        Connection conn = ds.getConnection();
        return conn;
	}

	/**For {@link Connects} creating Meta data before Datasource is usable.
	 * @param sql
	 * @return query results
	 * @throws SQLException
	 * @throws NamingException 
	 */
	public AnResultset select(String sql, int flags) throws SQLException, NamingException {
		Connection con = null;
        PreparedStatement pstmt;
        AnResultset icrs = null; 
        try {
        	// if (printSql) System.out.println(sql);
        	Connects.printSql(printSql, flags, sql);

        	con = getConnection();
            con.setAutoCommit(false);
            pstmt = con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            icrs = new AnResultset(rs);

            con.commit();
            pstmt.close();
		} finally {
            if (con != null) con.close();
        }
        if (icrs != null) icrs.beforeFirst();
        return icrs;
	}

//	public void readClob(AnResultset rs, String[] tabls) throws SQLException, IOException {
//		if (tabls == null) return;
//		for (String tabl : tabls) {
//			readClob(rs, clobMeta.get(tabl));
//		}
//	}

//	private void readClob(AnResultset rs, HashMap<String, OracleLob> tablobs) throws SQLException, IOException {
//		if (rs == null || tablobs == null) return;
//		rs.beforeFirst();
//		while (rs.next()) {
//			boolean foundClob = false;
//			for (int ci = 1; ci <= rs.getColCount(); ci++) {
//				Object obj = rs.getObject(ci);
//				if (obj instanceof CLOB) {
//					foundClob = true;
//					// read
//					OracleLob lob = tablobs.get(rs.getColumnName(ci));
//					if (lob == null) {
//						System.err.println("Can't find CLOB field: " + rs.getColumnName(ci));
//						System.err.println("Table CLOBs: " + tablobs.keySet().toString());
//						System.err.println("Tips:\n\tDon't use alais for a CLOB/text field. Can't handle this.");
//						continue;
//					}
//					AnResultset lobrs = select(String.format("select %s, length(%s) from %s where %s = '%s'",
//							lob.lobField(), lob.lobField(), lob.tabl(), lob.idField(), rs.getString(lob.idField())),
//							Connects.flag_nothing);
//
//					lobrs.beforeFirst().next();
//					int len = lobrs.getInt(2);
//					CLOB clob = (CLOB) lobrs.getObject(1); 
//					Reader chareader = clob.getCharacterStream();
//					char [] charray = new char [len];
//					@SuppressWarnings("unused")
//					int bytes_read = chareader.read(charray, 0, len);
//					//conn.close();
//					rs.set(ci, String.valueOf(charray));
//				}
//			}
//			if (!foundClob) break;
//		}
//		
//		rs.beforeFirst();
//	}

	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException, NamingException {
		Connects.printSql(printSql, flags, sqls);

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
//	public void updateLobs(ArrayList<OracleLob> lobs) throws SQLException {
//		if (!_isOrcl && lobs != null && lobs.size() > 0)
//			throw new SQLException(" Why updating lobs to a non oracle db ? ");
//
//		for (OracleLob lb : lobs) {
//			try { updateClob(lb); }
//			catch (Exception e) {
//				e.printStackTrace();
//				String msg = lb.lob() == null ? "" : lb.lob().toString();
//				msg = msg.length() > 30 ? msg.substring(0, 30) : msg;
//				System.err.println(String.format(
//					"ERROR - ignoring clob updating error on %s.%s = '%s', lob = %s ...",
//					lb.tabl(), lb.idField(), lb.recId(), msg));
//			}
//		}
//	}

//	private void updateClob(OracleLob lob) throws Exception {
//		String blobField = lob.lobField();
//	// private void insert_updateClob(String blobTable, String idField, String blobField, String recID, String v) throws Exception {
//		Connection conn = getConnection();
//		try{
//			conn.setAutoCommit(false);
//			
//			Statement stmt=conn.createStatement();
////			if (insertSql != null) {
////				if (enableSystemout) System.out.println("insert sql: " + insertSql);
////				stmt.executeUpdate(insertSql);
////			}
//			
//			String sql = String.format("SELECT %s FROM %s WHERE %s = '%s' FOR UPDATE",
//					blobField, lob.tabl(), lob.idField(), lob.recId());
//			ResultSet rs=stmt.executeQuery(sql);
//			
//			if(rs.next()){
//				// BLOB rsblob = (BLOB)rs.getBlob(1); rs.getClob(1);
//				CLOB rslob = (CLOB)rs.getClob(1);
//				if (rslob == null) {
//					System.out.println("CLOB " + blobField + " is null. insert '...' first");
//					// System.out.println("insert into myUploadTable(id, filedata) values('id.001', EMPTY_BLOB())");
//					return;
//				}
//				Writer out = rslob.getCharacterOutputStream(); // .getBinaryOutputStream();
//				
////				int size = rslob.getBufferSize();
////				byte[] buffer = new byte[size];
////				int len;
//				//while((len = inStream.read(buffer)) != -1)
//					// out.write(buffer,0,len);
//				out.write((String)lob.lob());
//				out.close();
//				conn.commit();
//				if (printSql) System.out.println("blob updated.");
//			}
//		}
//		catch(Exception ex){
//			conn.rollback();
//			throw ex;
//		}
//		finally {conn.close();}
//	}

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

	@Override
	public int[] commit(IUser log, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException {
		throw new SQLException ("Shouldn't reach here!");
	}
}
