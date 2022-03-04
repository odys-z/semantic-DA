package io.odysz.semantic.DA.drvmnger;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.sql.DataSource;

import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.OracleLob;
import io.odysz.semantics.IUser;
import oracle.sql.BLOB;

/**
 * @author odys-z@github.com
 */
@SuppressWarnings("deprecation")
public class OracleDriver extends AbsConnect<OracleDriver> {

	static DataSource ds;
	static boolean enableSystemout = false;
	static boolean inited = false;
	static String userName;
	static String pswd;
	static String connect;
	
	/**
	 * MUST CLOSE CONNECTION!
	 * @return Connection
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		if (!inited) {
//			// String isTrue = Configs.getCfg("printSQL.enable");
//			String isTrue = "true";
//			enableSystemout = isTrue != null && "true".equals(isTrue.toLowerCase());
//			
//			// This depends on servlet context. 
//			// To init connection without config, call initConnection() before any select() or commit()
//			connect = Configs.getCfg("com.ic.DA.Oracle.connect");
//			userName = Configs.getCfg("com.ic.DA.Oracle.username");
//			pswd = Configs.getCfg("com.ic.DA.Oracle.password");
//
////			connect = "jdbc:oracle:thin:@118.122.251.196:1521:orcl";
////			userName = "gzdx_yjpt";
////			pswd = "gzdx_yjpt";
//
//			//pswd = decryptDBpswd(pswd, userName);
//			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver()); 
//			
//			inited = true;

			throw new SQLException("connection must explicitly initialized first - call initConnection()");
		}

		Connection conn = DriverManager.getConnection(connect, userName, pswd);
		return conn;
	}

	public static OracleDriver initConnection(String conn, String user, String psword, int flag) throws SQLException {
		if (!inited) {
			enableSystemout = true;
			
			connect = conn;
			userName = user;
			pswd = psword;
			// FIXME decipher pswd
			// pswd = Encrypt.DecryptPswdImpl(pswd);
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver()); 
			try {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new SQLException(e.getMessage());
			}
			inited = true;
		}
		return new OracleDriver();
	}	
	/*
	public static Connection getSQLConnection() throws Exception {
		/* Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
		Connection connSQL=DriverManager.getConnection("jdbc:odbc:V4Alarm");* /
		// String url = Configs.getCfg("com.infochange.frameleisure.DA.Sqlserver.connect");
		String url = "jdbc:oracle:thin:@118.122.251.196:1521:orcl";
		
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		Connection connSQL = DriverManager.getConnection(url);


	    return connSQL;
	}*/
	
//	private static String decryptDBpswd(String cipher, String username) {
//		return username;
//	}

	public static AnResultset select(String sql) throws SQLException {
		if (enableSystemout) System.out.println(sql);
		Connection conn = getConnection();
//		try {
//			Class.forName( "oracle.jdbc.driver.OracleDriver" );
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//		Connection conn = DriverManager.getConnection( "jdbc:oracle:thin:@192.168.1.101:1521:emergenc", "emergency", "emergency" );
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		AnResultset icrs = new AnResultset(rs);
		rs.close();
		stmt.close();
		conn.close();
		
		return icrs;
	}
	
	/**@deprecated
	 * @param sqls
	 * @param userid
	 * @param username
	 * @param operTime
	 * @param funcName
	 * @param sysID
	 * @throws SQLException
	 */
	public static void executeBatch(ArrayList<String> sqls, String userid, String username, String operTime, String funcName, String sysID) throws SQLException {
		Connection conn = getConnection();
		excecuteBatch(conn, sqls, userid, username, operTime, funcName, sysID);
		conn.close();
	}
	
	/**@deprecated
	 * @param conn
	 * @param sqls
	 * @param userid
	 * @param username
	 * @param operTime
	 * @param funcName
	 * @param sysID
	 * @throws SQLException
	 */
	public static void excecuteBatch(Connection conn, ArrayList<String> sqls,
			String userid, String username, String operTime, String funcName, String sysID)
			throws SQLException {
		Statement stmt = null;
		try {
			if (conn != null) { 
				stmt = conn.createStatement();
				try {
					String logs = "";
					boolean noMoreLogs = false;
					stmt = conn.createStatement(
							ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					conn.setAutoCommit(false);
					for (String sql : sqls) {
						if (enableSystemout) System.out.println(sql + ";");
						stmt.addBatch(sql);
						if (!noMoreLogs && logs.length() + sql.length() + 1 + "...".length() <= 4000) {
							logs += sql.replaceAll("'", "''") + ";";
						}
						else if (!noMoreLogs && logs.length() + sql.length() + 1 + "...".length() > 4000) {
							noMoreLogs = true;
							if (logs.length() + "...".length() <= 4000) {
								logs += "...";
							}
						}
					}
					if (!"".equals(logs)) {
						String logsql = String.format("insert into frame_logs(operID, oper, operTime, funcName, sysID, remarks) values " +
								"('%s', '%s', to_date('%s', 'yyyy-MM-dd hh24:mi:ss'), '%s', '%s', '%s')", userid, username, operTime, funcName, sysID, logs);
						stmt.addBatch(logsql);
						if (enableSystemout) System.out.println(logsql);
					}
					stmt.executeBatch();
					conn.commit();
					if (enableSystemout) System.out.println("commit successes");
//					logOperations (logs, userid, operTime, funcName, remarks);
				} catch (Exception exx) {
					conn.rollback();
					exx.printStackTrace();
					throw new SQLException(exx);
				} finally {
				}
			} else {
				throw new SQLException("execute failed");
			}
		} catch (SQLException ex) {
			throw ex;
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				stmt = null;
			}
		}
	}

	public static void excecuteBatch(ArrayList<String> sqls) throws SQLException {
		Connection conn = getConnection(); 
		Statement stmt = null;
		try {
			if (conn != null) { 
				stmt = conn.createStatement();
				try {
					stmt = conn.createStatement(
							ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					conn.setAutoCommit(false);
					for (String sql : sqls) {
						if (enableSystemout) System.out.println(sql + ";");
						stmt.addBatch(sql);
					}
					stmt.executeBatch();
					conn.commit();
				} catch (Exception exx) {
					conn.rollback();
					exx.printStackTrace();
					throw new SQLException(exx);
				} finally {
				}
			} else {
				throw new SQLException("oracle connection is null");
			}
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				stmt = null;
			}
			conn.close();
		}
	}

	public static void insert_updateBlob(String insertSql, String blobTable, String idField, String blobField, String recID, InputStream inStream) throws Exception {
		Connection conn = getConnection();
		try{
			conn.setAutoCommit(false);
			
			Statement stmt=conn.createStatement();
			if (insertSql != null) {
				if (enableSystemout) System.out.println("insert sql: " + insertSql);
				stmt.executeUpdate(insertSql);
			}
			
			String sql = String.format("SELECT %s FROM %s WHERE %s = '%s' FOR UPDATE",
					blobField, blobTable, idField, recID);
			if (enableSystemout) System.out.println(sql);
			ResultSet rs=stmt.executeQuery(sql);
			
			if(rs.next()){
				BLOB rsblob = (BLOB)rs.getBlob(1);
				if (rsblob == null) {
					System.out.println("blob filed: " + blobField + " is null. inserting EMPTY_BLOB()...");
					System.out.println("insert into myUploadTable(id, filedata) values('id.001', EMPTY_BLOB())");
					return;
				}
				OutputStream out = rsblob.getBinaryOutputStream();
				
				if (enableSystemout) System.out.println("updating blob field...");
				int size = rsblob.getBufferSize();
				byte[] buffer=new byte[size];
				int len;
				while((len = inStream.read(buffer)) != -1)
					out.write(buffer,0,len);
				out.close();
				conn.commit();
				if (enableSystemout) System.out.println("blob updated.");
			}
		}
		catch(Exception ex){
			conn.rollback();
			throw ex;
		}
		finally {conn.close();}
	}

	/**
	 * @param sql
	 * @return result set
	 * @throws SQLException 
	 */
	public static AnResultset selectBlob(String sql) throws SQLException {
		if (enableSystemout) System.out.println(sql);
		Connection conn = getConnection();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = stmt.executeQuery(sql);
		AnResultset icrs = new AnResultset(rs, conn, stmt);
		return icrs;
	}

	public static void setClobs(ArrayList<OracleLob> lobs) {
		Connection conn = null;
		try {
			conn = getConnection();
			//conn.setAutoCommit(false);
			for (OracleLob lob : lobs)
				OracleLob.setClob(conn, lob);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
	}

	public OracleDriver() {
		super(dbtype.oracle);
	}

	@Override
	public AnResultset select(String sql, int flags) throws SQLException {
		Connection conn = getConnection();
		Connects.printSql(enableSystemout, flags, sql);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		AnResultset icrs = new AnResultset(rs);
		rs.close();
		stmt.close();
		conn.close();
		
		return icrs;
	}

	@Override
	public int[] commit(ArrayList<String> sqls, int flags) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int[] commit(IUser log, ArrayList<String> sqls, ArrayList<Clob> lobs, int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}


}
