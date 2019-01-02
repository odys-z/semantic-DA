package io.odysz.semantic.DA;

import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import oracle.sql.CLOB;

/**POJO class for oracle lob buffering.
 * @author ody
 */
@SuppressWarnings("deprecation")
public class OracleLob {

	String tabl;
	public String tabl() { return tabl; }
	String lobField;
	public String lobField() { return lobField; }
	Object lob;
	public Object lob() { return lob; }
	String idField;
	public String idField() { return idField; }
	String recId;
	public String recId() { return recId; }

	public OracleLob(String tabl, String lobField, Object lob) {
		this.tabl = tabl;
		this.lobField = lobField;
		this.lob = lob;
	}

	public void recId(String idField, String recId) {
		this.idField = idField;
		this.recId = recId;
	}

	public static OracleLob template(String tabl, String idField, String lobField) {
		OracleLob orclob = new OracleLob(tabl, lobField, null);
		orclob.idField = idField;
		return orclob;
	}
	
	public static void setClob(Connection conn, OracleLob lob) {
		try{
			Statement stmt=conn.createStatement();
			
			String sql = String.format("SELECT %s FROM %s WHERE %s = '%s' FOR UPDATE",
					lob.lobField, lob.tabl, lob.idField, lob.recId);
			ResultSet rs=stmt.executeQuery(sql);
			
			if(rs.next()){
				CLOB rslob = (CLOB)rs.getClob(1);
				if (rslob == null) {
					System.out.println(String.format("ERROR - record for lob updating is missing (%s: %s)", lob.tabl, lob.recId));
					return;
				}
				Writer out = rslob.getCharacterOutputStream();
				
				out.write((String)lob.lob);
				out.close();
				conn.commit();
				//if (enableSystemout) System.out.println("blob updated.");
			}
		} catch(Exception ex) {
			System.err.println("Ignoring clob of record " + lob.recId);
			System.err.println(ex.getMessage());
		}
	}
}
