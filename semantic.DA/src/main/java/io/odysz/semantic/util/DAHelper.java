package io.odysz.semantic.util;

import java.sql.SQLException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.syn.Nyquence;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.x.TransException;

public class DAHelper {

	/**
	 * Load a field from db.
	 * @param m the table must have a pk.
	 * @param recId
	 * @param field
	 * @return field value in row[pk = recId]
	 * @throws TransException 
	 * @throws SQLException 
	 */
	public static String loadRecString(Transcxt trb, String conn, TableMeta m, String recId, String field) throws SQLException, TransException {
		AnResultset rs = (AnResultset) trb.select(m.tbl)
				.col(field)
				.whereEq(m.pk, recId)
				.rs(trb.instancontxt(conn, DATranscxt.dummyUser()))
				.rs(0);
		
		if (rs.next())
			return rs.getString(field);
		else return null;
	}

	public static long loadRecLong(Transcxt trb, String conn, TableMeta m, String recId, String field) throws SQLException, TransException {
		AnResultset rs = (AnResultset) trb.select(m.tbl)
				.col(field)
				.whereEq(m.pk, recId)
				.rs(trb.instancontxt(conn, DATranscxt.dummyUser()))
				.rs(0);
		
		if (rs.next())
			return rs.getLong(field);
		else throw new SQLException(String
			.format("Record not found: %s.%s = '%s'", m.tbl, m.pk, recId));
	}

	public static Nyquence loadRecNyquence(Transcxt trb, String conn, TableMeta m, String recId, String field) throws SQLException, TransException {
		return new Nyquence(loadRecLong(trb, conn, m, recId, field));
	}

}
