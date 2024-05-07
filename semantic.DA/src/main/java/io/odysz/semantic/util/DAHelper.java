package io.odysz.semantic.util;

import java.sql.SQLException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.syn.Nyquence;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;
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
	public static String loadRecString(Transcxt trb, String conn, TableMeta m, String recId, String field)
			throws SQLException, TransException {
		AnResultset rs = (AnResultset) trb.select(m.tbl)
				.col(field)
				.whereEq(m.pk, recId)
				.rs(trb.instancontxt(conn, DATranscxt.dummyUser()))
				.rs(0);
		
		if (rs.next())
			return rs.getString(field);
		else return null;
	}

	public static long loadRecLong(DATranscxt trb, String conn, TableMeta m, String recId, String field)
			throws SQLException, TransException {
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

	public static Nyquence loadRecNyquence(DATranscxt trb, String conn, TableMeta m, String recId, String field)
			throws SQLException, TransException {
		return new Nyquence(loadRecLong(trb, conn, m, recId, field));
	}

	public static SemanticObject updateField(DATranscxt trb, String conn, TableMeta m, String recId,
			String field, Object v, IUser usr) throws TransException, SQLException {
		return trb.update(m.tbl, usr)
			.nv(field, v instanceof ExprPart ? (ExprPart)v : Funcall.constr(v.toString()))
			.whereEq(m.pk, recId)
			.u(trb.instancontxt(conn, usr));
	}

	public static int count(DATranscxt b, String conn, String t, String field, String v)
			throws SQLException, TransException {
		return ((AnResultset) b.select(t)
				.col(Funcall.count())
				.whereEq(field, v)
				.rs(b.instancontxt(conn, DATranscxt.dummyUser()))
				.rs(0)).nxt().getInt(field);
	}
}
