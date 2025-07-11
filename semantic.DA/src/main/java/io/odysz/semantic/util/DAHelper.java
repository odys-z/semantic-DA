package io.odysz.semantic.util;

import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.isPrimitive;

import java.sql.SQLException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

public class DAHelper {

	/**
	 * Load a field from table m.tbl.
	 * 
	 * @param trb transaction builder
	 * @param conn
	 * @param m the table must have a pk.
	 * @param valfield
	 * @param col_vs
	 * @return field's value in row[kvs[0] = kvs[1], kvs[1] = kvs[2], ...]
	 * @throws SQLException
	 * @throws TransException
	 */
	public static String getValstr(Transcxt trb, String conn, TableMeta m, String valfield,
			Object ... col_vs) throws SQLException, TransException {
		Query q = trb.select(m.tbl);

		for (int i = 0; i < col_vs.length; i+=2) {
			q.whereEq((String)col_vs[i], col_vs[i+1]);
		}

		AnResultset rs = (AnResultset) q
				.col(valfield)
				.rs(trb.instancontxt(conn, DATranscxt.dummyUser()))
				.rs(0);
		
		if (rs.next())
			return rs.getString(valfield);
		else return null;
	}
	
	/**
	 * select valexpr.sql() as as from [connid].m.tbl where (k=v)s.
	 * @param trb
	 * @param connid
	 * @param m
	 * @param valexpr
	 * @param as
	 * @param kvs
	 * @return
	 * @throws TransException
	 * @throws SQLException
	 */
	public static Object getExprstr(DATranscxt trb, String connid, TableMeta m,
			Funcall valexpr, String as, Object...kvs) throws TransException, SQLException {
		Query q = trb.select(m.tbl);

		for (int i = 0; i < kvs.length; i+=2) {
				q.whereEq((String)kvs[i], kvs[i+1]);
		}

		AnResultset rs = (AnResultset) q
				.col(valexpr, as)
				.rs(trb.instancontxt(connid, DATranscxt.dummyUser()))
				.rs(0);

		if (rs.next())
			return rs.getString(as);
		else return null;
	}

	/**
	 * @deprecated call {@link #getValong(DATranscxt, String, TableMeta, String, String...)},
	 * e.g. getValong(t, m, target-field, m.pk, recId, ...).
	 * @param trb
	 * @param conn
	 * @param m
	 * @param recId
	 * @param field
	 * @return long value
	 * @throws SQLException
	 * @throws TransException
	 */
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
	
	public static long getValong(DATranscxt trb, String conn, TableMeta m, String field, String ... kvs)
			throws SQLException, TransException {
		Query q = trb.select(m.tbl);
		
		for (int i = 0; i < kvs.length; i+=2)
			q.whereEq(kvs[i], kvs[i+1]);
		
		AnResultset rs = (AnResultset) q 
				.rs(trb.instancontxt(conn, DATranscxt.dummyUser()))
				.rs(0);
		
		if (rs.next())
			return rs.getLong(field);
		else throw new SQLException(String
			.format("Record not found: %s.%s = '%s' ... ", m.tbl, kvs[0], kvs[1]));
	}

	/**
	 * Commit to DB({@code conn}) as user {@code usr}, with SQL:<br>
	 * 
	 * update m.tbl set field = v where m.pk = recId
	 * 
	 * @param trb
	 * @param conn
	 * @param m
	 * @param recId
	 * @param vfield
	 * @param v
	 * @param usr
	 * @return affected rows
	 * @throws TransException
	 * @throws SQLException
	 */
	public static SemanticObject updateFieldByPk(DATranscxt trb, String conn, TableMeta m, String recId,
			String vfield, Object v, IUser usr) throws TransException, SQLException {
//		return trb.update(m.tbl, usr)
//			.nv(vfield, v instanceof ExprPart
//						? (ExprPart)v
//						: isPrimitive(v)
//						? new ExprPart(String.valueOf(v))
//						: Funcall.constr(v.toString()))
//			.whereEq(m.pk, recId)
//			.u(trb.instancontxt(conn, usr));
		return updateFieldByPk(trb, trb.instancontxt(conn, usr), m, recId, vfield, v, usr);
	}

	public static SemanticObject updateFieldByPk(DATranscxt tb, ISemantext semantxt, TableMeta m, String recId,
			String vfield, Object v, IUser usr) throws TransException, SQLException {
		return tb.update(m.tbl, usr)
			.nv(vfield, v instanceof ExprPart
						? (ExprPart)v
						: isPrimitive(v)
						? new ExprPart(String.valueOf(v))
						: Funcall.constr(v.toString()))
			.whereEq(m.pk, recId)
			.u(semantxt);

	}

	/**
	 * 
	 * Commit to DB({@code conn}) as user {@code usr}, with SQL:<br>
	 * 
	 * update m.tbl set field = v where m.pk = recId
	 * 
	 * @param usr
	 * @param trb
	 * @param conn
	 * @param m
	 * @param recId
	 * @param nvs n-v pairs
	 * @return updating result
	 * @throws TransException
	 * @throws SQLException
	 */
	public static SemanticObject updateFieldsByPk(IUser usr, DATranscxt trb, String conn,
			TableMeta m, String recId, Object... nvs) throws TransException, SQLException {
		Update u = trb.update(m.tbl, usr)
			.whereEq(m.pk, recId);
		
		for (int nvx = 0; nvx < nvs.length; nvx+=2)
			u.nv((String)nvs[nvx], nvs[nvx+1] instanceof ExprPart
							? (ExprPart)nvs[nvx+1]
							: isPrimitive(nvs[nvx+1])
							? new ExprPart(String.valueOf(nvs[nvx+1]))
							: Funcall.constr(nvs[nvx+1].toString()));
		
		return u.u(trb.instancontxt(conn, usr));
	}
	
	/**
	 * Commit to DB({@code conn}) as user {@code usr}, with SQL:<br>
	 * 
	 * update m.tbl set field = v where whereqs[0] = whereqs[1] and whereqs[2] = whereqs[3] ...
	 * 
	 * @since 2.0.0
	 * 
	 * @param trb
	 * @param conn
	 * @param usr
	 * @param m
	 * @param field
	 * @param v
	 * @param whereqs
	 * @return affected rows
	 * @throws TransException
	 * @throws SQLException
	 */
	public static SemanticObject updateFieldWhereEqs(DATranscxt trb, String conn, IUser usr,
			TableMeta m, String field, Object v, Object... whereqs) throws TransException, SQLException {
		Update u = trb.update(m.tbl, usr)
			.nv(field, v instanceof ExprPart
					? (ExprPart)v
					: isPrimitive(v)
					? new ExprPart(String.valueOf(v))
					: Funcall.constr(v.toString()));
		
		for (int x = 0; x < whereqs.length; x+=2) {
			u.whereEq((String)whereqs[x], whereqs[x+1]);
		}

		return u.u(trb.instancontxt(conn, usr));
	}

	/**
	 * @deprecated replaced by {@link #count(DATranscxt, String, String, Object...)}
	 * @return count
	 * @throws SQLException
	 * @throws TransException
	 */
	public static int count_(DATranscxt b, String conn, String tbl, String field, String v)
			throws SQLException, TransException {
		return ((AnResultset) b.select(tbl)
				.col(Funcall.count(), "cnt")
				.whereEq(field, v)
				.rs(b.instancontxt(conn, DATranscxt.dummyUser()))
				.rs(0)).nxt().getInt("cnt");
	}

	public static int count(DATranscxt b, String conn, String tbl, Object ... kvs)
			throws SQLException, TransException {
		Query q = b.select(tbl)
				.col(Funcall.count("*"), "cnt");
		if (!isNull(kvs))
			for (int i = 0; i < kvs.length; i+=2)
				if (kvs[i+1] == null)
					q.whereEq((String)kvs[i], new ExprPart());
				else
					q.whereEq((String)kvs[i], kvs[i+1]);

		return ((AnResultset) q.rs(b.instancontxt(conn, DATranscxt.dummyUser()))
				.rs(0)).nxt().getInt("cnt");
	}

	public static SemanticObject insert(IUser usr, DATranscxt t0, String conn, TableMeta snm,
			Object... nvs) throws TransException, SQLException {
		
		Insert ins = t0.insert(snm.tbl, usr);

		for (int x = 0; x < nvs.length; x+=2)
			ins.nv((String)nvs[x], nvs[x+1] instanceof ExprPart
							? (ExprPart)nvs[x+1]
							: isPrimitive(nvs[x+1])
							? new ExprPart(String.valueOf(nvs[x+1]))
							: Funcall.constr(nvs[x+1] == null ? null : nvs[x+1].toString()));
			
		return (SemanticObject) ins
				.ins(t0.instancontxt(conn, usr));
	}
	
	public static AnResultset getEntityById(DATranscxt b, TableMeta m, String id)
			throws SQLException, TransException {
		return ((AnResultset) b.select(m.tbl)
				.whereEq(m.pk, id).rs(b.instancontxt(b.basictx().connId(), DATranscxt.dummyUser()))
				.rs(0));
	}
}
