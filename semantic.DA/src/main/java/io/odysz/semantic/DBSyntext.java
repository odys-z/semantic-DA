package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io_odysz.FilenameUtils;

import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Statement.IPostOptn;
import io.odysz.transact.sql.Statement.IPostSelectOptn;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.AbsPart;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.x.TransException;

/**
 * An experimental for handling semantics for DB replication.
 * Configuration is runtime-root/syntext.xml.
 *
 * @author odys-z@github.com
 */
public class DBSyntext implements ISemantext {

	public enum syntype {
		pull,
		push
	}

	private SemanticObject autoVals;
	private static Transcxt rawst;

	/**Semantic Configurations */
	private HashMap<String, DBSynmantics> ss;
	private HashMap<String, TableMeta> metas;

	private IUser usr;
	private String connId;

	private String basePath;
	private ArrayList<IPostOptn> onRowsOk;
	private LinkedHashMap<String,IPostSelectOptn> onSelecteds;

	private LinkedHashMap<String, IPostOptn> onTableOk;

	DBSyntext(String connId, HashMap<String, DBSynmantics> smtcfg,
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
	 * @see ISemantext#onInsert(Insert, String, List)
	 */
	@Override
	public ISemantext onInsert(Insert insert, String tabl, List<ArrayList<Object[]>> rows)
			throws SemanticException {

		if (rows != null && ss != null)
			// second round
			for (ArrayList<Object[]> row : rows) {
				Map<String, Integer> cols = insert.getColumns();
				DBSynmantics s = ss.get(tabl);
				if (s == null)
					continue;
				s.onInsert(this, insert, row, cols, usr);
			}
		return this;
	}

	@Override
	public ISemantext onUpdate(Update update, String tabl, ArrayList<Object[]> nvs)
			throws SemanticException {

		if (nvs != null && ss != null) {
			Map<String, Integer> cols = update.getColumns();
			DBSynmantics s = ss.get(tabl);
			if (s != null)
				s.onUpdate(this, update, nvs, cols, usr);
		}
		return this;
	}

	@Override
	public ISemantext onDelete(Delete delete, String tabl, Condit whereCondt)
			throws TransException {

		if (ss != null) {
			DBSynmantics s = ss.get(tabl);
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
			DBSynmantics s = ss.get(tabl);
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
			return new DBSyntext(connId, ss, metas, usr, basePath);
		} catch (SemanticException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}

	private ISemantext clone(DBSyntext srctx, IUser... usr) {
		try {
			DBSyntext newInst = new DBSyntext(connId,
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
		return this;
	}

	@Override
	public String genId(String tabl, String col) throws SQLException, TransException {
		String newv = DASemantext.genId(tabl, col, null);

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

	public AnResultset entities(SyntityMeta m) {
		return null;
	}
}
