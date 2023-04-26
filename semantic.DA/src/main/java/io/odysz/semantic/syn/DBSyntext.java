package io.odysz.semantic.syn;

import java.sql.SQLException;

import io.odysz.semantic.DASemantext;
import io.odysz.semantic.DATranscxt.SemanticsMap;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;

/**
 * An experimental for handling semantics for DB replication.
 * Configuration is runtime-root/syntext.xml.
 *
 * @author odys-z@github.com
 */
public class DBSyntext extends DASemantext implements ISemantext {

//	private SemanticObject autoVals;
//	private static Transcxt rawst;

//	/**Semantic Configurations */
//	private HashMap<String, DBSynmantics> ss;
//	private HashMap<String, TableMeta> metas;

//	private IUser usr;
//	private String connId;
//
//	private String basePath;
//	private ArrayList<IPostOptn> onRowsOk;
//	private LinkedHashMap<String,IPostSelectOptn> onSelecteds;
//
//	private LinkedHashMap<String, IPostOptn> onTableOk;

	protected DBSyntext(String connId, SemanticsMap metas,
			IUser usr, String rtPath) throws SemanticException, SQLException {
		super(connId, metas, usr, rtPath);
	}

//	/**When inserting, process data row with configured semantics, like auto-pk, fk-ins, etc..
//	 * @throws SemanticException
//	 * @see ISemantext#onInsert(Insert, String, List)
//	 */
//	@Override
//	public ISemantext onInsert(Insert insert, String tabl, List<ArrayList<Object[]>> rows) throws SemanticException {
//
//		if (rows != null && ss != null)
//			// second round
//			for (ArrayList<Object[]> row : rows) {
//				Map<String, Integer> cols = insert.getColumns();
//				DBSynmantics s = (DBSynmantics) ss.get(tabl);
//				if (s == null)
//					continue;
//				s.onInsert(this, insert, row, cols, usr);
//			}
//		return this;
//	}
//
//	@Override
//	public ISemantext onUpdate(Update update, String tabl, ArrayList<Object[]> nvs)
//			throws SemanticException {
//
//		if (nvs != null && ss != null) {
//			Map<String, Integer> cols = update.getColumns();
//			DBSynmantics s = ss.get(tabl);
//			if (s != null)
//				s.onUpdate(this, update, nvs, cols, usr);
//		}
//		return this;
//	}
//
//	@Override
//	public ISemantext onDelete(Delete delete, String tabl, Condit whereCondt)
//			throws TransException {
//
//		if (ss != null) {
//			DBSynmantics s = ss.get(tabl);
//			if (s != null)
//				s.onDelete(this, delete, whereCondt, usr);
//		}
//		return this;
//	}
//
//	@Override
//	public ISemantext onPost(Statement<?> stmt, String tabl, ArrayList<Object[]> row, ArrayList<String> sqls) throws TransException {
//		if (row != null && ss != null) {
//			Map<String, Integer> cols = stmt.getColumns();
//			if (cols == null)
//				return this;
//			DBSynmantics s = ss.get(tabl);
//			if (s != null)
//				s.onPost(this, stmt, row, cols, usr, sqls);
//		}
//		return this;
//	}
//
//	@Override
//	public ISemantext insert(Insert insert, String tabl, IUser... usr) {
//		return clone(this, usr);
//	}
//
//	@Override
//	public ISemantext update(Update update, String tabl, IUser... usr) {
//		return clone(this, usr);
//	}


	@Override
	public ISemantext clone(IUser usr) {
		try {
			return new DBSyntext(connId, super.ss, usr, basePath);
		} catch (SQLException | SemanticException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}
	
	@Override
	protected ISemantext clone(DASemantext srctx, IUser usr) {
		try {
			DASemantext newInst = new DBSyntext(connId, ss, usr, basePath);
			return newInst;
		} catch (SemanticException | SQLException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}
}
