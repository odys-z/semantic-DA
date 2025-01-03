package io.odysz.semantic.syn;

import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.transact.x.TransException;

public class T_DA_PhotoMeta extends ExpDocTableMeta {

	public final String exif;

	public T_DA_PhotoMeta(String conn) throws TransException {
		super("h_photos", "pid", "device", conn);
		ddlSqlite = loadSqlite(T_DA_PhotoMeta.class, "h_photos.sqlite.ddl");

		exif = "exif";
		// synoder = "device";

		// ddlSqlite = loadTxt(T_DA_PhotoMeta.class, "h_photos.sqlite.ddl");
	}

	// public String device() { return synoder; }

//	@Override
//	public Object[] insertSelectItems(SynChangeMeta chgm, String entid,
//			AnResultset entities, AnResultset changes)
//			throws SemanticException, SQLException {
//		Object[] cols = entCols();
//		Object[] selects = new Object[cols.length];
//		for (int cx = 0; cx < cols.length; cx++) {
//			if (cols[cx] instanceof String) {
//				String val = entities.getStringByIndex((String)cols[cx], entid);
//				if (val != null)
//					selects[cx] = Funcall.constr(val);
//			}
//		}
//		return selects;
//	}
}
