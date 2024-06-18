package io.odysz.semantic.syn;

import static io.odysz.common.Utils.loadTxt;

import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

public class T_PhotoMeta extends T_DocTableMeta {

	public final String exif;

	public T_PhotoMeta(String conn) throws TransException {
		super("h_photos", "pid", "family", "device", conn);
		ddlSqlite = loadTxt(T_PhotoMeta.class, "h_photos.sqlite.ddl");

		exif = "exif";
		synoder = "device";
	}

	public String device() { return synoder; }

	@Override
	public ArrayList<Object[]> updateEntNvs(SynChangeMeta chgm, String entid,
			AnResultset entities, AnResultset challenges)
			throws SemanticException, SQLException {
		ArrayList<Object[]> row = new ArrayList<Object[]>();
		String[] updatingcols = challenges.getStrArray(chgm.updcols);
		for (String c : updatingcols)
			row.add(new Object[] {c, entities.getStringByIndex(c, entid)});
		return row;
	}

	@Override
	public Object[] insertSelectItems(SynChangeMeta chgm, String entid,
			AnResultset entities, AnResultset changes)
			throws SemanticException, SQLException {
		String[] cols = entCols();
		Object[] selects = new Object[cols.length];
		for (int cx = 0; cx < cols.length; cx++) {
			String val = entities.getStringByIndex(cols[cx], entid);
			if (val != null)
				selects[cx] = Funcall.constr(val);
		}
		return selects;
	}

}
