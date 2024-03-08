package io.odysz.semantic.syn;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.meta.SynChangeMeta;

public class ChangeLogs extends Anson {

	@AnsonField(ignoreTo=true)
	SynChangeMeta chm;
	
	String entble;
	/**
	 * 0: change statement, CRUD.C: insert, CRUD.U: remove-subs, CRUD.D: remove-log),<br>
	 * 1: change-crud,<br> 2: synoder,<br> 3: uids,<br> 4: nyquence<br> 
	 */
	ArrayList<ArrayList<Object>> changes;

	private HashMap<String, Object[]> columns;

	private AnResultset rs;

	private boolean dirty;

	/** max nyquence */
	public Nyquence maxn;

	public ChangeLogs(SynChangeMeta changemeta) {
		this.chm = changemeta;
		dirty = false;
	}

	public void remove_sub(AnResultset chgs, String synode) throws SQLException {
		if (this.columns == null)
			setColumms(chgs.colnames());
		ArrayList<Object> row = chgs.getRowAt(chgs.currentRow());
		row.add(getColIndex(chm.crud), CRUD.U);
		dirty = true;
	}

	public void remove(AnResultset chgs) throws SQLException {
		if (this.columns == null)
			setColumms(chgs.colnames());
		ArrayList<Object> row = chgs.getRowAt(chgs.currentRow());
		row.add(CRUD.D);
		dirty = true;
	}

	public void append(AnResultset dchgs) throws SQLException {
		if (this.columns == null)
			setColumms(dchgs.colnames());
		ArrayList<Object> row = dchgs.getRowAt(dchgs.currentRow());
		row.add(CRUD.C);
		dirty = true;
	}

	public static Nyquence parseNyq(Object[] c) {
		return new Nyquence((long)c[4]);
	}

	protected int getColIndex(String col) {
		return (int)columns.get(col.toUpperCase())[0];
	}

	protected int setColumms(HashMap<String, Object[]> colnames) {
		columns = colnames;
		if (!columns.containsKey(chm.crud)) {
			columns.put(chm.crud, new Object[] {Integer.valueOf(colnames.size()), chm.crud});
		}
		dirty = true;
		return columns.size();
	}
	
	AnResultset rs() {
		if (rs == null || dirty) {
			rs = new AnResultset(columns, true).results(changes);
		}
		return rs;
	}
}
