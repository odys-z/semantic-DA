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

	public static final String ChangeFlag = "change";

	@AnsonField(ignoreTo=true)
	SynChangeMeta chm;
	
	String entble;
	/**
	 * 0: change statement, CRUD.C: insert, CRUD.U: remove-subs, CRUD.D: remove-log),<br>
	 * 1: change-crud,<br> 2: synoder,<br> 3: uids,<br> 4: nyquence<br> 
	 */
	ArrayList<ArrayList<Object>> answers;

	HashMap<String, Object[]> columns;

	AnResultset rs;

	private boolean dirty;

	/** max nyquence */
	public Nyquence maxn;

	public ChangeLogs(SynChangeMeta changemeta) {
		this.chm = changemeta;
		this.answers = new ArrayList<ArrayList<Object>>();
		dirty = false;
	}

	/**
	 * Add remove command to change log, chgs.
	 * @param chgs
	 * @param synode
	 * @throws SQLException
	 */
	public void remove_sub(AnResultset chgs, String synode) throws SQLException {
		if (this.columns == null)
			setColumms(chgs.colnames());
		ArrayList<Object> row = chgs.getRowAt(chgs.currentRow()-1);
		row.add(CRUD.U);

		if (answers == null)
			answers = new ArrayList<ArrayList<Object>>();
		answers.add(row);

		dirty = true;
	}

	public void remove(AnResultset chgs) throws SQLException {
		if (this.columns == null)
			setColumms(chgs.colnames());
		ArrayList<Object> row = chgs.getRowAt(chgs.currentRow()-1);
		row.add(CRUD.D);

		if (answers == null)
			answers = new ArrayList<ArrayList<Object>>();
		answers.add(row);

		dirty = true;
	}

	/**
	 * Append changes' current row to committing tasks.
	 * @param remote
	 * @throws SQLException
	 */
	public void append(AnResultset remote) throws SQLException {
		if (this.columns == null)
			setColumms(remote.colnames());
		ArrayList<Object> row = remote.getRowAt(remote.currentRow()-1);
		row.add(CRUD.C);

		if (answers == null)
			answers = new ArrayList<ArrayList<Object>>();
		answers.add(row);

		dirty = true;
	}

	public static Nyquence parseNyq(Object[] c) {
		return new Nyquence((long)c[4]);
	}

	protected int getColIndex(String col) {
		return (int)columns.get(col.toUpperCase())[0];
	}

	/**
	 * Copy columns from resultset, adding field {@link #ChangeFlag} as last field
	 * @param colnames
	 * @return this
	 */
	protected ChangeLogs setColumms(HashMap<String, Object[]> colnames) {
		columns = colnames;
		if (!columns.containsKey(chm.crud)) {
			columns.put(ChangeFlag, new Object[] {Integer.valueOf(colnames.size()), chm.crud});
		}
		dirty = true;
		return this;
	}
	
	AnResultset rs() {
		if (rs == null || dirty) {
			if (answers != null)
				rs = new AnResultset(columns, true).results(answers);
		}
		return rs;
	}

	public ChangeLogs maxn(long n) {
		maxn = new Nyquence(n);
		return this;
	}

	AnResultset challenge;
	public ChangeLogs exchange(AnResultset challenge) {
		this.challenge = challenge;
		return this;
	}

	public void clear() {
		challenge = null;
		answers = null;
	}
}
