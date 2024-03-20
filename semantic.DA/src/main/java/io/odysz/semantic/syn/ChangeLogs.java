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

	/** Entity tables' column names */
	// public HashMap<String, HashMap<String, Object[]>> entCols;

	HashMap<String, Object[]> changeCols;

	@SuppressWarnings("unused")
	private boolean dirty;

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
		setChangeCols(chgs.colnames());
		ArrayList<Object> row = chgs.getRowAt(chgs.currentRow()-1);
		row.add(CRUD.U);

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
		setChangeCols(remote.colnames());
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

	/**
	 * Copy columns from resultset, adding field {@link #ChangeFlag} as last field
	 * @param answer
	 * @return this
	 */
	public ChangeLogs setChangeCols(ChangeLogs answer) {
		return setChangeCols(answer.changeCols);
	}
	
	protected ChangeLogs setChangeCols(HashMap<String, Object[]> colnames) {
		this.changeCols = colnames;
		
		if (!changeCols.containsKey(ChangeFlag.toUpperCase())) {
			changeCols.put(ChangeFlag.toUpperCase(),
				new Object[] {Integer.valueOf(changeCols.size() + 1), ChangeFlag});
		}
		return this;
	}

	public AnResultset challenge;

	public ChangeLogs challenge(AnResultset challenge) {
		this.challenge = challenge;
		return this;
	}

	public void clear() {
		challenge = null;
		answers = null;
		entities = null;
		dirty = false;
	}

	// public HashMap<String, HashMap<String, ? extends SynEntity>> entities;
	HashMap<String, AnResultset> entities;
	HashMap<String, Nyquence> nyquvect;

	public ChangeLogs nyquvect(HashMap<String, Nyquence> nyquvect) {
		this.nyquvect = nyquvect;
		return this;
	}

	public ChangeLogs entities(String tbl, AnResultset entities) {
		if (this.entities == null)
			this.entities = new HashMap<String, AnResultset>();
		this.entities.put(tbl, entities);
		return this;
	}

	/** Get challenge's row count */
	public int challenges() {
		return challenge == null ? 0 : challenge.getRowCount();
	}

	public AnResultset answers() {
		return new AnResultset(changeCols).results(answers);
	}
}
