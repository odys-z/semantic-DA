package io.odysz.semantic.syn;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantics.x.SemanticException;

/**
 * Data structure for exchange syn_change."
 * 
 * @since 1.5.0
 */
public class ChangeLogs extends Anson {

	public static final String ChangeFlag = "change";

	@AnsonField(ignoreTo=true)
	SynChangeMeta chm;
	
	HashMap<String, Nyquence> nyquvect;

	/** 
	 * clone {@code nyquvect} into my nyq-vector.
	 * @param nyquvect
	 * @return
	 */
	public ChangeLogs nyquvect(final HashMap<String, Nyquence> nyquvect) {
		this.nyquvect = Nyquence.clone(nyquvect);
		return this;
	}

	/**
	 * 0: change statement, CRUD.C: insert, CRUD.U: remove-subs, CRUD.D: remove-log),<br>
	 * 1: change-crud,<br> 2: synoder,<br> 3: uids,<br> 4: nyquence<br> 
	 */
	AnResultset answers;

	public AnResultset challenge;
	public ChangeLogs challenge(AnResultset challenge) {
		this.challenge = challenge;
		return this;
	}

	public ChangeLogs(SynChangeMeta changemeta) {
		this.chm = changemeta;
	}

	/**
	 * Add remove command to change log, chgs.
	 * @param challenge
	 * @param synode
	 * @throws SQLException
	 */
	@SuppressWarnings("serial")
	public void remove_sub(AnResultset challenge, String synode) throws SQLException {
		if (answers == null) 
			answers = new AnResultset(checkChangeCol(challenge.getColnames()))
						.results(new ArrayList<ArrayList<Object>>() {});

		ArrayList<Object> row = challenge.getRowAt(challenge.currentRow()-1);
		row.add(answers.getColumex(ChangeFlag)-1, CRUD.U);
		answers.append(row);
	}

	public static Nyquence parseNyq(Object[] c) {
		return new Nyquence((long)c[4]);
	}

	/**
	 * Check and extend column {@link #ChangeFlag}, which is for changing flag of change-logs.
	 * 
	 * @param colnames
	 * @return this
	 */
	public static HashMap<String,Object[]> checkChangeCol(HashMap<String, Object[]> colnames) {
		if (!colnames.containsKey(ChangeFlag.toUpperCase())) {
			colnames.put(ChangeFlag.toUpperCase(),
				new Object[] {Integer.valueOf(colnames.size() + 1), ChangeFlag});
		}
		return colnames;
	}

	public void clear() {
		challenge = null;
		answers = null;
		entities = null;
	}

	HashMap<String, AnResultset> entities;

	public ChangeLogs entities(String tbl, AnResultset entities) {
		if (this.entities == null)
			this.entities = new HashMap<String, AnResultset>();
		this.entities.put(tbl, entities);
		return this;
	}
	
	public ChangeLogs entities(HashMap<String, AnResultset> entities) throws SemanticException {
		if (this.entities != null)
			throw new SemanticException("There are entities already exist to be handled.");
		this.entities = entities;
		return this;
	}

	/** Get challenge's row count */
	public int challenges() {
		return challenge == null ? 0 : challenge.getRowCount();
	}

	public int enitities(String tbl) {
		return entities != null && entities.containsKey(tbl) ? entities.get(tbl).size() : 0;
	}

	public int answers() {
		return answers == null ? 0 : answers.getRowCount();
	}

	AnResultset synodes;

	public ChangeLogs synodes(AnResultset rs) {
		synodes = rs;
		return this;
	}

	public Object enitities() {
		return this.entities == null ? 0
			: this.entities.values().stream().mapToInt(r -> r.getRowCount()).sum();
	}

	private Exchanging step;
	public Exchanging stepping() {
		return step;
	}
	
	/**
	 * Set mode and state.
	 * @param m
	 * @param s
	 * @return this
	 */
	public ChangeLogs stepping(int m, int s) {
		step = new Exchanging(m);
		step.state = s;
		return this;
	}

	private String session;
	public String session() { return session; }
	public ChangeLogs session(String ss) { 
		session = ss;
		return this;
	}
}
