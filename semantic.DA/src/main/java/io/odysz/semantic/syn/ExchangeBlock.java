package io.odysz.semantic.syn;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.anson.Anson;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;

public class ExchangeBlock extends Anson {

	public static final String ExchangeFlagCol = "change";


	String srcnode;
	String peer;
	HashMap<String, Nyquence> nv;

	public AnResultset chpage;
	// public int challenges;
	public ArrayList<ArrayList<Object>> anspage;

	/**Server has more challenge blocks, which need to be pulled before closing exchange */
	public boolean moreChallenges;

	public ExchangeBlock(String src, String peer, String sessionId, ExessionAct exstate) {
		srcnode = src;
		session = sessionId;
		act = exstate.state;
		this.peer = peer;
	}
	
	public ExchangeBlock nv(HashMap<String, Nyquence> nyquvect) {
		nv = Nyquence.clone(nyquvect);
		return this;
	}

	public int enitities() { return 0; }

	public ArrayList<ArrayList<Object>>  answers() {
		return anspage;
	}

	public int enitities(String tbl) {
		return 0;
	}

	public ExchangeBlock answers(ExessionPersist p) {
		this.anspage = p.answerPage;
		return this;
	}

	public void removeChgsub(AnResultset challenpage, String synode) throws SQLException {
		if (anspage == null) 
			anspage = new ArrayList<ArrayList<Object>>();

		if (challenpage != null) {
			ArrayList<Object> row = challenpage.getRowAt(challenpage.currentRow() - 1);
			row.add(row.size(), CRUD.U);
			anspage.add(row);
		}
	}
	
	/**
	 * Check and extend column {@link #ChangeFlag}, which is for changing flag of change-logs.
	 * 
	 * @param answer
	 * @return this
	 */
	static HashMap<String,Object[]> extendFlagCol(HashMap<String, Object[]> colnames) {
		if (!colnames.containsKey(ExchangeFlagCol.toUpperCase())) {
			colnames.put(ExchangeFlagCol.toUpperCase(),
				new Object[] {Integer.valueOf(colnames.size() + 1), ExchangeFlagCol});
		}
		return colnames;
	}

	int chpagesize;
	public ExchangeBlock chpagesize(int size) {
		chpagesize = size;
		return this;
	}

	int totalChallenges;
	public ExchangeBlock totalChallenges(int count) {
		totalChallenges = count; 
		return this;
	}
	
	public ExchangeBlock totalChallenges(int count, int chsize) {
		totalChallenges = count; 
		chpagesize = chsize; 
		return this;
	}

	////////////////////////////////// states //////////////////////////////////

	String session;
	int act;
	int challengeSeq;
	int answerSeq;

	/**
	 * Set challengeId &amp; answerId
	 * <pre>
	 * this.challengeId = xp.challengeId;
	 * this.answerId = xp.answerId;</pre>
	 * 
	 * @param xp Persist
	 * @return this
	 */
	public ExchangeBlock seq(ExessionPersist xp) {
		return seq(xp.challengeSeq < xp.pages() ? xp.challengeSeq : -1, xp.answerSeq);
	}

	public ExchangeBlock requirestore() {
		this.act = ExessionAct.restore;
		return this;
	}

	public ExchangeBlock seq(int chidx, int ansidx) {
		challengeSeq = chidx;
		answerSeq    = ansidx;
		return this;
	}

//	public ExchangeBlock totalChanges(int total) {
//		this.challenges = total;
//		return this;
//	}

	public ExchangeBlock chpage(AnResultset rs) {
		this.chpage = rs;
		return this;
	}

	public boolean moreChallenge() {
		return challengeSeq >= 0 && totalChallenges > 0 && chpagesize > 0
				&& (challengeSeq + 1) * chpagesize < totalChallenges;
	}
}
