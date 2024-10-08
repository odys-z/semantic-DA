package io.odysz.semantic.syn;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;

public class ExchangeBlock extends Anson {

	public static final String ExchangeFlagCol = "change";

	public String domain;
	public String srcnode;
	public String peer;
	
	@AnsonField(valType="io.odysz.semantic.syn.Nyquence")
	public HashMap<String, Nyquence> nv;

	public AnResultset chpage;
	public HashMap<String, AnResultset> entities;

	public AnResultset anspage;

	public ExchangeBlock() { }

	/**
	 * Is the synode has more challenge blocks, which is
	 * need to behandled before closing exchange?
	 */
	public boolean hasmore() {
		return chpage != null && chpage.size() > 0
			|| anspage != null && anspage.size() > 0;
	}

	public ExchangeBlock(String domain, String src, String peer, String sessionId, ExessionAct exstate) {
		srcnode = src;
		session = sessionId;
		act = exstate.state;
		this.peer = peer;
		this.domain = domain;
	}
	
	public ExchangeBlock nv(HashMap<String, Nyquence> nyquvect) {
		nv = Nyquence.clone(nyquvect);
		return this;
	}

	public int enitities() {
		return entities == null ? 0
			: entities.values()
				.stream()
				.filter(rs -> rs != null)
				.mapToInt(rs -> rs.getRowCount()).sum();
	}

	public int answers() {
		return anspage != null ? anspage.size() : 0;
	}

	public int enitities(String tbl) {
		return entities == null || !entities.containsKey(tbl)
			? 0 : entities.get(tbl).getRowCount();
	}

	/**
	 * Copy answer from {@code p}
	 * 
	 * @param p
	 * @return this
	 */
	public ExchangeBlock answers(ExessionPersist p) {
		this.anspage = p.answerPage;
		return this;
	}

	public void removeChgsub(AnResultset challenpage, String synode) throws SQLException {
		if (challenpage == null)
			return;
		
		if (anspage == null) {
			// anspage = new ArrayList<ArrayList<Object>>();
			HashMap<String, Object[]> cols = challenpage.colnames();
			if (!cols.containsKey(ChangeLogs.ChangeFlag.toUpperCase()))
				cols.put(ChangeLogs.ChangeFlag.toUpperCase(),
						new Object[] {cols.size() + 1, // column index state at 1
								ChangeLogs.ChangeFlag});
			anspage = new AnResultset(cols);
		}

		if (challenpage != null) {
			ArrayList<Object> row = challenpage.getRowAt(challenpage.currentRow() - 1);
			int flagIx = (int) anspage.colnames().get(ChangeLogs.ChangeFlag.toUpperCase())[0];
			row.add(flagIx - 1, CRUD.U);
			anspage.append(row);
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
	/**
	 * Don't call this directly unless in test
	 * @param count
	 * @return this
	 */
	ExchangeBlock totalChallenges(int count) {
		totalChallenges = count; 
		return this;
	}
	
	public ExchangeBlock totalChallenges(int count, int chsize) {
		totalChallenges = count; 
		chpagesize = chsize; 
		return this;
	}
	
	/////////////////////// Protocol for synode management /////////////////////
	
	AnResultset synodes;

	public ExchangeBlock synodes(AnResultset rs) {
		synodes = rs;
		return this;
	}

	////////////////////////////////// states //////////////////////////////////

	String session;
	/**One of {@link ExessionAct}'s constants. */
	int act;
	/**One of {@link ExessionAct}'s constants. */
	public int synact () { return act; }
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
	 	return seq(xp.challengeSeq < xp.pages() ? xp.challengeSeq : -1, xp.answerSeq, xp.totalChallenges);
	 }

	public ExchangeBlock requirestore() {
		this.act = ExessionAct.restore;
		return this;
	}

	private ExchangeBlock seq(int chidx, int ansidx, int totalChgs) {
		challengeSeq    = chidx;
		answerSeq       = ansidx;
		totalChallenges = totalChgs;
		return this;
	}

	public ExchangeBlock chpage(AnResultset rs, HashMap<String, AnResultset> entities) {
		this.chpage = rs;
		this.entities = entities;
		return this;
	}

	public boolean moreChallenge() {
		return challengeSeq >= 0 && totalChallenges > 0 && chpagesize > 0
				&& (challengeSeq + 1) * chpagesize < totalChallenges;
	}

	public void print(PrintStream out) {
		out.println(String.format("Exchange Package: %s -> %s : %s", srcnode, peer, session));
		out.println(String.format("challenge seq: %s\tanswer-seq %s", challengeSeq, answerSeq));

		if (chpage != null ) {
			out.println("challenge page:");
			chpage.print(out);
		}
		
		if (entities != null) {
			out.println("entities:");
			for (String tbl : entities.keySet()) {
				out.print("\tname: ");
				out.println(tbl);
				entities.get(tbl).print(out);
			}
		}
		
		if (anspage != null) {
			out.println("answer page:");
			anspage.print(out);
		}
		
		if (synodes != null) {
			out.println("synodes:");
			synodes.print(out);
		}
	}
}
