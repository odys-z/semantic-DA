package io.odysz.semantic.syn;

import java.util.HashMap;

import io.odysz.anson.Anson;
import io.odysz.module.rs.AnResultset;

public class ExchangeBlock extends Anson {

	String srcnode;
	HashMap<String, Nyquence> nv;

	public int challengeId;
	public int answerId;

	public ExchangeBlock(String synode, HashMap<String, Nyquence> nyquvect) {
		srcnode = synode;
		nv = Nyquence.clone(nyquvect);
	}

	public int challenges() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int enitities() { return 0; }

	public AnResultset  answers() {
		return null;
	}

	public int enitities(String tbl) {
		return 0;
	}

	public int changes() {
		return 0;
	}

	String session;

}
