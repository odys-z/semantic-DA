package io.odysz.semantic.meta;

import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.semantic.syn.Nyquence;

public class PeersMeta extends SemanticTableMeta {

	/** The target id at this synode, nv.key. */
	public final String synid;
	/** Synssion peer */
	public final String peer;
	public final String domain;
	public final String nyq;

	public final String[] inscols;
	
	public PeersMeta(String... conn) {
		super("syn_peers", conn);
		synid  = "synid";
		pk     = synid;
		peer   = "peer";
		domain = "domain";
		nyq    = "nyq";
		
		inscols = new String[] {synid, peer, domain, nyq};

		ddlSqlite = loadSqlite(PeersMeta.class, "syn_peers.sqlite.ddl");
	}

	public ArrayList<ArrayList<Object[]>> insVals(HashMap<String, Nyquence> nv, String peer, String domain) {
		ArrayList<ArrayList<Object[]>> vals = new ArrayList<ArrayList<Object[]>>();
		if (nv != null)
			for (String n : nv.keySet()) {
				ArrayList<Object[]> row = new ArrayList<Object[]>();
				row.add(new Object[] {this.synid, n});
				row.add(new Object[] {this.peer, peer});
				row.add(new Object[] {this.domain, domain});
				row.add(new Object[] {this.nyq, nv.get(n).n});
				vals.add(row);
			}
		return vals;
	}

}
