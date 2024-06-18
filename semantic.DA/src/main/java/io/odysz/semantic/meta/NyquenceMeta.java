package io.odysz.semantic.meta;

import java.util.ArrayList;
import java.util.HashSet;

import io.odysz.module.rs.AnResultset;

/**
 * @deprecated
 * <a href='./syn_nyquence.sqlite.ddl'>syn_nyquence.ddl</a>
 * 
 * @author odys-z@github.com
 *
 */
public class NyquenceMeta extends SyntityMeta {
	
	public final String synode;
	// public final String entbl;
	public final String nyquence;
	public final String inc;

	public NyquenceMeta(String domain, String conn) {
		super("syn_nyquence", "synode", domain, "test", conn);
		
		nyquence = "nyquence";
		inc = "inc";
		// entbl = "tabl";
		synode = "synode";
	}

	@Override
	public HashSet<String> globalIds() {
		// shouldn't reach here
		return null;
	}

	@Override
	public ArrayList<Object[]> updateEntNvs(SynChangeMeta chgm, String entid, AnResultset e, AnResultset ch) {
		return null;
	}

	@Override
	public String[] insertSelectItems(SynChangeMeta chgm, String entid, AnResultset e, AnResultset ch) {
		return null;
	}

}
