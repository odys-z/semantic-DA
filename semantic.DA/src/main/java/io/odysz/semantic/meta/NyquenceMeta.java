package io.odysz.semantic.meta;

import java.util.HashSet;

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

	public NyquenceMeta(String org, String... conn) {
		super("syn_nyquence", "synode", org, conn);
		
		nyquence = "nyquence";
		inc = "inc";
		// entbl = "tabl";
		synode = "synode";
		this.org = org;
	}

	@Override
	public HashSet<String> globalIds() {
		// shouldn't reach here
		return null;
	}

}
