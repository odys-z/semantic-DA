package io.odysz.semantic.meta;

import java.util.HashSet;

import io.odysz.transact.x.TransException;

/**
 * 
 * <a href='./syn_nyquence.sqlite.ddl'>syn_nyquence.ddl</a>
 * 
 * @author odys-z@github.com
 *
 */
public class NyquenceMeta extends SyntityMeta {
	
	public final String org;
	public final String synode;
	public final String entbl;
	public final String nyquence;
	public final String inc;

	public NyquenceMeta(String... conn) throws TransException {
		super("syn_nyqunce", "synode", conn);
		
		nyquence = "nyquence";
		inc = "inc";
		entbl = "tabl";
		synode = "synode";
		org = "org";
	}

	@Override
	public HashSet<String> globalIds() {
		// shouldn't reach here
		return null;
	}

}
