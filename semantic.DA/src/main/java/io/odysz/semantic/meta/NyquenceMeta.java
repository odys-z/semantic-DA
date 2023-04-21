package io.odysz.semantic.meta;

import io.odysz.semantics.meta.TableMeta;

/**
 * 
 * <a href='./syn_nyquence.sqlite.ddl'>syn_nyquence.ddl</a>
 * 
 * @author odys-z@github.com
 *
 */
public class NyquenceMeta extends TableMeta {
	
	public final String org;
	public final String synode;
	public final String entbl;
	public final String nyquence;
	public final String inc;

	public NyquenceMeta(String... conn) {
		super("syn_nyqunce", conn);
		
		nyquence = "nyquence";
		inc = "inc";
		entbl = "tabl";
		synode = "synode";
		org = "org";
	}

}
