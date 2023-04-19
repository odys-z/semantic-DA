package io.odysz.semantic.meta;

import io.odysz.semantics.meta.TableMeta;

/**
 * Syncrhonizable entity table meta
 * 
 * @author odys-z@github.com
 *
 */
public abstract class SyntityMeta extends TableMeta {

	/** entity creator id used for identify globally (experimental) */
	public final String synoder;
	public final String clientpath;
	public final String nyquence;
	
	public SyntityMeta(String tbl, String pk, String... conn) {
		super(tbl, conn);

		this.pk = pk;
		
		synoder = "synoder";
		clientpath = "clientpath";
		nyquence = "nyquence";
	}
}
