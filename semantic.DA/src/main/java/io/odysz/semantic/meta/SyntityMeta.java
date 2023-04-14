package io.odysz.semantic.meta;

import io.odysz.semantics.meta.TableMeta;

/**
 * Syncrhonizable entity table meta
 * 
 * @author odys-z@github.com
 *
 */
public abstract class SyntityMeta extends TableMeta {

	public final String synoder;
	public final String clientpath;
	
	public SyntityMeta(String tbl, String pk, String... conn) {
		super(tbl, conn);

		this.pk = pk;
		
		synoder = "synoder";
		clientpath = "clientpath";
	}
}
