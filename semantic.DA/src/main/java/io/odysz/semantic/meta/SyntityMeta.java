package io.odysz.semantic.meta;

import io.odysz.semantics.meta.TableMeta;

/**
 * Syncrhonizable entity table meta
 * 
 * @author odys-z@github.com
 *
 */
public class SyntityMeta extends TableMeta {

	public final String oper;

	public final String synoder;

	public final String recTabl;
	public final String entFk;
	public final String clientpath;
	public final String clientpath2;

	public final String nyquence;

	public SyntityMeta(String tbl, String... conn) {
		super(tbl, conn);
		
		recTabl = "tabl";
		entFk = "recId";
		synoder = "synoder";
		clientpath = "clientpath";
		clientpath2 = "clientpath2";
		
		oper = "oper";
		nyquence = "nyq";
	}

	public String[] cols() {
		return new String[] {recTabl, entFk, synoder, clientpath, clientpath2, oper, nyquence};
	}
}
