package io.odysz.semantic.meta;

import java.util.HashSet;

import io.odysz.semantic.syn.DBSynmantics;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * Syncrhonizable entity table meta
 * 
 * @author odys-z@github.com
 *
 */
public abstract class SyntityMeta extends TableMeta {

	public final String org;
	/** entity creator id used for identify globally (experimental) */
	public final String synoder;
	public final String uids;
	// public final String nyquence;
	
	public SyntityMeta(String tbl, String pk, String... conn) throws TransException {
		super(tbl, conn);

		this.pk = pk;
		
		org = "org";
		synoder = "synoder";
		uids = "clientpath";
		// nyquence = "nyquence";
		
		DBSynmantics.replaceMeta(tbl, this, conn);
	}
	
	public abstract HashSet<String> globalIds();
}
