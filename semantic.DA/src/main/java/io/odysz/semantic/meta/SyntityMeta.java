package io.odysz.semantic.meta;

import java.util.HashSet;

import io.odysz.semantic.syn.DBSynmantics;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.transact.x.TransException;

/**
 * Syncrhonizable entity table meta
 * 
 * @since 1.5.0
 * @author odys-z@github.com
 *
 */
public abstract class SyntityMeta extends TableMeta {

	/** exposed to subclass to change
	 * @see SyntityMeta#SyntityMeta */
	protected String org;
	public String org() { return org; }

	/** entity creator id used for identify globally (experimental) */
	public final String synoder;
	public final String uids;
	
	/**
	 * @param tbl
	 * @param pk
	 * @param org DB field of orgnization - {@link io.odysz.semantic.syn.DBSynmantics}
	 * uses this to filter data for synchronization.
	 * Could be changed in the future.
	 * @param conn
	 */
	public SyntityMeta(String tbl, String pk, String org, String... conn) {
		super(tbl, conn);

		this.org = org;
		synoder = "synoder";
		uids = "clientpath";
	}
	
	/**
	 * Explicitly call this after this meta with semantics is created,
	 * to replace auto found meta from database.
	 * 
	 * @return this
	 * @throws TransException
	 */
	@SuppressWarnings("unchecked")
	public <T extends SyntityMeta> T replace() throws TransException {
		DBSynmantics.replaceMeta(tbl, this, conn);
		return (T) this;
	}
	
	public abstract HashSet<String> globalIds();
}
