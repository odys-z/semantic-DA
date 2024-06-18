package io.odysz.semantic.syn;

import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;

/**
 * A synchronizable entity managed by the package, also a server side
 * and jprotocol oriented data record, used for record synchronizing
 * in docsync.jserv. 
 * 
 * @author Ody
 */
public class SynEntity extends Anson {
	protected static String[] synpageCols;

	public String recId;
	public String recId() { return recId; }
	public SynEntity recId(String did) {
		recId = did;
		return this;
	}

	public String uids;

	/** Non-public: doc' device id is managed by session. */
	protected String synode;
	public String synode() { return synode; }
	public SynEntity synode(String synode) {
		this.synode = synode;
		return this;
	}

	@AnsonField(ignoreTo=true)
	protected SyntityMeta entMeta;

	@AnsonField(ignoreTo=true)
	protected SynSubsMeta subMeta;

	@AnsonField(ignoreTo=true)
	protected SynChangeMeta chgm;

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	ISemantext semantxt;

	protected ArrayList<String[]> subs;

	protected String synoder;
	protected Nyquence nyquence;
	
	public SynEntity(AnResultset rs, SyntityMeta entity, SynChangeMeta change) throws SQLException {
		this.entMeta = entity;
		this.chgm = change;
		this.subMeta = new SynSubsMeta(change);

		format(rs);
	}

	public SynEntity(SyntityMeta entm) {
		this.entMeta = entm;
		this.chgm = new SynChangeMeta();
		this.subMeta = new SynSubsMeta(chgm);
	}

	public SynEntity(AnResultset rs, SyntityMeta meta) throws SQLException {
		this(rs, meta, new SynChangeMeta());
	}

	public SynEntity check(String conn, DBSynsactBuilder tr0, ArrayList<String[]> subs) {
		this.subs = subs;
		return this;
	}

	/**
	 * Format entity synchronization task
	 * @return this
	 * @throws SQLException 
	 */
	public SynEntity format(AnResultset rs) throws SQLException {
		this.recId = rs.getString(entMeta.pk);
		this.synode =  rs.getString(chgm.synoder);
		return this;
	}
}