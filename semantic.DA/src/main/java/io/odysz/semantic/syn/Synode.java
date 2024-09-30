package io.odysz.semantic.syn;

import java.sql.SQLException;

import io.odysz.anson.Anson;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.x.TransException;

/**
 * Synode Information.
 * 
 * @author odys-z@github.com
 */
public class Synode extends Anson {

	public final String org;
	public final String synodeId;

	String mac;
	String domain;
	long nyquence;

	public Synode(String conn, String synid, String org, String domain) throws TransException {
		this.synodeId = synid;
		this.org = org;
		this.domain = domain;
	}
	
	public Synode(AnResultset r, SynodeMeta synm) throws SQLException {
		this.org = r.getString(synm.org);
		this.synodeId = r.getString(synm.pk);
		this.mac = r.getString(synm.device);
		this.domain = r.getString(synm.domain);
		this.nyquence = r.getLong(synm.nyquence);
	}

	/**
	 * Format the insert statement according to my fields.
	 * Example:<pre>insert(synm, synode, n0(), tranxbuilder.insert(synm.tbl, robot))</pre>
	 * 
	 * @param synm
	 * @param insert
	 * @return {@link Insert} statement
	 * @throws TransException
	 */
	public Insert insert(SynodeMeta synm, String syn_uid, Nyquence n0, Insert insert) throws TransException {
		return insert
			.nv(synm.pk, synodeId)
			.nv(synm.device, "#" + synodeId)
			.nv(synm.nyquence, n0.n)
			.nv(synm.domain, domain)
			// .nv(synm.synuid, SynChangeMeta.uids(creator, synodeId))
			.nv(synm.synuid, syn_uid)
			.nv(synm.org, org);
	}
	
}
