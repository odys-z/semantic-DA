package io.oz.syn;

import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.anson.Anson;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.x.TransException;

/**
 * Synode Information.
 * 
 * @author odys-z@github.com
 */
public class Synode extends Anson {

	public String org;
	public String synid;

	String mac;

	String domain;
	public String domain() { return domain; }
	
	/**
	 * @since 0.7.6 "hub" or null
	 */
	public String remarks;

	long nyquence;
	String syn_uid;

	public Synode() {
	}

	/**
	 * 
	 * @param synid
	 * @param synuid must be null when joining by peer, must be provided without synmantics handling 
	 * @param org
	 * @param domain
	 * @throws TransException
	 */
	public Synode(String synid, String synuid, String org, String domain, String remarks) throws TransException {
		this.synid = synid;
		this.org = org;
		this.domain  = domain;
		this.remarks = remarks;
		this.syn_uid = synuid;
	}
	
	public Synode(AnResultset r, SynodeMeta synm) throws SQLException {
		this.org = r.getString(synm.org);
		this.synid = r.getString(synm.pk);
		this.mac = r.getString(synm.device);
		this.domain = r.getString(synm.domain);
		this.remarks = r.getString(synm.remarks);
		this.nyquence = r.getLong(synm.nyquence);
		this.syn_uid = r.getString(synm.io_oz_synuid);
	}

	/**
	 * Format the insert statement according to my fields.
	 * Example:<pre>insert(synm, synode, n0(), tranxbuilder.insert(synm.tbl, robot))</pre>
	 * 
	 * @param synm
	 * @param syn_uid global synuid
	 * @param n0
	 * @return {@link Insert} statement
	 * @throws TransException
	 */
	public Insert insert(SynodeMeta synm, String syn_uid, Nyquence n0, Insert insert) throws TransException {
		return insert
			.nv(synm.pk, synid)
			.nv(synm.device, "#" + synid)
			.nv(synm.nyquence, n0.n)
			.nv(synm.domain, domain)
			.nv(synm.remarks, remarks)
			.nv(synm.io_oz_synuid, syn_uid)
			.nv(synm.org, org);
	}

	/**
	 * Format the insert statement according to my fields.
	 * @param domain 
	 * @param synm
	 * @param insert
	 * @return
	 * @throws TransException
	 * @since 
	 */
	public Insert insertRow(String domain, SynodeMeta synm, Insert insert) throws TransException {
		return insert
		  .cols(synm.pk, synm.device, synm.nyquence, synm.domain, synm.remarks, synm.io_oz_synuid, synm.org)
		  .value(new ArrayList<Object[]>() {
			private static final long serialVersionUID = 1L;
			{add(new Object[] {synm.pk, synid});}
			// {add(new Object[] {synm.device, "#" + synid});}
			// 0.7.6
			{add(new Object[] {synm.device, mac});}
			{add(new Object[] {synm.nyquence, nyquence});}
			{add(new Object[] {synm.domain, domain});}
			{add(new Object[] {synm.remarks, remarks});}
			{add(new Object[] {synm.io_oz_synuid, syn_uid});}
			{add(new Object[] {synm.org, org});}
		});
	}


}
