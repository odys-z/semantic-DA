package io.odysz.semantic.syn;

import io.odysz.anson.Anson;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.x.TransException;

/**
 * <b>Design Note</b>
 * <p>An entity type for used in the protocol layer.</p>
 * 
 * @author odys-z@github.com
 *
 */
public class Synode extends Anson {

	public final String org;
	public final String recId;

	public Synode(String conn, String synid, String family) throws TransException {
		this.recId = synid;
		this.org = family;
	}
	
	/**
	 * Format the insert statement according to my fields.
	 * 
	 * @param synm
	 * @param insert
	 * @return insert
	 * @throws TransException
	 */
	public Insert insert(SynodeMeta synm, Insert insert) throws TransException {
		return insert
			.nv(synm.pk, recId)
			.nv(synm.mac, "#")
			.nv(synm.domain, org);
	}
	
}
