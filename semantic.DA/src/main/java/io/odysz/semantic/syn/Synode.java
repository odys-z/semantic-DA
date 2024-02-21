package io.odysz.semantic.syn;

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
public class Synode extends SynEntity {

	public final String org;

	public Synode(String conn, String synid, String family) throws TransException {
		super(new SynodeMeta(conn));
		
		this.recId = synid;
		this.org = family;
	}
	
//	public Synode clientpath(String path, String... path2) {
//		clientpath = path;
//		clientpath2 = isNull(path2) ? null :path2[0];
//		return this;
//	}

	/**
	 * Format the table record according to my fields.
	 * 
	 * @param synm
	 * @param insert
	 * @return
	 * @throws TransException
	 */
	public Insert insert(SynodeMeta synm, Insert insert) throws TransException {
		return insert
			.nv(synm.pk, recId)
			.nv(synm.synoder, synoder)
			.nv(synm.org(), org);
	}
	
}
