package io.odysz.semantic;

/**
 * <pre>
 drop table if exists syn_change;
 create table syn_change (
 	nyquence    varchar2(12),          -- optional change id
	tabl        varchar2(64) not null, -- e.g. 'h_photos'
	recId       varchar2(12) not null, -- entity record Id
	synoder     varchar2(12) not null, -- publisher, fk-on-del, synode id for resource's PK
	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id, not null?
	clientpath2 text,                  -- support max 3 fields of composed PK, TODO any betther patterns?
	crud        char(1)      not null  -- I/U/D/R/E
 );</pre>
 *
 * @author Ody
 */
public class SynChangeMeta extends SynTableMeta {

	public String crud;
	public final String synoder;

	public SynChangeMeta(String ... conn) {
		super("syn_change", conn);

		pk = "nyquence";
		
		synoder = "synoder";
		crud = "crud";
	}

	public String[] cols() {
		return new String[] {pk, recTabl, recId, clientpath, clientpath2, synoder, crud};
	}

}
