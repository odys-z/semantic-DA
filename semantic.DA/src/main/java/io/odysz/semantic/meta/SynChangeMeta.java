package io.odysz.semantic.meta;

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
public class SynChangeMeta extends SyntityMeta {

	public String crud;
	public final String synoder;

	public static String ddlSqlite;
	
	static {
		ddlSqlite = "drop table if exists syn_change;\n" +
			"create table syn_change (\n" + 
			" 	nyquence    varchar2(12),          -- optional change id\n" + 
			"	tabl        varchar2(64) not null, -- e.g. 'h_photos'\n" + 
			"	recId       varchar2(12) not null, -- entity record Id\n" + 
			"	synoder     varchar2(12) not null, -- publisher, fk-on-del, synode id for resource's PK\n" + 
			"	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id, not null?\n" + 
			"	clientpath2 text,                  -- support max 3 fields of composed PK, TODO any betther patterns?\n" + 
			"	crud        char(1)      not null  -- I/U/D/R/E\n" + 
			");"; 
	}

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
