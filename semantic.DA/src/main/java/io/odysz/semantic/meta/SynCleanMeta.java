package io.odysz.semantic.meta;

/**
 * <pre>drop table if exists syn_clean;
 create table syn_clean (
	tabl        varchar2(64) not null, -- e.g. 'h_photos'
	synoder     varchar2(12) not null, -- publisher, fk-on-del, synode id for resource's PK
	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id
	clientpath2 text,
	synodee     varchar2(12) not null, -- subscriber, fk-on-del, synode id device to finish cleaning task
	flag        char(1)      not null, -- 'D' deleting, 'C' close (not exists),'R' rejected by device owner
	cleanyquist integer      not null  -- last Nyquist sequence number of synodee
 );</pre>
 *
 * @author odys-z@github.com
 */
public class SynCleanMeta extends SyntityMeta {

	public static String ddlSqlite;
	static {
		ddlSqlite = "drop table if exists syn_clean;\n" +
			"create table syn_clean (\n" + 
			"	tabl        varchar2(64) not null, -- e.g. 'h_photos'\n" + 
			"	synoder     varchar2(12) not null, -- publisher, fk-on-del, synode id for resource's PK\n" + 
			"	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id\n" + 
			"	clientpath2 text,\n" + 
			"	synodee     varchar2(12) not null, -- subscriber, fk-on-del, synode id device to finish cleaning task\n" + 
			"	flag        char(1)      not null, -- 'D' deleting, 'C' close (not exists),'R' rejected by device owner\n" + 
			"	cleanyquist integer      not null  -- last Nyquist sequence number of synodee\n" + 
			" )";
	}


	public final String synodee;
	public final String cleanyquist;
	
	public SynCleanMeta(String ... conn) {
		super("syn_clean", conn);
		
		synodee = "synodee";
		cleanyquist = "cleanyquiest"; 
	}

}
