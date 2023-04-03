package io.odysz.semantic.meta;

/**
 * <pre>drop table if exists syn_subscribe;
 create table syn_subscribe (
	tabl        varchar2(64) not null, -- e.g. 'h_photos'
	recId       varchar2(12) not null, -- entity record Id
	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id, not null?
	clientpath2 text,                  -- support max 3 fields of composed PK, TODO any betther patterns?
	synodee     varchar2(12) not null  -- subscriber, fk-on-del, synode id device to finish cleaning task
	synyquist   integer      not null  -- last Nyquist sequence number of synodee
 );</pre>
 *
 * @author Ody
 *
 */
public class SynSubsMeta extends SynTableMeta {
	
	public final String synodee;
	public final String nyquencee;

	public static String ddl;
	static {
		ddl = "drop table if exists syn_subscribe;\n" +
			"create table syn_subscribe (\n" + 
			"	tabl        varchar2(64) not null, -- e.g. 'h_photos'\n" + 
			"	recId       varchar2(12) not null, -- entity record Id\n" + 
			"	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id, not null?\n" + 
			"	clientpath2 text,                  -- support max 3 fields of composed PK, TODO any betther patterns?\n" + 
			"	synodee     varchar2(12) not null  -- subscriber, fk-on-del, synode id device to finish cleaning task\n" + 
			"	synyquist   integer      not null  -- last Nyquist sequence number of synodee\n" + 
			" )";
	}

	public SynSubsMeta(String ... conn) {
		super("syn_subscribe", conn);
		this.synodee = "synodee";
		this.nyquencee = "synyquist";
	}

	public String[] cols() {
		return new String[] {pk, recTabl, recId, clientpath, clientpath2, synodee, nyquencee};
	}

}
