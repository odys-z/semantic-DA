package io.odysz.semantic.meta;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;

/**
 * <a href="syn_sbuscribe.ddl.sqlite">syn_sbuscribe DDL</a>
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
public class SynSubsMeta extends SyntityMeta {
	
	public final String synodee;
	public final String nyquencee;
	public final String entId;

	public static String ddlSqlite;
	static {
		/*
		ddlSqlite = "drop table if exists syn_subscribe;\n" +
			"create table syn_subscribe (\n" + 
			"	tabl        varchar2(64) not null, -- e.g. 'h_photos'\n" + 
			"	recId       varchar2(12) not null, -- entity record Id\n" + 
			"	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id, not null?\n" + 
			"	clientpath2 text,                  -- support max 3 fields of composed PK, TODO any betther patterns?\n" + 
			"	synodee     varchar2(12) not null  -- subscriber, fk-on-del, synode id device to finish cleaning task\n" + 
			"	synyquist   integer      not null  -- last Nyquist sequence number of synodee\n" + 
			" )";
		*/
		try {
			ddlSqlite = Files.readAllLines(
					Paths.get(SynSubsMeta.class.getResource("syn_subscribe.ddl.sqlite").toURI()), Charset.defaultCharset())
					.stream().collect(Collectors.joining("\n"));
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public SynSubsMeta(String ... conn) {
		super("syn_subscribe", conn);
		this.synodee = "synodee";
		this.nyquencee = "synyquist";
		this.entId = "recId";
	}

	public String[] cols() {
		return new String[] {pk, recTabl, recId, clientpath, clientpath2, synodee, nyquencee};
	}

}
