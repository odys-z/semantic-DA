package io.odysz.semantic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import io.odysz.common.Configs;
import io.odysz.common.Utils;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

class DBSyntextTest {
	public static final String connId = "local-sqlite";
	private static DBSynsactBuilder ct;
	private static IUser robot;
	private static HashMap<String, DBSynmantics> smtcfg;
	private static HashMap<String, TableMeta> metas;

	public static final String rtroot = "src/test/res/";
	private static String runtimepath;

	static PhotoMeta m;

	static {
		try {
			Utils.printCaller(false);

			// File file = new File("src/test/res");
			File file = new File(rtroot);
			runtimepath = file.getAbsolutePath();
			Utils.logi(runtimepath);
			Configs.init(runtimepath);
			Connects.init(runtimepath);

			// load metas, then semantics
			DATranscxt.configRoot(rtroot, runtimepath);
			String rootkey = System.getProperty("rootkey");
			DATranscxt.key("user-pswd", rootkey);

			smtcfg = DBSynsactBuilder.loadSynmantics(connId, "src/test/res/synmantics.xml", true);
			ct = new DBSynsactBuilder(connId);
			metas = Connects.getMeta(connId);
			
			m = new PhotoMeta();
			metas.put(m.tbl, m);

			SemanticObject jo = new SemanticObject();
			jo.put("userId", "tester");
			SemanticObject usrAct = new SemanticObject();
			usrAct.put("funcId", "DASemantextTest");
			usrAct.put("funcName", "test ISemantext implementation");
			jo.put("usrAct", usrAct);
			robot = new LoggingUser(connId, "src/test/res/semantic-log.xml", "tester", jo);
		} catch (SemanticException | SQLException | SAXException | IOException e) {
			e.printStackTrace();
		}
	
	}

	@BeforeAll
	public static void testInit() throws SQLException, SAXException, IOException, TransException {
		ArrayList<String> sqls = new ArrayList<String>();
		sqls.add( "CREATE TABLE oz_autoseq (\n"
				+ "  sid text(50),\n"
				+ "  seq INTEGER,\n"
				+ "  remarks text(200),\n"
				+ "  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid))");
		sqls.add( "CREATE TABLE a_logs (\n"
				+ "  logId text(20),\n"
				+ "  funcId text(20),\n"
				+ "  funcName text(50),\n"
				+ "  oper text(20),\n"
				+ "  logTime text(20),\n"
				+ "  cnt int,\n"
				+ "  txt text(4000),\n"
				+ "  CONSTRAINT oz_logs_pk PRIMARY KEY (logId))");
		sqls.add("");
		sqls.add("drop table if exists syn_seq;\n"
				+ "create table syn_seq(\n"
				+ "	tabl    varchar2(64) not null, -- e.g. 'h_photos'\n"
				+ "	synodee varchar2(12) not null, -- subscriber, fk-on-del, client id asked for sychronizing\n"
				+ "	recount integer,\n"
				+ "	crud	char(1),\n"
				+ "	synyquist   integer not null,\n"
				+ "	cleanyquist integer not null\n"
				+ ");");
		sqls.add( "drop table if exists syn_change;\n" 
				+ "create table syn_change (\n"
				+ "	tabl        varchar2(64) not null, -- e.g. 'h_photos'\n"
				+ "	recId       varchar2(12) not null, -- entity record Id\n"
				+ "	synodee     varchar2(12) not null, -- subscriber, fk-on-del, synode id device to finish cleaning task\n"
				+ "	synoder     varchar2(12) not null, -- publisher, fk-on-del, synode id for resource's PK\n"
				+ "	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id, not null?\n"
				+ "	clientpath2 text,                  -- support max 3 fields of composed PK, TODO any betther patterns?\n"
				+ "	crud        char(1)      not null, -- I/U/D\n"
				+ "	flag        char(1)      not null, -- 'D' deleting, 'C' close (not exists),'R'/'E' rejected (then erased) by device owner\n"
				+ "	synyquist   integer      not null  -- last Nyquist sequence number of synodee\n"
				+ ");");
		sqls.add( "drop table if exists syn_seq;\n"
				+ "create table syn_seq (\n"
				+ "	tabl    varchar2(64) not null, -- e.g. 'h_photos'\n"
				+ "	synodee varchar2(12) not null, -- subscriber, fk-on-del, client id asked for sychronizing\n"
				+ "	recount integer,\n"
				+ "	crud	char(1),\n"
				+ "	synyquist   integer not null,\n"
				+ "	cleanyquist integer not null\n"
				+ ");");
		sqls.add( "drop table if exists h_potos;\n"
				+ "create table h_photos (\n"
				+ "	pid    varchar2(64) not null,\n"
				+ "	pname  varchar2(12) not null,\n"
				+ "	device varchar2(12) not null,\n"
				+ "	clientpath varchar2(12) not null,\n"
				+ " uri        text,"
				+ " size       number,"
				+ " mime       text,"
				+ " shareby    varchar2(12),"
				+ " sharedate  number,"
				+ "	synflag    char(1) not null,\n"
				+ "	synyquist  number  not null\n"
				+ ");");
		sqls.add( "delete from oz_autoseq;\n"
				+ "insert into oz_autoseq (sid, seq, remarks) values\n"
				+ "('h_photos.pid', 0, 'photo'),\n"
				+ "('a_logs.logId', 0, 'test');"
				);
		sqls.add("delete from h_photos");
		sqls.add("delete from a_logs");

		Connects.commit(robot, sqls, Connects.flag_nothing);
	}

	@Test
	void testChangeLog() throws TransException, SQLException {
		ct.insert(m.tbl, robot)
		  .nv(m.pk, "001")
		  .nv(m.fullpath, "/test/res/photo1.jpg")
		  .ins(ct.instancontxt(connId, robot));
		fail("Not yet implemented");
	}

}
