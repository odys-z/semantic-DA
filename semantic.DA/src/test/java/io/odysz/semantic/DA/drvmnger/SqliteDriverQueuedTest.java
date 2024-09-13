package io.odysz.semantic.DA.drvmnger;

import static io.odysz.common.Utils.loadTxt;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import io.odysz.common.Configs;
import io.odysz.common.Utils;
import io.odysz.semantic.DASemantextTest;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.x.TransException;

class SqliteDriverQueuedTest {
	public static final String connId = "queued-sqlite";
	private static DATranscxt st;

	public final static String rtroot = "src/test/res/";
	private static String runtimepath;

	static {
		try {
			Utils.printCaller(false);

			File db = new File("src/test/res/semantic-DA.db");
			if (!db.exists())
				fail("Create res/semantic-DA.db, clean project and retry...");

			File file = new File(rtroot);
			runtimepath = file.getAbsolutePath();
			Utils.logi(runtimepath);
			Configs.init(runtimepath);
			Connects.init(runtimepath);
			DATranscxt.configRoot(rtroot, runtimepath);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@BeforeAll
	public static void testInit() throws SQLException, SemanticException, SAXException, IOException {
		ArrayList<String> sqls = new ArrayList<String>();

		IUser usr = DATranscxt.dummyUser();
		try {
			for (String tbl : new String[] {
					"b_alarms", "b_alarm_logic"}) {
				sqls.add("drop table if exists " + tbl);
				Connects.commit(usr, sqls, Connects.flag_nothing);
				sqls.clear();
			}
			
			for (String tbl : new String[] {"b_alarms.ddl"}) {
				sqls.add(loadTxt(DASemantextTest.class, tbl));
				Connects.commit(usr, sqls, Connects.flag_nothing);
				sqls.clear();
			}
			st = new DATranscxt(connId);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	void testCommitst() throws TransException, SQLException {
		SqliteDriverQueued.test = true;
		
		int no = 0;
		IUser usr = DATranscxt.dummyUser();
		Insert i = st.insert("b_alarms", usr)
		  .nv("alarmId", no)
		  .nv("remarks", no++)
		  .nv("typeId", "t-1");
		
		for (; no < 101; no++)
		i.post(st.insert("b_alarms", usr)
		  .nv("alarmId", no)
		  .nv("remarks", no)
		  .nv("typeId", "t-1"));

		i.ins(st.instancontxt(connId, usr));

		assertEquals(no, DAHelper.count(st, connId, "b_alarms"));
		
		Utils.logi(T_ArrayBlockingQueue
				.qusizeOnTaking.stream()
				.map(String::valueOf)
				.collect(Collectors.joining(",")));
	}

}
