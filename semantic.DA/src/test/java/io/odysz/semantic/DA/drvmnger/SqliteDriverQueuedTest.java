package io.odysz.semantic.DA.drvmnger;

import static io.odysz.common.Utils.loadTxt;
import static io.odysz.common.Utils.awaitAll;
import static io.odysz.common.Utils.turnred;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
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
import io.odysz.transact.x.TransException;

/**
 * @since 1.4.45
 */
class SqliteDriverQueuedTest {
	public static final String connId = "queued-sqlite";
	private static DATranscxt st;

	public final static String rtroot = "src/test/res/";
	private static String runtimepath;

	static {
		SqliteDriverQueued.test = true;
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
					"b_alarms"}) {
				sqls.add("delete from " + tbl);
				Connects.commit(usr, sqls, Connects.flag_nothing);
				sqls.clear();
			}
			
			sqls.add("drop table if exists b_alarm_domain");
			sqls.add("delete from a_domain");

			for (String tbl : new String[] {"b_alarm_domain.ddl", "a_domain.sql"}) {
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
	
	private static int threads = 64;
	@Test
	void testCommitst() throws TransException, SQLException, InterruptedException {
		
		IUser usr = DATranscxt.dummyUser();
		Alarmer[] ths = new Alarmer[threads];
		int no = 0;
		int total = 0;

		Utils.logi("Total threads: %s", threads);
		Utils.logi("Committments in each thread:");
		for (; no < ths.length; no++) {
			int mx = (no * 29 + 511) / (no * 3 + 23 + 3);
			mx = Math.max(7, mx);
			System.out.print(mx);
			System.out.print(" ");
			ths[no] = new Alarmer(st, usr, connId, no, mx, "creator");
			total += mx;
		}

		// building transactions
		boolean[] lights = new boolean[ths.length];
		turnred(lights);
		long t0 = System.nanoTime();
		for (int x = 0; x < ths.length; x++)
			ths[x].start(lights);
		long t1 = System.nanoTime();
		Utils.logi("\nTime of building transactions: %s seconds", TimeUnit.SECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS));
		t0 = t1;
		
		try {
			awaitAll(lights, -1);
			t1 = System.nanoTime();

			Utils.logi("Expecting total committment %s", total);
			assertEquals(total, DAHelper.count(st, connId, "b_alarms", "typeId", "creator"));
			assertTrue(DAHelper.count(st, connId, "b_alarm_domain") > total);
			assertTrue(DAHelper.count(st, connId, "b_alarm_domain") < total * 2);
			
			Utils.logi(T_ArrayBlockingQueue
					.qusizeOnTaking.stream()
					.map(String::valueOf)
					.collect(Collectors.joining(", ", "Waiting queue's sizes on every background taking: \n", "")));
			
			boolean reached = false;
			for (int s : T_ArrayBlockingQueue.qusizeOnTaking)
				if (s > 1) {
					reached = true;
					break;
				}
		
			if (!reached)
				fail("Expecting case hasn't been tested.");

			Utils.logi("Time for completing commitments: %s seconds", TimeUnit.SECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS));
		}
		catch (InterruptedException e) {
			Utils.logi(T_ArrayBlockingQueue
				.qusizeOnTaking.stream()
				.map(String::valueOf)
				.collect(Collectors.joining(", ", "Blocking sizes on committing: ", "")));
		}
	}

}
