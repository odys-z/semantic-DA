package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.Utils.logi;
import static io.odysz.common.Utils.printCaller;
import static io.odysz.semantic.syn.Docheck.ck;
import static io.odysz.semantic.syn.DBSyntableTest.*;
import java.io.File;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

import io.odysz.common.AssertImpl;
import io.odysz.common.Configs;
import io.odysz.common.Utils;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SemanticTableMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.syn.registry.Syntities;
import io.odysz.semantics.x.SemanticException;

class DBSynTransBuilderTest {

	static {
		printCaller(false);

		File file = new File(rtroot);
		runtimepath = file.getAbsolutePath();
		logi(runtimepath);

		Configs.init(runtimepath);
		Connects.init(runtimepath);

		DATranscxt.configRoot(rtroot, runtimepath);
		String rootkey = System.getProperty("rootkey");
		DATranscxt.key("user-pswd", rootkey);
	}

	@Test
	void testSynmatics() throws Exception {
		ck = new Docheck[4];
		synodes = new String[] { "X", "Y", "Z", "W" };
		chm = new SynChangeMeta();
		sbm = new SynSubsMeta(chm);
		xbm = new SynchangeBuffMeta(chm);
		ssm = new SynSessionMeta();
		prm = new PeersMeta();

			String conn = conns[0];
			snm = new SynodeMeta(conn);

			Syntities regists = Syntities.load(runtimepath, "syntity.json", 
				(synreg) -> {
					if (eq(synreg.name, "T_PhotoMeta"))
						return new T_DA_PhotoMeta(conn);
					else
						throw new SemanticException("TODO %s", synreg.name);
				});

			T_DA_PhotoMeta phm = regists.meta("T_PhotoMeta");

			SemanticTableMeta.setupSqliTables(conn, true, snm, chm, sbm, xbm, prm, ssm, phm);
			// phm.replace();
			// DBSynTransBuilder.registerEntity(conn, phm);

			ArrayList<String> sqls = new ArrayList<String>();
			sqls.add("delete from oz_autoseq;");
			sqls.add(Utils.loadTxt("../oz_autoseq.sql"));
			sqls.add(String.format("update oz_autoseq set seq = %d where sid = 'h_photos.pid'", 64));

			sqls.add(String.format("delete from %s", snm.tbl));
			sqls.add(Utils.loadTxt("syn_nodes.sql"));

			sqls.add(String.format("delete from %s", phm.tbl));

			Connects.commit(conn, DATranscxt.dummyUser(), sqls);

			Docheck.ck[0] = new Docheck(new AssertImpl(), zsu, conn, synodes[0],
									SynodeMode.peer, phm);
			
			DBSyntableBuilder b = Docheck.ck[0]
								.trb.incNyquence0();

		
		DBSynTransBuilder tb = new DBSynTransBuilder(zsu, conn, synodes[0], SynodeMode.peer);

		// create photo
		tb.insert(phm.tbl, ck[0].robot())
		  .nv(phm.device, "device")
		  .nv(phm.fullpath, "full.path")
		  .nv(phm.folder, "folder").nv(phm.org, "org")
		  .nv(phm.uri, "").nv(phm.shareDate, "1911-10-10")
		  .ins(b.instancontxt(conn, ck[0].robot()))
		;
		
		ck[0].doc(1);
		ck[0].change_doclog(3, CRUD.C, null);
	}

}
