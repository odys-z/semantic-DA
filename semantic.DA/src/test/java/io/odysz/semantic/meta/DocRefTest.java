package io.odysz.semantic.meta;

import static io.odysz.common.AssertImpl.assertPathEquals;
import static io.odysz.common.LangExt.f;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import org.junit.jupiter.api.Test;

import io.odysz.anson.Anson;
import io.odysz.common.Configs;
import io.odysz.common.EnvPath;
import io.odysz.common.FilenameUtils;
import io.odysz.common.Utils;
import io.odysz.semantic.DASemantextTest;
import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.SessionInf;
import io.odysz.transact.sql.parts.ExtFilePaths;
import io.oz.syn.T_DA_PhotoMeta;

class DocRefTest {
	static final String configroot = "src/test/res";

	@Test
	void testCreateExtPaths() throws Exception {
		File file = new File(DASemantextTest.rtroot);
		String abspath = file.getAbsolutePath();
		Utils.logi(abspath);
		Configs.init(abspath);
		Connects.init(abspath);
		String conn_extpath_upload = "test-extpath-upload";
		String conn_extpath_volume = "test-extpath-volume";
		
		DATranscxt.configRoot(configroot, ".");
		new DATranscxt(conn_extpath_upload);
		DATranscxt st = new DATranscxt(conn_extpath_volume);

		String deploy_Y = "../deploy-Y";
		String volume_Y = "VOLUME_Y";
		String volume_old = System.getProperty(volume_Y);
		System.setProperty(volume_Y, deploy_Y);

		ExpDocTableMeta docm = new T_DA_PhotoMeta(conn_extpath_volume);
		
		// extpath to "uploads"
		ShExtFilev2 sh = (ShExtFilev2) DATranscxt.getHandler(conn_extpath_upload, docm.tbl, smtype.extFilev2);

		DocRef dr = (DocRef) Anson.fromJson(("{'type': 'io.odysz.semantic.meta.DocRef', "
				+ "'uri64': 'uploads/ody/h_photo/0001 Sun Yet-sen Portrait.jpg', "
				+ "'docId': '0001', "
				+ "'syntabl': 'h_photos', "
				+ "'pname': 'Sun Yet-sen Portrait.jpg', "
				+ "'uids': 'X,0001'}")
				.replaceAll("'", "\""));
		
		
		ExtFilePaths extpaths = new ExtFilePaths(sh.getFileRoot(), dr.docId, dr.pname)
							// .prefix(dr.relativeFolder(sh.getFileRoot()))
							.prefix(ExtFilePaths.relativeFolder(dr.uri64, sh.getFileRoot()))
							.filename(dr.pname);
		assertEquals("uploads/ody/h_photo/0001 Sun Yet-sen Portrait.jpg", extpaths.dburi(true));

		assertEquals(FilenameUtils.concat(
				configroot, "uploads/ody/h_photo/0001 Sun Yet-sen Portrait.jpg"),
				extpaths.decodeUriPath());

		// extpath to volume X
		sh = (ShExtFilev2) DATranscxt.getHandler(conn_extpath_volume, docm.tbl, smtype.extFilev2);
		dr = (DocRef) Anson.fromJson(("{'type': 'io.odysz.semantic.meta.DocRef', "
				+ "'uri64': '$VOLUME_X/ody/h_photo/0001 Sun Yet-sen Portrait.jpg', " // VOLUME_X, not _Y
				+ "'docId': '0001', "
				+ "'syntabl': 'h_photos', "
				+ "'pname': 'Sun Yet-sen Portrait.jpg', "
				+ "'uids': 'X,0001'}")
				.replaceAll("'", "\""));
		
		assertPathEquals("../deploy-Y/ody/h_photo/0001 Sun Yet-sen Portrait.jpg",
				EnvPath.decodeUri("$" + volume_Y, "ody/h_photo/0001 Sun Yet-sen Portrait.jpg"));

		String jpg = "resolve-docref-Y/ssid-test/ody/h_photo/0001 Sun Yet-sen Portrait.jpg";
		String absoluteVolumeJpg = FilenameUtils.concat(new File(".").getAbsolutePath(), configroot, EnvPath.decodeUri("$" + volume_Y, jpg));

		assertEquals(absoluteVolumeJpg,
				new File(dr.downloadPath("Y", conn_extpath_volume, new SessionInf("ssid-test", "uid-test"))).getAbsolutePath());

		// DATranscxt st = new DATranscxt(conn_extpath_volume);
		ISemantext s0 = st.instancontxt(conn_extpath_volume, DATranscxt.dummyUser());

		IUser u = DATranscxt.dummyUser();
		SemanticObject res = (SemanticObject) st
			.delete(docm.tbl, u)
			.whereEq(docm.io_oz_synuid,  dr.uids)
			.post(st
				.insert(docm.tbl, u)
				.nv(docm.org, "zsu.ua")
				.nv(docm.shareby, "ody")
				.nv(docm.folder, "2022-10")
				.nv(docm.resname, "Sun Yet-sen.jpg")
				.nv(docm.uri, dr.toBlock())
				.nv(docm.io_oz_synuid,  dr.uids))
			.d(s0);
		
		String docid = res.resulve(docm.tbl, docm.pk).get(0);

		assertEquals(EnvPath.decodeUri("$" + volume_Y, f("%sY/ssid-test", DocRef.resolve_prenom)),
				DocRef.resolveFolder("Y", conn_extpath_volume,
						dr.syntabl,
						new SessionInf("ssid-test", "uid-test")));

		extpaths = new ExtFilePaths(sh.getFileRoot(), docid, dr.pname)
							// .prefix(dr.relativeFolder(conn_extpath_volume))
							.prefix(ExtFilePaths.relativeFolder(dr.uri64, conn_extpath_volume))
							.filename(dr.pname);
		
		assertPathEquals(f("$%s/ody/h_photo/%s Sun Yet-sen Portrait.jpg", volume_Y, docid), extpaths.dburi(true));
		String relativUri = FilenameUtils.separatorsToSystem(
				f("%s/ody/h_photo/%s Sun Yet-sen Portrait.jpg", FilenameUtils.concat(configroot, deploy_Y), docid));

		assertEquals(relativUri, extpaths.decodeUriPath());
		
		extpaths = DocRef.createExtPaths(conn_extpath_volume, docm, dr);
		String targetpth = extpaths.decodeUriPath();
		assertEquals(relativUri, targetpth);

		if (volume_old != null)
			System.setProperty(volume_Y, volume_old);
	}

}
