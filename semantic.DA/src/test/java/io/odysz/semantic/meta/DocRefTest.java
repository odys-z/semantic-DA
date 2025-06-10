package io.odysz.semantic.meta;

import static io.odysz.common.FilenameUtils.normalize;
import static io.odysz.common.LangExt.f;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;

import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.Test;

import io.odysz.anson.Anson;
import io.odysz.common.Configs;
import io.odysz.common.Utils;
import io.odysz.semantic.DASemantextTest;
import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.syn.T_DA_PhotoMeta;
import io.odysz.transact.sql.parts.ExtFilePaths;

class DocRefTest {

	@Test
	void testCreateExtPaths() throws Exception {
		File file = new File(DASemantextTest.rtroot);
		String abspath = file.getAbsolutePath();
		Utils.logi(abspath);
		Configs.init(abspath);
		Connects.init(abspath);
		String conn_extpath_upload = "test-extpath-upload";
		String conn_extpath_volume = "test-extpath-volume";
		
		DATranscxt.configRoot("src/test/res", ".");
		new DATranscxt(conn_extpath_upload); //.initConfigs(conn_extpaths, (SemanticsMap c) -> new SemanticsMap(c));
		new DATranscxt(conn_extpath_volume); //.initConfigs(conn_extpaths, (SemanticsMap c) -> new SemanticsMap(c));

		String deploy_Y = "../deploy-Y";
		String volume_Y = "VOLUME_Y";
		String volume_old = System.getProperty(volume_Y);
		System.setProperty(volume_Y, deploy_Y);

		ExpDocTableMeta docm = new T_DA_PhotoMeta(null);
		
		// extpath to "uploads"
//		ShExtFilev2 sh = (ShExtFilev2) new DASemantics(new DATranscxt(), docm.tbl, docm.pk)
//				.parseHandler(new DATranscxt(), docm.tbl, smtype.extFilev2, docm.pk, 
//						new String[] {"uploads", "uri", "family", "shareby", "folder", "docname"});
		ShExtFilev2 sh = (ShExtFilev2) DATranscxt.getHandler(conn_extpath_upload, docm.tbl, smtype.extFilev2);

		DocRef dr = (DocRef) Anson.fromJson(("{'type': 'io.odysz.semantic.meta.DocRef', "
				+ "'uri64': 'uploads/ody/h_photo/0001 Sun Yet-sen Portrait.jpg', "
				+ "'docId': '0001', "
				+ "'syntabl': 'h_photos', "
				+ "'pname': 'Sun Yet-sen Portrait.jpg', "
				+ "'uids': 'X,0001'}")
				.replaceAll("'", "\""));
		
		
		ExtFilePaths extpaths = new ExtFilePaths(sh.getFileRoot(), dr.docId, dr.pname)
							.prefix(dr.relativeFolder(conn_extpath_upload))
							.filename(dr.pname);
		
		assertEquals("uploads/ody/h_photo/0001 Sun Yet-sen Portrait.jpg", extpaths.dburi(true));

		assertEquals(normalize(
				"uploads/ody/h_photo/0001 Sun Yet-sen Portrait.jpg"),
				extpaths.abspath());

		// extpath to volume X
		sh = (ShExtFilev2) DATranscxt.getHandler(conn_extpath_volume, docm.tbl, smtype.extFilev2);
		dr = (DocRef) Anson.fromJson(("{'type': 'io.odysz.semantic.meta.DocRef', "
				+ "'uri64': '$VOLUME_X/ody/h_photo/0001 Sun Yet-sen Portrait.jpg', " // VOLUME_X, not _Y
				+ "'docId': '0001', "
				+ "'syntabl': 'h_photos', "
				+ "'pname': 'Sun Yet-sen Portrait.jpg', "
				+ "'uids': 'X,0001'}")
				.replaceAll("'", "\""));

		extpaths = new ExtFilePaths(sh.getFileRoot(), dr.docId, dr.pname)
							.prefix(dr.relativeFolder(conn_extpath_volume))
							.filename(dr.pname);
		
		assertEquals(f("$%s/ody/h_photo/0001 Sun Yet-sen Portrait.jpg", volume_Y), extpaths.dburi(true));
		assertEquals(FilenameUtils.separatorsToSystem(
				f("%s/%s", deploy_Y, "ody/h_photo/0001 Sun Yet-sen Portrait.jpg")),
				extpaths.abspath());
		
		if (volume_old != null)
			System.setProperty(volume_Y, volume_old);
	}

}
