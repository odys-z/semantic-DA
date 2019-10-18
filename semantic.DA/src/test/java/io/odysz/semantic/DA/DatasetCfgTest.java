//package io.odysz.semantic.DA;
//
//
//import static org.junit.Assert.assertEquals;
//
//import java.io.File;
//import java.io.IOException;
//import java.sql.SQLException;
//import java.util.List;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import io.odysz.anson.x.AnsonException;
//import io.odysz.common.Utils;
//import io.odysz.semantics.x.SemanticException;
//
//public class DatasetCfgTest {
//
//	@Before
//	public void testInit() throws Exception {
//		File file = new File("src/test/res");
//		String path = file.getAbsolutePath();
//		Utils.logi(path);
//
//		Connects.init(path);
//		DatasetCfgV11.init(path);
//	}
//
//	@Test
//	public void testLoadStree() throws SemanticException, SQLException, AnsonException, IOException {
//		List<?> t = DatasetCfgV11.loadStree(
//				"local-sqlite", // expect "01"; for semantic-DA.db, see connects.xml
//				// "local-mysql", // expect "11"
//				// "local-orcl", 	// expect "10"
//				"test.tree", -1, 0, "admin");
//		if (t != null && t.size() > 0) {
//			print(t);
//			assertEquals("01", t.get(0).get("idVue"));
//		}
//	}
//
//	static void print(List<?> t) throws AnsonException, IOException {
//		for (Object e : t) {
//			// e.toBlock(System.out);
//
//		}
//	}
//
//}
