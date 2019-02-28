package io.odysz.semantic.DA;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.common.Utils;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;

class DatasetCfgTest {

	@BeforeAll
	static void testInit() throws Exception {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		Utils.logi(path);

		Connects.init(path);
		DatasetCfg.init(path);
	}

	@Test
	void testLoadStree() throws SemanticException, SQLException {
		List<SemanticObject> t = DatasetCfg.loadStree(
				"local", // local: semantic-DA.db, see connects.xml
				"test.tree", "admin");
		if (t != null && t.size() > 0)
			assertEquals(t.get(0).get("id"), "01");
	}

}
