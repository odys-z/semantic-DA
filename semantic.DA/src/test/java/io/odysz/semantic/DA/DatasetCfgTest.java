package io.odysz.semantic.DA;



import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.odysz.anson.AnsonException;
import io.odysz.common.Utils;
import io.odysz.semantic.DA.DatasetCfg.AnTreeNode;
import io.odysz.transact.x.TransException;

public class DatasetCfgTest {

	static {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		Utils.logi("DatasetCfgTest @BeforeAll\n");
		Utils.logi(path);

		Connects.init(path);
		try {
			DatasetCfg.init(path);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Init test failed");
		}
	}

	/**<pre>
orgId  |parent |orgName       |orgType |sort |fullpath             |
-------|-------|--------------|--------|-----|---------------------|
000000 |       |province A    |00      |1    |000000               |
00000N |000000 |food secut    |01      |1    |000000.00000N        |
00000O |000000 |special equi. |01      |2    |000000.00000O        |
00000c |00000O |33            |01      |2    |000000.00000O.00000c |
00000W |000000 |Dali county   |01      |3    |000000.00000W        |
00000Y |00000W |Yunlong conty |01      |1    |000000.00000W.00000Y |
00000b |00000W |test          |01      |2    |000000.00000W.00000b |
	 * </pre>
	 * @throws SQLException
	 * @throws AnsonException
	 * @throws IOException
	 * @throws TransException 
	 */
	@Test
	public void testLoadStree() throws SQLException, AnsonException, IOException, TransException {
		List<?> t = DatasetCfg.loadStree(
				"local-sqlite", // expect "01"; for semantic-DA.db, see connects.xml
				"test.tree", -1, 0, "admin");
		assertEquals(1, t.size());
		
		t = DatasetCfg.loadStree(
				"local-sqlite", // expect "01"; for semantic-DA.db, see connects.xml
				"tree-org", -1, 0, "admin");
		assertEquals(1, t.size());
		
		AnTreeNode trees = (AnTreeNode) t.get(0);
		assertEquals(3, trees.children().size());
		assertEquals("000000", trees.id());
		
		AnTreeNode child1 = (AnTreeNode) trees.child(0);
		assertEquals("00000N", child1.id());

		AnTreeNode child2 = (AnTreeNode) trees.child(1);
		assertEquals("00000O", child2.id());

		AnTreeNode child3 = (AnTreeNode) trees.child(2);
		assertEquals("00000W", child3.id());

		AnTreeNode grand31 = (AnTreeNode) child3.child(0);
		assertEquals("00000Y", grand31.id());

		AnTreeNode grand32 = (AnTreeNode) child3.child(1);
		assertEquals("00000b", grand32.id());
	}
}
