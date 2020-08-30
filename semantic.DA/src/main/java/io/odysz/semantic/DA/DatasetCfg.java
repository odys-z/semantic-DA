package io.odysz.semantic.DA;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io_odysz.FilenameUtils;
import org.xml.sax.SAXException;

import io.odysz.anson.Anson;
import io.odysz.common.LangExt;
import io.odysz.common.Utils;
import io.odysz.common.dbtype;
import io.odysz.module.rs.AnResultset;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactoryEx;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.util.LogFlags;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;

/**Configured dataset.xml manager and helper.<br>
 * - won't care using CP data source or DB manager (2019.2.28).
 * 
 * @author odys-z@github.com
 */
public class DatasetCfg {
	/**Data structure of s-tree configuration.
	 * @author odys-z@github.com
	 */
	public static class TreeSemantics extends Anson {
		/**
		 * s-tree tag's fields index.
		 */
		static class Ix {
			/** filed count: 9 */
			public static final int count = 9;
			/** the is-checked boolean field */
			public static final int chked = 0;
			/** main table name */
			public static final int tabl = 1;
			/** record pk filed (only single column) */
			public static final int recId = 2;
			/** parent field */
			public static final int parent = 3;
			/** fullpath field (optional) */
			public static final int fullpath = 4;
			/** sibling sort (fullpath first, optinal) */
			public static final int sort = 5;
			/** lable / text field for client binding */
			public static final int text = 6;
			/** paging by server */
			public static final int pageByServer = 8;
		}

		/**parse tree semantics like ",checked,table,recId id,parentId,itemName text,fullpath,siblingSort,false" to 2d array.
		 * @param semantic
		 * @return [0:[checked, null], 1:[tabl, null], 2:[areaId, id], ...]
		 */
		public static String[][] parseSemantics(String semantic) {
			if (semantic == null) return null;
			String[] sms = semantic.split(",");
			
			return parseSemantics(sms);
		}
		
		public static String[][] parseSemantics(String[] sms) {
			if (sms == null) return new String[0][2];

			String[][] sm = new String[Ix.count][];
			for (int ix = 0; ix < sms.length; ix++) {
				String smstr = sms[ix];
				if (smstr == null) continue;
				smstr = smstr.replaceAll("\\s+[aA][sS]\\s+", " "); // replace " as "
				String[] smss = smstr.split(" ");
				if (smss == null || smss.length > 2 || smss[0] == null)
					System.err.println(String.format("WARN - SematnicTree: ignoring semantics not understandable: %s", smstr));
				else {
					sm[ix] = new String[] {smss[0].trim(),
						(smss.length > 1 && smss[1] != null) ? smss[1].trim() : null};
				}
			}
			return sm;
		}
		
		@Override
		public String toString() {
			if (treeSmtcs != null) {
				return Arrays.stream(treeSmtcs)
						.map(e -> e == null ? null : String.format("[%s, %s]", e[0], e.length > 0 ? e[1] : null))
						.collect(Collectors.joining(", ", "[", "]"));
			}
			return "[]";
		}

		String[][] treeSmtcs;
		public String[][] treeSmtcs() { return treeSmtcs; }

		public TreeSemantics(String stree) {
			treeSmtcs = parseSemantics(stree);
		}

		public TreeSemantics(String[] stree) {
			treeSmtcs = parseSemantics(stree);
		}

		public TreeSemantics(String[][] stree) {
			treeSmtcs = stree;
		}
		
		public String tabl() {
			return alias(Ix.tabl);
		}

		/**Get raw expression of record id.
		 * @return column name of sql result
		 */
		public String dbRecId() {
			return exp(Ix.recId)[0];
		}
		
		public String dbParent() {
			return alias(Ix.parent);
		}
		
		public String dbFullpath() {
			return alias(Ix.fullpath);
		}
		
		public String dbSort() {
			return alias(Ix.sort);
		}
		
		private String[] exp(int ix) {
			return treeSmtcs != null && treeSmtcs.length > ix ? treeSmtcs[ix] : null;
		}
		
		/**Get column name, if there is an alias, get alias, otherwise get the db field name.
		 * @param ix
		 * @return
		 */
		String alias(int ix) {
			return  treeSmtcs != null && treeSmtcs.length > ix ?
				treeSmtcs[ix].length > 0 && treeSmtcs[ix][1] != null
				? treeSmtcs[ix][1] : treeSmtcs[ix][0]
				: null;
		}

		/**Is value of <i>col</i> should changed to boolean.<br>
		 * If the semantics configuration's first field is this col's name,
		 * the the value should changed into boolean. 
		 * @param col
		 * @return true: this column should covert to boolean.
		 */
		public boolean isColChecked(String col) {
			String chk = alias(Ix.chked);
			if (col == null || chk == null)
				return false;
			return (chk.equals(col.trim()));
		}

		public String aliasParent() {
			return alias(Ix.parent);
		}
		
		/**Get alias if possible, other wise the expr itself
		 * @param expr
		 * @return alias
		 */
		public String alias(String expr) {
			if (expr != null)
				// for (String[] colAls : treeSmtcs)
				for (int x = 0; x < treeSmtcs.length; x++) {
					String[] colAls = treeSmtcs[x];
					if (colAls != null && expr.equals(colAls[0]))
						return alias(x);
				}
			return expr;
		}
	}

	/**<p>Representing each tree node.</p>
	 * 
	 * Design Memo:<br>
	 * What easy ui tree control expected is this:<pre>
    [{"children":[
       { "fullpath":"01.0101",
         "checked":true,
         "text":"users","sort":"0101", "value":"0101", "parentId":"01" },
       { "fullpath":"01.0102",
         "checked":true,
         "text":"roles","sort":"0102","value":"0102","parentId":"01" },
       { "fullpath":"01.0103",
         "checked":true,
         "text":"sys log","sort":"0103","value":"0103","parentId":"01" } ],
       "fullpath":"01",
       "checked":true,
       "text":"system","sort":"01","value":"01","parentId":""
     },
     {"children":[
       { "fullpath":"03.0301",
         "checked":true,
         "text":"FUNC 0301","sort":"0301","value":"0301","parentId":"03"} ],
       "fullpath":"03",
       "checked":true,
       "text":"web contents","sort":"03","value":"03"
     }]</pre>
	 */
	public static class AnTreeNode extends Anson {
		HashMap<String, Object> node;
		String id;
		String parent;

		// Only for Anson parser
		public AnTreeNode() { }

		public AnTreeNode(String id, String parent) {
			this.id = id;
			this.parent = parent;
			node = new HashMap<String, Object>(TreeSemantics.Ix.count);
		}
		
		public AnTreeNode put(String k, Object v) {
			node.put(k, v);
			return this;
		}

		public Object get(String k) {
			return node == null ? null : node.get(k);
		}

		public String id() { return id; }
		public String parent() { return parent; }
		public String fullpath() { 
			return node == null ? null : (String) node.get("fullpath");
		}

		public List<?> children() {
			return node == null ? null : (List<?>) node.get("children");
		}

		public Object child(int cx) {
			return node == null ? null : ((List<?>) node.get("children")).get(cx);
		}

		public void children(List<Object> arrChildren) {
			put("children", arrChildren);
		}
	}

	public static final int ixMysql = 0;
	public static final int ixOrcl = 1;
	public static final int ixMs2k = 2;
	public static final int ixSqlit = 3;
	public static final int ixUnknow = 4;

	protected static final String tag = "DataSet";
	protected static final String cfgFile = "dataset.xml";
	protected static final String deftId = "ds";
	protected static HashMap<String, Dataset> dss;

	private static boolean inited = false;

	public static void init(String path) throws SAXException, IOException {
		if (inited  == false) {
			dss = new HashMap<String, Dataset>();
			load(dss, path);
			inited = true;
		}
	}

	/**Load all dataset.xml into the argument cfgs.<br>
	 * When return, cfgs is loaded with dataset configurations like [id, mysql:sql, orcl:sql, ...].
	 * @param cfgs
	 * @param xmlPath
	 * @throws IOException 
	 * @throws SAXException 
	 */
	protected static void load(HashMap<String, Dataset> cfgs, String xmlPath)
			throws SAXException, IOException {
		String fullpath = FilenameUtils.concat(xmlPath + "/", cfgFile);
		if (LogFlags.datasetCfg)
			Utils.logi("message file path: %s", fullpath);

		File f = new File(fullpath);
		if (!f.exists() || !f.isFile()) {
			Utils.warn("WARN - Can't find dataset.xml, configuration ignored. Check %s", fullpath);
			return;
		}

		LinkedHashMap<String,XMLTable> xtabs = XMLDataFactoryEx.getXtables(
				new Log4jWrapper("DA").setDebugMode(false), fullpath, new IXMLStruct() {
					@Override public String rootTag() { return "dataset"; }
					@Override public String tableTag() { return "t"; } 
					@Override public String recordTag() { return "c"; }
				});
		XMLTable deft = xtabs.get("ds");
		
		parseConfigs(cfgs, deft);
	}
		
	public static void parseConfigs(HashMap<String, Dataset> cfgs, XMLTable xSmtcs) {
		if (xSmtcs != null) {
			try {
				xSmtcs.beforeFirst();
				Dataset ds = null;
				while (xSmtcs.next()) {
					String[] sqls = new String[4];
					sqls[ixMysql] = xSmtcs.getString("mysql");
					sqls[ixOrcl] = xSmtcs.getString("orcl");
					sqls[ixSqlit] = xSmtcs.getString("sqlit");
					sqls[ixMs2k] = xSmtcs.getString("ms2k");

					// columns="id,tabls,cols,orcl,mysql,ms2k"
					ds = new Dataset(xSmtcs.getString("sk"), xSmtcs.getString("cols"),
									sqls, xSmtcs.getString("s-tree"));
					if (ds != null)
						cfgs.put(xSmtcs.getString("sk"), ds);
				}
			} catch (SAXException e) {
				e.printStackTrace();
			}
		}
	}

	public static AnResultset select(String conn, String sk,
			int page, int size, String... args) throws SemanticException, SQLException {
		String sql = getSql(conn, sk, args);
		if (page >= 0 && size > 0)
			sql = Connects.pagingSql(conn, sql, page, size);

		AnResultset rs = new AnResultset(Connects.select(conn, sql));
		return rs;
	}

	public static String getSql(String conn, String k, String... args) throws SQLException, SemanticException {
		if (dss == null)
			throw new SQLException("FATAL - dataset not initialized...");
		if (k == null || !dss.containsKey(k))
			throw new SQLException(String.format("No dataset configuration found for k = %s", k));

		if (conn == null) conn = Connects.defltConn();

		String sql = dss.get(k).getSql(Connects.driverType(conn));
		if (sql == null)
			throw new SemanticException("Sql not found for sk=%s, type = %s",
					k, Connects.driverType(conn));

		if (args == null || args.length == 0)
			return sql;
		else return String.format(sql, (Object[])args);
	}
	
	public static TreeSemantics getTreeSemtcs(String sk) {
		if (dss != null && dss.containsKey(sk))
			return dss.get(sk).treeSemtcs;
		else return null;
	}
	
	public static AnResultset loadDataset(String conn, String sk,
			int page, int size, String... args)
			throws SemanticException, SQLException {

		if (sk == null)
			throw new SemanticException("null semantic key");
		if (conn == null)
			conn = Connects.defltConn();
		return select(conn, sk, page, size, args);
	}

	public static List<?> loadStree(String conn, String sk,
			int page, int size, String... args)
			throws SemanticException, SQLException {
		if (dss == null || !dss.containsKey(sk))
			throw new SemanticException("Can't find tree semantics, dss %s, sk = %s. Check configuration.",
					dss == null ? "null" : dss.size(), sk);

		AnResultset rs = loadDataset(conn, sk, page, size, args);

		TreeSemantics smx = dss.get(sk).treeSemtcs;
		if (smx == null)
			throw new SemanticException("sk (%s) desn't configured with a tree semantics", sk);
		return buildForest(rs, smx);
	}
	
	/**Build forest.
	 * @param rs
	 * @param treeSemtcs
	 * @return built forest
	 * @throws SQLException
	 * @throws SemanticException data structure can not build  tree / forest 
	 */
	public static List<?> buildForest(AnResultset rs, TreeSemantics treeSemtcs)
			throws SQLException, SemanticException {
		// build the tree/forest
		List<Object> forest = new ArrayList<Object>();
		rs.beforeFirst();
		while (rs.next()) {
			// Map<String, Object> root  = formatSemanticNode(treeSemtcs, rs);
			AnTreeNode root = formatSemanticNode(treeSemtcs, rs);

			// sometimes error results from inconsistent data is confusing, so report an error here - it's debug experience.
			// if (!rs.getColnames().containsKey(treeSemtcs.dbRecId()))
			if (!rs.hasCol(treeSemtcs.dbRecId()))
				throw new SemanticException("Building s-tree requires column '%s'(configured id). You'd better check the query request and the semantics configuration:\n%s",
						treeSemtcs.dbRecId(), LangExt.toString(treeSemtcs.treeSmtcs()));

			// checkSemantics(rs, semanticss, Ix.recId);
			List<Object> children = buildSubTree(treeSemtcs, root,
					// rs.getString(treeSemtcs[Ix.recId] == null ? treeSemtcs[Ix.recId] : treeSemtcs[Ix.recId]),
					rs.getString(treeSemtcs.dbRecId()),
					rs);
			if (children.size() > 0)
				root.children(children);
			forest.add(root);
		}

		return forest;
	}

	/**Create a SemanticObject for tree node with current rs row.<br>
	 * TODO should this moved to TreeSemantics?
	 * @param treeSemtcs
	 * @param rs
	 * @return {@link SemanticObject} of node
	 * @throws SQLException
	 */
	private static AnTreeNode formatSemanticNode(TreeSemantics treeSemtcs,
			AnResultset rs) throws SQLException {
		// Map<String, Object> node = new HashMap<String, Object>();
		AnTreeNode node = new AnTreeNode(rs.getString(TreeSemantics.Ix.recId),
								rs.getString(TreeSemantics.Ix.parent));

		for (int i = 1;  i <= rs.getColCount(); i++) {
			String v = rs.getString(i);
			String col = rs.getColumnName(i);
			col = treeSemtcs.alias(col);
			if (v != null)
				node.put(col,  v);
		}
		return node;
	}

	private static List<Object> buildSubTree(TreeSemantics sm, AnTreeNode root,
			String parentId, AnResultset rs) throws SQLException, SemanticException {
		List<Object> childrenArray  = new ArrayList<Object>();
		while (rs.next()) {
			if (parentId == null || root == null) {
				Utils.warn("Found children for null parent. Parent: %s\n", root.toString());
				Utils.warn(rs.getRowAt(rs.getRow()));
				Utils.warn("\n-- -- -- -- This is a common error when tree structure is broken, check data of recently printed sql.");
				throw new SemanticException("Found children for null parent. Check the data queried by recent committed SQL.");
			}

			String currentParentID = rs.getString(sm.aliasParent());
			if (currentParentID == null || currentParentID.trim().length() == 0) {
				// new tree root
				rs.previous();
				if (childrenArray.size() > 0) 
					root.children(childrenArray);
				return childrenArray;
			}
			// HERE! ending adding children
			if (!currentParentID.trim().equals(parentId.trim())) {
				rs.previous();
				if (childrenArray.size() > 0)
					root.children(childrenArray);
				return childrenArray;
			}

			AnTreeNode child = formatSemanticNode(sm, rs);

			List<Object> subOrg = buildSubTree(sm, child,
					rs.getString(sm.dbRecId()), rs);

			if (subOrg.size() > 0)
				root.children(childrenArray);
			childrenArray.add(child);
		}
		return childrenArray;
	}

	/**POJO dataset element as configured in dataset.xml.<br>
	 * (oracle mapping information also initialized according to mapping file and the "cols" tag.)*/
	public static class Dataset {
		String k;

		String[] sqls;

		/**Configuration in dataset.xml/t/c/s-tree.<br>
		 * If the result set can be used to construct a tree, a tree semantics configuration is needed.
		 * "s-tree" tag is used to configure a tree semantics configuration.
		 * <p>s-tree tag can support optional alias, which is needed in Stree servlet
		 * as the client needing some special fields but with queried results from abstract sql construction.<br>
		 * Core data: String[][] treeSemtcs;
		 */
		TreeSemantics treeSemtcs;

		/**Create a dataset, with mapping prepared according with mapping file.
		 * @param k
		 * @param cols
		 * @param sqls
		 * @param stree
		 * @param orclMappings mappings from mapping the file.
		 * @throws SAXException 
		 */
		public Dataset(String k, String cols, String[] sqls, String stree)
				throws SAXException {
			this.k = k;

			this.sqls = sqls;
			
			this.treeSemtcs = stree == null ? null
					: new TreeSemantics(stree);
		}

		/**
		 * @param driver drv_orcl, drv_ms2k, drv_sqlit, drv_mysql(default)
		 * @return sql db version of the jdbc connection
		 * @throws SemanticException can't find correct sql version 
		 */
		public String getSql(dbtype driver) throws SemanticException {
			if (driver == null)
				return null;
			if (driver == dbtype.oracle)
				return sqls[ixOrcl];
			else if (driver == dbtype.ms2k)
				return sqls[ixMs2k];
			else if (driver == dbtype.sqlite)
				return sqls[ixSqlit];
			else if (driver == dbtype.mysql)
				return sqls[ixMysql];
			else
				throw new SemanticException("unsupported db type: %s", driver);
		}

		public String sk() {
			return k;
		}
	}

}
