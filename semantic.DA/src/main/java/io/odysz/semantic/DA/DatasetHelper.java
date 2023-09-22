package io.odysz.semantic.DA;

import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.len;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.odysz.common.LangExt;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.DatasetCfg.AnTreeNode;
import io.odysz.semantic.DA.DatasetCfg.Dataset;
import io.odysz.semantic.DA.DatasetCfg.TreeSemantics;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.PageInf;
import io.odysz.transact.x.TransException;

public class DatasetHelper {

	/**
	 * The reference implementation can be similar to 
	 * {@link DatasetHelper#formatSemanticNode(TreeSemantics, AnResultset, AnTreeNode, int)}.
	 * 
	 * @author Ody
	 */
	@FunctionalInterface
	public interface NodeFormatter {
		/**
		 * Create a SemanticObject for tree node with current rs row.<br>
		 * 
		 * @param sm
		 * @param rs with index to the current row, the row to be converted to a node
		 * @param root
		 * @param level
		 * @return tree node
		 */
		AnTreeNode format(TreeSemantics sm, AnResultset rs, AnTreeNode root, int level);
	}

	public static List<?> loadStree(String conn, String sk,
			int page, int size, String[] sqlArgs, NodeFormatter... optionalNoder)
			throws SQLException, TransException {
		HashMap<String, Dataset> dss = DatasetCfg.dss;
		if (dss == null || !dss.containsKey(sk))
			throw new SemanticException("Can't find tree semantics, dss count: %s, sk = %s. Check dataset.xml configuration.",
					dss == null ? "null" : dss.size(), sk);

		AnResultset rs = DatasetCfg.loadDataset(conn, sk, page, size, sqlArgs);

		TreeSemantics smx = dss.get(sk).treeSemtcs;
		if (smx == null)
			throw new SemanticException("sk (%s) desn't configured with a tree semantics", sk);
		return buildForest(rs, smx, optionalNoder);
	}

	/**
	 * Load semantic tree configured in dataset.xml.
	 * @param conn
	 * @param sk
	 * @param page cannot be null
	 * @return forest
	 * @throws SQLException
	 * @throws TransException
	 */
	public static List<?> loadStree(String conn, String sk, PageInf page, NodeFormatter...noder)
			throws SQLException, TransException {
		page.mergeArgs();
		return loadStree(conn, sk, (int)page.page, (int)page.size,
						len(page.arrCondts) > 0 ? page.arrCondts.get(0) : null, noder);
	}
	
	/**
	 * Convert the result set to a forest.
	 * 
	 * @param rs
	 * @param treeSemtcs
	 * @param noder optional to override default formatter {@link NodeFormatter}.
	 * @return built forest
	 * @throws SQLException
	 * @throws SemanticException data structure can not build  tree / forest 
	 */
	public static List<AnTreeNode> buildForest(AnResultset rs, TreeSemantics treeSemtcs, NodeFormatter... noder)
			throws SQLException, SemanticException {
		// build the tree/forest
		List<AnTreeNode> forest = new ArrayList<AnTreeNode>();
		rs.beforeFirst();
		while (rs.next()) {
			AnTreeNode root = formatSemanticNode(treeSemtcs, rs, null, 0);

			// sometimes error results from inconsistent data is confusing, so report an error here - it's debug experience.
			if (!rs.hasCol(treeSemtcs.dbRecId()))
				throw new SemanticException("Building s-tree requires column '%s'(configured id). You'd better check the query request and the semantics configuration:\n%s",
						treeSemtcs.dbRecId(), LangExt.str(treeSemtcs.treeSmtcs()));

			List<AnTreeNode> children = buildSubTree(treeSemtcs, root,
										rs.getString(treeSemtcs.dbRecId()),
										rs, 0, noder);

			if (children.size() > 0) 
				root.children_(children).tagLast();
	
			forest.add(root);
		}

		return forest;
	}

	static List<AnTreeNode> buildSubTree(TreeSemantics sm, AnTreeNode root,
				String parentId, AnResultset rs, int level, NodeFormatter... noder)
				throws SQLException, SemanticException {
		List<AnTreeNode> childrenArray  = new ArrayList<AnTreeNode>();
		while (rs.next()) {
			if (parentId == null || root == null) {
				Utils.warn("Found children for null parent. Parent: %s\n", root.toString());
				Utils.warn(rs.getRowAt(rs.getRow()));
				Utils.warn("\n-- -- -- -- This is a common error when tree structure is broken, check data of recently printed sql.");
				throw new SemanticException("Found children for null parent. Check the data queried by recent committed SQL.");
			}

			String currentParentID = rs.getString(sm.dbParent());
			if (currentParentID == null || currentParentID.trim().length() == 0) {
				// new tree root
				rs.previous();
				if (childrenArray.size() > 0) { 
					// childrenArray.get(childrenArray.size() - 1).asLastSibling();
					root.children_(childrenArray).tagLast();
				}
				return childrenArray;
			}
			// HERE! ending adding children
			if (!currentParentID.trim().equals(parentId.trim())) {
				rs.previous();
				if (childrenArray.size() > 0) {
					// childrenArray.get(childrenArray.size() - 1).asLastSibling();
					root.children_(childrenArray).tagLast();
				}
				return childrenArray;
			}

			// next child & it's subtree
			AnTreeNode child = isNull(noder) ? formatSemanticNode(sm, rs, root, level + 1)
											 : noder[0].format(sm, rs, root, level + 1);

			
			List<AnTreeNode> subOrg = buildSubTree(sm, child,
					rs.getString(sm.dbRecId()), rs, level + 1);

			if (subOrg.size() > 0)
				child.children_(subOrg);

			childrenArray.add(child);

			root.children_(childrenArray);
		}

		return childrenArray;
	}

	/**
	 * Create a SemanticObject for tree node with current rs row.<br>
	 * 
	 * TODO should this moved to TreeSemantics?
	 * @param treeSmx
	 * @param rs
	 * @param level depth of this node
	 * @return {@link SemanticObject} of node
	 * @throws SQLException
	 */
	static AnTreeNode formatSemanticNode(TreeSemantics treeSmx, AnResultset rs,
			AnTreeNode parent, int level) throws SQLException {
		AnTreeNode node = new AnTreeNode(rs.getString(treeSmx.dbRecId()),
								rs.getString(treeSmx.dbParent()), level, parent);

		for (int i = 1;  i <= rs.getColCount(); i++) {
			String v = rs.getString(i);
			String col = rs.getColumnName(i);
			col = treeSmx.alias(col);
			if (v != null)
				node.put(col,  v);
		}
		return node;
	}

}
