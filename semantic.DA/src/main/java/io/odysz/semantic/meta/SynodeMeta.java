package io.odysz.semantic.meta;

import java.util.HashSet;

import io.odysz.semantics.x.SemanticException;

/**
 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
 * 
 * @author odys-z@github.com
 *
 */
public class SynodeMeta extends SyntityMeta {
	static {
		sqlite = "syn_node.sqlite.ddl";
	}
	
	public final String org;
	/** organization's nodes */
	public final String synode;
	public final String entbl;
	public final String inc;
	final HashSet<String> globalPks;

	@SuppressWarnings("serial")
	/**
	 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
	 * 
	 * @param conn
	 * @throws SemanticException 
	 */
	public SynodeMeta(String... conn) throws SemanticException {
		super("syn_node", "synid", conn);
		
		entbl = "tabl";
		org = "org";
		synode = "synode";
		
		inc = "inc";
		globalPks = new HashSet<String>() { {add(org);}; {add(synode);} };
	}

	@Override
	public HashSet<String> globalIds() {
		return globalPks;
	}
}
