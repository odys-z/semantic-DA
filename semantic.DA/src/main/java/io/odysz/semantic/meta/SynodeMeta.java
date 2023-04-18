package io.odysz.semantic.meta;

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
	public final String synode;
	public final String entbl;
	public final String inc;

	/**
	 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
	 * 
	 * @param conn
	 */
	public SynodeMeta(String... conn) {
		super("syn_node", "synid", conn);
		
		entbl = "tabl";
		org = "org";
		synode = "synode";
		
		inc = "inc";
	}
}
