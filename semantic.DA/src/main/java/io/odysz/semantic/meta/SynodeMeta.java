package io.odysz.semantic.meta;

import java.util.HashSet;

import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

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
	public final String inc;
	final HashSet<String> globalPks;

	@SuppressWarnings("serial")
	/**
	 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
	 * 
	 * @param conn
	 * @throws SemanticException 
	 */
	public SynodeMeta(String... conn) throws TransException {
		super("syn_node", "synid", conn);
		
		org = "org";
		synode = "synode";
		
		inc = "inc";
		globalPks = new HashSet<String>() { {add(org);}; {add(synode);} };
	}

	@Override
	public HashSet<String> globalIds() {
		return globalPks;
	}
	
	@Override
	public SynodeMeta clone(TableMeta dbm) throws TransException {
		super.clone(dbm);
		if (dbm.coltype(org) == null)
			throw new SemanticException("Internal Error");
		return this;
	}
}
