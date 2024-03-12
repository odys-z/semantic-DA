package io.odysz.semantic.meta;

import static io.odysz.common.Utils.loadTxt;

import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
 * 
 * @author odys-z@github.com
 *
 */
public class SynodeMeta extends SyntityMeta {

	public final String domain;
	public final String nyquence;
	public final String mac;

	/**
	 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
	 * 
	 * @param conn
	 * @throws SemanticException 
	 */
	public SynodeMeta(String conn) throws TransException {
		super("syn_node", "synid", "org", conn, "syn_node.sqlite.ddl");

		pk = "synid";
		synoder = "synid";
		mac = "mac";
		nyquence = "nyq";
		domain = "domain";

		ddlSqlite = loadTxt(SyntityMeta.class, "syn_node.sqlite.ddl");
	}

//	@Override
//	public SynodeMeta clone(TableMeta dbm) throws TransException {
//		super.clone(dbm);
//		return this;
//	}
}
