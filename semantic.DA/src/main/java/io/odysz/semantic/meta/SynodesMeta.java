package io.odysz.semantic.meta;

import io.odysz.semantics.meta.TableMeta;

/**
 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
 * 
 * @author odys-z@github.com
 *
 */
public class SynodesMeta extends TableMeta {
	static {
		sqlite = "syn_node.sqlite.ddl";
	}

	public SynodesMeta(String... conn) {
		super("syn_node", conn);
	}
}
