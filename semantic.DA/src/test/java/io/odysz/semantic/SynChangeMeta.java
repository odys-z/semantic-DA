package io.odysz.semantic;

import io.odysz.semantics.meta.TableMeta;

public class SynChangeMeta extends TableMeta {

	public String crud;
	public String flag;
	public String tablefield;

	public SynChangeMeta(String ... conn) {
		super("syn_change", conn);
		
		crud = "crud";
		flag = "flag";
		tablefield = "tabl";
		pk = "recId";
	}

	public String[] cols() {
		return new String[] {tablefield, pk, crud, flag};
	}

}
