package io.odysz.semantic.meta;

import io.odysz.semantics.meta.TableMeta;

public class DomainMeta extends TableMeta {

	public final String parent;
	public final String domainName;
	public final String domainValue;
	public final String sort;
	public final String fullpath;

	public DomainMeta(String... conn) {
		super("a_domain", conn);
		this.pk = "domainId";
		this.parent = "parentId";
		this.domainName = "domainName";
		this.domainValue = "domainValue";
		this.sort = "sort";
		this.fullpath = "fullpath";
	}

}
