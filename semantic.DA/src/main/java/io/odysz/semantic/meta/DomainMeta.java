package io.odysz.semantic.meta;

public class DomainMeta extends SemanticTableMeta {

	public final String parent;
	public final String domainName;
	public final String domainValue;
	public final String sort;
	public final String fullpath;

	public DomainMeta(String... conn) {
		super("a_domain", conn);
		this.pk          = "domainId";
		this.parent      = "parentId";
		this.domainName  = "domainName";
		this.domainValue = "domainValue";
		this.sort        = "sort";
		this.fullpath    = "fullpath";

		ddlSqlite = "CREATE TABLE a_domain (\n"
					+ "  domainId    varchar2(12) NOT NULL,\n"
					+ "  parentId 	 varchar2(12),\n"
					+ "  domainName  varchar2(50),\n"
					+ "  domainValue varchar2(50),\n"
					+ "  sort        int DEFAULT 0,\n"
					+ "  fullpath    varchar2(80),\n"
					+ "  PRIMARY KEY (\"domainId\")\n"
					+ ");";
	}

}
