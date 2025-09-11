package io.odysz.semantic.meta;

public class ALogsMeta extends SemanticTableMeta {
	public ALogsMeta(String... conn) {
		super("a_logs", conn);
	 
		ddlSqlite = "CREATE TABLE a_logs (\n"
				+ "  logId    varchar2(20) NOT NULL,\n"
				+ "  funcId   varchar2(20),\n"
				+ "  funcName varchar2(50),\n"
				+ "  oper     varchar2(20),\n"
				+ "  logTime  datetime,\n"
				+ "  cnt      INTEGER,\n"
				+ "  txt      text,\n"
				+ "  CONSTRAINT oz_logs_pk PRIMARY KEY (logId));";
	}
}
