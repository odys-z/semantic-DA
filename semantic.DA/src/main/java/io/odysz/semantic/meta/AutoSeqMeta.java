package io.odysz.semantic.meta;

/**
 * For Sqlite3,
 * <pre>CREATE TABLE if not exists %s (
 * sid text(50),
 * seq INTEGER,
 * remarks text(200),
 * CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid));</pre>
 * 
 * Max rows (sid) is 2 ^ 64, which is not reachable in Sqlite3,
 * see <a href='https://www.sqlite.org/limits.html'>
 * 13. Maximum Number Of Rows In A Table</a>
 * 
 * @author odys-z@github.com
 */
public class AutoSeqMeta extends SemanticTableMeta {

	public AutoSeqMeta(String... conn) {
		super("oz_autoseq", conn);
	 
		ddlSqlite = String.format(
			"CREATE TABLE if not exists %s (\n" +
			"  sid text(50),\n" +
			"  seq INTEGER,\n" +
			"  remarks text(200),\n" +
			"  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid)\n" +
			");", tbl);
	}
}
