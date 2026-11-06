package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.isblank;

import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.x.SemanticException;

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
 * @author odys
 */
public class AutoSeqMeta extends SemanticTableMeta {

	/**
	 * @param conn optional; if given, it must be a sqlite connection
	 * @throws SemanticException the connection is not sqlite (or unknown): oz_autoseq
	 * is only for sqlite, other DBs generate auto keys with their own stored function.
	 * @since 1.5.27 checks the connection's type
	 */
	public AutoSeqMeta(String... conn) throws SemanticException {
		super("oz_autoseq", conn);

		if (!isNull(conn) && !isblank(conn[0])) {
			boolean sqlite;
			try { sqlite = Connects.isqlite(conn[0]); }
			catch (NullPointerException e) {
				throw new SemanticException("Can't create AutoSeqMeta for an unknown connection: %s", conn[0]);
			}
			if (!sqlite)
				throw new SemanticException(
					"AutoSeqMeta (oz_autoseq) is only for sqlite, but connection %s is %s.",
					conn[0], Connects.driverType(conn[0]));
		}
	 
		ddlSqlite = String.format(
			"CREATE TABLE if not exists %s (\n" +
			"  sid text(50),\n" +
			"  seq INTEGER,\n" +
			"  remarks text(200),\n" +
			"  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid)\n" +
			");", tbl);
	}
}
