package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.len;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.odysz.common.Utils;
import io.odysz.common.dbtype;
import io.odysz.module.xtable.XMLTable.IMapValue;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

public abstract class SemanticTableMeta extends TableMeta implements IMapValue {

	private static FileSystem zipfs;

	protected static String loadSqlite(Class<?> clzz, String filename) {
		try {
			// https://stackoverflow.com/a/46468788/7362888
			// URI uri = Paths.get(clzz.getResource(filename).toURI()).toUri();
			URI uri = clzz.getResource(filename).toURI();
			if (!eq(uri.getScheme(), "file") && zipfs == null)
				try {
					Map<String, String> env = new HashMap<>(); 
					env.put("create", "true");
					zipfs = FileSystems.newFileSystem(uri, env);
				}
				catch (Exception e) {
					Utils.warnT(new Object() {},
						"File %s shouldn't be load in the runtime environment.\ntarget URI: %s",
						filename, uri);
					e.printStackTrace();
					return null;
				}

			uri = Paths.get(uri).toUri();

			return Files.readAllLines(
				Paths.get(uri), Charset.defaultCharset())
				.stream().collect(Collectors.joining("\n"));
		} catch (Exception e) {
			Utils.warnT(new Object() {},
				"File %s can't be loaded in the runtime environment.\n%s",
				filename, e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public String mapKey() {
		return tbl;
	}

	public SemanticTableMeta(String tbl, String... conn) {
		super(tbl, conn); 
	}

	/**
	 * Explicitly call this after this meta with semantics is created,
	 * to replace auto found meta from database, which is managed by {@link Connects}.
	 * 
	 * @return this
	 * @throws TransException
	 * @throws SQLException 
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	public <T extends SemanticTableMeta> T replace() throws TransException, SQLException {
		TableMeta mdb = Connects.getMeta(conn, tbl);

		if (len(this.ftypes) == 0)
			DASemantics.replaceMeta(tbl, this, conn);

		if (isNull(this.ftypes) && mdb.ftypes() != null)
			this.ftypes = mdb.ftypes();
		return (T) this;
	}
	
	/**
	 * Commit Sqlite3 table ddls defined by m[i].ddlSqlite.
	 * 
	 * @param conn
	 * @param force_drop force dropping table before commit {@link TableMeta#ddlSqlite}.
	 * @param ms
	 * @throws SQLException
	 * @throws TransException
	 */
	public static void setupSqliTables(String conn, boolean force_drop, SemanticTableMeta ... ms)
			throws SQLException, TransException {
		if (ms != null && Connects.isqlite(conn))
		for (TableMeta m : ms)
			if (!isblank(m.ddlSqlite)) {
				Utils.logi("[%s] DDL by TableMeta.ddlSqlite: %s, table: %s", conn, m.getClass().getName(), m.tbl);

				if (force_drop) Connects.commit(conn, DATranscxt.dummyUser(),
						String.format("drop table if exists %s;", m.tbl));

				Connects.commit(conn, DATranscxt.dummyUser(), m.ddlSqlite);
			}
			else if (force_drop)
				throw new SemanticException("Modifying table %s without DDL provided?", m.tbl);
	}

	/**
	 * @deprecated since 0.7.6, redundant
	 * @param conn
	 * @param force_drop
	 * @param ms
	 * @throws SQLException
	 * @throws TransException
	 */
	public static void setupSqlitables(String conn, boolean force_drop, Iterable<SemanticTableMeta> ms)
			throws SQLException, TransException {
		dbtype dt = Connects.driverType(conn);
		if (dt != dbtype.sqlite && dt != dbtype.sqlite_queue)
			Utils.warnT(new Object(){},
				"This method is only used for sqlite DB. [%s]", conn);

		if (ms != null && Connects.isqlite(conn))
		for (TableMeta m : ms)
			if (!isblank(m.ddlSqlite)) {
				if (force_drop) Connects.commit(conn, DATranscxt.dummyUser(),
						String.format("drop table if exists %s;", m.tbl));

				Connects.commit(conn, DATranscxt.dummyUser(), m.ddlSqlite);
			}
			else if (debug && (dt == dbtype.sqlite || dt == dbtype.sqlite_queue))
				Utils.warn("Table meta's ddl is null. The table needs to be created manually. %s [%s]",
						m.tbl, conn);

	}

}
