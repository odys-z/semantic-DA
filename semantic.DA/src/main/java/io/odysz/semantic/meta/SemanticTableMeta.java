package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.len;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.stream.Collectors;

import io.odysz.common.Utils;
import io.odysz.module.xtable.XMLTable.IMapValue;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

public abstract class SemanticTableMeta extends TableMeta implements IMapValue {

	protected static String loadSqlite(Class<?> clzz, String filename) {
		try {
			// https://stackoverflow.com/a/46468788/7362888
			URI uri = Paths.get(clzz.getResource(filename).toURI()).toUri();
//			if (zipfs == null)
//				try {
//					Map<String, String> env = new HashMap<>(); 
//					env.put("create", "true");
//					zipfs = FileSystems.newFileSystem(uri, env);
//				}
//				catch (Exception e) {
//					Utils.warnT(new Object() {},
//						"File %s shouldn't be load in the runtime environment.\ntarget URI: %s",
//						filename, uri);
//					e.printStackTrace();
//					return null;
//				}

			return Files.readAllLines(
				Paths.get(uri), Charset.defaultCharset())
				.stream().collect(Collectors.joining("\n"));
		} catch (Exception e) {
			Utils.warnT(new Object() {},
				"File %s can't be loaded in the runtime environment.\n%s",
				filename, e.getMessage());
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
	@SuppressWarnings("unchecked")
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
				if (force_drop) Connects.commit(conn, DATranscxt.dummyUser(),
						String.format("drop table if exists %s;", m.tbl));

				Connects.commit(conn, DATranscxt.dummyUser(), m.ddlSqlite);
			}
			else if (force_drop)
				throw new SemanticException("Forcing drop table %s without DDL provided.", m.tbl);
	}

	public static void setupSqlitables(String conn, boolean force_drop, Iterable<SyntityMeta> ms)
			throws SQLException, TransException {
		if (ms != null && Connects.isqlite(conn))
		for (TableMeta m : ms)
			if (!isblank(m.ddlSqlite)) {
				if (force_drop) Connects.commit(conn, DATranscxt.dummyUser(),
						String.format("drop table if exists %s;", m.tbl));

				Connects.commit(conn, DATranscxt.dummyUser(), m.ddlSqlite);
			}
	}

}
