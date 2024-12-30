package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.replacele;
import static io.odysz.transact.sql.parts.condition.Funcall.extfile;

import java.sql.SQLException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.x.TransException;

/**
 * Experimental: document entity table meta.
 * 
 * TODO rename together with ExpDoctier, ExpSynodetier.
 *
 * @author odys-z@github.com
 */
public abstract class ExpDocTableMeta extends SyntityMeta {

	public final String fullpath;
	/** aslo named as pname, clientname or filename previously */
	public final String resname;
	/**
	 * FIXME not implemented: 2dbdbdfc2adae1a56c6dca8d435d915e580758fb
	 * Resource identity, reading with {@link io.odysz.transact.sql.parts.condition.Funcall.Func#extFile extFile}
	 * and updating with {@link io.odysz.semantic.DASemantics.ShExtFilev2 ShExtFile}.
	 */
	public final String uri;
	public final String device;
	public final String org;
	public final String createDate;
	public final String shareDate;
	public final String shareby;

	public final String folder;
	public final String mime;
	public final String size;

	public final String shareflag;

	public ExpDocTableMeta(String conn) throws TransException {
		this("syn_docs", "docId", "device", conn);
	}

	public ExpDocTableMeta(String tbl, String pk, String device, String conn)
			throws TransException {
		super(tbl, pk, device, conn);

		this.device = device;

		resname = "docname";
		uri = "uri";
		folder = "folder";
		createDate = "pdate";
		org = "family";

		mime = "mime";
		size = "filesize";
		fullpath = "clientpath";

		shareDate = "sharedate";
		shareby = "shareby";
		shareflag = "shareflag";

		uids.add(device);
		uids.add(fullpath);
	}

	/**
	 * Create select with cols can be understand by {@link #getPathInfo(AnResultset)}.
	 * 
	 * @param st
	 * @param devid
	 * @return {@link Query}
	 * @throws TransException
	 */
	public Query selectSynPaths(DATranscxt st, String devid) throws TransException {
		return  st.select(tbl, "t")
				  .cols(device, shareflag, shareby, shareDate);

	}

	/**
	 * Get fields from rs where cols is selcted with stamement generated
	 * by {@link #selectSynPaths(DATranscxt, String)}.
	 * @param rs
	 * @return strings
	 * @throws SQLException
	 */
	public Object[] getPathInfo(AnResultset rs) throws SQLException {
		return rs.getFieldArray(device, shareflag, shareby, shareDate);
	}

	@Override
	public Object[] entCols() throws SemanticException {
		// Object[] cols = super.entCols();
		// return cols;
		// FIXME but why the tests in Semantic.DA are all passed?
		throw new SemanticException("FIXME Not reachable.");
	}

	@Override
	public Query onselectSyntities(Query select) throws TransException {
		String a = tbl; 
		if (select.alias() != null)
			a = select.alias().toString();
		return select
				.clos_clear()
				.cols_byAlias(a,
					DATranscxt.hasSemantics(conn, tbl, smtype.extFilev2)
					? replacele(entCols(), uri, extfile(a + "." + uri))
					: entCols());
	}
}
