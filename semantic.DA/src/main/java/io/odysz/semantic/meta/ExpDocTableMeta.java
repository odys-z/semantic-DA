package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.f;
import static io.odysz.common.LangExt.replacele;
import static io.odysz.transact.sql.parts.condition.Funcall.refile;
import java.sql.SQLException;

import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.syn.SyndomContext;
import io.odysz.semantics.ISemantext;
import io.odysz.semantic.DATranscxt;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.parts.AnDbField;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**
 * Experimental: document entity table meta.
 * 
 * TODO rename together with ExpDoctier, ExpSynodetier.
 *
 * @author odys-z@github.com
 */
public abstract class ExpDocTableMeta extends SyntityMeta {

	public class DocRef extends AnDbField {
		String synode;
		String tbl;
		String uri64;

		@AnsonField(ignoreTo=true)
		String docId;
		
		@AnsonField(ignoreTo=true)
		ExpDocTableMeta met;
		
		@AnsonField(ignoreTo=true, ignoreFrom=true)
		final String clsname;

		@AnsonField(ignoreTo=true, ignoreFrom=true)
		Funcall concats;

		public DocRef() {
			clsname = getClass().getName();
		}


		public DocRef(String synode, ExpDocTableMeta m) {
			this();
			this.synode = synode;
			this.tbl = m.tbl;
			this.met = m;

			concats = Funcall.concat(
				f("'{\"type\": \"%s\", \"synode\": \"%s\", \"docId\": \"'", clsname, synode),
				met.pk,
				f("'\", \"tbl\": \"%s\", \"uri64\": \"'", tbl),
				m.uri,
				"'\"}'");
		}

		@Override
		public String sql(ISemantext context) throws TransException {
			return concats.sql(context);
		}

	}

	public final String fullpath;
	/** Also named as pname, clientname or filename previously. */
	public final String resname;
	/**
	 * @see io.odysz.semantic.DASemantics.ShExtFilev2 ShExtFile.
	 */
	public final String uri;
	
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
	public Query onselectSyntities(SyndomContext syndomx, Query select) throws TransException {
		String a = tbl; 
		if (select.alias() != null)
			a = select.alias().toString();
		return select
			.clos_clear()
			.cols_byAlias(a,
				DATranscxt.hasSemantics(conn, tbl, smtype.extFilev2)
				// 2025-05-23: Root of OutOfMemoryError.
				// ? replacele(entCols(), uri, extfile(a + "." + uri))
				? replacele(entCols(), uri, refile(new DocRef(syndomx.synode, this)))
				: entCols());
	}
}
