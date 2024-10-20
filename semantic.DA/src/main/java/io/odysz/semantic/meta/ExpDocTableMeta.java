package io.odysz.semantic.meta;

import java.sql.SQLException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
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
	/**
	 * consts of share type: pub | priv 
	 */
	public static class Share {
		/** public asset */
		public static final String pub = "pub";
		/** private asset */
		public static final String priv = "priv";

		public static boolean isPub(String s) {
			if (pub.equals(s)) return true;
			return false;
		}

		public static boolean isPriv(String s) {
			if (priv.equals(s)) return true;
			return false;
		}
	}

	public final String fullpath;
	/** aslo named as pname, clientname or filename previously */
	public final String resname;
	/**
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
}
