package io.odysz.semantic.meta;

import io.odysz.transact.x.TransException;

/**
 * Experimental: document entity table meta.
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

	public ExpDocTableMeta(String tbl, String pk, String device, String conn) throws TransException {
		super(tbl, pk, device, conn);

		resname = "pname";
		this.device = device;
		uri = "uri";
		folder = "folder";
		createDate = "pdate";
		org = "family";

		mime = "mime";
		size = "filesize";
		synoder = "device";
		fullpath = "clientpath";

		shareDate = "sharedate";
		shareby = "shareby";
		shareflag = "shareflag";
	}

	/**
	 * Design Memo / issue: currently org is the default synchronizing domain? 
	 * @return org id
	public String org() { return domain; }
	 */


}
