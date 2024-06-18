package io.odysz.semantic.syn;

import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.transact.x.TransException;

/**
 * Document entity table meta.
 *
 * @author odys-z@github.com
 */
public abstract class T_DocTableMeta extends SyntityMeta {
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
	public final String createDate;
	public final String shareDate;
	public final String shareby;

	public final String folder;
	public final String mime;
	public final String size;

	// public final String syncflag;
	public final String shareflag;

	public T_DocTableMeta(String tbl, String pk, String domain, String device, String conn) throws TransException {
		super(tbl, pk, domain, device, conn);

		resname = "pname";
		uri = "uri";
		folder = "folder";
		createDate = "pdate";
		// org = "family";
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
	 */
	public String org() { return domain; }


}
