package io.odysz.semantic.syn;

import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.transact.x.TransException;

/**
 * Document entity table meta.
 *
 * @author odys-z@github.com
 */
public class T_DocTableMeta extends SyntityMeta {
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

	/**
	 * @deprecated DBSynmantics now is using Nyquence.
	 * DB column for automatic time stamp. 
	 * Sqlite:<pre>syncstamp DATETIME DEFAULT CURRENT_TIMESTAMP not NULL</pre>
	public final String stamp;
	 */
	/** resource's creating node's device id, originally named as device */
	public final String synoder;
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

//	public final String syncflag;
	public final String shareflag;

	// final HashSet<String> globalIds;

	public T_DocTableMeta(String tbl, String pk, String org, String conn) throws TransException {
		super(tbl, pk, org, conn);

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
		
		// uids = new HashSet<String>() { {add(synoder);}; {addAll(uids);} };
	}

//	public HashSet<String> globalIds() {
//		return globalIds;
//	}

}
