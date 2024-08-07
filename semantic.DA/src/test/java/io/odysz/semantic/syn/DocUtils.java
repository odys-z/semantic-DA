//package io.odysz.semantic.syn;
//
//import java.io.IOException;
//import java.sql.SQLException;
//
//import io.odysz.common.EnvPath;
//import io.odysz.common.LangExt;
//import io.odysz.module.rs.AnResultset;
//import io.odysz.semantic.DASemantics.ShExtFilev2;
//import io.odysz.semantic.DASemantics.smtype;
//import io.odysz.semantic.meta.ExpDocTableMeta;
//import io.odysz.semantic.DATranscxt;
//import io.odysz.semantics.ISemantext;
//import io.odysz.semantics.IUser;
//import io.odysz.semantics.SemanticObject;
//import io.odysz.semantics.x.SemanticException;
//import io.odysz.transact.sql.Insert;
//import io.odysz.transact.sql.Update;
//import io.odysz.transact.x.TransException;
//
//public class DocUtils {
//	/**
//	 * <p>Create a doc record with a local file, e.g. h_photos - call this after duplication is checked.</p>
//	 * <p>This method will insert record, and can trigger ExtFilev2 handling.</p>
//	 * <p>Photo is created as in the folder of user/[photo.folder]/;<br>
//	 * Photo's device and family are replaced with session information.</p>
//	 * 
//	 * @since 1.4.19, this method need DB triggering timestamp
//	 * <pre>
//	 * sqlite example:
//	 * syncstamp DATETIME DEFAULT CURRENT_TIMESTAMP not NULL
//	 * </pre>
//	 * @param conn
//	 * @param photo with photo.uri that is the entire base-64 encoded string
//	 * @param usr
//	 * @param meta 
//	 * @param st 
//	 * @param onDocreate post statement to be committed when doc created
//	 * @return doc id, e.g. pid
//	 * @throws TransException
//	 * @throws SQLException
//	 * @throws IOException
//	 */
//	public static String createFileB64(String conn, ExpSyncDoc photo, IUser usr, ExpDocTableMeta meta, DATranscxt st, Update onDocreate)
//			throws TransException, SQLException, IOException {
//		if (LangExt.isblank(photo.clientpath))
//			throw new SemanticException("Client path can't be null/empty.");
//		
//		if (LangExt.isblank(photo.folder(), " - - "))
//			throw new SemanticException("Folder of managed doc can not be empty - which is important for saving file. It's required for creating media file.");
//
//		Insert ins = st.insert(meta.tbl, usr)
//				.nv(meta.org, usr.orgId())
//				.nv(meta.uri, photo.uri)
//				.nv(meta.resname, photo.pname)
//				.nv(meta.synoder, usr.deviceId())
//				.nv(meta.fullpath, photo.fullpath())
//				.nv(meta.createDate, photo.createDate)
//				.nv(meta.folder, photo.folder())
//				.nv(meta.shareflag, photo.shareflag)
//				.nv(meta.shareby, photo.shareby)
//				.nv(meta.shareDate, photo.sharedate)
//				.nv(meta.size, photo.size)
//				;
//		
//		// add a synchronizing task
//		// - also triggered as private storage jserv, but no statement will be added
//		if (onDocreate != null)
//			ins.post(onDocreate);
//
//		ISemantext insCtx = st.instancontxt(conn, usr);
//		SemanticObject res = (SemanticObject) ins.ins(insCtx);
//		String pid = ((SemanticObject) ((SemanticObject) res.get("resulved"))
//				.get(meta.tbl))
//				.getString(meta.pk);
//		return pid;
//	}
//
//	/**
//	 * Resolve file uri with configured Semantics handler, {@link smtype#extFile}.
//	 * @param uri
//	 * @param meta
//	 * @param conn
//	 * @return decode then concatenated absolute path, for file accessing.
//	 * @see EnvPath#decodeUri(String, String)
//	 */
//	public static String resolvePrivRoot(String uri, ExpDocTableMeta meta, String conn) {
//		String extroot = ((ShExtFilev2) DATranscxt
//				.getHandler(conn, meta.tbl, smtype.extFilev2))
//				.getFileRoot();
//		return EnvPath.decodeUri(extroot, uri);
//	}
//
//	public static String resolvExtroot(DATranscxt st, String conn, String docId, IUser usr, ExpDocTableMeta meta) throws TransException, SQLException {
//		ISemantext stx = st.instancontxt(conn, usr);
//		AnResultset rs = (AnResultset) st
//				.select(meta.tbl)
//				.col("uri").col("folder")
//				.whereEq("pid", docId).rs(stx)
//				.rs(0);
//	
//		if (!rs.next())
//			throw new SemanticException("Can't find file for id: %s (permission of %s)", docId, usr.uid());
//	
//		String extroot = ((ShExtFilev2) DATranscxt.getHandler(conn, meta.tbl, smtype.extFilev2)).getFileRoot();
//		return EnvPath.decodeUri(extroot, rs.getString("uri"));
//	}
//}
