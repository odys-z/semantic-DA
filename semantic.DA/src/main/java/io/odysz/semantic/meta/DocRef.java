package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.f;
import static io.odysz.common.FilenameUtils.concat;

import io.odysz.anson.AnsonField;
import io.odysz.common.EnvPath;
import io.odysz.common.FilenameUtils;
import io.odysz.common.Regex;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.SessionInf;
import io.odysz.transact.sql.parts.AnDbField;
import io.odysz.transact.sql.parts.ExtFilePaths;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

public class DocRef extends AnDbField {
	/**
	 * 
	 */
	public String synoder;
	public String syntabl;
	public String uri64;

	public String uids;
	public DocRef uids(String uids) {
		this.uids = uids;
		return this;
	}

	/** 
	 * @since 1.5.18
	 * This is generated at server node, directly send and write into client's db,
	 * so not usable when loaded from entity table for resolving references.
	 */
	public String docId;

	public long breakpoint;
	public DocRef breakpoint(int b) {
		breakpoint = b;
		return this;
	}
	
	@AnsonField(ignoreTo=true)
	public ExpDocTableMeta docm;
	public DocRef docm(ExpDocTableMeta docm) {
		this.docm = docm;
		return this;
	}
	
	@AnsonField(ignoreTo=true, ignoreFrom=true)
	final String clsname;

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	/** SQL boilerplate for generating serialized json string from doc-tables. */
	Funcall concats;

	/** E.g. the h_photos.pname, must not null for avoid conflicts,
	 * by padding pid, at other synodes.
	 */
	public String pname;

	/**
	 * Usually environment volume path.
	 * 
	 * <h5>Issue:</h5>
	 * Changing the configurable volume variable name will fail for replacing old data,
	 * so it's simple replaced with the string format convention, the first "&.../" as volume name.
	 * But this value is generated while reading entities for exchange.
	 * 
	 * @see ExpDocTableMeta#onselectSyntities(io.odysz.semantic.syn.SyndomContext, io.odysz.transact.sql.Query)
	 */
	public String volume;

	public DocRef() {
		clsname = getClass().getName();
		breakpoint = 0;
	}

	public DocRef(String synoder, ExpDocTableMeta m, String volume_extroot, ISemantext dbcontext) throws TransException {
		this();
		this.synoder = synoder;
		this.syntabl = m.tbl;
		this.docm = m;
		this.volume = volume_extroot;

		concats = Funcall.concat(
			f("'{\"type\": \"%s\", \"synoder\": \"%s\", \"docId\": \"'", clsname, synoder),
			docm.pk,
			f("'\", \"syntabl\": \"%s\", \"uri64\": \"'", syntabl),
			Funcall.isnull(m.uri, "'null'").sql(dbcontext),
			f("'\", \"breakpoint\": %s, \"uids\": \"'", breakpoint),
			Funcall.isnull(m.io_oz_synuid, "'null'").sql(dbcontext),
			"'\", \"pname\": \"'", m.resname,
			"'\"}'");
	}

	@Override
	public String sql(ISemantext context) throws TransException {
		return concats.sql(context);
	}


	public static String resolveFolder(String peer, String conn, String syntabl, SessionInf ssInfo) {
		ShExtFilev2 h = ((ShExtFilev2) DATranscxt
				.getHandler(conn, syntabl, smtype.extFilev2));
		
		return EnvPath.decodeUri(h.getFileRoot(), "resolve-" + peer, ssInfo.ssid());
	}

	/**
	 * Get temporary path for downloading in asyn-resolver.
	 * @param peer
	 * @param doconn
	 * @param ssinf
	 * @return temp path, subfolder of {@link #resolveFolder(String, String, String, SessionInf) resolve-folder}.
	 * @throws TransException
	 */
	public String downloadPath(String peer, String doconn, SessionInf ssinf) throws TransException {
		ShExtFilev2 h = ((ShExtFilev2) DATranscxt
				.getHandler(doconn, syntabl, smtype.extFilev2));

		return h.getExtPaths(docId, pname)
				.prefix(concat("resolve-" + peer, ssinf.ssid(), relativeFolder(h.getFileRoot())))
				.decodeUriPath();
	}

	public DocRef docId(String pk) {
		this.docId = pk;
		return this;
	}

	public DocRef resname(String fn) {
		this.pname = fn;
		return this;
	}

	public static ExtFilePaths createExtPaths(String conn, String tbl, DocRef ref)
			throws TransException {
		ShExtFilev2 sh = ((ShExtFilev2) DATranscxt
				.getHandler(conn, tbl, smtype.extFilev2));
		
		return sh.getExtPaths(ref.docId, ref.pname)
				.prefix(ref.relativeFolder(sh.getFileRoot()))
				;
	}

	public String relativeFolder(String extroot) {
		return isblank(uri64) ? uri64
				: FilenameUtils.getPathNoEndSeparator(
				  Regex.removeVolumePrefix(uri64, extroot));
	}

}