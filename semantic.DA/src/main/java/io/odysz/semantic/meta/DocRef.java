package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.f;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.odysz.anson.AnsonField;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SessionInf;
import io.odysz.transact.sql.parts.AnDbField;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

public class DocRef extends AnDbField {
	/**
	 * 
	 */
	public String synode;
	public String syntabl;
	public String uri64;
	public String uids;
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
	
	@AnsonField(ignoreTo=true, ignoreFrom=true)
	final String clsname;

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	Funcall concats;

	public DocRef() {
		clsname = getClass().getName();
		breakpoint = 0;
	}

	public DocRef(String synode, ExpDocTableMeta m) {
		this();
		this.synode = synode;
		this.syntabl = m.tbl;
		this.docm = m;

		concats = Funcall.concat(
			f("'{\"type\": \"%s\", \"synode\": \"%s\", \"docId\": \"'", clsname, synode),
			docm.pk,
			f("'\", \"syntabl\": \"%s\", \"uri64\": \"%s\", \"breakpoint\": %s, \"uids\": \"'", syntabl, m.uri, breakpoint),
			m.io_oz_synuid,
			"'\"}'");
	}

	@Override
	public String sql(ISemantext context) throws TransException {
		return concats.sql(context);
	}

	public Path downloadPath(String peer, String doconn, SessionInf ssinf) {
		String extroot = ((ShExtFilev2) DATranscxt
				.getHandler(doconn, syntabl, smtype.extFilev2))
				.getFileRoot();
		return Paths.get(IUser.tempDir(extroot, "resolve-" + peer, ssinf.ssid(), syntabl));
	}
}