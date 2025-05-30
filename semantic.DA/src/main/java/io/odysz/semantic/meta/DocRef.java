package io.odysz.semantic.meta;

import static io.odysz.common.LangExt.f;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.odysz.anson.AnsonField;
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
	public String tbl;
	public String uri64;
	public String uids;
	public String docId;

	public long breakpoint;
	public DocRef breakpoint(int b) {
		breakpoint = b;
		return this;
	}
	
	@AnsonField(ignoreTo=true)
	public ExpDocTableMeta meta;
	
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
		this.tbl = m.tbl;
		this.meta = m;

		concats = Funcall.concat(
			f("'{\"type\": \"%s\", \"synode\": \"%s\", \"docId\": \"'", clsname, synode),
			meta.pk,
			f("'\", \"tbl\": \"%s\", \"uri64\": \"%s\", \"breakpoint\": %s, \"uids\": \"'", tbl, m.uri, breakpoint),
			m.io_oz_synuid,
			"'\"}'");
	}

//	public DocRef(String synode, AnResultset rs, ExpDocTableMeta docm) throws SQLException {
//		this(synode, docm);
//		tbl = docm.tbl;
//		uri64 = rs.getString(docm.uri);
//		uids = rs.getString(docm.io_oz_synuid);
//		docId = rs.getString(docm.pk);
//	}


	@Override
	public String sql(ISemantext context) throws TransException {
		return concats.sql(context);
	}


	public Path downloadPath(SessionInf ssinf) {
		return Paths.get(IUser.tempDir(synode, uids, docId, ssinf.ssid()));
	}

}