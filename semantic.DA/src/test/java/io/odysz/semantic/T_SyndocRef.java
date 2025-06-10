package io.odysz.semantic;

import static io.odysz.common.LangExt.f;

import io.odysz.anson.AnsonField;
import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.transact.sql.parts.AnDbField;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**
 * Emulator of ExpSyndoc Referencing object, used in docsync.jserv.
 * @since 1.5.18
 */
public class T_SyndocRef extends AnDbField {

	String synode;
	String tbl;
	String uri64;
	String uids;

	@AnsonField(ignoreTo=true)
	String docId;
	
	@AnsonField(ignoreTo=true)
	ExpDocTableMeta met;
	
	@AnsonField(ignoreTo=true, ignoreFrom=true)
	final String clsname;

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	Funcall concats;

	public T_SyndocRef() {
		clsname = getClass().getName();
	}

	public T_SyndocRef(String synode, ExpDocTableMeta m, ISemantext dbcontext) throws TransException {
		this();
		this.synode = synode;
		this.tbl = m.tbl;
		this.met = m;

		concats = Funcall.concat(
			f("'{\"type\": \"%s\", \"synode\": \"%s\", \"docId\": \"'", clsname, synode),
			met.pk,
			f("'\", \"tbl\": \"%s\", \"uri64\": \"'", tbl),
			m.uri,
			"'\"uids\": '",
			Funcall.isnull(m.io_oz_synuid, "'null'").sql(dbcontext),
			"'\"}'");
	}

	@Override
	public String sql(ISemantext context) throws TransException {
		return concats.sql(context);
	}

}
