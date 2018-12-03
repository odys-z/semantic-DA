package io.odysz.semantic.DA;

import io.odysz.semantics.ISemantext;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Transcxt;

public class DATranscxt extends Transcxt {

	@Override
	public Query select(String tabl, String... alias) {
		Query q = super.select(tabl, alias);
		q.postOp(sql -> Connects.select(sql));
		return q;
	}

	public DATranscxt(ISemantext semantext) {
		super(semantext);
	}

}
