package io.odysz.semantic.DA;

import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Transcxt;

public class DATranscxt extends Transcxt {

	@Override
	public Query select(String tabl, String... alias) {
		Query q = super.select(tabl, alias);
		q.doneOp(sqls -> Connects.select(sqls.get(0)));
		return q;
	}
	
	public Insert insert(String tabl, IUser usr) {
		Insert i = super.insert(tabl);
		i.doneOp(sqls -> Connects.commit(usr, sqls));
		return i;
	}

	public DATranscxt(ISemantext semantext) {
		super(semantext);
	}

}
