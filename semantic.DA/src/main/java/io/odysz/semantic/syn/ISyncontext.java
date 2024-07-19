package io.odysz.semantic.syn;

import io.odysz.semantics.ISemantext;
import io.odysz.transact.sql.parts.AbsPart;

public interface ISyncontext extends ISemantext {
	DBSyntableBuilder synbuilder();

	Long nyquence();

	String domain();

	Nyquence stamp();

	long incSeq();
}
