package io.odysz.semantic.syn;

import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.ISemantext;

public interface ISyncontext extends ISemantext {
	public <T extends DATranscxt> T synbuilder();

//	Long nyquence();

//	String domain();

//	Nyquence stamp();

//	long incSeq();
}
