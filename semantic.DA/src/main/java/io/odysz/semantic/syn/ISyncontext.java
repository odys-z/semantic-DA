package io.odysz.semantic.syn;

import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.ISemantext;

/**
 * Semantic context for synchronizer, {@link DBSyntableBuilder}
 */
public interface ISyncontext extends ISemantext {
	/** Get a new builder */
	public <T extends DATranscxt> T synbuilder();
}
