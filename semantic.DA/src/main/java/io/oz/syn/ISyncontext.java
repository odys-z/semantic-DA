package io.oz.syn;

import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.ISemantext;

/**
 * Semantic context for synchronizer, {@link DBSyntableBuilder}
 * @since 1.5.16
 */
public interface ISyncontext extends ISemantext {
	/** Get a new builder */
	public DATranscxt synbuilder();

	public SyndomContext syndomContext();
}
