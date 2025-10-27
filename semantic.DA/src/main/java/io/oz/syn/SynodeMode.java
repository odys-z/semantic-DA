package io.oz.syn;

/**
 * Synode working modes
 * @since 1.5.18
 */
public enum SynodeMode {
	/** device or transction builder, etc. are not for synodes */
	nonsyn,
	
	/** Jserv node mode: cloud hub or peers, accepting application from others. */
	peer,

	/**
	 * @since 0.7.6
	 */
	hub,
	/**
	 * @deprecated since 0.7.6
	 * Work as a tree branch mode, tree's middle and leaf node,
	 * via filing application to {@link #peer}
	 */
	leaf_
}