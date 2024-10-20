package io.odysz.semantic.syn;

/**
 * Synode working modes
 */
public enum SynodeMode {
	/** device or transction builder, etc. are not for synodes */
	nonsyn,
	
	/** Jserv node mode: cloud hub, accepting application from {@link #child} */
	peer,
	/**
	 * Work as a tree branch mode, tree's middle and leaf node,
	 * via filing application to {@link #hub}
	 */
	leaf
}