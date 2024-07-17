package io.odysz.semantic.syn;

/**
 * jserv-node states
 */
public enum SynodeMode {
	/** jserv node mode: cloud hub, equivalent of jserv/Docsyncer.cloudHub */
	// hub,
	/** jserv node mode: private main, equivalent of jserv/Docsyncer.mainStorage */
	// main,
	/** jserv node mode: bridge , equivalent of jserv/Docsyncer.privateStorage*/
	// bridge,
	/** jserv client device */
	// device
	
	/** device or so without DB */
	nondb,
	
	/** Jserv node mode: cloud hub, accepting application from {@link #child} */
	peer,
	/**
	 * Work as a tree branch mode, tree's middle and leaf node,
	 * via filing application to {@link #hub}
	 */
	leaf
}