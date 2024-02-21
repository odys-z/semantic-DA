package io.odysz.semantic.syn;

/**
 * jserv-node states
 */
public enum SynodeMode {
	/** jserv node mode: cloud hub, equivalent of jserv/Docsyncer.cloudHub */
	hub,
	/** jserv node mode: private main, equivalent of jserv/Docsyncer.mainStorage */
	main,
	/** jserv node mode: bridge , equivalent of jserv/Docsyncer.privateStorage*/
	bridge,
	/** jserv client device */
	device
}