package io.odysz.semantic.syn;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A synode context, a database cache, per domain, for managing and sharing domain
 * wide information, e.g. stamp, n0, Nyquvect, etc., across multiple syn-change
 * handlers.
 * 
 * @author ody
 *
 */
class SyndomContext {

	final String synode;
	final String domain;
	final String synconn;

	Nyquence stamp;
	ReentrantLock stamplock;
	
	HashMap<String, Nyquence> nv;
	ReentrantLock nvlock;

	Nyquence stamp() {
		return stamp;
	}
	
	HashMap<String, Nyquence> nv() {
		return nv;
	}
	
	SyndomContext(String synode, String dom, String synconn) {
		this.synode  = synode;
		this.domain  = dom;
		this.synconn = synconn;
		
		stamplock = new ReentrantLock();
		nvlock    = new ReentrantLock();
	}
	
	Nyquence incN0(Nyquence maxn) {
		try {
			nvlock.lock();
			return nv.get(synode).inc(maxn);
		}
		finally { nvlock.unlock(); }
	}
	
}
