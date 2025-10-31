package io.oz.syn;

import static io.odysz.common.LangExt._0;
import static io.odysz.common.LangExt.musteqi;
import static io.odysz.common.LangExt.musteqs;
import static io.oz.syn.DBSyn2tableTest.chpageSize;
import static io.oz.syn.DBSyn2tableTest.zsu;

import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.semantic.meta.SyntityMeta;


/**
 * Domain manager for test.
 * 
 * <p>Note: domain lock is not testable in DA layer.</p>
 * @since 1.5.18
 */
public class T_SynDomanager extends SyndomContext {

	public DBSyntableBuilder synb;
	public SyntityMeta devm;
	public ExpDocTableMeta docm;
	public ExessionPersist xp;

	protected T_SynDomanager(SynodeMode mod, String org, String domain, String synode, String synconn, boolean... debug)
			throws Exception {
		super(mod, chpageSize, org, domain, synode, synconn, _0(debug, false));
		musteqs(zsu, domain);
		musteqi(16, chpageSize);
	}

	public T_SynDomanager(SyndomContext dx, DBSyntableBuilder synb) throws Exception {
		this(dx.mode, dx.org, dx.domain, dx.synode, dx.synconn);
		this.synb = synb;
	}

	public T_SynDomanager(Docheck ck) throws Exception {
		this(ck.synb.syndomx, ck.synb);
		this.devm = ck.devm;
		this.docm = ck.docm;
	}

	public T_SynDomanager xp(ExessionPersist cp) {
		this.xp = cp;
		return this;
	}

	static public T_SynDomanager reboot(Docheck ck) throws Exception {
		T_SynDomanager domx = new T_SynDomanager(ck) ;
		return (T_SynDomanager) domx.loadomainx();
	}
	
	/**
	 * Equivalent to Docsync-jserv: SyssionPeer.exesrestore(), for tests
	 * @param peer
	 * @return restore request
	 * @throws Exception
	 */
	ExchangeBlock syssionPeer_exesrestore(String peer) throws Exception {
		DBSyntableBuilder b0 = new DBSyntableBuilder(this);
		xp = new ExessionPersist(b0, peer, null);
		return b0.restorexchange(xp);
	}

	public void breakdown() {
		this.xp = null;
	}

}
