package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.notBlank;
import static io.odysz.common.LangExt.notNull;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.semantic.syn.Nyquence.maxn;

import java.sql.SQLException;
import java.util.HashMap;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.x.TransException;

/**
 * A synode context, a database cache, per domain, for managing and sharing domain
 * wide information, e.g. stamp, n0, Nyquvect, etc., across multiple syn-change
 * handlers.
 * 
 * @author ody
 *
 */
public class SyndomContext {

	String domain;
	public String domain() { return domain; }
	
	public final String synode;
	public final String synconn;

	public final SynodeMeta synm;
	protected final PeersMeta pnvm;
	protected final SynSubsMeta subm;
	protected final SynChangeMeta chgm;
	protected final SynchangeBuffMeta exbm;

	public final SynodeMode mode;
	long seq;

	HashMap<String, Nyquence> nv;
	Nyquence stamp;

	protected SyndomContext(SynodeMode mod, String dom, String synode, String synconn)
			throws TransException, SQLException {

		this.synode  = notBlank(synode);
		this.domain  = dom;
		this.synconn = notBlank(synconn);
		this.mode    = notNull(mod);
		
		this.chgm = new SynChangeMeta(synconn).replace();
		this.subm = new SynSubsMeta(chgm, synconn).replace();
		this.synm = new SynodeMeta(synconn).replace();
		this.exbm = new SynchangeBuffMeta(chgm, synconn).replace();
		this.pnvm = new PeersMeta(synconn).replace();
	}
	
	public Nyquence n0() { return nv.get(synode); }

	Nyquence incN0(DBSyntableBuilder synb, Nyquence... maxn) throws TransException, SQLException {
		nv.get(synode).inc(maxn);

		return persistNyquence(synb, domain, nv.get(synode));
	}

	public void incStamp(DBSyntableBuilder synb) throws TransException, SQLException {
			
		if (nv.containsKey(synode)
			&& Nyquence.abs(stamp, nv.get(synode)) >= 1)
			throw new SemanticException(
				"Nyquence stamp is going to increase too much or out of range.");

		stamp.inc();

		stamp = persistamp(synb);
		seq = 0;
	}

	public Nyquence incN0(DBSyntableBuilder b) throws TransException, SQLException {
		persistNyquence(b, synode, nv.get(synode).inc());

		stamp.inc();

		persistamp(b);

		return nv.get(synode);
	}
	
	public Nyquence n0(DBSyntableBuilder synb, Nyquence maxn)
			throws TransException, SQLException {
		Nyquence n;
		n = maxn(nv.get(synode), maxn);
		nv.put(synode, n);

		persistNyquence(synb, synode, n);

		return n;
	}


	public HashMap<String, Nyquence> loadNvstamp(DBSyntableBuilder synb) throws TransException, SQLException {
		loadNvstamp(synb, synb.robot);
		return nv;
	}
	
	public SyndomContext loadNvstamp(DATranscxt tb, IUser usr) throws TransException, SQLException {
		AnResultset rs = ((AnResultset) tb.select(synm.tbl)
				.cols(synm.pk, synm.nyquence, synm.nstamp)
				.whereEq(synm.domain, domain)
				.rs(tb.instancontxt(synconn, usr))
				.rs(0));
		
		nv = new HashMap<String, Nyquence>(rs.getRowCount());
		while (rs.next()) {
			String nid = rs.getString(synm.synoder);
			nv.put(nid, new Nyquence(rs.getLong(synm.nyquence)));
			if (eq(nid, synode))
				stamp = new Nyquence(rs.getLong(synm.nstamp));
		}
	
		return this;
	}

	/**
	 * For test.
	 * @param trb
	 * @param x
	 * @return
	 * @throws SQLException
	 * @throws TransException
	 */
	public static Nyquence getNyquence(DBSyntableBuilder trb, SyndomContext x)
			throws SQLException, TransException {
		return getNyquence(trb, x.synconn, trb.synrobot(),
				x.synm, x.synm.nyquence, x.synm.synoder, x.synode, x.synm.domain, x.domain);
	}

	/**
	 * Load nyquence without triggering semantics handling.
	 * 
	 * @param trb
	 * @param conn
	 * @param m
	 * @param nyqfield
	 * @param where_eqs
	 * @return nyquence
	 * @throws SQLException
	 * @throws TransException
	 */
	public static Nyquence getNyquence(DATranscxt trb, String conn, IUser usr, SynodeMeta m, String nyqfield, String... where_eqs)
			throws SQLException, TransException {
		Query q = trb.select(m.tbl);
		
		for (int i = 0; i < where_eqs.length; i+=2)
			q.whereEq(where_eqs[i], where_eqs[i+1]);
		
		AnResultset rs = (AnResultset) q 
				.rs(trb.instancontxt(conn, usr))
				.rs(0);
		
		if (rs.next())
			return new Nyquence(rs.getLong(nyqfield));
		else throw new SQLException(String
			.format("Record not found: %s.%s = '%s' ... ", m.tbl, where_eqs[0], where_eqs[1]));
	}

	/**
	 * Update domain context by upper node's reply.
	 * 
	 * @param b
	 * @param dom
	 * @param n0
	 * @return 
	 * @throws TransException
	 * @throws SQLException
	 */
	public SyndomContext domainitOnjoin(DBSyntableBuilder b, String dom, Nyquence n0) throws TransException, SQLException {
		DAHelper.updateFieldWhereEqs(b, synconn, b.synrobot(), synm, synm.domain, dom,
				synm.synoder, synode, synm.domain, this.domain);
		domain = dom;

		persistNyquence(b, synm.nyquence, n0);
		persistamp(b, n0);
		return this;
	}

	public Nyquence persistamp(DBSyntableBuilder trb, Nyquence... up2max)
			throws TransException, SQLException {

		if (stamp == null)
			stamp = new Nyquence(up2max[0].n);
		else if (!isNull(up2max) && Nyquence.compareNyq(up2max[0], stamp) > 0)
				stamp.n = up2max[0].n;
		
		DAHelper.updateFieldWhereEqs(trb, synconn, trb.synrobot(), synm,
				synm.nstamp, stamp.n,
				synm.pk, synode,
				synm.domain, domain);

		return stamp;
	}

	public Nyquence persistNyquence(DBSyntableBuilder trb, String synid, Nyquence n)
			throws TransException, SQLException {

		DAHelper.updateFieldWhereEqs(trb, synconn, trb.synrobot(), synm,
				synm.nyquence, n.n,
				synm.pk, synode,
				synm.domain, domain);
		return n;
	}
}
