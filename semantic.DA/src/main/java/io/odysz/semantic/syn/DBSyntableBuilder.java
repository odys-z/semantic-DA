package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.hasGt;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.str;
import static io.odysz.semantic.syn.ExessionAct.restore;
import static io.odysz.semantic.syn.ExessionAct.setupDom;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.getn;
import static io.odysz.semantic.syn.Nyquence.maxn;
import static io.odysz.semantic.syn.Nyquence.sqlCompare;
import static io.odysz.semantic.util.DAHelper.getNstamp;
import static io.odysz.semantic.util.DAHelper.updateFieldByPk;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.Sql;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**
 * Sql statement builder for {@link DBSyntext} for handling database synchronization. 
 * 
 * Improved with temporary tables for broken network (and shutdown), concurrency and memory usage.
 * 
 * <h5>Issue ee153bcb30c3f3b868413beace8cc1f3cb5c3f7c</h5>
 * <pre>
 * version: Semantic-DA 2.0.0-SNAPSHOT, jserv.docsyc 0.2.0-SNAPSHOT
 * commit:  ee153bcb30c3f3b868413beace8cc1f3cb5c3f7c & ee153bcb30c3f3b868413beace8cc1f3cb5c3f7c
 * About:
 * DBSyntableBuilder.stamp is managed not for each domain.
 * To use stamp in this way, nyquence numbers should be synchronized
 * in a cross-domain style;
 * To use stamps for each domain, multiple change-logs for each
 * domain of an entity changing operation must be generated.
 * </pre>
 *
 * @author Ody
 */
public class DBSyntableBuilder extends DATranscxt {
	public static class SynmanticsMap extends SemanticsMap {
		String synode;
	
		public SynmanticsMap(String synode, String conn) {
			super(conn);
			this.synode = synode;
		}
	
		@Override
		public DASemantics createSemantics(Transcxt trb, String tabl, String pk, boolean debug) {
			return new DBSynmantics(trb, synode, tabl, pk, debug);
		}
	}

	private final boolean debug;

	public    SynodeMeta synm;
	protected PeersMeta pnvm;
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;
	protected SynchangeBuffMeta exbm;

	final SynodeMode synmode;

	/**
	 * Get synchronization meta connection id.
	 * 
	 * @return conn-id
	 */
	public String synconn() { return basictx.connId(); }

	public String synode() { return ((DBSyntext)this.basictx).synode; }

	/* */
	private Nyquence stamp;
	public long stamp() { return stamp.n; }
	public Nyquence stampN() { return stamp; }
	
	protected Nyquence persistamp(Nyquence n) throws TransException, SQLException {
		stamp.n = n.n;
		DAHelper.updateFieldWhereEqs(this, synconn(), synrobot(), synm,
				synm.nstamp, n.n,
				synm.pk, synode(),
				synm.domain, perdomain);
		return stamp;
	}

	DBSyntableBuilder incStamp(ExessionPersist xp) throws TransException, SQLException {
		if (xp.nyquvect.containsKey(synode()) && Nyquence.abs(stamp, xp.nyquvect.get(synode())) >= 1)
			throw new ExchangeException(0, xp, "Nyquence stamp increaseing too much or out of range.");
		stamp.inc();
		persistamp(stamp);
		seq = 0;
		return this;
	}

	/*
	public Nyquence n0() { return nyquvect.get(synode()); }
	protected DBSyntableBuilder n0(Nyquence nyq) {
		nyquvect.put(synode(), new Nyquence(nyq.n));
		return this;
	}
	*/

	String perdomain;
	public String domain() { return perdomain; }
	private DBSyntableBuilder domain(String domain) {
		this.perdomain = domain;
		return this;
	}

	public IUser synrobot() { return ((DBSyntext) this.basictx).usr(); }

	static HashMap<String, HashMap<String, SyntityMeta>> entityRegists;

	private final boolean force_clean_subs;

	private long seq;
	public long incSeq() { return ++seq; }

	public SyntityMeta getSyntityMeta(String tbl) {
		return entityRegists != null
			&& entityRegists.containsKey(synconn())
				? entityRegists.get(synconn()).get(tbl)
				: null;
	} 

	public DBSyntableBuilder(String domain, String conn, String synodeId, SynodeMode mode)
			throws Exception {
		this(domain, conn, synodeId, mode,
			new SynChangeMeta(conn),
			new SynodeMeta(conn));
	}
	
	public DBSyntableBuilder(String domain, String conn, String nid,
			SynodeMode mode, SynChangeMeta chgm, SynodeMeta synm)
			throws Exception {

		super ( new DBSyntext(conn,
			    	initConfigs(conn, loadSemantics(conn), (c) -> new DBSyntableBuilder.SynmanticsMap(nid, c)),
			    	(IUser) new SyncRobot(nid, nid, "rob@" + nid, nid),
			    	runtimepath));
		
		debug    = Connects.getDebug(conn);
		perdomain= domain;
		synmode  = mode;

		// wire up local identity
		DBSyntext tx = (DBSyntext) this.basictx;
		tx.synode = nid;
		// dom = getValstr((Transcxt) this, conn, synm, synm.domain, synm.pk, synodeId);
//		((SyncRobot)tx.usr())
//			.orgId(getValstr((Transcxt) this, conn, synm, synm.org, synm.pk, nid))
//			.domain(getValstr((Transcxt) this, conn, synm, synm.domain, synm.pk, nid));

		this.chgm = chgm != null ? chgm : new SynChangeMeta(conn);
		this.chgm.replace();
		this.subm = subm != null ? subm : new SynSubsMeta(chgm, conn);
		this.subm.replace();
		this.synm = synm != null ? synm : (SynodeMeta) new SynodeMeta(conn).autopk(false);
		this.synm.replace();

		this.exbm = exbm != null ? exbm : new SynchangeBuffMeta(chgm, conn);
		this.exbm.replace();
		this.pnvm = pnvm != null ? pnvm : new PeersMeta(conn);
		this.pnvm.replace();
		
		// seq = 0;
		force_clean_subs = true;

		if (mode != SynodeMode.nonsyn) {
			if (DAHelper.count(this, conn, synm.tbl,
						synm.synoder, nid, synm.domain, perdomain) <= 0) {
				if (debug) Utils
					.warnT(new Object() {},
						  "\nThis syntable builder is being buit for node %s which doesn't exists in domain %s." +
						  "\nThis instence can only be useful if is used to initialize the domain for the node",
						  nid, perdomain);
			}
			else
				stamp = DAHelper.getNyquence(this, conn, synm, synm.nyquence,
						synm.synoder, nid, synm.domain, perdomain);

			registerEntity(conn, synm);
		}
		else if (isblank(perdomain))
			Utils.warn("[%s] Synchrnizer builder (id %s) created without domain specified",
				this.getClass().getName(), tx.synode);

		if (debug && force_clean_subs) Utils
			.logT(new Object() {}, "Transaction builder created with forcing cleaning stale subscriptions.");
	}
	
	////////////////////////////// protocol API ////////////////////////////////
	/**
	 * insert into exchanges select * from change_logs where n > nyquvect[target].n
	 * 
	 * @param cp
	 * @param target
	 * @return {total: change-logs to be exchanged} 
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExchangeBlock initExchange(ExessionPersist cp)
			throws TransException, SQLException {
		if (DAHelper.count(this, basictx().connId(), exbm.tbl, exbm.peer, cp.peer) > 0)
			throw new ExchangeException(Exchanging.ready, cp,
				"Can't initate new exchange session. There are exchanging records to be finished.");

		return cp.init(); //  .nv(nyquvect);
	}
	
	/**
	 * insert into exchanges select * from change_logs where n > nyquvect[sx.peer].n
	 * 
	 * @param sp
	 * @param inireq
	 * @return response block
	 * @throws TransException 
	 * @throws SQLException 
	 */
	public ExchangeBlock onInit(ExessionPersist sp, ExchangeBlock inireq)
			throws SQLException, TransException {
		try {
			cleanStale(inireq.nv, sp.peer);

			// insert into exchanges select * from change_logs where n > nyquvect[sx.peer].n
			return sp.onInit(inireq); // .nv(nyquvect);
		} finally {
			incStamp(sp);
		}
	}

	public void abortExchange(ExessionPersist cx, String target, ExchangeBlock rep) {
	}

	/**
	 * Push a change-logs page, with piggyback answers of previous confirming's challenges.
	 * 
	 * @param cp
	 * @param peer
	 * @param lastconf
	 * @return pushing block
	 * @throws SQLException 
	 * @throws TransException 
	 */
	ExchangeBlock exchangePage(ExessionPersist cp, ExchangeBlock lastconf)
			throws SQLException, TransException {
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		return cp
			.commitAnswers(lastconf, cp.peer, cp.n0().n)
			.exchange(cp.peer, lastconf)
			// .nv(nyquvect)
			.answers(answer_save(cp, lastconf, cp.peer))
			.seq(cp.persisession());
	}
	
	ExchangeBlock onExchange(ExessionPersist sp, String peer, ExchangeBlock req)
			throws SQLException, TransException {
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		sp.expect(req).exstate(ExessionAct.exchange);

		return sp
			.commitAnswers(req, peer, sp.n0().n)
			.onExchange(peer, req) // The challenge page is ready
			// .nv(nyquvect)
			.answers(answer_save(sp, req, peer))
			.seq(sp.persisession());
	}

	/**
	 * clean: change[z].n < y.z when X &lt;= Y or change[z].n <= z.synoder when Y &lt;= Z ,
	 * i.e.<pre>
	 * my-change[him].n < your-nv[him]
	 * or
	 * my-change[by him][you].n < your-nv[him]</pre>
	 * 
	 * <p>Clean staleness. (Your knowledge about Z is newer than what I am presenting to)</p>
	 * 
	 * <ul>
	 * <li>When propagating 3rd node's information, clean the older one
	 * <pre>
	 *    X         Y
	 * [n.z=1] → [n.z=2]
	 * clean late   ↓
	 *     →x       Z
	 *            [n=2]
	 *            [n=1]
	 * </pre></li>
	 * <li>When accepting subscriptions, clean the older one than got from the other route
	 * <pre>
	 * synoder     X
	 *  n=2   →  [n=1]
	 *   ↓         ↓
	 *   Y         Z
	 * [n=1]  → way 1: n.synoder=1 + 1
	 *          (can't be concurrently working with multiple peers in the same domain)
	 * </pre></li> 
	 * </ul>
	 * 
	 * <h5>Case 1. X vs. Y</h5>
	 * <pre>
	 * X cleaned X,000021[Z] for X,000021[Z].n &lt; Y.z
	 * X cleaned Y,000401[Z] for Y,000401[Z].n &lt; Y.z
	 * 
	 * I  X  X,000021    1  Z    |                           |                           |                          
	 * I  Y  Y,000401    1  Z    |                           |                           |                          
     *       X    Y    Z    W
	 * X [   2,   1,   0,     ]
	 * Y [   2,   3,   2,     ]
	 * Z [   1,   2,   3,     ]
	 * W [    ,    ,    ,     ]</pre>
	 * 
	 * <h5>Case 2. X vs. Y</h5>
	 * <pre>
	 * X keeps change[z] for change[Z].n &ge; Y.z
	 * 
	 *              X                          Y                          Z                          W             
	 * U  X  X,000023   10  Y    |                           |                           |                          
	 * U  X  X,000023   10  Z    |                           |                           |                          
	 *                           | U  Y  Y,000401   11  W    |                           |                          
	 * U  X  X,000023   10  W    |                           |                           |                          
	 *                           | U  Y  Y,000401   11  Z    |                           |                          
	 *                           | U  Y  Y,000401   11  X    |                           |                          
	 *       X    Y    Z    W
	 * X [  10,   9,   7,   8 ]
	 * Y [   9,  11,  10,   8 ]
	 * Z [   9,  10,  11,   8 ]
	 * W [   6,   8,   7,   9 ]</pre>
	 * 
	 * <h5>Case 3. X &lt;= Y</h5>
	 * For a change log where synodee is the target / peer node, it is stale {@code iff} X.change[Y] < Y.X,
	 * <pre>e.g. in
	 * 2 testBranchPropagation - must call testJoinChild() first
	 * 2.4 X vs Y
	 * 2.4.1 Y initiate
	 * 2.4.2 X on initiate,
	 * 
	 *                X                    |                  Y                 |                  Z                 |                  W                 
	 * ------------------------------------+------------------------------------+------------------------------------+------------------------------------
	 *  I  X.000001  X,000021  2  W [    ] |                                    |                                    |                                    
	 *                                     |                                    | I  Z.000001  Z,008002  3  W [ Y:0] |                                    
	 *  I  X.000001  X,000021  2  Y [    ] |                                    | I  X.000001  X,000021  2  Y [ Y:0] |                                    
	 *       X    Y    Z    W
	 * X [   3,   1,   2,   0 ]
	 * Y [   1,   2,   0,   0 ]
	 * Z [   2,   1,   3,   0 ]
	 * W [    ,   1,    ,   2 ]</pre>
	 * 
	 * where y.x = 1 < X.00001[Y] = 2, so the subscribes can not be cleared,
	 * while Y,W should be cleared in the following when Y &lt;= Z, in which it has already been synchronized via X.
	 * 
	 * <pre>
	 * 
	 *                   X                 |                  Y                 |                  Z                 |                  W                 
	 * ------------------------------------+------------------------------------+------------------------------------+------------------------------------
	 *                                     | I  Y.000001       Y,W  1  X [    ] |                                    |                                    
	 *                                     | I  Y.000001       Y,W  1  Z [    ] |                                    |                                    
	 *       X    Y    Z    W
	 * X [   1,   0,   0,     ]
	 * Y [   0,   1,   0,   0 ]
	 * Z [   0,   0,   1,     ]
	 * W [    ,   1,    ,   1 ]
	 * 
	 * 1.2 X vs Y
	 *                   X                 |                  Y                 |                  Z                 |                  W                 
	 * ------------------------------------+------------------------------------+------------------------------------+------------------------------------
	 *  I  Y.000001       Y,W  1  Z [    ] | I  Y.000001       Y,W  1  Z [    ] |                                    |                                    
	 *  
	 * 1.3 X create photos
	 * 
	 * ------------------------------------+------------------------------------+------------------------------------+------------------------------------
	 *  I  X.000001  X,000021  2  W [    ] |                                    |                                    |                                    
	 *  I  X.000001  X,000021  2  Z [    ] |                                    |                                    |                                    
	 *  I  X.000001  X,000021  2  Y [    ] |                                    |                                    |                                    
	 *  I  Y.000001       Y,W  1  Z [    ] | I  Y.000001       Y,W  1  Z [    ] |                                    |                                    
	 *
	 * 1.4 X vs Z
	 * 
	 * ------------------------------------+------------------------------------+------------------------------------+------------------------------------
	 * I  X.000001  X,000021  2  W [    ] |                                    |                                    |                                    
	 * I  X.000001  X,000021  2  Y [    ] |                                    | I  X.000001  X,000021  2  Y [    ] |                                    
	 *                                    | I  Y.000001       Y,W  1  Z [    ] |                                    |                                    
	 *
	 * 1.5 Z vs W
	 * 2 testBranchPropagation - must call testJoinChild() first
	 * 
	 * ------------------------------------+------------------------------------+------------------------------------+------------------------------------
	 *  I  X.000001  X,000021  2  W [    ] |                                    |                                    |                                    
	 *                                     |                                    | I  Z.000001  Z,008002  3  W [    ] |                                    
	 *  I  X.000001  X,000021  2  Y [    ] |                                    | I  X.000001  X,000021  2  Y [    ] |                                    
	 *                                     |                                    | I  Z.000001  Z,008002  3  X [    ] |                                    
	 *                                     |                                    | I  Z.000001  Z,008002  3  Y [    ] |                                    
	 *                                     | I  Y.000001       Y,W  1  Z [    ] |                                    |                                    
	 *                                     
	 *       X    Y    Z    W
	 * X [   3,   1,   2,   0 ]
	 * Y [   1,   2,   0,   0 ]
	 * Z [   2,   1,   3,   0 ]
	 * W [    ,   1,    ,   2 ]                                    
	 * 
	 * 2.2 Y vs Z
	 * 2.2.1 Z initiate
	 * 2.2.2 Y on initiate
	 * 
	 * NOW THE TIME TO REMOVE REDUNDENT RECORD ON Y. That is
	 * at Y, y.z = 0 < Z.z
	 * </pre>
	 * 
	 * <ol>
	 * <li>Y has later knowledge about Z. X should clean staleness</li>
	 * <li>X has later knowledge about Z. X has no staleness, and Y will update with it</li>
	 * <li>Z has later knowledge from Y via X. Y can ignore it (clean staleness)</li>
	 * </ol>
	 * @param srcnv
	 * @param srcn
	 * @throws TransException
	 * @throws SQLException
	 */
	void cleanStale(HashMap<String, Nyquence> srcnv, String peer)
			throws TransException, SQLException {
		if (srcnv == null) return;
		
		if (debug)
			Utils.logi("Cleaning staleness at %s, peer %s ...", synode(), peer);

		delete(pnvm.tbl, synrobot())
			.whereEq(pnvm.peer, peer)
			.whereEq(pnvm.domain, domain())
			.post(insert(pnvm.tbl)
				.cols(pnvm.inscols)
				.values(pnvm.insVals(srcnv, peer, domain())))
			.d(instancontxt(synconn(), synrobot()));

		SemanticObject res = (SemanticObject) ((DBSyntableBuilder)
			// clean while accepting subscriptions
			with(select(chgm.tbl, "cl")
				.cols("cl.*").col(subm.synodee)
				.je_(subm.tbl, "sb", constr(peer), subm.synodee, chgm.pk, subm.changeId)
				.je_(pnvm.tbl, "nv", chgm.synoder, synm.pk, constr(domain()), pnvm.domain, constr(peer), pnvm.peer)
				.where(op.le, sqlCompare("cl", chgm.nyquence, "nv", pnvm.nyq), 0))) // 631e138aae8e3996aeb26d2b54a61b1517b3eb3f
			.delete(subm.tbl, synrobot())
				.where(op.exists, null, select("cl")
				.where(op.eq, subm.changeId, chgm.pk)
				.whereEq(subm.tbl, subm.synodee,  "cl", subm.synodee))

			// clean 3rd part nodes' propagation
			.post(with(select(chgm.tbl, "cl")
					.cols("cl.*").col(subm.synodee)
					.j(subm.tbl, "sb", Sql.condt(op.eq, chgm.pk, subm.changeId)
											.and(Sql.condt(op.ne, constr(peer), subm.synodee)))
					.je_(synm.tbl, "sn", chgm.synoder, synm.pk, constr(domain()), synm.domain)
					.je_(pnvm.tbl, "nver", "cl." + chgm.synoder, pnvm.synid, constr(domain()), pnvm.domain, constr(peer), pnvm.peer)
					// see 6b8b2cae7c07c427b82c2aec626b46e4fb9ab4f3
					.je_(pnvm.tbl, "nvee", "sb." + subm.synodee, pnvm.synid, constr(domain()), pnvm.domain, constr(peer), pnvm.peer)
					.where(op.le, sqlCompare("cl", chgm.nyquence, "nver", pnvm.nyq), 0)
					.where(op.le, sqlCompare("cl", chgm.nyquence, "nvee", pnvm.nyq), 0))
				.delete(subm.tbl)
					.where(op.exists, null, select("cl")
					.where(op.eq, subm.changeId, chgm.pk)
					.whereEq(subm.tbl, subm.synodee,  "cl", subm.synodee)))

			// clean changes without subscribes
			.post(delete(chgm.tbl)
				.where(op.notexists, null,
						with(select(chgm.tbl, "cl")
							.cols(subm.changeId, chgm.domain, chgm.entbl, subm.synodee)
							.je_(subm.tbl, "sb", chgm.pk, subm.changeId))
						.select("cl")
						.je_(chgm.tbl, "ch",
							"ch." + chgm.domain,  "cl." + chgm.domain,
							"ch." + chgm.entbl, "cl." + chgm.entbl,
							subm.changeId, chgm.pk)))
			.d(instancontxt(basictx.connId(), synrobot()));
			
		if (debug) {
			try {
				@SuppressWarnings("unchecked")
				ArrayList<Integer> chgsubs = ((ArrayList<Integer>)res.get("total"));
				if (chgsubs != null && chgsubs.size() > 1 && hasGt(chgsubs, 0)) {
					Utils.logi("Subscribe record(s) are affected:");
					Utils.logi(str(chgsubs, new String[] {"subscribes", "propagations", "change-logs"}));
				}
			} catch (Exception e) { e.printStackTrace(); }
		}
	}

	ExessionPersist answer_save(ExessionPersist xp, ExchangeBlock req, String peer)
			throws SQLException, TransException {
		if (req == null || req.chpage == null) return xp;
		
		AnResultset changes = new AnResultset(req.chpage.colnames());
		ExchangeBlock resp = new ExchangeBlock(synode(), peer, xp.session(), xp.exstat())
							.nv(xp.nyquvect);

		AnResultset reqChgs = req.chpage;

		HashSet<String> warnsynodee = new HashSet<String>();
		HashSet<String> warnsynoder = new HashSet<String>();

		while (req.totalChallenges > 0 && reqChgs.next()) {
			String synodee = reqChgs.getString(subm.synodee);
			String synoder = reqChgs.getString(chgm.synoder);

			if (!xp.nyquvect.containsKey(synoder)) {
				if (!warnsynoder.contains(synoder)) {
					warnsynoder.add(synoder);
					Utils.warn("%s has no idea about %s. The changes %s -> %s are ignored.",
							synode(), synoder, reqChgs.getString(chgm.uids), synodee);
				}
				continue;
			}
			else if (eq(synodee, synode())) {
				resp.removeChgsub(req.chpage, synode());	
				changes.append(reqChgs.getRowAt(reqChgs.getRow() - 1));
			}
			else {
				Nyquence subnyq = getn(reqChgs, chgm.nyquence);
				if (!xp.nyquvect.containsKey(synodee) // I don't have information of the subscriber
					&& eq(synm.tbl, reqChgs.getString(chgm.entbl))) // adding synode
					changes.append(reqChgs.getRowAt(reqChgs.getRow() - 1));
				else if (!xp.nyquvect.containsKey(synodee)) {
					; // I have no idea
					if (synmode != SynodeMode.leaf) {
						if (!warnsynodee.contains(synodee)) {
							warnsynodee.add(synodee);
							Utils.warn("%s has no idea about %s. The change is committed at this node. This can either be automatically fixed or causing data lost later.",
									synode(), synodee);
						}
						changes.append(reqChgs.getRowAt(reqChgs.getRow() - 1));
					}
					else // leaf
						if (!warnsynodee.contains(synodee)) {
							warnsynodee.add(synodee);
							Utils.warn("%s has no idea about %s. Ignoring as is working in leaf mode. (Will filter data at server side in the near future)",
									synode(), synodee);
						}	
				}
				// see alse ExessionPersist#saveChanges()
				else if (compareNyq(subnyq, xp.nyquvect.get(reqChgs.getString(chgm.synoder))) > 0) {
					// should suppress the following case
					changes.append(reqChgs.getRowAt(reqChgs.getRow() - 1));
				}
				else if (compareNyq(subnyq, xp.nyquvect.get(peer)) <= 0) {
					// 2024.6.5 client shouldn't have older knowledge than me now,
					// which is cleanded when initiating.
					if (debug) Utils.warn("Ignore this?");
				}
				else
					changes.append(reqChgs.getRowAt(reqChgs.getRow() - 1));
			}
		}

		return xp.saveAnswer(resp.anspage)
				.saveChanges(changes.beforeFirst(), req.nv, req.entities);
	}

	public ExchangeBlock closexchange(ExessionPersist cx, ExchangeBlock rep)
			throws TransException, SQLException {

		// cx.clear();
		HashMap<String, Nyquence> nv = rep.nv; 

		Nyquence peerme = nv.get(synode());
		if (peerme != null && Nyquence.compareNyq(cx.n0(), peerme) < 0)
			throw new SemanticException("Synchronizing Nyquence exception: my.n0 = %d < peer.nv[me] = %d, at %s (me).",
					cx.n0().n, peerme.n, synode());

		if (Nyquence.compareNyq(stamp, cx.n0()) < 0)
			throw new SemanticException("Synchronizing Nyquence exception: %s.stamp = %d < n0 = %d.",
					synode(), stamp.n, cx.n0().n);
		
		HashMap<String, Nyquence> snapshot = synyquvectMax(cx, rep.nv);
		persistamp(maxn(stamp, cx.n0()));

		return cx.closexchange(rep).nv(snapshot); // cx.clear();
	}

	/**
	 * insert into exbm-buffer select peer's-subscriptions union 3rd parties subscriptions
	 * 
	 * @see #closexchange(ExessionPersist, ExchangeBlock)
	 * 
	 * @return insert statement to exchanging page buffer.
	 * @throws TransException
	 */
	Insert insertExbuf(String peer) throws TransException {
		return insert(exbm.tbl, synrobot())
			.cols(exbm.insertCols())
			.select(
				// target is the subscriber
				select(chgm.tbl, "cl")
					.cols(constr(peer), chgm.pk, new ExprPart(-1))
					.je_(subm.tbl, "sb", constr(peer), subm.synodee, chgm.pk, subm.changeId, chgm.synoder, constr(synode()))
					.je_(pnvm.tbl, "nv", chgm.synoder, synm.pk, constr(domain()), pnvm.domain, constr(peer), pnvm.peer)
					.where(op.gt, sqlCompare("cl", chgm.nyquence, "nv", pnvm.nyq), 0)

				// propagating 3rd parties' subscriptions
				.union(select(chgm.tbl, "cl")
					.cols(constr(peer), chgm.pk, new ExprPart(-1))
					.j(subm.tbl, "sb", Sql.condt(op.eq, chgm.pk, subm.changeId)
											.and(Sql.condt(op.ne, constr(synode()), subm.synodee)))
					.je_(pnvm.tbl, "nvee", "sb." + subm.synodee, pnvm.synid, constr(domain()), pnvm.domain, constr(peer), pnvm.peer)
					.where(op.gt, sqlCompare("cl", chgm.nyquence, "nvee", pnvm.nyq), 0)
				)
			);
	}
	
	public HashMap<String, Nyquence> synyquvectMax(ExessionPersist xp, // String peer,
			HashMap<String, Nyquence> xnv) throws TransException, SQLException {

		Update u = null;

		HashMap<String, Nyquence> snapshot =  new HashMap<String, Nyquence>();

		for (String n : xnv.keySet()) {
			if (!xp.nyquvect.containsKey(n))
				continue;
			Nyquence nyq = null;
			
			if (eq(synode(), n)) {
				Nyquence mx = maxn(xnv.get(n), xp.n0(), xnv.get(xp.peer));
				snapshot.put(n, new Nyquence(mx.n));
				xp.incN0(mx);
			}
			else
				snapshot.put(n, new Nyquence(maxn(xnv.get(n), xp.nyquvect.get(n)).n));

			nyq = maxn(xnv.get(n), xp.nyquvect.get(n));

			if (compareNyq(nyq, xp.nyquvect.get(n)) != 0) {
				if (u == null)
					u = update(synm.tbl, synrobot())
						.nv(synm.nyquence, nyq.n)
						.whereEq(synm.pk, n);
				else
					u.post(update(synm.tbl)
						.nv(synm.nyquence, nyq.n)
						.whereEq(synm.pk, n));
			}
		}

		if (u != null) {
			u.u(instancontxt(synconn(), synrobot()));

			xp.loadNyquvect(synconn());
		}

		return snapshot;
	}

	public ExchangeBlock abortExchange(ExessionPersist cx)
			throws TransException, SQLException {
		HashMap<String, Nyquence> snapshot = Nyquence.clone(cx.nyquvect);
		// cx.incN0(stamp);
		// persistamp(maxn(stamp, cx.n0()));
		stamp.n = getNstamp(this).n;
		return cx.abortExchange().nv(snapshot);
	}
	
	public ExchangeBlock onclosexchange(ExessionPersist sx,
			ExchangeBlock rep) throws TransException, SQLException {
		return closexchange(sx, rep);
	}
	
	public void onAbort(ExchangeBlock req)
			throws TransException, SQLException {
	}
	
	public ExchangeBlock requirestore(ExessionPersist xp, String peer) {
		return new ExchangeBlock(synode(), peer, xp.session(), xp.exstat())
				.nv(xp.nyquvect)
				.requirestore()
				.seq(xp);
	}
	
	public void onRequires(ExessionPersist cp, ExchangeBlock req) throws ExchangeException {
		if (req.act == restore) {
			if (cp.challengeSeq <= req.challengeSeq) {
				// server is actually handled my challenge. Just step ahead 
				cp.challengeSeq = req.challengeSeq;
			}
			else if (cp.challengeSeq == req.challengeSeq + 1) {
				// server haven't got the previous package
				// setup for send again
				cp.challengeSeq = req.challengeSeq;
			}
			else {
				// not correct
				cp.challengeSeq = req.challengeSeq;
			}
	
			if (cp.answerSeq < req.answerSeq) {
				cp.answerSeq = req.answerSeq;
			}
			
			cp.expAnswerSeq = cp.challengeSeq;
		}
		else throw new ExchangeException(0, cp, "TODO");
	}

	/**
	 * Client have found unfinished exchange session then retry it.
	 */
	public void restorexchange() { }

	/////////////////////////////////////////////////////////////////////////////////////////////
	public int deleteEntityBySynuid(SyntityMeta entm, String synuid)
			throws TransException, SQLException {
		if (existsnyuid(entm, synuid)) {
			SemanticObject res = (SemanticObject) DBSynmantics
					.logChange(this, delete(entm.tbl, synrobot())
					.whereEq(entm.synuid, synuid), entm, synuid)
					.d(instancontxt(basictx.connId(), synrobot()));
			return res.total();
		}
		else return 0;
	}

	private boolean existsnyuid(SyntityMeta entm, String suid)
			throws SQLException, TransException {
		return ((AnResultset) select(entm.tbl, "t")
			.col(Funcall.count(), "c")
			.rs(instancontxt(synconn(), synrobot()))
			.rs(0))
			.nxt()
			.getInt("c") > 0;
	}

	/**
	 * NOTE: This method sholdn't be used other than tests,
	 * or don't configure syn-change semantics for the table.
	 * 
	 * @param m
	 * @param e
	 * @return [entity-id, change-id]
	 * @throws TransException
	 * @throws SQLException
	 */
	public String[] insertEntity(SyntityMeta m, SynEntity e)
			throws TransException, SQLException {
		String conn   = synconn();
		SyncRobot rob = (SyncRobot) synrobot();

		Insert inst = e.insertEntity(m, insert(m.tbl, rob));
		SemanticObject u = (SemanticObject) DBSynmantics
				.logChange(this, inst, m, synode())
				.ins(instancontxt(conn, rob));

		String phid = u.resulve(m, -1);
		String chid = u.resulve(chgm, -1);
		return new String[] {phid, chid};
	}
	
	@SuppressWarnings("serial")
	public String updateEntity(String synoder, String synuid, SyntityMeta entm, Object ... nvs)
			throws TransException, SQLException, IOException {
		List<String> updcols = new ArrayList<String>(nvs.length/2);
		for (int i = 0; i < nvs.length; i += 2)
			updcols.add((String) nvs[i]);

		return DBSynmantics
			.logChange(this, update(entm.tbl, synrobot())
						.nvs((Object[])nvs)
						.whereEq(entm.synuid, synuid),
					entm, synoder,
					new ArrayList<String>() {{add(synuid);}},
					updcols)
			.u(instancontxt(basictx.connId(), synrobot()))
			.resulve(chgm.tbl, chgm.pk, -1);
	}
	
	public static void registerEntity(String conn, SyntityMeta m)
			throws SemanticException, TransException, SQLException {
		if (entityRegists == null)
			entityRegists = new HashMap<String, HashMap<String, SyntityMeta>>();
		if (!entityRegists.containsKey(conn))
			entityRegists.put(conn, new HashMap<String, SyntityMeta>());

		entityRegists.get(conn).put(m.tbl, (SyntityMeta) m.clone(Connects.getMeta(conn, m.tbl)));
		// return this;
	}
	
	public SyntityMeta getEntityMeta(String entbl) throws SemanticException {
		if (entityRegists == null || !entityRegists.containsKey(synconn())
			|| !entityRegists.get(synconn()).containsKey(entbl))
			throw new SemanticException("Register %s first.", entbl);
			
		return entityRegists.get(synconn()).get(entbl);
	}
	
	/**
	 * Inc my n0, then reload from DB.
	 * @return this
	 * @throws TransException
	 * @throws SQLException
	 */
	public DBSyntableBuilder incNyquence0() throws TransException, SQLException {
		update(synm.tbl, synrobot())
			.nv(synm.nyquence, Funcall.add(synm.nyquence, 1))
			.whereEq(synm.pk, synode())
			.whereEq(synm.domain, perdomain)
			.u(instancontxt(basictx.connId(), synrobot()));
		
		stamp.inc();
		persistamp(stamp);
		
		return this;
	}
	
	@Override
	public ISemantext instancontxt(String conn, IUser usr) throws TransException {
		try {
			ISemantext syntext = new DBSyntext(conn,
				initConfigs(conn, loadSemantics(conn),
						(c) -> new DBSyntableBuilder.SynmanticsMap(synode(), c)),
				usr, runtimepath).creator(this);

			// stamp = getNstamp(this);
			return syntext;
		} catch (Exception e) {
			e.printStackTrace();
			throw new TransException(e.getMessage());
		}
	}
	
	public ExchangeBlock domainSignup(ExessionPersist app, String admin) throws TransException, SQLException {
		try {
			stamp = getNstamp(this);
			return app.signup(admin);}
		finally { 
			incStamp(app);
		}
	}

	public ExchangeBlock domainOnAdd(ExessionPersist ap, ExchangeBlock req, String org)
			throws TransException, SQLException {
	
		ap.loadNyquvect(this.synconn());

		String childId = req.srcnode;
		IUser robot = synrobot();
	
		Synode apply = new Synode(basictx.connId(), childId, org, domain());
	
		@SuppressWarnings("unused")
		String chgid = ((SemanticObject) apply.insert(synm, synode(), ap.n0(), insert(synm.tbl, robot))
			.post(insert(chgm.tbl, robot)
				.nv(chgm.entbl, synm.tbl)
				.nv(chgm.crud, CRUD.C)
				.nv(chgm.synoder, synode())
				.nv(chgm.uids, SynChangeMeta.uids(synode(), apply.synodeId))
				.nv(chgm.nyquence, ap.n0().n)
				.nv(chgm.seq, incSeq())
				.nv(chgm.domain, domain())
				.post(insert(subm.tbl)
					.cols(subm.insertCols())
					.select((Query)select(synm.tbl)
						.col(new Resulving(chgm.tbl, chgm.pk))
						.col(synm.synoder)
						.where(op.ne, synm.synoder, constr(synode()))
						.where(op.ne, synm.synoder, constr(childId))
						.whereEq(synm.domain, domain()))))
			.ins(instancontxt(basictx.connId(), robot)))
			.resulve(chgm.tbl, chgm.pk, -1);
		
		ap.nyquvect.put(apply.synodeId, new Nyquence(apply.nyquence));
	
		ExchangeBlock rep = new ExchangeBlock(synode(), childId, ap.session(), ap.exstat())
			.nv(ap.nyquvect)
			.synodes(req.act == ExessionAct.signup
			? ((AnResultset) select(synm.tbl, "syn")
				.whereIn(synm.synoder, childId, synode())
				.whereEq(synm.domain, domain())
				.whereEq(synm.org, org)
				.rs(instancontxt(basictx.connId(), synrobot()))
				.rs(0))
			: null);
		return rep;
	}

	public ExchangeBlock domainitMe(ExessionPersist cp, String admin, ExchangeBlock domainstatus)
			throws TransException, SQLException {

		Nyquence mxn = domainstatus.nv.get(admin); 

		if (domainstatus.synodes != null) {
			AnResultset ns = domainstatus.synodes.beforeFirst();
			cp.nyquvect = new HashMap<String, Nyquence>(ns.getRowCount());

			while (ns.next()) {
				if (eq(synode(), ns.getString(synm.pk))) {
					update(synm.tbl, synrobot())
					.nv(synm.domain, ns.getString(synm.domain))
					.whereEq(synm.pk, synode())
					.whereEq(synm.org, synrobot().orgId())
					.whereEq(synm.domain, domain())
					.u(instancontxt(basictx.connId(), synrobot()));

					domain(ns.getString(synm.domain));
					cp.nyquvect.put(synode(), new Nyquence(mxn.n));
					
					stamp = new Nyquence(mxn.n); // don't delete: all local data before joining is ignored
				}
				else {
					Synode n = new Synode(ns, synm);
					mxn = maxn(domainstatus.nv);
					n.insert(synm, synode(), mxn, insert(synm.tbl, synrobot()))
						.ins(instancontxt(basictx.connId(), synrobot()));

					cp.nyquvect.put(n.synodeId, new Nyquence(mxn.n));
				}
			}
		}

		// persistamp(mxn);

		return new ExchangeBlock(synode(), admin, domainstatus.session,
				new ExessionAct(ExessionAct.mode_client, setupDom)
			).nv(cp.nyquvect);
	}

	/**
	 * close joining session.
	 * <p>Debug Notes:<br>
	 *  {@code rep.nv} is polluted, but should be safely dropped.</p>
	 *  
	 * @param cp
	 * @param rep
	 * @return reply
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExchangeBlock domainCloseJoin(ExessionPersist cp, ExchangeBlock rep)
			throws TransException, SQLException {
		// stamp = rep.nv.get(rep)
		if (isblank(perdomain))
			throw new ExchangeException(Exchanging.ready, cp, "domain is empty when closing domain joining?");

		updateFieldByPk(this, synconn(), synm, synode(), synm.domain, perdomain, synrobot());

		// Stamp will be persisted in closexchange().
		stamp = maxn(rep.nv.get(cp.peer), stamp, cp.n0());

		return closexchange(cp, rep);
	}
	
	/**
	 * Clean any subscriptions that should been accepted by the peer in
	 * the last session, but was not accutally accepted. Such case can be
	 * the peer node rejected data when no knowledge about the synoder.
	 * 
	 * @param peer
	 */
	protected void cleanStaleSubs(String peer) {
		if (force_clean_subs) {
			if (debug) {
				Utils.logT(new Object() {},
						"Cleaning changes that's not accepted in session %s -> %s.",
						synode(), peer);
				try {
					((AnResultset) select(chgm.tbl, "ch")
						.cols(chgm.pk, chgm.uids, chgm.nyquence, subm.synodee)
						.je_(subm.tbl, "sb", chgm.pk, subm.changeId)
						.whereEq(subm.synodee, peer)
						.rs(instancontxt(synconn(), synrobot()))
						.rs(0))
						.print();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			try {
				SemanticObject res = (SemanticObject) delete(subm.tbl, synrobot())
					.whereEq(subm.synodee, peer)
					.post(del0subchange(peer))
					.d(instancontxt(synconn(), synrobot()));

				@SuppressWarnings("unchecked")
				int cnt = ((ArrayList<Integer>)res.get("total")).get(0);
				if (cnt > 0 && debug) {
					Utils.warnT(new Object() {} ,
						"Cleaned changes in %s -> %s: %s changes",
						synode(), peer, cnt);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Delete change log if no subscribers accept.
	 *  
	 * @param domain
	 * @param iffnode delete the change-log iff the node, i.e. the subscriber, exists.
	 * For answers, it's the node himself, for challenge, it's the source node.
	 * @return the delete statement
	 * @throws TransException
	 */
	protected Statement<?> del0subchange(String iffnode)
				throws TransException {
		return delete(chgm.tbl)
			.whereEq(chgm.domain, domain())
			.whereEq("0", (Query)select(subm.tbl)
				.col(count(subm.synodee))
				.where(op.eq, chgm.pk, subm.changeId))
			;
	}

	/**
	 * Check and extend column {@link #ChangeFlag}, which is for changing flag of change-logs.
	 * 
	 * @param answer
	 * @return this
	 */
	public static HashMap<String,Object[]> checkChangeCol(HashMap<String, Object[]> colnames) {
		if (!colnames.containsKey(ExchangeBlock.ExchangeFlagCol.toUpperCase())) {
			colnames.put(ExchangeBlock.ExchangeFlagCol.toUpperCase(),
				new Object[] {Integer.valueOf(colnames.size() + 1), ExchangeBlock.ExchangeFlagCol});
		}
		return colnames;
	}
	
	/**
	 * Get entity count, of {@code m.tbl}.
	 * @param m
	 * @return count
	 * @throws SQLException
	 * @throws TransException
	 */
	public int entities(SyntityMeta m) throws SQLException, TransException {
		return DAHelper.count(this, synconn(), m.tbl);
	}

	ExessionPersist xp;
	public DBSyntableBuilder xp(ExessionPersist exessionPersist) {
		this.xp = exessionPersist;
		return this;
	}


	public DBSyntableBuilder loadNstamp() throws SQLException, TransException {
		stamp = DAHelper.getNyquence(this, synconn(), synm, synm.nyquence,
						synm.synoder, synode(), synm.domain, perdomain);
		return this;
	}
}
