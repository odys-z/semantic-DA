package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.hasGt;
import static io.odysz.common.LangExt.str; 
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.sqlCompare;
import static io.odysz.semantic.syn.Nyquence.getn;
import static io.odysz.semantic.syn.Nyquence.maxn;
import static io.odysz.semantic.syn.ExessionAct.*;

import static io.odysz.semantic.util.DAHelper.getNyquence;
import static io.odysz.semantic.util.DAHelper.getValstr;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.concatstr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.syn.DBSynsactBuilder.SynmanticsMap;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
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
 * Improved by temporary tables for broken network (and shutdown), concurrency and memory usage.
 * 
 * <pre>
 * 1 X <= Y
 * 1.1 X.inc(nyqstamp), Y.inc(nyqstamp), and later Y insert new entity
 * NOTE: in concurrency, inc(nyqstamp) is not reversible, so an exchange is starting from here
 * 
 * X.challenge = 0, X.answer = 0
 * Y.challenge = 0, Y.answer = 0
 * 
 *                X               |               Y               |               Z               |               W               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *  I  X.000001  X,000021    1  Z |                               |                               |                               
 *  I  X.000001  X,000021    1  Y |                               |                               |                               
 *                                | I  Y.000001  Y,000401    1  X |                               |                               
 *                                | I  Y.000001  Y,000401    1  Z |                               |                               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *                                | I  Y.000002  Y,000402    2  X |                               |                               
 *                                | I  Y.000002  Y,000402    2  Z |                               |                               
 *      X    Y    Z    W
 * X [   1,   0,   0,     ]
 * Y [   0,   1,   0,     ]
 * Z [   0,   0,   1,     ]
 * W [    ,    ,    ,     ]
 * 
 * 1.2 Y init exchange
 * 
 * Y.exchange[X] = select changes where n > Y.x
 * X.exchange[Y] = select changes where n > X.y
 * 
 * x.expectChallenge = y.challengeId = 0
 * y.expectAnswer = x.answerId = 0
 * 
 * y.challenge = y.exchange[X][i]
 * yreq = { y.challengeId, y.challenge
 * 			y.answerId, answer: null}
 * y.challengeId++
 *     
 * for i++:
 *     if x.expectChallenge != yreq.challengeId:
 *         xrep = {requires: x.expectChallenge, answered: x.answerId}
 *     else:
 *         xrep = { x.challengeId, challenge: X.exchange[Y][j],
 *     			answerId: y.challengeId, answer: X.answer(yreq.challenge)}
 *     x.challengeId++
 * 
 * 1.2.1 onRequires()
 * Y:
 *     if rep.answerId == my.challengeId:
 *         # what's here?
 *     else:
 *         i = rep.requires
 *         go for i loop
 * 
 * 1.3 Y closing
 * 
 * Y update challenges with X's answer, block by block
 * Y clear saved answers, block-wisely
 * 
 * Y.ack = {challenge, ..., answer: rep.challenge}
 * 
 *                X               |               Y               |               Z               |               W               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *  I  X.000001  X,000021    1  Z | I  X.000001  X,000021    1  Z |                               |                               
 *  I  X.000001  X,000021    1  Y |                               |                               |                               
 *                                | I  Y.000001  Y,000401    1  Z |                               |                               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *                                | I  Y.000002  Y,000402    2  X |                               |                               
 *                                | I  Y.000002  Y,000402    2  Z |                               |                               
 *       X    Y    Z    W
 * X [   1,   0,   0,     ]
 * Y [   1,   1,   0,     ]
 * Z [   0,   0,   1,     ]
 * W [    ,    ,    ,     ]
 * 
 * 1.4 X on finishing
 *                X               |               Y               |               Z               |               W               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *  I  X.000001  X,000021    1  Z | I  X.000001  X,000021    1  Z |                               |                               
 *  I  Y.000001  Y,000401    1  Z | I  Y.000001  Y,000401    1  Z |                               |                               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *                                | I  Y.000002  Y,000402    2  X |                               |                               
 *                                | I  Y.000002  Y,000402    2  Z |                               |                               
 *  
 *       X    Y    Z    W
 * X [   1,   1,   0,     ]
 * Y [   1,   1,   0,     ]
 * Z [   0,   0,   1,     ]
 * W [    ,    ,    ,     ]
 * 
 * 1.5 Y closing exchange (no change.n < nyqstamp)
 *                X               |               Y               |               Z               |               W               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *  I  X.000001  X,000021    1  Z | I  X.000001  X,000021    1  Z |                               |                               
 *  I  Y.000001  Y,000401    1  Z | I  Y.000001  Y,000401    1  Z |                               |                               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *                                | I  Y.000002  Y,000402    2  X |                               |                               
 *                                | I  Y.000002  Y,000402    2  Z |                               |                               
 *       X    Y    Z    W
 * X [   1,   1,   0,     ]
 * Y [   1,   2,   0,     ]
 * Z [   0,   0,   1,     ]
 * W [    ,    ,    ,     ]
 *
 * 1.3.9 X on closing exchange
 *                X               |               Y               |               Z               |               W               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *  I  X.000001  X,000021    1  Z | I  X.000001  X,000021    1  Z |                               |                               
 *  I  Y.000001  Y,000401    1  Z | I  Y.000001  Y,000401    1  Z |                               |                               
 * -------------------------------+-------------------------------+-------------------------------+-------------------------------
 *                                | I  Y.000002  Y,000402    2  X |                               |                               
 *                                | I  Y.000002  Y,000402    2  Z |                               |                               
 *       X    Y    Z    W
 * X [   2,   1,   0,     ]
 * Y [   1,   2,   0,     ]
 * Z [   0,   0,   1,     ]
 * W [    ,    ,    ,     ]
 * </pre>
 * 
 * @author Ody
 */
public class DBSyntableBuilder extends DATranscxt {

	protected SynodeMeta synm;
	protected PeersMeta pnvm;
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;
	protected SynchangeBuffMeta exbm;

	public static final int peermode = 0; 
	public static final int leafmode = 1; 

	final int nodemode;

	/**
	 * Get synchronization meta connection id.
	 * 
	 * @return conn-id
	 */
	public String synconn() { return basictx.connId(); }

	protected String synode() { return ((DBSyntext)this.basictx).synode; }

	protected Nyquence stamp;
	void stampersist(Nyquence n) throws TransException, SQLException {
		DAHelper.updateFieldWhereEqs(this, synconn(), synrobot(), synm, synm.nstamp, n.n,
				synm.pk, synode());
		stamp.n = n.n;
	}

	DBSyntableBuilder incStamp(ExessionPersist xp) throws TransException, SQLException {
		stamp.inc();
		if (Nyquence.abs(stamp, nyquvect.get(synode())) >= 2)
			throw new ExchangeException(0, xp, "Nyquence stamp increased too much or out of range.");
		stampersist(stamp);
		return this;
	}

	/** Nyquence vector [{synode, Nyquence}]*/
	protected HashMap<String, Nyquence> nyquvect;
	protected Nyquence n0() { return nyquvect.get(synode()); }
	protected DBSyntableBuilder n0(Nyquence nyq) {
		nyquvect.put(synode(), new Nyquence(nyq.n));
		return this;
	}

	public String domain() {
		return basictx() == null ? null : ((DBSyntext) basictx()).domain;
	}

	private DBSyntableBuilder domain(String domain) {
		if (basictx() != null)
			((DBSyntext) basictx()).domain = domain;
		return this;
	}

	public IUser synrobot() { return ((DBSyntext) this.basictx).usr(); }

	private HashMap<String, SyntityMeta> entityRegists;

	public SyntityMeta getSyntityMeta(String tbl) {
		return entityRegists == null ? null : entityRegists.get(tbl);
	} 

	public DBSyntableBuilder(String conn, String synodeId, String syndomain, int mode)
			throws SQLException, SAXException, IOException, TransException {
		this(conn, synodeId, syndomain, mode,
			new SynChangeMeta(conn),
			new SynodeMeta(conn));
	}
	
	public DBSyntableBuilder(String conn, String synodeId, String syndomain,
			int mode, SynChangeMeta chgm, SynodeMeta synm)
			throws SQLException, SAXException, IOException, TransException {

		super ( new DBSyntext(conn,
			    	initConfigs(conn, loadSemantics(conn), (c) -> new SynmanticsMap(c)),
			    	(IUser) new SyncRobot("rob-" + synodeId, synodeId, syndomain)
			    	, runtimepath));
		
		nodemode = mode;

		// wire up local identity
		DBSyntext tx = (DBSyntext) this.basictx;
		tx.synode = synodeId;
		// tx.domain = loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain);
		tx.domain = getValstr((Transcxt) this, conn, synm, synm.domain, synm.pk, synodeId);
		// ((SyncRobot)tx.usr()).orgId = loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain);
		((SyncRobot)tx.usr())
			.orgId(getValstr((Transcxt) this, conn, synm, synm.org(), synm.pk, synodeId))
			.domain(getValstr((Transcxt) this, conn, synm, synm.domain, synm.pk, synodeId));

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
		
		stamp = DAHelper.getNyquence(this, conn, synm, synm.nyquence,
				synm.synoder, synodeId, synm.domain, tx.domain);

		if (isblank(tx.domain))
			Utils.warn("[%s] Synchrnizer builder (id %s) created without domain specified",
				this.getClass().getName(), tx.synode);
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
	public ExchangeBlock initExchange(ExessionPersist cp, String target)
			throws TransException, SQLException {
		if (DAHelper.count(this, basictx().connId(), exbm.tbl, exbm.peer, target) > 0)
			throw new ExchangeException(Exchanging.ready, cp,
				"Can't initate new exchange session. There are exchanging records to be finished.");

		// select change 
		incStamp(cp);
		return cp.init().nv(nyquvect);
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
			return sp.onInit(inireq).nv(nyquvect);
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
	public ExchangeBlock exchangePage(ExessionPersist cp, ExchangeBlock lastconf)
			throws SQLException, TransException {
		cp.expect(lastconf);
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		
		if (lastconf != null && lastconf.act == init)
			// cleanStaleThan(lastconf.nv, cp.peer);
			cleanStale(lastconf.nv, cp.peer);
		
		return cp
			.commitAnswers(lastconf, cp.peer, n0().n)
			.exchange(cp.peer, lastconf)
			.nv(nyquvect)
			.answers(answer_save(cp, lastconf, cp.peer))
			.seq(cp.persisession());
	}
	
	public ExchangeBlock onExchange(ExessionPersist sp, String peer, ExchangeBlock req)
			throws SQLException, TransException {
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		// for ch in challenges:
		//     answer.add(answer(ch))
		sp.expect(req);

		return sp
			.commitAnswers(req, peer, n0().n)
			.onExchange(peer, req) // The challenge page is ready
			.nv(nyquvect)
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
	 * on Y, y.z = 0 < z.z
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
		
		if (Connects.getDebug(basictx.connId()))
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
				.where(op.lt, sqlCompare("cl", chgm.nyquence, "nv", pnvm.nyq), 0)))
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
					.je_(pnvm.tbl, "nv", chgm.synoder, synm.pk, constr(domain()), pnvm.domain, constr(peer), pnvm.peer)
					.where(op.lt, sqlCompare("cl", chgm.nyquence, "nv", pnvm.nyq), 0))
					// .where(op.ne, subm.synodee, constr(peer)))
				.delete(subm.tbl)
					.where(op.exists, null, select("cl")
					.where(op.eq, subm.changeId, chgm.pk)
					.whereEq(subm.tbl, subm.synodee,  "cl", subm.synodee)))

			// clean changes without subscribes
			.post(with(select(chgm.tbl, "cl")
					.cols(chgm.pk, chgm.domain, chgm.entbl)
					.col(count(subm.synodee), "subs")
					.je_(subm.tbl, "sb", constr(peer), subm.synodee, chgm.pk, subm.changeId))
				.delete(chgm.tbl)
					.where(op.notexists, null, select("cl")
						.whereEq(chgm.tbl, chgm.domain,  "cl", chgm.domain)
						.whereEq(chgm.tbl, chgm.entbl,"cl", chgm.entbl)
						.whereEq(chgm.tbl, chgm.pk, "cl", chgm.pk)))
						// .whereEq(chgm.tbl, chgm.uids, "cl", chgm.uids)))
			.d(instancontxt(basictx.connId(), synrobot()));
			
		if (Connects.getDebug(basictx.connId())) {
			try {
				@SuppressWarnings("unchecked")
				ArrayList<Integer> chgsubs = ((ArrayList<Integer>)res.get("total"));
				if (chgsubs != null && chgsubs.size() > 1 && hasGt(chgsubs, 0)) {
					Utils.logi("Subscribe record(s) are affected:");
					Utils.logi(str(chgsubs, new String[] {"subscribes", "change-logs", "propagations"}));
				}
			} catch (Exception e) { e.printStackTrace(); }
		}
	}

	/**
	 * <p>Clean staleness. (Your knowledge about Z is newer than what I am presenting to)</p>
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
	 * @param srcnv
	 * @param srcn
	 * @throws TransException
	 * @throws SQLException
	 */
	void cleanStaleThan(HashMap<String, Nyquence> srcnv, String srcn)
			throws TransException, SQLException {
		for (String sn : srcnv.keySet()) {
			if (eq(sn, synode()) || eq(sn, srcn))
				continue;
			if (!nyquvect.containsKey(sn))
				continue;

			if (Connects.getDebug(basictx.connId()))
				Utils.logi("%1$s: cleanStaleThan(): Deleting staleness where for peer %2$s, %2$s.%4$s = %3$d > my-change[%4$s].n ...",
						synode(), srcn, srcnv.get(sn).n, sn);

			SemanticObject res = (SemanticObject) ((DBSyntableBuilder)
				with(select(chgm.tbl, "cl")
					.cols("cl.*").col(subm.synodee)
					.je_(subm.tbl, "sb", constr(sn), subm.synodee, chgm.pk, subm.changeId)
					.where(op.ne, subm.synodee, constr(srcn))
					.where(op.lt, chgm.nyquence, srcnv.get(sn).n))) // FIXME nyquence compare
				.delete(subm.tbl, synrobot())
					.where(op.exists, null, select("cl")
						.where(op.eq, subm.changeId, chgm.pk)
						.whereEq(subm.tbl, subm.synodee,  "cl", subm.synodee))
					.post(with(select(chgm.tbl, "cl")
							.cols("cl.*").col(subm.synodee)
							.je_(subm.tbl, "sb",
								chgm.pk, subm.changeId)
							.where(op.ne, subm.synodee, constr(srcn)))
						.delete(chgm.tbl)
						.where(op.lt, chgm.nyquence, srcnv.get(sn).n) // FIXME nyquence compare
						.where(op.notexists, null, select("cl")
							.col("1")
							.whereEq(chgm.tbl, chgm.domain,  "cl", chgm.domain)
							.whereEq(chgm.tbl, chgm.entbl,"cl", chgm.entbl)
							.whereEq(chgm.tbl, chgm.uids, "cl", chgm.uids)))
				.d(instancontxt(basictx.connId(), synrobot()));
			
			if (Connects.getDebug(basictx.connId())) {
				try {
					@SuppressWarnings("unchecked")
					ArrayList<Integer> chgsubs = ((ArrayList<Integer>)res.get("total"));
					if (chgsubs != null && chgsubs.size() > 1 && hasGt(chgsubs, 0)) {
						Utils.logi("Subscribe record(s) are affected:");
						Utils.logi(str(chgsubs, new String[] {"subscribes", "change-log"}));
					}
				} catch (Exception e) { e.printStackTrace(); }
			}
		}
	}

	ExessionPersist answer_save(ExessionPersist xp, ExchangeBlock req, String peer)
			throws SQLException, TransException {
		if (req == null || req.chpage == null) return xp;
		
		AnResultset changes = new AnResultset(req.chpage.colnames());
		ExchangeBlock resp = new ExchangeBlock(synode(), peer, xp.session(), xp.exstat()).nv(nyquvect);

		AnResultset reqChgs = req.chpage;

		while (req.totalChallenges > 0 && reqChgs.next()) {
			String subscribe = req.chpage.getString(subm.synodee);

			if (eq(subscribe, synode())) {
				resp.removeChgsub(req.chpage, synode());	
				changes.append(reqChgs.getRowAt(reqChgs.getRow() - 1));
			}
			else {
				Nyquence subnyq = getn(reqChgs, chgm.nyquence);
				if (!nyquvect.containsKey(subscribe) // I don't have information of the subscriber
					&& eq(synm.tbl, reqChgs.getString(chgm.entbl))) // adding synode
					changes.append(reqChgs.getRowAt(reqChgs.getRow() - 1));
				else if (!nyquvect.containsKey(subscribe))
						; // I have no idea
				else if (compareNyq(subnyq, nyquvect.get(peer)) <= 0) {
					// ref: _answer-to-remove
					// knowledge about the sub from req is older than this node's knowledge 
					// see #commitChallenges ref: merge-older-version
					// FIXME how to abstract into one method?
					
					// resp.remove_sub(req.challenge, synode());	
					resp.removeChgsub(req.chpage, subscribe);	
				}
				else
					changes.append(reqChgs.getRowAt(reqChgs.getRow() - 1));
			}
		}

		return xp.saveAnswer(resp.anspage)
				.saveChanges(changes, req.nv, req.entities);
	}

	public ExchangeBlock closexchange(ExessionPersist cx,
			ExchangeBlock rep) throws TransException, SQLException {

		cx.clear();
		HashMap<String, Nyquence> nv = rep.nv; 

		if (Nyquence.compareNyq(n0(), nv.get(synode())) < 0)
			throw new SemanticException("Synchronizing Nyquence exception: my.n0 = %d < peer.nv[me] = %d",
					n0().n, nv.get(synode()).n);

		synyquvectWith(cx.peer, nv);
		HashMap<String, Nyquence> snapshot = Nyquence.clone(nyquvect);

		// nv.get(sn).inc();
		incN0(maxn(nv));
		
		if (Nyquence.compareNyq(stamp, n0()) > 0)
			throw new SemanticException("Synchronizing Nyquence exception: stamp = %d > n0 = %d",
					stamp.n, n0().n);
		
		stampersist(maxn(stamp, n0()));
		// cx.exstat().onclose();

		return cx.closexchange(rep).nv(snapshot);

		// return snapshot;
	}
	
	public ExchangeBlock abortExchange(ExessionPersist cx)
			throws TransException, SQLException {
		HashMap<String, Nyquence> snapshot = Nyquence.clone(nyquvect);
		incN0(stamp);
		stampersist(maxn(stamp, n0()));
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
				.nv(nyquvect)
				.requirestore()
				.seq(xp);
	}
	
	public void onRequires(ExessionPersist cp, ExchangeBlock req) throws ExchangeException {
		if (req.act == restore) {
			// TODO check step leakings
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
			
			// cp.expChallengeId = rep.challengeId;
			cp.expAnswerSeq = cp.challengeSeq;
		}
		else throw new ExchangeException(0, cp, "TODO");
	}

	/**
	 * Client have found unfinished exchange session then retry it.
	 */
	public void restorexchange() { }

	/////////////////////////////////////////////////////////////////////////////////////////////
	public String updateEntity(String synoder, String pid, SyntityMeta entm, Object ... nvs)
			throws TransException, SQLException, IOException {
		String [] updcols = new String[nvs.length/2];
		for (int i = 0; i < nvs.length; i += 2)
			updcols[i/2] = (String) nvs[i];

		String chgid = update(entm.tbl, synrobot())
			.nvs((Object[])nvs)
			.whereEq(entm.pk, pid)
			.post(insert(chgm.tbl, synrobot())
				.nv(chgm.entfk, pid)
				.nv(chgm.entbl, entm.tbl)
				.nv(chgm.crud, CRUD.U)
				.nv(chgm.synoder, synode()) // U.synoder != uids[synoder]
				.nv(chgm.uids, concatstr(synoder, chgm.UIDsep, pid))
				.nv(chgm.nyquence, stamp.n)
				.nv(chgm.domain, synrobot().orgId())
				.nv(chgm.updcols, updcols)
				.post(insert(subm.tbl)
					.cols(subm.insertCols())
					.select((Query)select(synm.tbl)
						.col(new Resulving(chgm.tbl, chgm.pk))
						.col(synm.synoder)
						.where(op.ne, synm.synoder, constr(synode()))
						.whereEq(synm.domain, synrobot().orgId()))))
			.u(instancontxt(basictx.connId(), synrobot()))
			.resulve(chgm.tbl, chgm.pk);
		return chgid;
	}
	
	public DBSyntableBuilder registerEntity(String conn, SyntityMeta m)
			throws SemanticException, TransException, SQLException {
		if (entityRegists == null)
			entityRegists = new HashMap<String, SyntityMeta>();
		entityRegists.put(m.tbl, (SyntityMeta) m.clone(Connects.getMeta(conn, m.tbl)));
		return this;
	}
	
	public SyntityMeta getEntityMeta(String entbl) throws SemanticException {
		if (entityRegists == null || !entityRegists.containsKey(entbl))
			throw new SemanticException("Register %s first.", entbl);
			
		return entityRegists.get(entbl);
	}
	
	/**
	 * Inc my n0, then reload from DB.
	 * @return this
	 * @throws TransException
	 * @throws SQLException
	 */
	public DBSyntableBuilder incNyquence() throws TransException, SQLException {
		update(synm.tbl, synrobot())
			.nv(synm.nyquence, Funcall.add(synm.nyquence, 1))
			.whereEq(synm.pk, synode())
			.u(instancontxt(basictx.connId(), synrobot()));
		
		nyquvect.put(synode(), getNyquence(this, basictx.connId(), synm, synm.nyquence,
				synm.pk, synode(), synm.domain, ((DBSyntext)this.basictx).domain));
		return this;
	}
	
	/**
	 * this.n0++, this.n0 = max(n0, maxn)
	 * @param maxn
	 * @return n0
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public Nyquence incN0(Nyquence... n) throws TransException, SQLException {
		n0().inc(isNull(n) ? nyquvect.get(synode()).n : n[0].n);
		DAHelper.updateFieldByPk(this, basictx.connId(), synm, synode(),
				synm.nyquence, new ExprPart(n0().n), synrobot());
		return n0();
	}
	
	DBSyntableBuilder loadNyquvect0(String conn) throws SQLException, TransException {
		AnResultset rs = ((AnResultset) select(synm.tbl)
				.cols(synm.pk, synm.nyquence)
				.rs(instancontxt(conn, synrobot()))
				.rs(0));
		
		nyquvect = new HashMap<String, Nyquence>(rs.getRowCount());
		while (rs.next()) {
			nyquvect.put(rs.getString(synm.synoder), new Nyquence(rs.getLong(synm.nyquence)));
		}
		
		return this;
	}

	private DBSyntableBuilder synyquvectWith(String peer, HashMap<String, Nyquence> nv) 
		throws TransException, SQLException {
		if (nv == null) return this;

		Update u = null;

		if (compareNyq(nv.get(peer), nyquvect.get(peer)) < 0)
			throw new SemanticException(
				"[%s.%s()] Updating my (%s) nyquence with %s's value early than already knowns.",
				getClass().getName(), new Object(){}.getClass().getEnclosingMethod().getName(),
				synode(), peer);

		for (String n : nv.keySet()) {
			// if (!eq(n, sn) && nyquvect.containsKey(n)
			if (nyquvect.containsKey(n)
				&& compareNyq(nv.get(n), nyquvect.get(n)) > 0)
				if (u == null)
					u = update(synm.tbl, synrobot())
						.nv(synm.nyquence, n0().n)
						.whereEq(synm.pk, n);
				else
					u.post(update(synm.tbl)
						.nv(synm.nyquence, nv.get(n).n)
						.whereEq(synm.pk, n));

			if (nyquvect.containsKey(n))
				nyquvect.get(n).n = maxn(nv.get(n).n, nyquvect.get(n).n);
			else nyquvect.put(n, new Nyquence(nv.get(n).n));
		}

		nyquvect.put(synode(), maxn(n0(), nv.get(peer)));
		if (u != null)
			u.u(instancontxt(basictx.connId(), synrobot()));

		return this;
	}

	@Override
	public ISemantext instancontxt(String conn, IUser usr) throws TransException {
		try {
			return new DBSyntext(conn,
				initConfigs(conn, loadSemantics(conn),
						(c) -> new SynmanticsMap(c)),
				usr, runtimepath);
		} catch (SAXException | IOException | SQLException e) {
			e.printStackTrace();
			throw new TransException(e.getMessage());
		}
	}
	
	public ExchangeBlock domainSignup(ExessionPersist app, String admin) {
		return app.signup(admin);
	}

	public ExchangeBlock initDomain(ExessionPersist cp, String admin, ExchangeBlock domainstatus)
			throws TransException, SQLException {

		Nyquence mxn = domainstatus.nv.get(admin); 

		if (domainstatus.synodes != null) {
			AnResultset ns = domainstatus.synodes.beforeFirst();
			nyquvect = new HashMap<String, Nyquence>(ns.getRowCount());

			while (ns.next()) {
				if (eq(synode(), ns.getString(synm.pk))) {
					update(synm.tbl, synrobot())
					.nv(synm.domain, ns.getString(synm.domain))
					.whereEq(synm.pk, synode())
					.whereEq(synm.org(), synrobot().orgId())
					.whereEq(synm.domain, domain())
					.u(instancontxt(basictx.connId(), synrobot()));

					domain(ns.getString(synm.domain));
					nyquvect.put(synode(), new Nyquence(mxn.n));
				}
				else {
					Synode n = new Synode(ns, synm);
					mxn = maxn(domainstatus.nv);
					n.insert(synm, mxn, insert(synm.tbl, synrobot()))
						.ins(instancontxt(basictx.connId(), synrobot()));

					nyquvect.put(n.recId, new Nyquence(mxn.n));
				}
			}
		}

		stampersist(mxn);

		return new ExchangeBlock(synode(), admin, domainstatus.session,
				new ExessionAct(ExessionAct.mode_client, setupDom)
			).nv(nyquvect);
	}

	public HashMap<String, Nyquence> closeJoining(ExessionPersist cp,
			HashMap<String, Nyquence> clone) {
		return null;
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
	
	public ExchangeBlock addMyChild(ExessionPersist ap, ExchangeBlock req, String org)
			throws TransException, SQLException {

		String childId = req.srcnode;
		IUser robot = synrobot();

		Synode apply = new Synode(basictx.connId(), childId, org, domain());

		@SuppressWarnings("unused")
		String chgid = ((SemanticObject) apply.insert(synm, n0(), insert(synm.tbl, robot))
			.post(insert(chgm.tbl, robot)
				.nv(chgm.entfk, apply.recId)
				.nv(chgm.entbl, synm.tbl)
				.nv(chgm.crud, CRUD.C)
				.nv(chgm.synoder, synode())
				.nv(chgm.uids, concatstr(synode(), chgm.UIDsep, apply.recId))
				.nv(chgm.nyquence, n0().n)
				.nv(chgm.domain, domain())
				.post(insert(subm.tbl)
					// .cols(subm.entbl, subm.synodee, subm.uids, subm.domain)
					.cols(subm.insertCols())
					.select((Query)select(synm.tbl)
						// .col(constr(synm.tbl))
						.col(new Resulving(chgm.tbl, chgm.pk))
						.col(synm.synoder)
						// .col(concatstr(synode(), chgm.UIDsep, apply.recId))
						// .col(constr(robot.orgId()))
						.where(op.ne, synm.synoder, constr(synode()))
						.where(op.ne, synm.synoder, constr(childId))
						.whereEq(synm.domain, domain()))))
			.ins(instancontxt(basictx.connId(), robot)))
			.resulve(chgm.tbl, chgm.pk);
		
		nyquvect.put(apply.recId, new Nyquence(apply.nyquence));

		ExchangeBlock rep = new ExchangeBlock(synode(), childId, ap.session(), ap.exstat())
			.nv(nyquvect)
			.synodes(req.act == ExessionAct.signup
				? ((AnResultset) select(synm.tbl, "syn")
					.whereIn(synm.synoder, childId, synode())
					.whereEq(synm.domain, domain())
					.whereEq(synm.org(), org)
					.rs(instancontxt(basictx.connId(), synrobot()))
					.rs(0))
				: null);
		return rep;
	}
}
