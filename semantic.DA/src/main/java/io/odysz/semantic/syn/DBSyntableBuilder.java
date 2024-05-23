package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.hasGt;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.getn;
import static io.odysz.semantic.syn.Nyquence.maxn;
import static io.odysz.semantic.syn.ExessionAct.*;

import static io.odysz.semantic.util.DAHelper.getNyquence;
import static io.odysz.semantic.util.DAHelper.getValstr;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.concatstr;

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
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;
	protected SynchangeBuffMeta exbm;

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

	DBSyntableBuilder incStamp() throws TransException, SQLException {
		stamp.inc();
		if (Nyquence.abs(stamp, nyquvect.get(synode())) >= 2)
			throw new ExchangeException(0, "Nyquence stamp increased too much or out of range.");
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

	public IUser synrobot() { return ((DBSyntext) this.basictx).usr(); }

	private HashMap<String, SyntityMeta> entityRegists;

	public SyntityMeta getSyntityMeta(String tbl) {
		return entityRegists == null ? null : entityRegists.get(tbl);
	} 

	public DBSyntableBuilder(String conn, String synodeId, String syndomain)
			throws SQLException, SAXException, IOException, TransException {
		this(conn, synodeId, syndomain,
			new SynChangeMeta(conn),
			new SynodeMeta(conn));
	}
	
	public DBSyntableBuilder(String conn, String synodeId, String syndomain, SynChangeMeta chgm, SynodeMeta synm)
			throws SQLException, SAXException, IOException, TransException {

		super ( new DBSyntext(conn,
			    	initConfigs(conn, loadSemantics(conn), (c) -> new SynmanticsMap(c)),
			    	(IUser) new SyncRobot("rob-" + synodeId, synodeId, syndomain)
			    	, runtimepath));

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
		this.exbm = exbm != null ? exbm : new SynchangeBuffMeta(chgm, conn);
		this.exbm.replace();
		this.subm = subm != null ? subm : new SynSubsMeta(chgm, conn);
		this.subm.replace();
		this.synm = synm != null ? synm : (SynodeMeta) new SynodeMeta(conn).autopk(false);
		this.synm.replace();
		
		stamp = DAHelper.getNyquence(this, conn, synm, synm.nyquence, synm.synoder, synodeId, synm.domain, tx.domain);

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
	public ExchangeBlock initExchange(ExessionPersist cp, String target
//			HashMap<String, Nyquence> srcnv
			) throws TransException, SQLException {
		if (DAHelper.count(this, basictx().connId(), exbm.tbl, exbm.peer, target) > 0)
			throw new ExchangeException(Exchanging.ready,
				"Can't initate new exchange session. There are exchanging records to be finished.");

		// select change 
		try { return cp.init().nv(nyquvect); }
		finally { incStamp(); }

//		return new ExchangeBlock(synode(), cp.session()).nv(nyquvect)
//				.totalChallenges(DAHelper.count(this, synconn(), exbm.tbl, exbm.peer, cp.peer))
//				.seq(cp);
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
		
			cleanStaleThan(inireq.nv, sp.peer);

			// insert into exchanges select * from change_logs where n > nyquvect[sx.peer].n
			return sp.onInit(inireq).nv(nyquvect);

//			return new ExchangeBlock(synode(), sp.session()).nv(nyquvect)
//				.totalChallenges(DAHelper.count(this, synconn(), exbm.tbl, exbm.peer, sp.peer))
//				.seq(sp);
		} finally {
			incStamp();
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
			cleanStaleThan(lastconf.nv, cp.peer);
		
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

		// return new ExchangeBlock(synode(), peer, sp.session(), sp.exstat())
		return sp
			.commitAnswers(req, peer, n0().n)
			.onExchange(peer, req) // The challenge page is ready
			.nv(nyquvect)
			.answers(answer_save(sp, req, peer))
			.seq(sp.persisession());
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
				Utils.logi("%1$s.cleanStateThan(): Deleting staleness where for source %2$s, my-change[%4$s].n < src-nyq[%1$s] = %3$d ...",
						synode(), srcn, nyquvect.get(srcn).n, sn);

			SemanticObject res = (SemanticObject) ((DBSyntableBuilder)
				with(select(chgm.tbl, "cl")
					.cols("cl.*").col(subm.synodee)
					// .je2(subm.tbl, "sb", constr(sn), subm.synodee,
					// 	chgm.domain, subm.domain, chgm.entbl, subm.entbl,
					// 	chgm.uids, subm.uids)
					.je_(subm.tbl, "sb", constr(sn), subm.synodee, chgm.pk, subm.changeId)
					.where(op.ne, subm.synodee, constr(srcn))
					.where(op.lt, chgm.nyquence, srcnv.get(sn).n))) // FIXME nyquence compare
				.delete(subm.tbl, synrobot())
					.where(op.exists, null, select("cl")
						// .whereEq(subm.tbl, subm.domain,"cl", chgm.domain)
						// .whereEq(subm.tbl, subm.entbl, "cl", chgm.entbl)
						// .whereEq(subm.tbl, subm.uids,  "cl", chgm.uids)
						.where(op.eq, subm.changeId, chgm.pk)
						.whereEq(subm.tbl, subm.synodee,  "cl", subm.synodee))
					.post(with(select(chgm.tbl, "cl")
							.cols("cl.*").col(subm.synodee)
							.je_(subm.tbl, "sb",
							//	chgm.domain, subm.domain, chgm.entbl, subm.entbl,
							//	chgm.uids, subm.uids)
								chgm.pk, subm.changeId)
							.where(op.ne, subm.synodee, constr(srcn)))
						.delete(chgm.tbl)
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
						Utils.<Integer>logi(chgsubs);
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

	public ExchangeBlock onclosexchange(ExessionPersist sx,
			ExchangeBlock rep) throws TransException, SQLException {
		return closexchange(sx, rep);
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
		else throw new ExchangeException(0, "TODO");
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
				.nv(chgm.nyquence, n0().n)
				.nv(chgm.domain, synrobot().orgId())
				.nv(chgm.updcols, updcols)
				.post(insert(subm.tbl)
					// .cols(subm.entbl, subm.synodee, subm.uids, subm.domain)
					.cols(subm.insertCols())
					.select((Query)select(synm.tbl)
						// .col(constr(entm.tbl))
						.col(new Resulving(chgm.tbl, chgm.pk))
						.col(synm.synoder)
						// .col(concatstr(synode(), chgm.UIDsep, pid))
						// .col(constr(synrobot().orgId()))
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

	public ExchangeBlock addChild(ExessionPersist ap, String synode, SynodeMode child,
			IUser robot, String org, String domain) {
		return null;
	}

	public ExchangeBlock initDomain(ExessionPersist cp, String synode, ExchangeBlock resp) {
		return null;
	}

	public HashMap<String, Nyquence> closeJoining(ExessionPersist cp, String synode,
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

	public String domain() {
		return basictx() == null ? null : ((DBSyntext) basictx()).domain;
	}
}
