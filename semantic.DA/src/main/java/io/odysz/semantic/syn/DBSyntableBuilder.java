package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.getn;
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
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
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
	public DBSyntableBuilder incStamp() throws ExchangeException {
		stamp.inc();
		if (Nyquence.abs(stamp, nyquvect.get(synode())) >= 2)
			throw new ExchangeException(0, "Nyquence stamp increased too much or out of range.");
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
	 * @param nv
	 * @return {total: change-logs to be exchanged} 
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExchangeBlock initExchange(ExessionPersist cp, String target,
			HashMap<String, Nyquence> srcnv) throws TransException, SQLException {
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
	public ExchangeBlock exchangePage(ExessionPersist cp, String peer,
			ExchangeBlock lastconf) throws SQLException, TransException {
		cp.expect(lastconf);
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		return cp
			.exchange(peer, lastconf)
			.nv(nyquvect)
			.answers(answer_save(cp, lastconf, peer));
	}
	
	public ExchangeBlock onExchange(ExessionPersist sp, String peer, ExchangeBlock req)
			throws SQLException, TransException {
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		// for ch in challenges:
		//     answer.add(answer(ch))
		sp.expect(req)
		  .onExchange(peer, req); // The challenge page is ready

		return new ExchangeBlock(synode(), peer, sp.session(), sp.exstat())
			.nv(nyquvect)
			.answers(answer_save(sp, req, peer))
			.seq(sp);
	}
	
	ExessionPersist answer_save(ExessionPersist xp, ExchangeBlock req, String srcn)
			throws SQLException {
		if (req == null || req.chpage == null) return xp;
		
		ArrayList<ArrayList<Object>> changes = new ArrayList<ArrayList<Object>>();
		ExchangeBlock resp = new ExchangeBlock(synode(), srcn, xp.session(), xp.exstat()).nv(nyquvect);

		AnResultset reqChgs = req.chpage;

		while (req.totalChallenges > 0 && reqChgs.next()) {
			String subscribe = req.chpage.getString(subm.synodee);

			if (eq(subscribe, synode())) {
				// resp.remove_sub(req.challenge, synode());	
				resp.removeChgsub(req.chpage, synode());	
				changes.add(reqChgs.getRowAt(reqChgs.getRow() - 1));
			}
			else {
				Nyquence subnyq = getn(reqChgs, chgm.nyquence);
				if (!nyquvect.containsKey(subscribe) // I don't have information of the subscriber
					&& eq(synm.tbl, reqChgs.getString(chgm.entbl))) // adding synode
					changes.add(reqChgs.getRowAt(reqChgs.getRow() - 1));
				else if (!nyquvect.containsKey(subscribe))
						; // I have no idea
				else if (compareNyq(subnyq, nyquvect.get(srcn)) <= 0) {
					// ref: _answer-to-remove
					// knowledge about the sub from req is older than this node's knowledge 
					// see #commitChallenges ref: merge-older-version
					// FIXME how to abstract into one method?
					
					// resp.remove_sub(req.challenge, synode());	
					resp.removeChgsub(req.chpage, subscribe);	
				}
				else
					changes.add(reqChgs.getRowAt(reqChgs.getRow() - 1));
			}
		}

		return xp.saveAnswer(resp.answers()).addChangeBuf(changes);
	}

	public HashMap<String, Nyquence> closexchange(ExessionPersist cx, String synode,
			HashMap<String, Nyquence> nv) {
		return nv;
	}

	public void onclosexchange(ExessionPersist sx, String synode, HashMap<String, Nyquence> nv) {
	}
	
	public void onRequires(ExessionPersist cp, ExchangeBlock req) throws ExchangeException {
		if (req.act == ExessionAct.restore) {
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
	
	public ExchangeBlock requirestore(ExessionPersist xp, String peer) {
		return new ExchangeBlock(synode(), peer, xp.session(), xp.exstat())
				.nv(nyquvect)
				.requirestore()
				.seq(xp);
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

//	public Query selectCols(Query subq, String changeId) {
//		Query q = select(subq);
//		return q;
//	}
//
//	private Query select(Query subq) {
//		return null;
//	}
}
