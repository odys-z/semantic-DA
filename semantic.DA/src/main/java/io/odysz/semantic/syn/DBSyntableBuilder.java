package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.getn;
import static io.odysz.semantic.util.DAHelper.loadRecNyquence;
import static io.odysz.semantic.util.DAHelper.loadRecString;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.concatstr;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.xml.sax.SAXException;

import io.odysz.anson.x.AnsonException;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynExhangeBuffMeta;
import io.odysz.semantic.meta.SynSubsMeta;
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
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.Logic.op;
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
	protected SynExhangeBuffMeta exbm;

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

	public DBSyntableBuilder(String conn, String synodeId)
			throws SQLException, SAXException, IOException, TransException {
		this(conn, synodeId,
			new SynChangeMeta(conn),
			new SynodeMeta(conn));
	}
	
	public DBSyntableBuilder(String conn, String synodeId, SynChangeMeta chgm, SynodeMeta synm)
			throws SQLException, SAXException, IOException, TransException {

		super ( new DBSyntext(conn,
			    	initConfigs(conn, loadSemantics(conn), (c) -> new SynmanticsMap(c)),
			    	(IUser) new SyncRobot("rob-" + synodeId, synodeId)
			    	, runtimepath));

		// wire up local identity
		DBSyntext tx = (DBSyntext) this.basictx;
		tx.synode = synodeId;
		tx.domain = loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain);
		((SyncRobot)tx.usr()).orgId = loadRecString((Transcxt) this, conn, synm, synodeId, synm.domain);

		this.chgm = chgm != null ? chgm : new SynChangeMeta(conn);
		this.chgm.replace();
		this.subm = subm != null ? subm : new SynSubsMeta(chgm, conn);
		this.subm.replace();
		this.synm = synm != null ? synm : (SynodeMeta) new SynodeMeta(conn).autopk(false);
		this.synm.replace();
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
		incStamp();
		cp.init(target, 0);

		return new ExchangeBlock(synode(), cp.session()).nv(nyquvect)
				.totalChallenges(DAHelper.count(this, synconn(), exbm.tbl, exbm.peer, cp.peer))
				.seq(cp);
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
			return new ExchangeBlock(synode(), sp.session()).nv(nyquvect)
				.totalChallenges(DAHelper.count(this, synconn(), exbm.tbl, exbm.peer, sp.peer))
				.seq(sp);
		} finally {
			incStamp();
		}
	}

	public void abortExchange(ExessionPersist cx, String target, ExchangeBlock rep) {
	}

	/**
	 * Push a block, with piggyback answers of previous confirming's challenges.
	 * 
	 * @param cp
	 * @param peer
	 * @param lastcnf
	 * @return pushing block
	 * @throws SQLException 
	 * @throws ExchangeException 
	 */
	public ExchangeBlock exchangeBlock(ExessionPersist cp, String peer,
			ExchangeBlock lastcnf) throws SQLException, ExchangeException {
		cp.expect(peer, lastcnf);
		AnResultset rs = cp.chpage();
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		return cp.exchange(peer, lastcnf)
			.nv(nyquvect)
			.challengePage(rs)
			.answers(answer(cp, lastcnf, peer));
			// .seq(cp.exchange(peer, lastcnf));
	}
	
	public ExchangeBlock onExchange(ExessionPersist sp, String peer, ExchangeBlock req)
			throws ExchangeException, SQLException {
		// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
		// for ch in challenges:
		//     answer.add(answer(ch))
		sp.expect(peer, req)
		  .onExchange(peer, req);

		return new ExchangeBlock(peer, sp.session())
			.nv(nyquvect)
			.challengePage(sp.chpage())
			.answers(answer(sp, req, peer))
			.seq(sp);
	}
	
	ExessionPersist answer(ExessionPersist sp, ExchangeBlock req, String srcn)
			throws SQLException {
		if (req == null) return sp;
		
		ArrayList<ArrayList<Object>> changes = new ArrayList<ArrayList<Object>>();
		ExchangeBlock resp = new ExchangeBlock(srcn, sp.session()).nv(nyquvect);

		AnResultset challenge = req.chpage;
		if (challenge == null) return sp;

		while (req != null && req.challenges > 0 && req.chpage.next()) {
			String subscribe = req.chpage.getString(subm.synodee);

			if (eq(subscribe, synode())) {
				resp.removeChgsub(synode());	
				changes.add(challenge.getRowAt(challenge.getRow() - 1));
			}
			else {
				Nyquence subnyq = getn(challenge, chgm.nyquence);
				if (!nyquvect.containsKey(subscribe) // I don't have information of the subscriber
					&& eq(synm.tbl, challenge.getString(chgm.entbl))) // adding synode
					changes.add(challenge.getRowAt(challenge.getRow() - 1));
				else if (!nyquvect.containsKey(subscribe))
						; // I have no idea
				else if (compareNyq(subnyq, nyquvect.get(srcn)) <= 0) {
					// ref: _answer-to-remove
					// knowledge about the sub from req is older than this node's knowledge 
					// see #commitChallenges ref: merge-older-version
					// FIXME how to abstract into one method?
					resp.removeChgsub(subscribe);	
				}
				else
					changes.add(challenge.getRowAt(challenge.getRow() - 1));
			}
		}

		return sp.saveChallenges(changes);
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
			if (cp.challengeSeq <= req.challengeId) {
				// server is actually handled my challenge. Just step ahead 
				cp.challengeSeq = req.challengeId;
			}
			else if (cp.challengeSeq == req.challengeId + 1) {
				// server haven't got the previous package
				// setup for send again
				cp.challengeSeq = req.challengeId;
			}
			else {
				// not correct
				cp.challengeSeq = req.challengeId;
			}

			if (cp.answerSeq < req.answerId) {
				cp.answerSeq = req.answerId;
			}
			
			// cp.expChallengeId = rep.challengeId;
			cp.expAnswerSeq = cp.challengeSeq;
		}
		else throw new ExchangeException(0, "TODO");
	}
	
	public ExchangeBlock requirestore(ExessionPersist xp, String peer) {
		return new ExchangeBlock(peer, xp.session())
				.nv(nyquvect)
				.requirestore()
				.seq(xp);
	}

	/**
	 * Client found unfinished exchange session and retry it.
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
		
		nyquvect.put(synode(), loadRecNyquence(this, basictx.connId(), synm, synode(), synm.nyquence));
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

//	static void verifyseq(ExessionPersist ex, ExchangeBlock rep) throws ExchangeException {
//		throw new ExchangeException(ex.expChallengeId, "TODO");
//	}
}
