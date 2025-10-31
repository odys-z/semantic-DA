package io.oz.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.hasGt;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.str;
import static io.odysz.common.Utils.logi;
import static io.odysz.semantic.util.DAHelper.updateFieldByPk;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.oz.syn.ExessionAct.setupDom;
import static io.oz.syn.Nyquence.compareNyq;
import static io.oz.syn.Nyquence.getn;
import static io.oz.syn.Nyquence.maxn;
import static io.oz.syn.Nyquence.sqlCompare;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
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
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.ExtFilePaths;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.Sql;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;
import io.oz.syn.registry.Syntities;

/**
 * SQL statement builder for {@link DBSynmantext} for handling database synchronization. 
 * 
 * Be improved with temporary tables for broken network (and shutdown), concurrency and memory usage.
 * 
 * @author Ody
 */
public class DBSyntableBuilder extends DATranscxt {
	boolean debug;

	public SyndomContext syndomx;

	SyncUser locrobot;

	public SyncUser synrobot() { return locrobot; }

	private final boolean force_clean_subs;

	private long seq;
	public long incSeq() { return ++seq; }

	/**
	 * 
	 * @param x
	 */
	public DBSyntableBuilder(SyndomContext x) throws TransException, SQLException, SAXException, IOException {

		super(x.synconn);	

		syndomx = x;
		locrobot = new SyncUser(x.synode, x.synode, "rob@" + x.synode, x.synode);
		
		debug    = Connects.getDebug(x.synconn);
		dbgStack = debug;
		
		// seq = 0;
		force_clean_subs = true;

		if (x.mode != SynodeMode.nonsyn) {
			if (DAHelper.count(this, x.synconn, syndomx.synm.tbl,
						x.synm.synoder, x.synode, x.synm.domain, x.domain) <= 0) {
				if (debug) Utils
					.warnT(new Object() {},
						  "\nThis syntable builder is being built for node %s which doesn't exists in domain %s." +
						  "\nThis instence can only be useful if is used to initialize the domain for the node",
						  x.synode, x.domain);
			}
		}
		else if (isblank(x.domain))
			Utils.warn("[%s] Synchrnizer builder (id %s) created without domain specified",
				this.getClass().getName(), x.synode);	

		if (debug && force_clean_subs) Utils
			.logT(new Object() {}, "Transaction builder created with forcing cleaning stale subscriptions.");
	}
	

	////////////////////////////// protocol API ////////////////////////////////
	/**
	 * Client have found unfinished exchange session then retry it.
	 * @return null or restore-request
	 * @throws SQLException 
	 * @throws TransException 
	 * @since 1.5.18
	 * @see ExessionPersist#restore()
	 */
	public ExchangeBlock restorexchange(ExessionPersist xp) throws TransException, SQLException {
		if (DAHelper.count(this, this.syndomx.synconn, xp.sysm.tbl, xp.sysm.peer, xp.peer) == 0)
			return null;
		else
			return xp.restore();
	}

	/**
	 * @see ExessionPersist#init()
	 * @param cp
	 * @return {total: change-logs to be exchanged} 
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExchangeBlock initExchange(ExessionPersist cp)
			throws TransException, SQLException {
		if (DAHelper.count(this, basictx().connId(), syndomx.exbm.tbl, syndomx.exbm.peer, cp.peer) > 0)
			throw new ExchangeException(ExessionAct.ready, cp,
				"Can't initate new exchange session. There are exchanging records to be finished.");

		return cp.init();
	}
	
	/**
	 * @see ExessionPersist#onInit(ExchangeBlock)
	 * @param sp
	 * @param inireq
	 * @return response block
	 * @throws TransException 
	 * @throws SQLException 
	 */
	public ExchangeBlock onInit(ExessionPersist sp, ExchangeBlock inireq)
			throws SQLException, TransException {
		try {
			cleanStale_initPeers(inireq.nv, sp.peer);

			// insert into exchanges_buff select * from change_logs where n > nyquvect[sx.peer].n
			return sp.onInit(inireq);
		} finally {
			syndomx.incStamp(sp.trb);
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
			.answers(answer_save(cp, lastconf, cp.peer))
			// .seq(cp.persisession());
			.seq(cp);
	}
	
	/**
	 * Clean N.change[.].nyq <= NVp.[.], where,
	 * N is synoder;
	 * P is the intiator, NVp is P's nv;
	 * "." is the subsrciber.
	 * 
	 * Side effects: also insert synssion peers into syndomx.pnvm.
	 * 
	 * @param srcnv
	 * @param srcn
	 */
	void cleanStale_initPeers(HashMap<String, Nyquence> srcnv, String peer)
			throws TransException, SQLException {
		if (srcnv == null) return;
		
		String      synode = syndomx.synode;
		String      domain = syndomx.domain;
		String     synconn = syndomx.synconn;
		PeersMeta     pnvm = syndomx.pnvm;
		SynodeMeta    synm = syndomx.synm;
		SynChangeMeta chgm = syndomx.chgm;
		SynSubsMeta   subm = syndomx.subm;
		
		if (debug)
			Utils.logi("Cleaning staleness at %s, peer %s ...", synode, peer);

		pushDebug(true);
		delete(pnvm.tbl, locrobot)
			.whereEq(pnvm.peer, peer)
			.whereEq(pnvm.domain, domain)
			.post(insert(pnvm.tbl)
				.cols(pnvm.inscols)
				.values(pnvm.insVals(srcnv, peer, domain)))
			.d(instancontxt(synconn, locrobot));
		popDebug();

		// FIXME 1.5.14 This should can be simplified now.
		SemanticObject res = (SemanticObject) ((DBSyntableBuilder)
			// clean while accepting subscriptions
			with(select(chgm.tbl, "cl")
				.cols("cl.*").col(subm.synodee)
				.je_(subm.tbl, "sb", constr(peer), subm.synodee, chgm.pk, subm.changeId)
				.je_(pnvm.tbl, "nv", chgm.synoder, synm.pk, constr(domain), pnvm.domain, constr(peer), pnvm.peer)
				.where(op.le, sqlCompare("cl", chgm.nyquence, "nv", pnvm.nyq), 0))) // 631e138aae8e3996aeb26d2b54a61b1517b3eb3f
			.delete(subm.tbl, locrobot)
				.where(op.exists, null, select("cl")
				.where(op.eq, subm.changeId, chgm.pk)
				.whereEq(subm.tbl, subm.synodee,  "cl", subm.synodee))

			// clean 3rd part nodes' propagation
			.post(with(select(chgm.tbl, "cl")
					.cols("cl.*").col(subm.synodee)
					.j(subm.tbl, "sb", Sql.condt(op.eq, chgm.pk, subm.changeId)
											.and(Sql.condt(op.ne, constr(peer), subm.synodee)))
					.je_(synm.tbl, "sn", chgm.synoder, synm.pk, constr(domain), synm.domain)
					.je_(pnvm.tbl, "nver", "cl." + chgm.synoder, pnvm.synid, constr(domain), pnvm.domain, constr(peer), pnvm.peer)
					// see 6b8b2cae7c07c427b82c2aec626b46e4fb9ab4f3
					.je_(pnvm.tbl, "nvee", "sb." + subm.synodee, pnvm.synid, constr(domain), pnvm.domain, constr(peer), pnvm.peer)
					.where(op.le, sqlCompare("cl", chgm.nyquence, "nver", pnvm.nyq), 0)
					.where(op.le, sqlCompare("cl", chgm.nyquence, "nvee", pnvm.nyq), 0))
				.delete(subm.tbl)
				.where(op.exists, null,
					select("cl")
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
			.d(instancontxt(synconn, locrobot));
			
		if (debug) {
			try {
				@SuppressWarnings("unchecked")
				ArrayList<Integer> chgsubs = ((ArrayList<Integer>)res.get("total"));
				if (chgsubs != null && chgsubs.size() > 1 && hasGt(chgsubs, 0)) {
					Utils.warn("[%s : %s (n0: %s, n-stamp: %s, peer: %s)] Subscribe record(s) has been affected:",
								synode, domain, syndomx.n0(), syndomx.stamp(), peer);
					Utils.warn(str(chgsubs, new String[] {"subscribes", "propagations", "change-logs"}));
				}
			} catch (Exception e) { e.printStackTrace(); }
		}
	}

	/**
	 * Commit changes by req's challenge page.
	 * 
	 * @param xp
	 * @param req
	 * @param peer
	 * @return my {@link #xp}
	 * @throws SQLException
	 * @throws TransException
	 */
	@SuppressWarnings("deprecation")
	ExessionPersist answer_save(ExessionPersist xp, ExchangeBlock req, String peer)
			throws SQLException, TransException {
		if (req == null || req.chpage == null) return xp;

		if (ExessionPersist.dbgExchangePaging)
			printAnswer_persisting(peer, xp, req);

		String      synode = syndomx.synode;
		SynodeMeta    synm = syndomx.synm;
		SynChangeMeta chgm = syndomx.chgm;
		SynSubsMeta   subm = syndomx.subm;
		SynodeMode synmode = syndomx.mode;
		

		AnResultset changes = new AnResultset(req.chpage.colnames());
		ExchangeBlock resp = new ExchangeBlock(syndomx.domain, syndomx.synode, peer, xp.session(), xp.exstat())
							.nv(xp.synx.nv);

		HashSet<String> warnsynodee = new HashSet<String>();
		HashSet<String> warnsynoder = new HashSet<String>();

		while (req.totalChallenges > 0 && req.chpage.next()) { // FIXME performance issue
			String synodee = req.chpage.getString(subm.synodee);
			String synoder = req.chpage.getString(chgm.synoder);

			if (!xp.synx.nv.containsKey(synoder)) {
				if (!warnsynoder.contains(synoder)) {
					warnsynoder.add(synoder);
					Utils.warnT(new Object() {},
							"%s has no idea about %s. The changes %s -> %s are ignored.",
							synode, synoder, req.chpage.getString(chgm.uids), synodee);
				}
				continue;
			}
			else if (eq(synodee, synode)) {
				resp.removeChgsub(req.chpage);	
				changes.append(req.chpage.getRowAt(req.chpage.getRow() - 1));
			}
			else {
				Nyquence subnyq = getn(req.chpage, chgm.nyquence);
				if (!xp.synx.nv.containsKey(synodee) // I don't have information of the subscriber
					&& eq(synm.tbl, req.chpage.getString(chgm.entbl))) // adding synode
					changes.append(req.chpage.getRowAt(req.chpage.getRow() - 1));
				else if (!xp.synx.nv.containsKey(synodee)) {
					; // I have no idea
					if (synmode != SynodeMode.leaf_) {
						if (!warnsynodee.contains(synodee)) {
							warnsynodee.add(synodee);
							Utils.warnT(new Object() {},
									"%s has no idea about %s. The change is committed at this node. This can either be automatically fixed or causing data lost later.",
									synode, synodee);
						}
						changes.append(req.chpage.getRowAt(req.chpage.getRow() - 1));
					}
					else // leaf
						if (!warnsynodee.contains(synodee)) {
							warnsynodee.add(synodee);
							Utils.warnT(new Object(){},
									"%s has no idea about %s. Ignoring as is working in leaf mode. (Will filter data at server side in the near future)",
									synode, synodee);
						}	
				}
				// see also ExessionPersist#saveChanges()
				else if (compareNyq(subnyq, xp.synx.nv.get(req.chpage.getString(chgm.synoder))) > 0) {
					// should suppress the following case
					changes.append(req.chpage.getRowAt(req.chpage.getRow() - 1));
				}
				else if (compareNyq(subnyq, xp.synx.nv.get(peer)) <= 0) {
					// 2024.6.5 client shouldn't have older knowledge than me now,
					// which is cleaned when initiating.
					if (debug) Utils.warnT(new Object(){}, "Ignore this?");
				}
				else
					changes.append(req.chpage.getRowAt(req.chpage.getRow() - 1));
			}
		}

		return xp.saveAnswer(resp.anspage)
				.saveChanges(changes, req.nv, req.entities);
	}

	private void printAnswer_persisting(String peer, ExessionPersist xp, ExchangeBlock req) {
		logi("====== %s <- %s ====== Answering: ======", syndomx.synode, peer);
		logi("%s\npage-index: %s,\tchallenging size: %s\nSyntities:\n",
			syndomx.synode, xp.answerSeq(), xp.challengeSeq(), xp.chsize);
		
		if (xp.chEntities != null)
			for (String tbl : xp.chEntities.keySet())
				logi("%s\t%s", tbl, xp.chEntities.size());
	}

	/**
	 * Step n0, reply with closing Exchange-block.
	 * @param cx
	 * @param rep
	 * @return reply
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExchangeBlock closexchange(ExessionPersist cx, ExchangeBlock rep)
			throws TransException, SQLException {

		HashMap<String, Nyquence> nv = rep.nv; 

		Nyquence peerme = nv.get(syndomx.synode);
		if (peerme != null && Nyquence.compareNyq(cx.n0(), peerme) < 0)
			throw new SemanticException("Synchronizing Nyquence exception: my.n0 = %d < peer.nv[me] = %d, at %s (me).",
					cx.n0().n, peerme.n, syndomx.synode);

		if (Nyquence.compareNyq(syndomx.stamp, cx.n0()) < 0)
			throw new SemanticException("Synchronizing Nyquence exception: %s.stamp = %d < n0 = %d.",
					syndomx.synode, syndomx.stamp.n, cx.n0().n);
		
		HashMap<String, Nyquence> snapshot = synXnv(cx, rep.nv);

		syndomx.n0(this, syndomx.persistamp(this, maxn(syndomx.stamp, cx.n0())));

		return cx.closexchange(rep).nv(snapshot);
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
		PeersMeta pnvm = syndomx.pnvm;

		// 2025-01-14: Shouldn't care about Syntities here. Bug?
		@SuppressWarnings("unused")
		SynodeMeta synm = syndomx.synm;

		SynChangeMeta chgm = syndomx.chgm;
		SynSubsMeta subm = syndomx.subm;
		
		// FIXME Move this to static selectExbuf(),
		// and count total changes before exbuf has been inserted?
		Query q_exchgs = select(chgm.tbl, "cl")
				.distinct(true)
				.cols(constr(peer), chgm.pk, new ExprPart(-1))
				.j(subm.tbl, "sb", Sql.condt(op.eq, chgm.pk, subm.changeId)
										.and(Sql.condt(op.eq, constr(syndomx.domain), "cl." + chgm.domain))
										.and(Sql.condt(op.ne, constr(syndomx.synode), subm.synodee)))
				.je_(pnvm.tbl, "nvee", "cl." + chgm.synoder, pnvm.synid,
										constr(syndomx.domain), pnvm.domain, constr(peer), pnvm.peer)
				.where(op.gt, sqlCompare("cl", chgm.nyquence, "nvee", pnvm.nyq), 0);
		
		if (debug)
			try {
				Utils.logT(new Object() {},
						"%s is inserting exchange buffers (domain %s, peer %s)::",
						syndomx.synode, syndomx.domain, peer);
				((AnResultset) q_exchgs.rs(instancontxt()).rs(0)).print();
			} catch (Exception e) {
				e.printStackTrace();
			}

		return insert(syndomx.exbm.tbl, locrobot)
			.cols(syndomx.exbm.insertCols())
			.select(q_exchgs);
	}
	
	/**
	 * Orthogonally synchronize n-vectors locally.
	 * 
	 * @param xp
	 * @param xnv
	 * @return the snapshot before stepping the stamp, for exchanging to the peer.
	 * @throws TransException
	 * @throws SQLException
	 */
	public HashMap<String, Nyquence> synXnv(ExessionPersist xp,
			HashMap<String, Nyquence> xnv) throws TransException, SQLException {

		Update u = null;

		HashMap<String, Nyquence> snapshot =  new HashMap<String, Nyquence>();

		
		for (String n : xnv.keySet()) {
			if (!xp.synx.nv.containsKey(n))
				continue;
			
			Nyquence mxn = xp.synx.nv.get(n);
			mxn = maxn(xnv.get(n), mxn);

			snapshot.put(n, new Nyquence(mxn.n));
			
			if (eq(n, xp.synx.synode))
				mxn = maxn(xp.synx.stamp, mxn); 

			if (compareNyq(mxn, xp.synx.nv.get(n)) < 0) 
				throw new ExchangeException(xp.exstat().state, xp, "Something Wrong!");
			else if (compareNyq(mxn, xp.synx.nv.get(n))  > 0)
				u = persistNyq(u, n, mxn);
		}

		if (u != null) {
			try {
				// TODO FIXME move, merge, with ExessionPerist
				xp.synx.lockx(locrobot);
				u.u(instancontxt());
				xp.synx.loadNvstamp(this);
			}finally {
				xp.synx.unlockx(locrobot);
			}
		}
	
		return snapshot;
	}
	
	private Update persistNyq(Update u, String nodeId, Nyquence nyq) {
		SynodeMeta synm = syndomx.synm;
		if (u == null)
			u = update(synm.tbl, locrobot)
				.nv(synm.nyquence, nyq.n)
				.whereEq(synm.domain, syndomx.domain)
				.whereEq(synm.pk, nodeId);
		else
			u.post(update(synm.tbl)
				.nv(synm.nyquence, nyq.n)
				.whereEq(synm.domain, syndomx.domain)
				.whereEq(synm.pk, nodeId));
		
		return u;
	}

	public ExchangeBlock abortExchange(ExessionPersist cx)
			throws TransException, SQLException {
		HashMap<String, Nyquence> snapshot = Nyquence.clone(cx.synx.nv);

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
		return new ExchangeBlock(syndomx.domain, syndomx.synode, peer, xp.session(), xp.exstat())
				.nv(xp.synx.nv)
				.requirestore()
				.seq(xp);
	}
	
	/////////////////////////////////////////////////////////////////////////////////////////////
	public int deleteEntityBySynuid(SyndomContext syndomContext, SyntityMeta entm, String synuid)
			throws TransException, SQLException {
		AnResultset hittings = (AnResultset) select(entm.tbl)
					.col(entm.io_oz_synuid).cols(entm.uids)
					.whereEq(entm.io_oz_synuid, synuid)
					.rs(instancontxt(syndomx.synconn, locrobot))
					.rs(0);

		if (existsnyuid(entm, synuid)) {
			SemanticObject res = (SemanticObject) DBSynmantics
					.logChange(syndomContext, this, delete(entm.tbl, locrobot)
					.whereEq(entm.io_oz_synuid, synuid), entm, hittings)
					.d(instancontxt(syndomx.synconn, locrobot));
			return res.total();
		}
		else return 0;
	}

	private boolean existsnyuid(SyntityMeta entm, String suid)
			throws SQLException, TransException {
		return ((AnResultset) select(entm.tbl, "t")
			.col(Funcall.count(), "c")
			.rs(instancontxt(syndomx.synconn, locrobot))
			.rs(0))
			.nxt()
			.getInt("c") > 0;
	}

	/**
	 * NOTE: This method sholdn't be used other than tests.
	 * <p>FYI: There is no syn-change semantics, and don't
	 * configure syn-change semantics for the table.</p>
	 * 
	 * @param m
	 * @param e
	 * @param entitypk must be ignored if is inserting an entity of auto-key
	 * @return [entity-id, change-id]
	 * @throws TransException
	 * @throws SQLException
	 */
	public String[] insertEntity(SyndomContext sdx, SyntityMeta m, SynEntity e, Object... entitypk)
			throws TransException, SQLException {
		checkEntityRegistration(m, sdx.synconn);

		SyncUser rob = (SyncUser) locrobot;

		Insert inst = e.insertEntity(m, insert(m.tbl, rob));
		SemanticObject u = (SemanticObject) DBSynmantics
				.logChange(sdx, this, inst, m, syndomx.synode, entitypk)
				.ins(instancontxt());

		String phid = isblank(e.recId) ? u.resulve(m, -1) : e.recId;
		String chid = u.resulve(syndomx.chgm, -1);
		return new String[] {phid, chid};
	}
	
	/**
	 * NOTE: This method sholdn't be used other than tests.
	 * <p>FYI: There is no syn-change semantics, and don't
	 * configure syn-change semantics for the table.</p>
	 * 
	 * @param sdx
	 * @param synoder
	 * @param uids
	 * @param entm
	 * @param nvs
	 * @return
	 * @throws TransException
	 * @throws SQLException
	 * @throws IOException
	 */
	public String updateEntity(SyndomContext sdx, String synoder, String[] uids, SyntityMeta entm, Object ... nvs)
			throws TransException, SQLException, IOException {
		checkEntityRegistration(entm, sdx.synconn);

		List<String> updcols = new ArrayList<String>(nvs.length/2);
		for (int i = 0; i < nvs.length; i += 2)
			updcols.add((String) nvs[i]);

		Update u = update(entm.tbl, locrobot)
					.nvs((Object[])nvs);

		for (int ix = 0; ix < uids.length; ix++)
			u.whereEq(entm.uids.get(ix), uids[ix]);

		AnResultset hittings = ((AnResultset) select(entm.tbl)
				.col(entm.io_oz_synuid).cols(entm.uids)
				.where(u.where())
				.rs(instancontxt())
				.rs(0))
				.beforeFirst();

		return DBSynmantics
			.logChange(sdx, this, u,
					entm, synoder, hittings, updcols)
			.u(instancontxt(syndomx.synconn, locrobot))
			.resulve(syndomx.chgm.tbl, syndomx.chgm.pk, -1);
	}

	/**
	 * Should only reach here while testing
	 * @param entm
	 * @param synconn 
	 * @throws SemanticException no registered entity can be found
	 */
	static void checkEntityRegistration(SyntityMeta entm, String synconn) throws SemanticException {
		if (Syntities.get(synconn).meta(entm.tbl) == null)
			throw new SemanticException("No syntity registration is found for: %s [%s]", entm.tbl, synconn);
	}

	public void incN0(Nyquence... maxn) throws TransException, SQLException {
		syndomx.incN0(this, maxn);
	}
	
	/**
	 * Set n-stamp, then create a request package.
	 * @param app
	 * @param admin
	 * @return the request
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExchangeBlock domainSignup(ExessionPersist app, String admin)
			throws TransException, SQLException {
		try {
			return app.signup(admin);
		}
		finally { 
			syndomx.incStamp(app.trb);
		}
	}

	public ExchangeBlock domainOnAdd(ExessionPersist ap, ExchangeBlock req, String org)
			throws TransException, SQLException {
	
		syndomx.incStamp(ap.trb);
		syndomx.loadNvstamp(this);

		String synode = syndomx.synode;
		String domain = syndomx.domain;
		String synconn= syndomx.synconn;
		SynodeMeta synm = syndomx.synm;
		SynChangeMeta chgm = syndomx.chgm;
		SynSubsMeta subm = syndomx.subm;

		String childId = req.srcnode;
		IUser robot = locrobot;
	
		// 0.7.6: remarks == null, as there is only one, the first, is the Hub node.
		Synode apply = new Synode(childId, null, org, domain, null);

		req.synodes.beforeFirst().next();
		
		String syn_uids = req.synodes.getString(synm.io_oz_synuid);

		((SemanticObject) apply
			.insert(synm, syn_uids, ap.n0(), insert(synm.tbl, robot))
			.post(insert(chgm.tbl, robot)
				.nv(chgm.entbl, synm.tbl)
				.nv(chgm.crud, CRUD.C)
				.nv(chgm.synoder, synode)
				.nv(chgm.uids, syn_uids)

				// .nv(chgm.nyquence, ap.n0().n)
				.nv(chgm.nyquence, ap.stamp().n)

				.nv(chgm.seq, incSeq())
				.nv(chgm.domain, domain)
				.post(insert(subm.tbl) // TODO the tree mode is different here
					.cols(subm.insertCols())
					.select((Query)select(synm.tbl)
						.col(new Resulving(chgm.tbl, chgm.pk))
						.col(synm.synoder)
						.where(op.ne, synm.synoder, constr(synode))
						.where(op.ne, synm.synoder, constr(childId))
						.whereEq(synm.domain, domain))))
			.ins(instancontxt(synconn, robot)))
			.resulve(chgm.tbl, chgm.pk, -1);
		
		ap.synx.nv.put(apply.synid, new Nyquence(apply.nyquence));
	
		ExchangeBlock rep = new ExchangeBlock(domain, synode, childId, ap.session(), ap.exstate(setupDom))
			.nv(ap.synx.nv)
			.synodes(req.act == ExessionAct.signup
			? ((AnResultset) select(synm.tbl, "syn")
				.whereIn(synm.synoder, childId, synode)
				.whereEq(synm.domain, domain)
				.whereEq(synm.org, org)
				.rs(instancontxt(synconn, locrobot))
				.rs(0))
			: null);
		return rep;
	}

	public ExchangeBlock domainitMe(ExessionPersist cp, String admin, String adminserv,
			String domain, ExchangeBlock domainof) throws TransException, SQLException {
		
		String synode = syndomx.synode;
		String synconn= syndomx.synconn;
		SynodeMeta synm = syndomx.synm;

		if (!eq(syndomx.domain, domain))
			throw new ExchangeException(setupDom, cp,
					"Unexpected domain. me: %s, peer: %s", syndomx.domain, domain);

		Nyquence mxn = domainof.nv.get(admin); 

		if (domainof.synodes != null) {
			AnResultset ns = domainof.synodes.beforeFirst();

			while (ns.next()) {
				if (eq(synode, ns.getString(synm.pk))) {
					update(synm.tbl, locrobot)
					.nv(synm.domain, ns.getString(synm.domain))
					.whereEq(synm.pk, synode)
					.whereEq(synm.org, locrobot.orgId())
					.whereEq(synm.domain, domain)
					.u(instancontxt(synconn, locrobot));

					// don't delete: all local data before joining is ignored
					syndomx.domainitOnjoin(this, locrobot.orgId(), domain, mxn);
				}
				else {
					Synode n = new Synode(ns, synm);
					mxn = maxn(domainof.nv);
					n.insert(synm, ns.getString(synm.io_oz_synuid), mxn, insert(synm.tbl, locrobot))
					 .nv(synm.jserv, adminserv)
					 .ins(instancontxt(synconn, locrobot));

					cp.synx.persistNyquence(this, n.synid, mxn);
				}
			}
		}

		return new ExchangeBlock(domain, synode, admin, domainof.session,
				new ExessionAct(ExessionAct.mode_client, setupDom))
				.nv(cp.synx.nv);
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
		if (isblank(syndomx.domain))
			throw new ExchangeException(ExessionAct.ready, cp, "domain is empty when closing domain joining?");

		updateFieldByPk(this, syndomx.synconn, syndomx.synm,
				syndomx.synode, syndomx.synm.domain, syndomx.domain, locrobot);

		syndomx.persistamp(this, maxn(rep.nv.get(cp.peer), syndomx.stamp, cp.n0()));

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
			String synode = syndomx.synode;
			String synconn= syndomx.synconn;
			SynChangeMeta chgm = syndomx.chgm;
			SynSubsMeta subm = syndomx.subm;
			PeersMeta pnvm = syndomx.pnvm;

			Query qstales = null;
			try {
				// 2025-01-14
				qstales = select(chgm.tbl, "ch")
						.je_(subm.tbl, "sb", chgm.pk, subm.changeId)
						.je_(pnvm.tbl, "nv", "nv." + pnvm.peer, constr(peer), "sb." + subm.synodee, pnvm.synid)
						.where(op.le, Nyquence.sqlCompare("ch", chgm.nyquence, "nv", pnvm.nyq), 0);
				if (debug) {
				Utils.logT(new Object() {},
						"Cleaning changes' subscriptions that won't be accepted in session %s -> %s::",
						synode, peer);
				
			
					((AnResultset) qstales.rs(instancontxt()).rs(0)).print();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				SemanticObject res = (SemanticObject) delete(subm.tbl, locrobot)
						
					// 2025-01-14
					.where(op.in, subm.changeId, qstales.col(chgm.pk))

					.whereEq(subm.synodee, peer)
					.post(del0subchange(peer))
					.d(instancontxt(synconn, locrobot));

				@SuppressWarnings("unchecked")
				int cnt = ((ArrayList<Integer>)res.get("total")).get(0);
				if (cnt > 0 && debug) {
					Utils.warnT(new Object() {} ,
						"Cleaned changes in %s -> %s: %s changes",
						synode, peer, cnt);
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
	 * @param iffnode delete the change-log iff the node, i.e. the subscriber, exists.
	 * For answers, it's the node himself, for challenge, it's the source node.
	 * @return the delete statement
	 * @throws TransException
	 */
	protected Statement<?> del0subchange(String iffnode)
				throws TransException {
		SynChangeMeta chgm = syndomx.chgm;
		SynSubsMeta subm = syndomx.subm;

		return delete(chgm.tbl)
			.whereEq(chgm.domain, syndomx.domain)
			.whereEq("0", (Query)select(subm.tbl)
				.col(count(subm.synodee))
				.where(op.eq, chgm.pk, subm.changeId))
			;
	}

	/**
	 * Check and extend column {@link ExchangeBlock#ExchangeFlagCol},
	 * which is for changing flag of change-logs.
	 * 
	 * @param colnames
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
		return DAHelper.count(this, syndomx.synconn, m.tbl);
	}

	public AnResultset entitySynuids(SyntityMeta m) throws SQLException, TransException {
		return (AnResultset) select(m.tbl).col(m.io_oz_synuid).orderby(m.io_oz_synuid).rs(basictx).rs(0);
	}

	ExessionPersist xp;
	public DBSyntableBuilder xp(ExessionPersist exessionPersist) {
		this.xp = exessionPersist;
		return this;
	}

	public ISemantext instancontxt() throws TransException {
		return instancontxt(syndomx.synconn, locrobot);
	}
	
	/**
	 * %VOLUME/folder/sub.../id name.ext -> 
	 * volume/folder/sub.../id name.ext 
	 * 
	 * @return the absolute file path
	 * @throws TransException 
	 */
	public String decodeExtfile(String extfile) throws TransException {
		// return ExtFilePaths.decodeUri(instancontxt().containerRoot(), extfile);
		return ExtFilePaths.decodeUriPath(extfile);
	}

	/**
	 * Create a semantic context, without semantics handlers.
	 * @return
	 * @throws SQLException 
	 * @throws SemanticException 
	 */
	public ISemantext nonsemantext() throws SemanticException, SQLException {
		return DATranscxt.instanonSemantext(syndomx.synconn);
	}

	boolean dbgStack;
	public DBSyntableBuilder pushDebug(boolean dbg) {
		this.dbgStack = debug;
		debug = dbg;
		Connects.setDebug(syndomx.synconn, dbg);
		return this;
	}

	public DBSyntableBuilder popDebug() {
		Connects.setDebug(syndomx.synconn, dbgStack);
		debug = dbgStack;
		return this;
	}
}
