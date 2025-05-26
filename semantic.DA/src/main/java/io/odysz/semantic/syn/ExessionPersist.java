package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.semantic.syn.ExessionAct.close;
import static io.odysz.semantic.syn.ExessionAct.exchange;
import static io.odysz.semantic.syn.ExessionAct.init;
import static io.odysz.semantic.syn.ExessionAct.mode_client;
import static io.odysz.semantic.syn.ExessionAct.mode_server;
import static io.odysz.semantic.syn.ExessionAct.ready;
import static io.odysz.semantic.syn.ExessionAct.restore;
import static io.odysz.semantic.syn.ExessionAct.signup;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.getn;
import static io.odysz.transact.sql.parts.condition.ExprPart.constVal;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import io.odysz.common.CheapMath;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSessionMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.QueryPage;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**
 * Persisting exchange session with remote node, using temporary tables.
 * This is a session context, and different from {@link DBSynmantext} which
 * is used for handling local data integration and database semantics. 
 * 
 * @author Ody
 */
public class ExessionPersist {
	final SyndomContext synx;
	final SynChangeMeta chgm;
	final SynSubsMeta subm;
	final SynchangeBuffMeta exbm;
	final SynodeMeta synm;
	final SynSessionMeta sysm;
	final PeersMeta pnvm;

	final String peer;
	public String peer() { return peer; }

	final boolean debug;

	AnResultset answerPage;

	public DBSyntableBuilder trb;

	/**
	 * Set nswers to the challenges, page {@link #challengeSeq}, with entities.
	 * 
	 * @param answer
	 * @param cols
	 * @return this
	 */
	ExessionPersist saveAnswer(AnResultset answer) {
		answerPage = answer;
		return this;
	}

	ExessionPersist commitAnswers(ExchangeBlock conf, String srcnode, long tillN0_delete)
			throws SQLException, TransException {
		
		if (conf == null || conf.anspage == null || conf.anspage.getRowCount() <= 0)
			return this;
	
		List<Statement<?>> stats = new ArrayList<Statement<?>>();
	
		AnResultset rply = conf.anspage.beforeFirst();
		rply.beforeFirst();
		String recId = null;
		while (rply.next()) {
//			if (compareNyq(rply.getLong(chgm.nyquence), tillN0) > 0)
//				break; // FIXME continue? Or throw?
	
			SyntityMeta entm = DBSynTransBuilder.getEntityMeta(synx.synconn, rply.getString(chgm.entbl));

			String change = rply.getString(ChangeLogs.ChangeFlag);
			HashMap<String, AnResultset> entbuf = chEntities;
			
			String rporg  = rply.getString(chgm.domain);
			String rpent  = rply.getString(chgm.entbl);
			String rsynuid= rply.getString(chgm.uids);
			String rpnodr = rply.getString(chgm.synoder);
			String rpscrb = rply.getString(subm.synodee);
			String rpchid = rply.getString(chgm.pk);
	
			if (debug && !eq(change, CRUD.D)
				&& (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).rowIndex0(rsynuid) < 0)) {
				Utils.warnT(new Object() {},
						"Missing entity. This happens when the entity is deleted locally.\n" +
						"entity name: %s\tsynode(peer): %s\tsynode(local): %s\tentity id(by peer): %s",
						entm.tbl, srcnode, synx.synode, rsynuid);
				continue;
			}
				
			stats.add(eq(change, CRUD.C)
				// create an entity, and trigger change log
				? !eq(recId, rsynuid)
					? // TODO FIXME a branch that tests never reached?
					  trb.insert(entm.tbl, trb.synrobot())
						.cols(null) // (String[])entm.entCols())
						.value(entm.insertChallengeEnt(rsynuid, entbuf.get(entm.tbl)))
						.post(trb.insert(chgm.tbl)
							.nv(chgm.pk, rpchid)
							.nv(chgm.crud, CRUD.C)
							.nv(chgm.domain, rporg)
							.nv(chgm.entbl, rpent)
							.nv(chgm.synoder, rpnodr)
							.nv(chgm.uids, rsynuid)
							.post(trb.insert(subm.tbl)
								.cols(subm.insertCols())
								.value(subm.insertSubVal(rply, new Resulving(chgm.tbl, chgm.pk)))))
					: trb.insert(subm.tbl)
						.cols(subm.insertCols())
						.value(subm.insertSubVal(rply, new Resulving(chgm.tbl, chgm.pk)))

	
				// remove subscribers & backward change logs's deletion propagation
				: trb.delete(subm.tbl, trb.synrobot())
					.whereEq(subm.changeId, rpchid)
					.whereEq(subm.synodee, rpscrb)
					.post(del0subchange(entm, rporg, rpnodr, rsynuid, rpchid, rpscrb)
					));
			recId = rsynuid;
		}
	
		Utils.logT(new Object() {}, "Locally committing answers to %s ...", peer);
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			s.commit(sqls, trb.synrobot());
		Connects.commit(synx.synconn, trb.synrobot(), sqls);
		
		return this;
	}

	/**
	 * Save local changes according to challenges by the peer.
	 * @param changes 
	 * @param cols [name, [NAME, index]]
	 * @param srcnv 
	 * @param entites 
	 * @return this
	 * @throws TransException 
	 * @throws SQLException 
	 */
	ExessionPersist saveChanges(AnResultset changes, HashMap<String, Nyquence> srcnv,
			HashMap<String, AnResultset> ents) throws SQLException, TransException {

		List<Statement<?>> stats = new ArrayList<Statement<?>>();

		changes.beforeFirst();

		HashSet<String> missings = new HashSet<String>(synx.nv.keySet());
		missings.removeAll(srcnv.keySet());
		missings.remove(synx.synode);

		if (debug)
			Utils.logT(new Object() {}, "\n[%1$s <- %2$s] : %1$s saving changes to local entities...", synx.synode, peer);

		while (changes.next()) {
			String change = changes.getString(chgm.crud);
			Nyquence chgnyq = getn(changes, chgm.nyquence);

			SyntityMeta entm = DBSynTransBuilder.getEntityMeta(synx.synconn, changes.getString(chgm.entbl));

			String synodr = changes.getString(chgm.synoder);
			String chuids = changes.getString(chgm.uids);
			// String chgid  = changes.getString(chgm.pk);

			// HashMap<String, AnResultset> entbuf = entites; // x.onchanges.entities;
			if (!eq(change, CRUD.D)
				&& (ents == null || !ents.containsKey(entm.tbl) || ents.get(entm.tbl).rowIndex0(chuids) < 0)) {
				Utils.warnT(new Object() {},
						"Missing entity. This happens when the peer has updated then deletd the entity.\n" +
						"entity name: %s\tsynode(answering): %s\tsynode(local): %s\tentity uid(by challenge): %s",
						entm.tbl, peer, synx.synode, chuids);
				continue;
			}
			
			String domain = changes.getString(chgm.domain);
			String chentbl = changes.getString(chgm.entbl);
			
			// current entity's subscribes
			ArrayList<Statement<?>> subscribeUC = new ArrayList<Statement<?>>();

			boolean iamSynodee = false;

			while (changes.validx()) {
				String subsrb = changes.getString(subm.synodee);
				if (!synx.nv.containsKey(synodr))
					Utils.warn("This node (%s) don't care changes from %s, and sholdn't be here.",
							synx.synode, synodr);

				if (compareNyq(chgnyq, synx.nv.get(synodr)) > 0
					&& eq(subsrb, synx.synode))
					iamSynodee = true;

				else if (compareNyq(chgnyq, synx.nv.get(synodr)) > 0
					&& !eq(subsrb, synx.synode))
					subscribeUC.add(trb.insert(subm.tbl)
						.cols(subm.insertCols())
						// .value(subm.insertSubVal(changes))); 
						.value(subm.insertSubVal(changes, new Resulving(chgm.tbl, chgm.pk)))); 
				
				if (ofLastEntity(changes, chuids, chentbl, domain))
					break;
				changes.next();
			}

			appendMissings(stats, missings, changes);
			
			Insert chlog = subscribeUC.size() <= 0 ? null : trb
					.insert(chgm.tbl)
					// .nv(chgm.pk, chgid)
					.nv(chgm.domain, domain)
					.nv(chgm.crud, change).nv(chgm.updcols, changes.getString(chgm.updcols))
					.nv(chgm.entbl, chentbl).nv(chgm.synoder, synodr)
					.nv(chgm.nyquence, changes.getLong(chgm.nyquence))
					.nv(chgm.seq, trb.incSeq()).nv(chgm.uids, chuids)
					.post(subscribeUC)
					// .post(del0subchange(entm, domain, synodr, chuids, chgid, synx.synode))
					;

			if (iamSynodee || subscribeUC.size() > 0) {
				stats.add(
					eq(change, CRUD.C)
					? eq(entm.tbl, synm.tbl)
						&& eq(ents.get(entm.tbl).getStringByIndex(synm.synoder, chuids), synx.synode)
						? null // ignore myself
						: trb.insert(entm.tbl, trb.synrobot())
							.cols(ents.get(entm.tbl).getFlatColumns0())
							.row(ents.get(entm.tbl).getColnames(),
									ents.get(entm.tbl).getRowById(chuids))
							.post(chlog)

					: eq(change, CRUD.U)
					? trb.update(entm.tbl, trb.synrobot())
						.nvs(entm.updateEntNvs(chgm, chuids, ents.get(entm.tbl), changes))
						.whereEq(entm.io_oz_synuid, chuids)
						.post(chlog)

					: eq(change, CRUD.D)
					? trb.delete(entm.tbl, trb.synrobot())
						.whereEq(entm.io_oz_synuid, chuids)
						.post(chlog)

					: null);

				subscribeUC = new ArrayList<Statement<?>>();
				iamSynodee  = false;
				chlog = null;
			}
		}

		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			if (s != null)
				s.commit(sqls, trb.synrobot());
		Connects.commit(synx.synconn, trb.synrobot(), sqls);
		
		return this;
	}

	/**
	 * Create context at client side.
	 * @param tb 
	 * @param target
	 */
	public ExessionPersist(DBSyntableBuilder tb, String target) {
		this(tb, target, null);
	}

	/**
	 * Create context at server side.
	 * @param tb 
	 */
	public  ExessionPersist(DBSyntableBuilder tb, String peer, ExchangeBlock ini) {

		if (tb != null && eq(tb.syndomx.synode, peer))
			Utils.warn("Creating persisting context for local builder, i.e. peer(%s) = this.synode?", peer);;

		this.synx = tb.syndomx;
		this.trb = tb.xp(this);
		this.exbm = synx.exbm;
		this.session = ini == null ? null : ini.session;
		this.peer = peer;
		this.chgm = synx.chgm;
		this.subm = synx.subm;
		this.synm = synx.synm;
		this.sysm = new SynSessionMeta(synx.synconn);
		this.pnvm = synx.pnvm;
		this.exstate = new ExessionAct(mode_server, ready);
		// this.chsize = 480;
		this.chsize = tb.syndomx.pageSize;

		debug = trb == null ? true : Connects.getDebug(synx.synconn);
	}

	/**
	 * Create a signing up request.
	 * @param admin
	 * @return the request
	 * @throws SQLException
	 * @throws TransException
	 */
	public ExchangeBlock signup(String admin) throws SQLException, TransException {
		synx.loadNvstamp(trb);
		exstate.state = signup;
	
		return new ExchangeBlock(synx.domain,
					trb == null ? null : synx.synode,
					peer, session, exstate)
//				.totalChallenges(1)
//				.chpagesize(this.chsize)
				.totalChallenges(1, this.chsize)
				.synodes(DAHelper.getEntityById(trb, synm, synx.synode))
				.seq(this);
	}

	/**
	 * Setup exchange buffer table.
	 * <pre>
	 * exstate.state = init;
	 * expAnswerSeq = 0;
	 * challengeSeq = 0;
	 * answerSeq = 0;
	 * </pre>
	 * @throws TransException 
	 * @throws SQLException 
	 */
	public ExchangeBlock init() throws TransException, SQLException {
		if (trb != null) {
			synx.loadNvstamp(trb);
			Nyquence dn = synx.nv.get(peer);

			if (dn == null) {
				throw new ExchangeException(ready, this,
					"%1$s.init(): %1$s doesn't have knowledge about %2$s.",
					synx.synode, peer);
			}
		}
		else 
			Utils.warnT(new Object() {}, "Null transaction builder. - null builder only for test");
		
		challengeSeq = -1;
		expAnswerSeq = -1; //challengeSeq;
		answerSeq = -1;

		if (trb != null)
			totalChallenges = DAHelper.count(trb, synx.synconn, exbm.tbl, exbm.peer, peer);
		
		exstate = new ExessionAct(mode_client, init);

		return new ExchangeBlock(synx.domain,
				trb == null ? null : synx.synode,
				peer, session, exstate)
			.totalChallenges(totalChallenges, this.chsize)
			// .chpagesize(this.chsize)
			.seq(persistarting(peer))
			.nv(synx.nv);
	}

	/**
	 * insert into exchanges select * from change_logs where n > nyquvect[sx.peer].n
	 * 
	 * @param ini
	 * @return new message
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExchangeBlock onInit(ExchangeBlock ini) throws TransException, SQLException {
		if (trb != null) {
			synx.loadNvstamp(trb);

			trb.pushDebug(true);
			int total = ((SemanticObject) trb
				.insertExbuf(peer)
				.ins(trb.instancontxt())
				).total();
			trb.popDebug();

			if (total > 0 && debug) {
				Utils.logi("Changes in buffer for %s -> %s: %s",
					synx.synode, peer, total);
			}
		}
		else 
			Utils.warnT(new Object() {}, "Null transaction builder. - null builder only for test");
		
		challengeSeq = -1;
		expAnswerSeq = ini.answerSeq;
		answerSeq = ini.challengeSeq;
	
		if (trb != null) 
			totalChallenges = DAHelper.count(trb, synx.synconn, exbm.tbl, exbm.peer, peer);
		chsize = ini.chpagesize > 0 ? ini.chpagesize : -1;

		exstate = new ExessionAct(mode_server, init);

		return new ExchangeBlock(synx.domain,
					trb == null ? ini.peer : synx.synode,
					peer, session, exstate)
				// .totalChallenges(totalChallenges)
				// .chpagesize(ini.chpagesize)
				.totalChallenges(totalChallenges, ini.chpagesize)
				.seq(persistarting(peer))
				.nv(synx.nv);
	}

	public void clear() { }

	private String session;
	public String session() { return session; }

	private ExessionAct exstate;
	public int exstate() { return exstate.state; }
	public ExessionPersist exstate(int state) {
		exstate.state = state;
		return this;
	}
	public ExessionAct exstat() { return exstate; }

	/**
	 * Counted when in {@link #init()}, and not correct after
	 * {@link DBSyntableBuilder#cleanStale(HashMap, String)} has been called.
	 */
	public int totalChallenges;

	public int expAnswerSeq;
	/** Challenging sequence number, i. e. current page */
	public int challengeSeq;
	/** challenge page size */
	protected int chsize;

	public int answerSeq;

	/**
	 * Has another page in {@link SynchangeBuffMeta}.tbl to be send to.
	 * @param b synchronizing transaction builder
	 * @return true if has another page
	 * @throws SQLException
	 * @throws TransException
	 */
	public boolean hasNextChpages(DBSyntableBuilder b)
			throws SQLException, TransException {
		int pages = pages();
		if (pages > 0 && challengeSeq + 1 < pages)
			return true;
		else
			return false;
	}

	public ExessionPersist expect(ExchangeBlock req) throws ExchangeException {
		if (req == null)
			return this;

		if (!eq(req.srcnode, this.peer) || !eq(session, req.session))
			throw new ExchangeException(ExessionAct.unexpect, this,
					"Session Id or peer mismatched [%s : %s vs %s : %s]",
					this.peer, session, req.srcnode, req.session);
		
		if (req.challengeSeq >= 0 && (req.act != restore && answerSeq + 1 != req.challengeSeq))
			throw new ExchangeException(ExessionAct.unexpect, this,
					"Challenge page lost, expecting %s",
					answerSeq + 1);
		
		if (req.act == restore
			|| expAnswerSeq == 0 && req.answerSeq == -1 // first exchange
			|| expAnswerSeq == req.answerSeq) {
			answerSeq = req.challengeSeq;
			return this;
		}

		throw new ExchangeException(ExessionAct.unexpect, this,
			"for challenge %s, got answer %s, expecting %s",
			req.challengeSeq, req.answerSeq, expAnswerSeq);
	}
	
	public ExchangeBlock nextExchange(ExchangeBlock rep)
			throws SQLException, TransException {

		ExessionPersist me = expect(rep); //.exstate(exchange);
		me.exstate(nextChpage() ? exchange : close);

		return trb == null // null for test
			? new ExchangeBlock(synx.domain, rep.peer, peer, session, me.exstate).seq(this)
			: trb.exchangePage(this, rep);
	}

	private boolean nextChpage() throws TransException, SQLException {
		int pages = pages();
		if (challengeSeq < pages) {
			challengeSeq++;
		
			if (trb != null) {
				// select ch.*, synodee from changelogs ch join syn_subscribes limit 100 * i, 100
				QueryPage page = (QueryPage) trb
					.selectPage(trb
						.select(exbm.tbl, "bf")
						.col(exbm.pagex, "page")
						.cols_byAlias("bf", exbm.changeId)
						.col("sub." + subm.synodee)
						.je_(chgm.tbl, "chg", exbm.changeId, chgm.pk, exbm.peer, Funcall.constr(peer))
						.je_(subm.tbl, "sub", "chg." + chgm.pk, subm.changeId)
						.whereEq(exbm.peer, peer))
					.page(challengeSeq, chsize)
					.col(exbm.changeId);

				trb.pushDebug(true);
				trb.update(exbm.tbl, trb.synrobot())
					.nv(exbm.pagex, challengeSeq)
					.whereIn(exbm.changeId, page)
					.u(trb.instancontxt(synx.synconn, trb.synrobot()))
					;
				trb.popDebug();
			}
		}

		expAnswerSeq = challengeSeq < pages ? challengeSeq : -1;
		return challengeSeq < pages;
	}

	/**
	 * Reset to last page
	 * @return this
	 */
	ExessionPersist pageback() {
		if (challengeSeq < 0)
			return this;

		challengeSeq--;
		expAnswerSeq = challengeSeq;
		return this;
	}

	ExchangeBlock exchange(String peer, ExchangeBlock rep)
			throws TransException, SQLException {
		/*
		if (exstate.state == restore && rep.act == restore)
			; // got answer
		else if (exstate.state != init && exstate.state != exchange)
			throw new ExchangeException(exchange, this,
				"Can't handle exchanging states from %s to %s.",
				ExessionAct.nameOf(exstate.state), ExessionAct.nameOf(exchange)); 
		*/

		if (rep != null)
			answerSeq = rep.challengeSeq;
		expAnswerSeq = challengeSeq < pages() ? challengeSeq : -1; 

		AnResultset rs = chpage();

		// exstate.state = exchange;

		return new ExchangeBlock(synx.domain,
					trb == null ? rep.peer : synx.synode,
					peer, session, exstate)
				.chpage(rs, chEntities)
				// .totalChallenges(totalChallenges)
				// .chpagesize(this.chsize)
				.totalChallenges(totalChallenges, this.chsize)
				.seq(this)
				.nv(synx.nv);
	}

	ExchangeBlock onExchange(String peer, ExchangeBlock req)
			throws TransException, SQLException {

		if (req != null)
			answerSeq = req.challengeSeq;
		expAnswerSeq = challengeSeq < pages() ? challengeSeq : -1; 

		exstate.state = exchange;

		return new ExchangeBlock(synx.domain,
					trb == null ? req.peer
					: synx.synode, peer, session, exstate)
				.chpage(chpage(), chEntities)
				// .totalChallenges(totalChallenges)
				// .chpagesize(this.chsize)
				.totalChallenges(totalChallenges, this.chsize)
				.seq(this)
				.nv(synx.nv);
	}

	public ExchangeBlock closexchange(ExchangeBlock rep) throws ExchangeException {
		/* enable?
		if (exstate.state != init && exstate.state != exchange)
			throw new ExchangeException(exchange, this,
					"Can't handle closing state on state %s", exstate.state); 
		*/

		try {
			expAnswerSeq = -1; 
			if (rep != null)
				answerSeq = rep.challengeSeq;
			else answerSeq = -1;
			challengeSeq = -1; 

			exstate.state = ready;

			return new ExchangeBlock(synx.domain, 
						trb == null ? rep.peer
						: synx.synode, peer, session, new ExessionAct(exstate.exmode, close))
					// .totalChallenges(totalChallenges)
					// .chpagesize(this.chsize)
					.totalChallenges(totalChallenges, this.chsize)
					.seq(this);
		} finally {
			if (trb != null)
			try {
				trb.delete(exbm.tbl, trb.synrobot())
					.whereEq(exbm.peer, peer)
					.d(trb.instancontxt());
			} catch (TransException | SQLException e) {
				e.printStackTrace();
			}
			finally {
				// TODO cleared too many here
				trb.cleanStaleSubs(peer);
			}
		}
	}
	
	public ExchangeBlock abortExchange() {
		try {
			expAnswerSeq = -1; 
			answerSeq = -1;
			challengeSeq = -1; 
			totalChallenges = 0;

			exstate.state = ready;

			return new ExchangeBlock(synx.domain,
						trb == null ? null : synx.synode,
						peer, session, new ExessionAct(exstate.exmode, close))
//					.totalChallenges(totalChallenges)
//					.chpagesize(this.chsize)
					.totalChallenges(totalChallenges, this.chsize)
					.seq(this);
		} finally {
			try {
				trb.delete(exbm.tbl, trb.synrobot())
					.whereEq(exbm.peer, peer)
					.d(trb.instancontxt(synx.synconn, trb.synrobot()));
			} catch (TransException | SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Retry last page
	 * @param peer
	 * @return request message
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public ExchangeBlock retryLast(String peer) throws TransException, SQLException {

		pageback();
		nextChpage();
		exstate.state = restore;

		return new ExchangeBlock(synx.domain,
					trb == null ? null : synx.synode,
					peer, session, exstate)
				.requirestore()
//				.totalChallenges(totalChallenges)
//				.chpagesize(this.chsize)
				.totalChallenges(totalChallenges, this.chsize)
				.seq(this);
	}

	/**Challenging Entities */
	HashMap<String, AnResultset> chEntities;

	/**
	 * Information used by upper level, such as semantic.jser.
	 */
	public String[] ssinf;

	public Nyquence n0() { return synx.nv.get(synx.synode); }

	/**
	 * <p>Get a challenging page.</p>
	 * 
	 * Entities affected are also be loaded, and the entity metas get a chance to handling
	 * the loaded records by calling {@link SyntityMeta#onselectSyntities(Query)}.
	 * 
	 * @return a change log page, with entities saved in {@link #chEntities}.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	AnResultset chpage() throws TransException, SQLException {
		// 
		if (trb == null) return null; // test

		AnResultset entbls = (AnResultset) trb.select(chgm.tbl, "ch")
				.je_(exbm.tbl, "bf", chgm.pk, exbm.changeId, "bf." + exbm.peer, constr(peer), constVal(challengeSeq), exbm.pagex)
				.col(chgm.entbl)
				.groupby(chgm.entbl)
				.rs(trb.instancontxt(synx.synconn, trb.synrobot()))
				.rs(0);

		if (chEntities != null)
			chEntities.clear();

		while (entbls.next()) {
			String tbl = entbls.getString(chgm.entbl);

			// SyntityMeta entm = eq(tbl, synm.tbl) ? synm : Syntities.get(synx.synconn).meta(tbl);
			SyntityMeta entm = DBSynTransBuilder.getEntityMeta(synx.synconn, tbl);

			// FIXME duplicates of entity records here
			boolean dbg = Connects.getDebug(synx.synconn);
			Connects.setDebug(synx.synconn, true);

			AnResultset entities = ((AnResultset) entm
				// .onselectSyntities(trb.select(tbl, "e").cols_byAlias("e", entm.entCols()))
				.onselectSyntities(synx, trb.select(tbl, "e").distinct(true).cols("e.*"))
				.je_(chgm.tbl, "ch", "ch." + chgm.entbl, constr(tbl), entm.io_oz_synuid, chgm.uids)
				.je_(exbm.tbl, "bf", "ch." + chgm.pk, exbm.changeId,
					 constr(peer), exbm.peer, constVal(challengeSeq), exbm.pagex)
				.rs(trb.instancontxt(synx.synconn, trb.synrobot()))
				.rs(0))
				.index0(entm.io_oz_synuid);
			Connects.setDebug(synx.synconn, dbg);
			
			entities(tbl, entities);
		}
			
		return trb == null ? null : (AnResultset)trb
			.select(chgm.tbl, "ch")
			.cols(exbm.pagex, "ch.*", "sb." + subm.synodee)
			.je_(exbm.tbl, "bf", chgm.pk, exbm.changeId,
				 constr(peer), exbm.peer, constVal(challengeSeq), exbm.pagex)
			.je_(subm.tbl, "sb", chgm.pk, subm.changeId)
			.orderby(chgm.synoder)
			.orderby(chgm.entbl)
			.orderby(chgm.seq)
			.rs(trb.instancontxt())
			.rs(0);
	}

	public ExessionPersist entities(String tbl, AnResultset ents) {
		if (chEntities == null)
			chEntities = new HashMap<String, AnResultset>();
		chEntities.put(tbl, ents);
		return this;
	}

	public int pages() {
		return CheapMath.blocks(totalChallenges, chsize);
	}

	/**
	 * update syn_node set session = {session, challengeSeq, answers, ...}
	 * @return this
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public ExessionPersist persisession() throws TransException, SQLException {
		if (trb != null) {
			sysm.update(trb.update(sysm.tbl, trb.synrobot()))
				.nv(sysm.chpage, challengeSeq)
				.nv(sysm.answerx, answerSeq)
				.nv(sysm.expansx, expAnswerSeq)
				.nv(sysm.mode,  exstate.exmode)
				.nv(sysm.state, exstate.state)
				.whereEq(sysm.peer, peer)
				.u(trb.instancontxt(synx.synconn, trb.synrobot()));
		}
		return this;
	}

	/**
	 * Save starting session.
	 * @param peer
	 * @return this
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExessionPersist persistarting(String peer) throws TransException, SQLException {
		if (trb != null) {
			trb.delete(sysm.tbl, trb.synrobot())
				.whereEq(sysm.peer, peer)
				.post(sysm.insertSession(trb.insert(sysm.tbl), peer))
				.d(trb.instancontxt(synx.synconn, trb.synrobot()));
		}
		return this;
	}

	///////////////////////////////////////////////////////////////////////////////////
	ExessionPersist forcetest(int total, int... chsize) {
		totalChallenges = total;
		if (!isNull(chsize))
			this.chsize = chsize[0];
		return this;
	}
	
	/**
	 * Append inserting statement for missing subscribes, for the knowledge
	 * that the source node dosen't know.
	 * 
	 * @param stats
	 * @param missing missing knowledge of synodees that the source node doesn't know
	 * @param chlog
	 * @return {@code missing }
	 * @throws TransException
	 * @throws SQLException
	 */
	HashSet<String> appendMissings(List<Statement<?>> stats, HashSet<String> missing, AnResultset chlog)
			throws TransException, SQLException {
		if(missing != null && missing.size() > 0
			&& eq(chlog.getString(chgm.crud), CRUD.C)) {
			String domain = chlog.getString(chgm.domain);
			String entbl  = chlog.getString(chgm.entbl);
			String uids   = chlog.getString(chgm.uids);

			for (String sub : missing) 
				stats.add(trb.insert(subm.tbl)
					.cols(subm.insertCols())
					.value(subm.insertSubVal(domain, entbl, sub, uids)));
		}
		return missing;
	}

	/**
	 * Is next row in {@code chlogs} a change log for different entity?
	 * @param chlogs
	 * @param curEid
	 * @param curEntbl
	 * @param curDomain
	 * @return true if next row is new enitity's change log.
	 * @throws SQLException 
	 */
	boolean ofLastEntity(AnResultset chlogs, String curEid, String curEntbl, String curDomain)
			throws SQLException {
		return !chlogs.hasnext()
			|| !eq(curEid, chlogs.nextString(chgm.uids))
			|| !eq(curEntbl, chlogs.nextString(chgm.entbl))
			|| !eq(curDomain, chlogs.nextString(chgm.domain));
	}
	
	boolean isAnotherEntity (AnResultset chlogs, String curUid, String curEntbl, String curDomain)
			throws SQLException {
		return !chlogs.hasprev()
			|| !eq(curUid, chlogs.prevString(chgm.uids))
			|| !eq(curEntbl, chlogs.prevString(chgm.entbl))
			|| !eq(curDomain, chlogs.prevString(chgm.domain));
	}

	/**
	 * Generate delete statement when change logs don't have synodees. 
	 * 
	 * @param entitymeta
	 * @param org
	 * @param synoder
	 * @param uids
	 * @param deliffnode delete the change-log iff the node, i.e. only the subscriber, exists.
	 * For answers, it's the node himself, for challenge, it's the source node.
	 * @return the delete statement
	 * @throws TransException
	 */
	Statement<?> del0subchange(SyntityMeta entitymeta,
			String org, String synoder, String uids, String changeId, String deliffnode)
				throws TransException {
		return trb.delete(chgm.tbl) // delete change log if no subscribers exist
			.whereEq(chgm.pk, changeId)
			.whereEq(chgm.entbl, entitymeta.tbl)
			.whereEq(chgm.domain, org)
			.whereEq(chgm.synoder, synoder)
			.whereEq("0", (Query)trb.select(subm.tbl)
				.col(count(subm.synodee))
				.whereEq(chgm.pk, changeId)
				.where(op.eq, chgm.pk, subm.changeId)
				.where(op.ne, subm.synodee, constr(deliffnode)))
			;
	}

	public Nyquence stamp() {
		return synx.stamp;
	}
}
