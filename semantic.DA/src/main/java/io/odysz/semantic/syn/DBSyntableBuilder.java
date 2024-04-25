package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.semantic.syn.Exchanging.confirming;
import static io.odysz.semantic.syn.Exchanging.init;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.getn;
import static io.odysz.semantic.syn.Nyquence.maxn;
import static io.odysz.semantic.util.DAHelper.loadRecNyquence;
import static io.odysz.semantic.util.DAHelper.loadRecString;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.concatstr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.xml.sax.SAXException;

import io.odysz.anson.x.AnsonException;
import io.odysz.common.Utils;
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
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.ExchangeException;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.Sql;
import io.odysz.transact.sql.parts.condition.Condit;
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
	protected SynExhangeBuffMeta exbm;

	protected String synode() { return ((DBSyntext)this.basictx).synode; }

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
	
	public DBSyntableBuilder(String conn, String synodeId,
			SynChangeMeta chgm, SynodeMeta synm)
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
	
	/**
	 * insert into exchanges select * from change_logs where n > nyquvect[target].n
	 * 
	 * @param cx
	 * @param target
	 * @param nv
	 * @return {total: change-logs to be exchanged} 
	 * @throws TransException
	 * @throws SQLException
	 */
	public ExchangeBlock initExchange(ExessionPersisting cx, String target,
			HashMap<String, Nyquence> nv) throws TransException, SQLException {
		if (DAHelper.count(this, basictx().connId(), exbm.tbl, exbm.peer, target) > 0)
			throw new ExchangeException(Exchanging.ready, "Can't initate new exchange session. There are exchanging records to be finished.");

		return new ExchangeBlock(synode(), nyquvect);
	}

	public void abortExchange(ExessionPersisting cx, String target, Object object) {
	}
	
	public ExchangeBlock onInit(ExessionPersisting sx, ExchangeBlock inireq) {
		return null;
	}

	public Object nextblock() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public ExchangeBlock exchangeBlock(ExessionPersisting cx, String synode, Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	public ExchangeBlock confirm(ExessionPersisting cx, ExchangeBlock rep, String synode,
			HashMap<String, Nyquence> nyquvect2) {
		return null;
	}
	
	public ExchangeBlock onExchange(ExessionPersisting sx, String synode,
			HashMap<String, Nyquence> nyquvect2, ExchangeBlock req) throws ExchangeException {
		return null;
	}

	public HashMap<String, Nyquence> closexchange(ExessionPersisting cx, String synode,
			HashMap<String, Nyquence> clone) {
		return null;
	}

	public void onclosexchange(ExessionPersisting sx, String synode, HashMap<String, Nyquence> nv) {
	}
	
	public void onRequires(ExessionPersisting cx, ExchangeBlock rep) { }
	
	public ExchangeBlock requirestore(ExessionPersisting sx, String peer, HashMap<String,Nyquence> mynv) {
		return null;
	}

	/**
	 * Client found unfinished exchange session and retry it.
	 */
	public void restorexchange() { }

	/////////////////////////////////////////////////////////////////////////////////////////////
	public String updateEntity(String synodr, String pid, SyntityMeta entm, String resname,
			String format, String createDate, Funcall now) {
		return null;
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

	public ExchangeBlock addChild(ExessionPersisting ap, String synode, SynodeMode child,
			IUser robot, String org, String domain) {
		return null;
	}

	public ExchangeBlock initDomain(ExessionPersisting cp, String synode, ExchangeBlock resp) {
		return null;
	}

	public HashMap<String, Nyquence> closeJoining(ExessionPersisting cp, String synode,
			HashMap<String, Nyquence> clone) {
		return null;
	}
}
