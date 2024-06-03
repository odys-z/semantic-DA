package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.hasGt;
import static io.odysz.common.LangExt.str;
import static io.odysz.semantic.syn.Exchanging.*;
import static io.odysz.semantic.syn.Nyquence.compareNyq;
import static io.odysz.semantic.syn.Nyquence.*;
import static io.odysz.semantic.util.DAHelper.loadRecNyquence;
import static io.odysz.semantic.util.DAHelper.loadRecString;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.concatstr;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.transact.sql.parts.condition.Funcall.max;

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
import io.odysz.semantic.DASemantics;
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
import io.odysz.semantics.x.SemanticException;
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
 * @author Ody
 *
 */
public class DBSynsactBuilder extends DATranscxt {
	public static class SynmanticsMap extends SemanticsMap {
		public SynmanticsMap(String conn) {
			super(conn);
		}

		@Override
		public DASemantics createSemantics(Transcxt trb, String tabl, String pk, boolean debug) {
			return new DBSynmantics(trb, tabl, pk, debug);
		}
	}

	protected SynodeMeta synm;
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;
	protected PeersMeta pnvm;

	/**
	 * Get synchronization meta connection id.
	 * 
	 * @return conn-id
	 */
	public String synconn() { return basictx.connId(); }

	protected String synode() { return ((DBSyntext)this.basictx).synode; }

	/** Nyquence vector [{synode, Nyquence}]*/
	protected HashMap<String, Nyquence> nyquvect;
	protected Nyquence n0() { return nyquvect.get(synode()); }
	protected DBSynsactBuilder n0(Nyquence nyq) {
		nyquvect.put(synode(), new Nyquence(nyq.n));
		return this;
	}

	public String domain() {
		return basictx() == null ? null : ((DBSyntext) basictx()).domain;
	}

	public IUser synrobot() { return ((DBSyntext) this.basictx).usr(); }

	private HashMap<String, SyntityMeta> entityRegists;

	public DBSynsactBuilder(String conn, String synodeId)
			throws SQLException, SAXException, IOException, TransException {
		this(conn, synodeId,
			new SynChangeMeta(conn),
			new SynodeMeta(conn));
	}
	
	public DBSynsactBuilder(String conn, String synodeId,
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
		this.pnvm = pnvm != null ? pnvm : new PeersMeta(conn);
		this.pnvm.replace();
	}
	
	DBSynsactBuilder loadNyquvect0(String conn) throws SQLException, TransException {
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

	/**
	 * Inc my n0, then reload from DB.
	 * @return this
	 * @throws TransException
	 * @throws SQLException
	 */
	public DBSynsactBuilder incNyquence() throws TransException, SQLException {
		update(synm.tbl, synrobot())
			.nv(synm.nyquence, Funcall.add(synm.nyquence, 1))
			.whereEq(synm.pk, synode())
			.u(instancontxt(basictx.connId(), synrobot()));
		
		nyquvect.put(synode(), loadRecNyquence(this, basictx.connId(), synm, synode(), synm.nyquence));
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

	public AnResultset entities(SyntityMeta phm, String connId, IUser usr)
			throws TransException, SQLException {
		return (AnResultset)select(phm.tbl, "ch")
				.rs(instancontxt(connId, usr))
				.rs(0);
	}

	/**
	 * Compare source and destination record, using both current row.
	 * @param a initiator
	 * @param b acknowledger
	 * @return
	 * -2: ack to ini deletion propagation,
	 * -1: ack to ini appending,
	 * 0: needing to be merged (same record),
	 * 1: ini to ack appending
	 * 2: ini to ack deletion propagation
	 * @throws SQLException 
	 */
	int compare(long srcn, long dstn) throws SQLException {
		// long dn = chgs.getLong(chgm.nyquence);
		int diff = compareNyq(srcn, dstn);
		if (diff == 0)
			return 0;
		
		return (int) Math.min(2, Math.max(-2, (srcn - dstn)));
	}

	/**
	 * 
	 * @param commitBuff
	 * @param srcnode
	 * @param srcn1
	 * @return current log index with current row index of first in-ignorable. If all ignored, return the size
	 * @throws SQLException
	 */
	int ignoreUntil(List<ChangeLogs> commitBuff, String srcnode, long srcn1) throws SQLException {
		// FIXME, binary search?
		for (int lx = 0; lx < commitBuff.size(); lx++) {
			ChangeLogs log = commitBuff.get(lx);
			AnResultset rs = log.answers;
			rs.beforeFirst();
			while (rs.next() && rs.getLong(0) > srcn1)
				;
			if (rs.hasnext()) 
				return lx;
		}
		return commitBuff.size();
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
	 * [n=1]  → way 1: z.synoder=1 + 1
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
		
		if (srcnv == null) return;
		
		if (Connects.getDebug(basictx.connId()))
			Utils.logi("Cleaning staleness at %s, peer %s ...", synode(), peer);

		delete(pnvm.tbl, synrobot())
			.whereEq(pnvm.peer, peer)
			.whereEq(pnvm.domain, domain())
			.post(insert(pnvm.tbl)
				.cols(pnvm.inscols)
				.values(pnvm.insVals(srcnv, peer, domain())))
			.d(instancontxt(synconn(), synrobot()));

		SemanticObject res = (SemanticObject) ((DBSynsactBuilder)
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
					.where(op.le, sqlCompare("cl", chgm.nyquence, "nv", pnvm.nyq), 0))
					// .where(op.ne, subm.synodee, constr(peer)))
				.delete(subm.tbl)
					.where(op.exists, null, select("cl")
					.where(op.eq, subm.changeId, chgm.pk)
					.whereEq(subm.tbl, subm.synodee,  "cl", subm.synodee)))

			// clean changes without subscribes
			// 
			// delete from syn_change where not exists ( 
			//   with cl as (select changeId, domain, tabl, synodee 
			//               from syn_change cl join syn_subscribe sb on cid = changeId)
			//   select * from cl join syn_change ch on changeId = cid and cl.domain = ch.domain);

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
			
		if (Connects.getDebug(synconn())) {
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
	void cleanStaleThan(HashMap<String, Nyquence> srcnv, String srcn)
			throws TransException, SQLException {
		for (String sn : srcnv.keySet()) {
			if (eq(sn, synode()) || eq(sn, srcn))
				continue;
			if (!nyquvect.containsKey(sn))
				continue;

			if (Connects.getDebug(basictx.connId()))
				Utils.logi("%1$s.cleanStaleThan(): Deleting staleness where for source %2$s, %2$s.%4$s = %3$d > my-change[%4$s].n ...",
					synode(), srcn, srcnv.get(sn).n, sn);


			SemanticObject res = (SemanticObject) ((DBSynsactBuilder)
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
						.where(op.lt, chgm.nyquence, srcnv.get(sn).n) // FIXME nyquence compare
						.where(op.notexists, null, select("cl")
							.col("1")
							.whereEq(chgm.tbl, chgm.domain,  "cl", chgm.domain)
							.whereEq(chgm.tbl, chgm.entbl,"cl", chgm.entbl)
							.whereEq(chgm.tbl, chgm.uids, "cl", chgm.uids)))
				.d(instancontxt(basictx.connId(), synrobot()));
			
			if (Connects.getDebug(basictx.connId()))
				// Utils.logi("%d subscribe record(s) are affected.", ((ArrayList<?>)res.get("total")).get(0));
				try {
					@SuppressWarnings("unchecked")
					ArrayList<Integer> chgsubs = ((ArrayList<Integer>)res.get("total"));
					if (chgsubs != null && chgsubs.size() > 1 && hasGt(chgsubs, 0)) {
						Utils.logi("Subscribe record(s) are affected:");
						Utils.logi(str(chgsubs, new String[] {"subscribe", "change"}));
					}
				} catch (Exception e) { e.printStackTrace(); }
		}
	}
	 */

	/**
	 * commit answers in x.
	 * @param x
	 * @param srcnode
	 * @param tillN max nyquence. Later than this is ignored - shouldn't happen
	 * @return this
	 * @throws SQLException
	 * @throws TransException
	 */
	DBSynsactBuilder commitAnswers(ExchangeContext x, String srcnode, long tillN)
			throws SQLException, TransException {

		List<Statement<?>> stats = new ArrayList<Statement<?>>();

		AnResultset rply = x.answer.beforeFirst();
		rply.beforeFirst();
		String entid = null;
		while (rply.next()) {
			if (compareNyq(rply.getLong(chgm.nyquence), tillN) > 0)
				break;

			SyntityMeta entm = getEntityMeta(rply.getString(chgm.entbl));
			String change = rply.getString(ChangeLogs.ChangeFlag);
			HashMap<String, AnResultset> entbuf = x.mychallenge.entities;
			
			// current entity
			String entid1 = rply.getString(chgm.entfk);

			String rporg  = rply.getString(chgm.domain);
			String rpent  = rply.getString(chgm.entbl);
			String rpuids = rply.getString(chgm.uids);
			String rpnodr = rply.getString(chgm.synoder);
			String rpscrb = rply.getString(subm.synodee);
			String rpcid  = rply.getString(chgm.pk);

			if (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).rowIndex0(entid1) < 0) {
				Utils.warn("[DBSynsactBuilder commitTill] Fatal error ignored: can't restore entity record answered from target node.\n"
						+ "entity name: %s\nsynode(answering): %s\nsynode(local): %s\nentity id(by answer): %s", entm.tbl, srcnode, synode(), entid1);
				continue;
			}
				
			stats.add(eq(change, CRUD.C)
				// create an entity, and trigger change log
				? !eq(entid, entid1)
					? insert(entm.tbl, synrobot())
						.cols(entm.entCols())
						.value(entm.insertChallengeEnt(entid1, entbuf.get(entm.tbl)))
						.post(insert(chgm.tbl)
							.nv(chgm.pk, rpcid)
							.nv(chgm.crud, CRUD.C)
							.nv(chgm.domain, rporg)
							.nv(chgm.entbl, rpent)
							.nv(chgm.synoder, rpnodr)
							.nv(chgm.uids, rpuids)
							.nv(chgm.entfk, entid1)
							.post(insert(subm.tbl)
								.cols(subm.insertCols())
								.value(subm.insertSubVal(rply))))
					: insert(subm.tbl)
						.cols(subm.insertCols())
						.value(subm.insertSubVal(rply))

				// remove subscribers & backward change logs's deletion propagation
				: delete(subm.tbl, synrobot())
					// .whereEq(subm.entbl, entm.tbl)
					// .whereEq(subm.uids, rpuids)
					.whereEq(subm.changeId, rpcid)
					.whereEq(subm.synodee, rpscrb)
					.post(del0subchange(entm, rporg, rpnodr, rpuids, rpcid, rpscrb)
					));
			entid = entid1;
		}

		Utils.logi("[DBSynsactBuilder.commitAnswers()] updating change logs without modifying entities...");
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			s.commit(sqls, synrobot());
		Connects.commit(basictx.connId(), synrobot(), sqls);
		
		x.answer = null;
		return this;
	}

	/**
	 * Generate delete statement when change logs don't have synodees. 
	 * @param entitymeta
	 * @param org
	 * @param synoder
	 * @param uids
	 * @param deliffnode delete the change-log iff the node, i.e. only the subscriber, exists.
	 * For answers, it's the node himself, for challenge, it's the source node.
	 * @return
	 * @throws TransException
	 */
	Statement<?> del0subchange(SyntityMeta entitymeta,
			String org, String synoder, String uids, String changeId, String deliffnode) throws TransException {
		return delete(chgm.tbl) // delete change log if no subscribers exist
			.whereEq(chgm.pk, changeId)
			.whereEq(chgm.entbl, entitymeta.tbl)
			.whereEq(chgm.domain, org)
			.whereEq(chgm.synoder, synoder)
			// .whereEq(chgm.uids,    uids)
			.whereEq("0", (Query)select(subm.tbl)
				.col(count(subm.synodee))
				// .whereEq(subm.domain, org)
				// .whereEq(subm.entbl, entitymeta.tbl)
				// .whereEq(subm.uids,  uids))
				.whereEq(chgm.pk, changeId)
				.where(op.eq, chgm.pk, subm.changeId)
				.where(op.ne, subm.synodee, constr(deliffnode)))
			;
	}

	/**
	 * <p>Commit challenges buffered in context.</p>
	 * 
	 * Challenges must grouped by synodee, entity-table and domain (org). 
	 * 
	 * @param x
	 * @param srcnode
	 * @param srcnv
	 * @param tillN
	 * @return this
	 * @throws SQLException
	 * @throws TransException
	 */
	DBSynsactBuilder commitChallenges(ExchangeContext x, String srcnode, HashMap<String, Nyquence> srcnv, long tillN)
			throws SQLException, TransException {
		List<Statement<?>> stats = new ArrayList<Statement<?>>();

		AnResultset chal = x.onchanges.challenge.beforeFirst();

		HashSet<String> missings = new HashSet<String>(nyquvect.keySet());
		missings.removeAll(srcnv.keySet());
		missings.remove(synode());

		while (chal.next()) {
			// if (compareNyq(chal.getLong(chgm.nyquence), tillN) > 0) break;

			String change = chal.getString(chgm.crud);
			Nyquence chgnyq = getn(chal, chgm.nyquence);

			SyntityMeta entm = getEntityMeta(chal.getString(chgm.entbl));
			// create / update / delete an entity
			String entid  = chal.getString(chgm.entfk);
			String synodr = chal.getString(chgm.synoder);
			String chuids = chal.getString(chgm.uids);
			String chgid  = chal.getString(chgm.pk);

			HashMap<String, AnResultset> entbuf = x.onchanges.entities;
			if (entbuf == null || !entbuf.containsKey(entm.tbl) || entbuf.get(entm.tbl).rowIndex0(entid) < 0) {
				Utils.warn("[DBSynsactBuilder commitChallenges] Fatal error ignored: can't restore entity record answered from target node.\n"
						+ "entity name: %s\nsynode(answering): %s\nsynode(local): %s\nentity id(by challenge): %s",
						entm.tbl, srcnode, synode(), entid);
				continue;
			}
			
			String chorg = chal.getString(chgm.domain);
			String chentbl = chal.getString(chgm.entbl);
			
			// current entity's subscribes
			ArrayList<Statement<?>> subscribeUC = new ArrayList<Statement<?>>();

			if (eq(change, CRUD.D)) {
				String subsrb = chal.getString(subm.synodee);
				stats.add(delete(subm.tbl, synrobot())
					.whereEq(subm.synodee, subsrb)
					// .whereEq(subm.entbl, chentbl)
					// .whereEq(subm.domain, chorg)
					// .whereEq(subm.uids, chuids)
					.whereEq(subm.changeId, chgid)
					.post(ofLastEntity(chal, entid, chentbl, chorg)
						? delete(chgm.tbl)
							.whereEq(chgm.entbl, chentbl)
							.whereEq(chgm.domain, chorg)
							.whereEq(chgm.synoder, synodr)
							.whereEq(chgm.uids, chuids)
							.post(delete(entm.tbl)
								.whereEq(entm.org(), chorg)
								.whereEq(entm.synoder, synodr)
								.whereEq(entm.pk, chentbl))
						: null));
			}
			else { // CRUD.C || CRUD.U
				boolean iamSynodee = false;

				while (chal.validx()) {
					String subsrb = chal.getString(subm.synodee);
					if (eq(subsrb, synode())) {
					/** conflict: Y try send Z a record that Z already got from X.
					 *        X           Y               Z
                     *             | I Y Y,W 4 Z -> 4 < Z.y, ignore |
					 *
      				 *		  X    Y    Z    W
					 *	X [   7,   5,   3,   4 ]
					 *	Y [   4,   6,   1,   4 ]
					 *	Z [   6,   5,   7,   4 ]   change[Z].n < Z.y, that is Z knows later than the log
					 */
						Nyquence my_srcn = nyquvect.get(srcnode);
						if (my_srcn != null && compareNyq(chgnyq, my_srcn) >= 0)
							// conflict & override
							iamSynodee = true;
					}
					else if (compareNyq(chgnyq, nyquvect.get(srcnode)) > 0
						// ref: _merge-older-version
						// knowledge about the sub from req is older than this node's knowledge 
						// see #onchanges ref: answer-to-remove
						// FIXME how to abstract into one method?
						&& !eq(subsrb, synode()))
						subscribeUC.add(insert(subm.tbl)
							.cols(subm.insertCols())
							.value(subm.insertSubVal(chal))); 
					
					if (ofLastEntity(chal, entid, chentbl, chorg))
						break;
					chal.next();
				}

				appendMissings(stats, missings, chal);

				if (iamSynodee || subscribeUC.size() > 0) {
					stats.add(eq(change, CRUD.C)
					? insert(entm.tbl, synrobot())
						.cols(entm.entCols())
						.value(entm.insertChallengeEnt(entid, entbuf.get(entm.tbl)))
						.post(subscribeUC.size() <= 0 ? null :
							insert(chgm.tbl)
							.nv(chgm.pk, chgid)
							.nv(chgm.crud, CRUD.C).nv(chgm.domain, chorg)
							.nv(chgm.entbl, chentbl).nv(chgm.synoder, synodr).nv(chgm.uids, chuids)
							.nv(chgm.nyquence, chal.getLong(chgm.nyquence))
							.nv(chgm.entfk, entm.autopk() ? new Resulving(entm.tbl, entm.pk) : constr(entid))
							.post(subscribeUC)
							.post(del0subchange(entm, chorg, synodr, chuids, chgid, synode())))
					: eq(change, CRUD.U)
					? update(entm.tbl, synrobot())
						.nvs(entm.updateEntNvs(chgm, entid, entbuf.get(entm.tbl), chal))
						.whereEq(entm.synoder, synodr)
						.whereEq(entm.org(), chorg)
						.whereEq(entm.pk, entid)
						// FIXME there shouldn't be an UPSERT if the change-log is handled in orignal order?
//						.post(insert(entm.tbl, synrobot()) 
//							.cols(entm.entCols())
//							.select(select(null)
//									.cols(entm.insertSelectItems(chgm, entid, entbuf.get(entm.tbl), chal)))
//							.where(op.notexists, null,
//								select(entm.tbl)
//								.whereEq(entm.synoder, synodr)
//								.whereEq(entm.org(), entbuf.get(entm.tbl).getStringByIndex(entm.org(), entid))
//								.whereEq(entm.pk, entid)))
						.post(subscribeUC.size() <= 0
							? null : insert(chgm.tbl)
							.nv(chgm.pk, chgid)
							.nv(chgm.crud, CRUD.U).nv(chgm.domain, chorg)
							.nv(chgm.entbl, chentbl).nv(chgm.synoder, synodr).nv(chgm.uids, chuids)
							.nv(chgm.nyquence, chgnyq.n)
							.nv(chgm.entfk, constr(entid))
							.post(subscribeUC)
							.post(del0subchange(entm, chorg, synodr, chuids, chgid, synode())))
					: null);
				}

				subscribeUC = new ArrayList<Statement<?>>();
				iamSynodee  = false;
			}
		}

		Utils.logi("[DBSynsactBuilder.commitChallenges()] update entities...");
		ArrayList<String> sqls = new ArrayList<String>();
		for (Statement<?> s : stats)
			if (s != null)
				s.commit(sqls, synrobot());
		Connects.commit(basictx.connId(), synrobot(), sqls);
		
		x.onchanges = null;
		return this;
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
		return !chlogs.hasnext() || !eq(curEid, chlogs.nextString(chgm.entfk))
			|| !eq(curEntbl, chlogs.nextString(chgm.entbl)) || !eq(curDomain, chlogs.nextString(chgm.domain));
	}
	
	boolean isAnotherEntity (AnResultset chlogs, String curEid, String curEntbl, String curDomain)
			throws SQLException {
		return !chlogs.hasprev() || !eq(curEid, chlogs.prevString(chgm.entfk))
			|| !eq(curEntbl, chlogs.prevString(chgm.entbl)) || !eq(curDomain, chlogs.prevString(chgm.domain));
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
				stats.add(insert(subm.tbl)
					.cols(subm.insertCols())
					.value(subm.insertSubVal(domain, entbl, sub, uids)));
		}
		return missing;
	}

	public SyntityMeta getEntityMeta(String entbl) throws SemanticException {
		if (entityRegists == null || !entityRegists.containsKey(entbl))
			throw new SemanticException("Register %s first.", entbl);
			
		return entityRegists.get(entbl);
	}
	
	public DBSynsactBuilder registerEntity(String conn, SyntityMeta m)
			throws SemanticException, TransException, SQLException {
		if (entityRegists == null)
			entityRegists = new HashMap<String, SyntityMeta>();
		entityRegists.put(m.tbl, (SyntityMeta) m.clone(Connects.getMeta(conn, m.tbl)));
		return this;
	}

	/**
	 * this.n0++, this.n0 = max(n0, maxn)
	 * @param maxn
	 * @return n0
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public Nyquence incN0(long maxn) throws TransException, SQLException {
		n0().inc(maxn);
		DAHelper.updateField(this, basictx.connId(), synm, synode(),
				synm.nyquence, new ExprPart(n0().n), synrobot());
		return n0();
	}

	public Nyquence incN0(Nyquence n) throws TransException, SQLException {
		return incN0(n == null ? nyquvect.get(synode()).n : n.n);
	}

	/**
	 * Find if there are change logs such that chg.n &gt; myvect[remote].n, to be exchanged.
	 * @param cx 
	 * @param <T>
	 * @param target exchange target (server)
	 * @param nv nyquevect from target 
	 * @return logs such that chg.n > nyv[target], i.e there are change logs to be exchanged.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public <T extends SynEntity> ChangeLogs initExchange(ExchangeContext x, String target,
			HashMap<String, Nyquence> nv) throws TransException, SQLException {

		x.exstate.can(init);

		cleanStale(nv, target);
		HashMap<String,Long> xnv = sessionMaxnv(domain());
		if (Connects.getDebug(synconn()))
			Utils.logMap(xnv);
		// x.maxnv = xnv;

		// synyquvectWith(target, nv);

		ChangeLogs diff = initChallenges(x, target);
		return diff;
	}
	
	protected ChangeLogs initChallenges(ExchangeContext x, String peer)
			throws SecurityException, TransException, SQLException {

		ChangeLogs diff = new ChangeLogs(chgm);
		Nyquence dn = this.nyquvect.get(peer);
		if (dn == null) {
			Utils.warn("ERROR [%s#%s]: Me, %s, don't have knowledge about %s.",
					this.getClass().getName(),
					new Object(){}.getClass().getEnclosingMethod().getName(),
					synode(), peer);
			throw new SemanticException("%s#%s(), don't have knowledge about %s.",
					synode(), new Object(){}.getClass().getEnclosingMethod().getName(), peer);
		}
		else {
			AnResultset challenge = (AnResultset) select(chgm.tbl, "ch")
				// .je("ch", subm.tbl, "sb", chgm.entbl, subm.entbl, chgm.uids, subm.uids)
				.je_(subm.tbl, "sb", chgm.pk, subm.changeId)
				.cols("ch.*", subm.synodee)
				// FIXME not op.lt, must implement a function to compare nyquence.
				.where(op.gt, chgm.nyquence, dn.n) // FIXME
				.orderby(chgm.entbl)
				.orderby(chgm.nyquence)
				.orderby(chgm.synoder)
				.orderby(subm.synodee)
				.rs(instancontxt(basictx.connId(), synrobot()))
				.rs(0);

			AnResultset entbls = (AnResultset) select(chgm.tbl, "ch")
				.col(chgm.entbl)
				.where(op.gt, chgm.nyquence, dn.n)
				.groupby(chgm.entbl)
				.rs(instancontxt(basictx.connId(), synrobot()))
				.rs(0);

			diff.challenge(challenge);
			while (entbls.next()) {
				String tbl = entbls.getString(chgm.entbl);
				SyntityMeta entm = entityRegists.get(tbl);

				AnResultset entities = ((AnResultset) select(tbl, "e")
					// .je("e", chgm.tbl, "ch", "ch." + chgm.entbl, constr(tbl), entm.pk, chgm.entfk)
					.je_(chgm.tbl, "ch", "ch." + chgm.entbl, constr(tbl), entm.pk, chgm.entfk)
					.cols_byAlias("e", entm.entCols()).col("e." + entm.pk)
					.where(op.gt, chgm.nyquence, dn.n)
					.orderby(chgm.nyquence)
					.orderby(chgm.synoder)
					.rs(instancontxt(basictx.connId(), synrobot()))
					.rs(0))
					.index0(entm.pk);
				
				diff.entities(tbl, entities);
			}
		
			x.initChallenge(peer, diff);
		
			x.exstate.initexchange();

			diff.stepping(x.exstate.mode, x.exstate.state)
				.session(x.session())
				.nyquvect(this.nyquvect);
		}

		return diff;
	}
	
	public <T extends SynEntity> ChangeLogs onExchange(ExchangeContext x, String from,
			HashMap<String, Nyquence> remotv, ChangeLogs req) throws SQLException, TransException {

		x.exstate.can(req.stepping().state);
		
		if (x.exstate.state == ready) {
			if (Connects.getDebug(synconn())) {
				HashMap<String, Long> maxnv = sessionMaxnv(domain());
				Utils.warn("Should only once to be here.\nSession Max Nv:");
				Utils.logi(maxnv);
			}

			cleanStale(req.nyquvect, from);
		}

		if (x.onchanges != null && x.onchanges.challenges() > 0)
			Utils.warn("There are challenges buffered to be commited: %s@%s", from, synode());;

		// ChangeLogs myanswer = initExchange(x, from, null);
		ChangeLogs myanswer = initChallenges(x, from);

		x.buffChanges(nyquvect, req.challenge.colnames(), onchanges(myanswer, req, from), req.entities);

		x.exstate.onExchange();
		return myanswer.session(x.session()).nyquvect(nyquvect);
	}

	/**
	 * Get max nyquence for each synodee, the knowledge of max nyquvect
	 * that this node knows for each synodee.
	 * 
	 * @param domain
	 * @return {synodee: max Nyquence group by domian, synodee}
	 * @throws TransException
	 * @throws SQLException
	 */
	HashMap<String,Long> sessionMaxnv(String domain) throws TransException, SQLException {
		HashMap<String, Long> maxnv = new HashMap<String, Long> ();
		AnResultset rs = (AnResultset) select(chgm.tbl, "cl")
			.je_(subm.tbl, "sb", chgm.pk, subm.changeId,
					chgm.domain, constr(domain))
			.cols(chgm.entbl, subm.synodee)
			.col(max(chgm.nyquence), "n")
			// .groupby(chgm.entbl)
			.groupby(chgm.domain)
			.groupby(subm.synodee)
			.rs(instancontxt(synconn(), synrobot()))
			.rs(0);
		
		if (rs.next()) {
			maxnv.put(rs.getString(subm.synodee), rs.getLong("n"));
		}
		return maxnv;
	}

	ArrayList<ArrayList<Object>> onchanges(ChangeLogs resp, ChangeLogs req, String srcn)
			throws SQLException {
		ArrayList<ArrayList<Object>> changes = new ArrayList<ArrayList<Object>>();
		while (req != null && req.challenge != null && req.challenge.next()) {
			String subscribe = req.challenge.getString(subm.synodee);

			if (eq(subscribe, synode())) {
				resp.remove_sub(req.challenge, synode());	
				changes.add(req.challenge.getRowAt(req.challenge.getRow() - 1)); // FIXME what about conflict? someone else already pushed to me?
			}
			else {
				Nyquence subnyq = getn(req.challenge, chgm.nyquence);
				if (!nyquvect.containsKey(subscribe) // I don't have information of the subscriber
					&& eq(synm.tbl, req.challenge.getString(chgm.entbl))) // adding synode
					changes.add(req.challenge.getRowAt(req.challenge.getRow() - 1));
				else if (!nyquvect.containsKey(subscribe))
						; // I have no idea
				else if (compareNyq(subnyq, nyquvect.get(srcn)) <= 0) {
					// ref: _answer-to-remove
					// knowledge about the sub from req is older than this node's knowledge 
					// see #commitChallenges ref: merge-older-version
					// FIXME how to abstract into one method?
					
					// 2024.6.2 client shouldn't have older knowledge than me now,
					// which is cleanded when initiating.
					// resp.remove_sub(req.challenge, subscribe);	
				}
				else
					changes.add(req.challenge.getRowAt(req.challenge.getRow() - 1));
			}
		}

		return changes;
	}

	/**
	 * Client node acknowledge destionation's response (from server),
	 * i.e. check answers up to n = my-n0
	 * If there is no more challenges, increase my.n0.
	 * @param x exchange execution instance 
	 * @param answer
	 * @param sn 
	 * @param srcnv 
	 * @return acknowledge, answer to the destination node's commitment, and have be committed at source.
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public ChangeLogs ackExchange(ExchangeContext x, ChangeLogs answer, String sn)
			throws SQLException, TransException, IOException {
		x.exstate.can(confirming);

		ChangeLogs myack = new ChangeLogs(chgm);
		if (answer.answers != null && answer.answers.getRowCount() > 0) {
			x.addAnswer(answer.answers);
			commitAnswers(x, sn, n0().n);
		}

		// cleanStaleThan(answer.nyquvect, sn);

		x.buffChanges(nyquvect, answer.challenge.colnames(), onchanges(myack, answer, sn), answer.entities);
		if (x.onchanges.challenges() > 0) {
			commitChallenges(x, sn, answer.nyquvect, nyquvect.get(synode()).n);
		}

		// synyquvectWith(sn, answer.nyquvect);

		myack.nyquvect(Nyquence.clone(nyquvect));

		x.exstate.ack();
		return myack.session(x.session());
	}

	/**
	 * Commit buffered answer's changes as client node acknowledged the answers with {@code ack}.
	 * @param x exchange execution's instance
	 * @param ack answer to previous challenge
	 * @param target 
	 * @param srcnv 
	 * @param entm 
	 * @return nyquvect
	 * @throws TransException 
	 * @throws SQLException 
	 */
	public HashMap<String, Nyquence> onAck(ExchangeContext x, ChangeLogs ack, String target,
			HashMap<String, Nyquence> srcnv, SyntityMeta entm) throws SQLException, TransException {
		cleanAckBuffer(x, ack, target, srcnv, entm);
		
		// synyquvectWith(target, ack.nyquvect);
		// synyquvect(target, ack.nyquvect);

		// n0(maxn(ack.nyquvect, n0()));
		
		return nyquvect;
	}
	
	/**
	 * Commit challenges, append answers.
	 * 
	 * @param x
	 * @param ack
	 * @param target
	 * @param srcnv
	 * @param entm
	 * @throws SQLException
	 * @throws TransException
	 */
	void cleanAckBuffer(ExchangeContext x, ChangeLogs ack, String target,
			HashMap<String, Nyquence> srcnv, SyntityMeta entm) throws SQLException, TransException {

		// cleanStaleThan(ack.nyquvect, target);
		
		x.exstate.can(confirming);

		if (ack != null && compareNyq(ack.nyquvect.get(synode()), nyquvect.get(synode())) <= 0) {
			commitChallenges(x, target, srcnv, nyquvect.get(synode()).n);
		}

		if (ack.answers != null && ack.answers.getRowCount() > 0) {
			x.addAnswer(ack.answers);
			commitAnswers(x, target, n0().n);
		}
		// synyquvectWith(target, x.exNyquvect);
		x.exstate.onAck();
	}
	
	public HashMap<String, Nyquence> closexchange(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws TransException, SQLException {
		x.clear();

		/*
		HashMap<String, Nyquence> snapshot = Nyquence.clone(nyquvect);
		incN0(maxn(maxn(nv), new Nyquence(nv.get(sn).n).inc()));

		synyquvect(sn, nv);
		*/
		HashMap<String, Nyquence> snapshot = synyquvectMax(sn, nv, nyquvect);

		x.exstate.close();
		return snapshot;
	}

	protected HashMap<String, Nyquence> synyquvectMax(String peer,
			HashMap<String, Nyquence> nv, HashMap<String, Nyquence> mynv)
					throws TransException, SQLException {

		if (nv == null) return mynv;
		Update u = null;

		HashMap<String, Nyquence> snapshot =  new HashMap<String, Nyquence>();

		for (String n : nv.keySet()) {
			if (!nyquvect.containsKey(n))
				continue;
			Nyquence nyq = null;
//			if (eq(n, synode()))
//				continue;
//			else
//			if (eq(peer, n))
//				nyq = maxn(new Nyquence(nv.get(n).n).inc(), n0());
//			else if (nyquvect.containsKey(n))
//				nyq = maxn(nv.get(n), nyquvect.get(n));
			
			if (eq(synode(), n)) {
				Nyquence mx = maxn(nv.get(n), n0(), nv.get(peer));
				snapshot.put(n, new Nyquence(mx.n));
				incN0(mx);
			}
			else
				snapshot.put(n, nv.get(n));

			nyq = maxn(nv.get(n), nyquvect.get(n));

			if (compareNyq(nyq, nyquvect.get(n)) != 0) {
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

			loadNyquvect0(synconn());
		}

		return snapshot;
	}

	public HashMap<String, Nyquence> closeJoining(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws TransException, SQLException {
		return closexchange(x, sn, nv);
	}
	
	public void onclosexchange(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws SQLException, TransException {

		try { closexchange(x, sn, nv); }
		finally { x.exstate.onclose(); }
	}
	
	public void oncloseJoining(ExchangeContext x, String sn, HashMap<String, Nyquence> nv)
			throws SQLException, TransException {
		onclosexchange(x, sn, nv);
	}

	/**
	 * Update / step my nyquvect with {@code nv}, using max(my.nyquvect, nv).
	 * If nv[sn] &lt; my_nv[sn], throw SemanticException: can't update my nyquence with early knowledge.
	 * 
	 * @param peer whose nyquence in {@code nv} is required to be newer.
	 * @param nv
	 * @param maxnv 
	 * @return this
	 * @throws TransException
	 * @throws SQLException
	 */
	DBSynsactBuilder synyquvect(String peer, HashMap<String, Nyquence> nv)
			throws TransException, SQLException {
		if (nv == null) return this;
		Update u = null;

		if (compareNyq(nv.get(peer), nyquvect.get(peer)) < 0)
			throw new SemanticException(
				"[DBSynsactBuilder.synyquvectWith()] Updating nyquence at %s, peer %s, unexpected: %2$s.nv[%s] < %1$s.nv[peer].",
				synode(), peer);

		for (String n : nv.keySet()) {
			Nyquence nyq = null;
//			if (eq(n, synode()))
//				continue;
//			else
//			if (eq(peer, n))
//				nyq = maxn(new Nyquence(nv.get(n).n).inc(), n0());
//			else if (nyquvect.containsKey(n))
//				nyq = maxn(nv.get(n), nyquvect.get(n));
			nyq = maxn(nv.get(n), nyquvect.get(n));

			if (compareNyq(nyq, nyquvect.get(n)) != 0) {
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

			loadNyquvect0(synconn());
		}

		return this;
	}
		
	/**
	 * A {@link SynodeMode#hub hub} node uses this to setup change logs for joining nodes.
	 * @param x 
	 * 
	 * @param childId
	 * @param robot
	 * @param org
	 * @param domain
	 * @return {session: changeId, uids: "My-id,applyid"}
	 * @throws TransException
	 * @throws SQLException
	 */
	public ChangeLogs addChild(ExchangeContext x, String childId, SynodeMode reqmode, IUser robot, String org, String domain)
			throws TransException, SQLException {
		Synode apply = new Synode(basictx.connId(), childId, org, domain);
		String chgid = ((SemanticObject) apply.insert(synm, n0(), insert(synm.tbl, robot))
			.post(insert(chgm.tbl, robot)
				.nv(chgm.entfk, apply.recId)
				.nv(chgm.entbl, synm.tbl)
				.nv(chgm.crud, CRUD.C)
				.nv(chgm.synoder, synode())
				.nv(chgm.uids, concatstr(synode(), chgm.UIDsep, apply.recId))
				.nv(chgm.nyquence, n0().n)
				.nv(chgm.domain, robot.orgId())
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
						.whereEq(synm.domain, domain))))
			.ins(instancontxt(basictx.connId(), robot)))
			.resulve(chgm.tbl, chgm.pk);
		
		nyquvect.put(apply.recId, new Nyquence(apply.nyquence));

		ChangeLogs log = new ChangeLogs(chgm)
			.nyquvect(nyquvect)
			.synodes(reqmode == SynodeMode.child
				? ((AnResultset) select(synm.tbl, "syn")
					.whereIn(synm.synoder, childId, synode())
					.whereEq(synm.domain, domain)
					.whereEq(synm.org(), org)
					.rs(instancontxt(basictx.connId(), synrobot()))
					.rs(0))
				: null); // Following exchange is needed
		return log.session(chgid);
	}

	public ChangeLogs onJoining(SynodeMode reqmode, String joining, String domain, String org)
			throws TransException, SQLException {
		insert(synm.tbl, synrobot())
			.nv(synm.pk, joining)
			.nv(synm.nyquence, new ExprPart(n0().n))
			.nv(synm.mac,  "#"+joining)
			.nv(synm.org(), org)
			.nv(synm.domain, domain)
			.ins(instancontxt(basictx.connId(), synrobot()));

		if (reqmode == SynodeMode.hub)
			incNyquence();

		ChangeLogs log = new ChangeLogs(chgm)
			.nyquvect(Nyquence.clone(nyquvect))
			.synodes(reqmode == SynodeMode.child
				? ((AnResultset) select(synm.tbl, "syn")
					.whereEq(synm.synoder, synode())
					.whereEq(synm.domain, domain)
					.whereEq(synm.org(), org)
					.rs(instancontxt(basictx.connId(), synrobot()))
					.rs(0))
				: null); // Following exchange is needed
		return log;
	}

	public ChangeLogs initDomain(ExchangeContext x, ChangeLogs domainstatus)
			throws SQLException, TransException {
		AnResultset ns = domainstatus.synodes.beforeFirst();
		nyquvect = new HashMap<String, Nyquence>(ns.getRowCount());
		while (ns.next()) {
			Synode n = new Synode(ns, synm);
			Nyquence mxn = maxn(domainstatus.nyquvect);
			n.insert(synm, mxn, insert(synm.tbl, synrobot()))
				.ins(instancontxt(basictx.connId(), synrobot()));

			nyquvect.put(n.recId, new Nyquence(mxn.n));
		}

		return new ChangeLogs(chgm).nyquvect(nyquvect);
	}
	
	/**
	 * Syntity table updating. 
	 * Synchronized entity table can only be updated with a pk condition.
	 * @param entm
	 * @param pid
	 * @param field
	 * @param nvs name-value pairs
	 * @return 
	 * @return
	 * @throws TransException
	 * @throws SQLException
	 * @throws IOException 
	 * @throws AnsonException 
	 */
	public String updateEntity(String synoder, String pid, SyntityMeta entm, Object ... nvs)
			throws TransException, SQLException, AnsonException, IOException {
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
}
