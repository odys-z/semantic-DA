package io.odysz.semantic.syn;

import static io.odysz.transact.sql.parts.condition.Funcall.add;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.indexOf;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.removele;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.xml.sax.SAXException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.meta.NyquenceMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
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
	protected NyquenceMeta nyqm;
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;

	protected String synode;
	/** Nyquence vector [{synode, n0}]*/
	protected HashMap<String, Nyquence> nyquect;
	protected Nyquence n0;

	public DBSynsactBuilder(String conn, IUser robot)
			throws SQLException, SAXException, IOException, TransException {
		this(conn,
			new SynSubsMeta(conn),
			new SynChangeMeta(conn),
			new NyquenceMeta(conn), robot);
	}
	
	public DBSynsactBuilder(String conn, SynSubsMeta subm, SynChangeMeta chgm, NyquenceMeta nyqm, IUser robot)
			throws SQLException, SAXException, IOException, TransException {
		super ( new DBSyntext(conn,
			    initConfigs(conn, loadSemantics(conn),
						(c) -> new SynmanticsMap(c)),
				robot, runtimepath));

		this.subm = subm != null ? subm : new SynSubsMeta(conn);
		this.chgm = chgm != null ? chgm : new SynChangeMeta(conn);
		this.nyqm = nyqm != null ? nyqm : new NyquenceMeta(conn);
		
		AnResultset rs = ((AnResultset) select(nyqm.tbl)
				.rs(instancontxt(conn, robot))
				.rs(0))
				.nxt();
		
		if (rs == null) {
			// register myself, wait for others to join
			// - this will minimize unknown records transit ?
			// TODO insert synode
		}
		
		this.synode = rs.getString(nyqm.synode);
		this.nyquect = toNyquect(rs);
		this.n0 = nyquect.get(synode);
	}
	
	/**
	 * Create a basic sync-builder, without semantics.
	 * 
	 * @param tsx
	 * @throws SemanticException 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws SQLException 
	 */
	public DBSynsactBuilder(Transcxt tsx) throws SemanticException, SQLException, SAXException, IOException {
		super(tsx.basictx().connId());
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

	/**
	 * Get DB record change's subscriptions.
	 * 
	 * @param conn
	 * @param uids
	 * @param entm
	 * @param robot
	 * @return results with count's field named as 'cnt'
	 * @throws TransException
	 * @throws SQLException
	 */
	public AnResultset subscripts(String conn, String uids, SyntityMeta entm, IUser robot)
			throws TransException, SQLException {
		return (AnResultset) select(subm.tbl, "ch")
				.cols(subm.cols())
				.col(count(subm.subs), "cnt")
				.whereEq(subm.entbl, entm.tbl)
				.whereEq(subm.uids, uids)
				.rs(instancontxt(conn, robot))
				.rs(0);
	}
	

	public Nyquence nyquence(String conn, String org, String synid, String entity)
			throws SQLException, TransException {
		return new Nyquence(((AnResultset) select(nyqm.tbl)
				.col(nyqm.nyquence, "n")
				.whereEq(nyqm.entbl, entity)
				.whereEq(nyqm.org(), org)
				.whereEq(nyqm.synode, synid)
				.rs(instancontxt(conn, dummy))
				.rs(0))
				.nxt()
				.getInt("n"));
	}

	/**
	 * nyquence += inc;<br>
	 * inc = 0;
	 * 
	 * @param conn
	 * @param synid
	 * @param entity
	 * @param usr
	 * @return affected row count
	 * @throws TransException
	 * @throws SQLException
	 */
	public int incNyquence(String conn, String synid, String entity, IUser usr)
			throws TransException, SQLException {
		return update(nyqm.tbl, usr)
			.nv(nyqm.nyquence, select(nyqm.tbl).col(add(nyqm.nyquence, nyqm.inc)))
			.nv(nyqm.inc, 0)
			.whereEq(nyqm.entbl, entity)
			.whereEq(nyqm.org(), usr.orgId())
			.whereEq(nyqm.synode, synid)
			.u(instancontxt(conn, usr))
			.total()
			;
	}

	public void addSynode(String conn, Synode node, IUser robot)
			throws TransException, SQLException {
		node.insert(synm, insert(synm.tbl, robot))
			.ins(this.instancontxt(conn, robot));
	}

	public SynEntity loadEntity(String eid, String conn, IUser usr, SyntityMeta phm)
			throws TransException, SQLException {
		AnResultset ent = (AnResultset)select(phm.tbl, "ch")
				.whereEq(phm.pk, eid)
				.rs(instancontxt(conn, usr))
				.rs(0);

		AnResultset subs = (AnResultset)select(chgm.tbl, "ch")
				.je("ch", subm.tbl, "sb", chgm.uids, subm.uids, chgm.org, subm.org)
				.whereEq("ch", chgm.entbl, phm.tbl)
				.whereEq("sb", subm.entbl, phm.tbl)
				.whereEq(chgm.entfk, eid)
				.rs(instancontxt(conn, usr))
				.rs(0);

		SynEntity entA = new SynEntity(ent, phm, chgm, subm);
		return entA.format(subs);
	}

	public AnResultset entities(SyntityMeta phm, String connId, IUser usr)
			throws TransException, SQLException {
		return (AnResultset)select(phm.tbl, "ch")
				.rs(instancontxt(connId, usr))
				.rs(0);
	}

	/**
	 * Collect Nyquense vector.
	 * @param phm
	 * @param synode
	 * @param connId
	 * @param robot
	 * @return e.g. Nyguenses
	 */
	public AnResultset tobegin(SyntityMeta phm, String synode, String connId, IUser robot) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * FIXME not correct if
	 * 
	 * Check change differences,
	 * e.g, with dst.n, check is there any changes for dst, with dst.n. 
	 * 
	 * @return true if has changes
	 * @throws SQLException 
	 * @throws TransException 
	private boolean shouldExchange(SynChangeMeta chm, String conn, IUser robot)
			throws TransException, SQLException {
		AnResultset chs = ((AnResultset) select(chm.tbl, "ch")
				.col(String.format("count(%s)", chm.nyquence), "cnt")
				.rs(instancontxt(conn, robot))
				.rs(0)).nxt();
		
		return chs.getInt("cnt") > 0;
	}
	 */

	/**
	 * Client/slave initiate a change logs exchange
	 * @return change logs
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public AnResultset toexchange(IUser robot, String destnode)
			throws TransException, SQLException {

		AnResultset chs = (AnResultset)select(chgm.tbl, "ch")
				.whereEq(chgm.synoder, destnode)
				.rs(instancontxt(this.getSysConnId(), robot))
				.rs(0);

		return chs;
	}

	/**
	 * Server/hub handle a change-logs' exchange
	 * 
	 * @param srcnode source node id
	 * @param sn source Nyquence
	 * @param srchgs change logs
	 * @param localbuf [in/out] local commitment buffer 
	 * @return change logs to reply
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public ChangeLogs onexchange(String srcnode, Nyquence sn, AnResultset srchgs, IUser robot, List<ChangeLogs> localbuf)
			throws TransException, SQLException {
		
		if (isNull(srchgs)) return null;

		srchgs.beforeFirst();
		// long sn0 = srchgs.hasnext() ? srchgs.getLong(chgm.nyquence) : 0;
		long sn1 = srchgs.getLongAtRow(chgm.nyquence, srchgs.getRowCount() - 1);
		
		// select order by n, s
		AnResultset dchgs = (AnResultset) select(chgm.tbl)
				.rs(instancontxt(this.basictx.connId(), robot))
				.rs(0);
		
		ChangeLogs localog = new ChangeLogs(chgm);
		ChangeLogs remolog = new ChangeLogs(chgm);
		
		long srcn1 = nyquect.get(srcnode).n;
		// in case unfinished previous synchronizing
		// i.e. B haven't got last acknowledge from A since buffer is not empty 
		while (localbuf.size() > 0) {
			if (Nyquence.compareNyq(srchgs.getLong(chgm.nyquence), srcn1) > 0)
				commitUntil(localbuf, srcnode, srcn1);
			// i.e. B has operation haven't committed yet since A failed acknowledge
			else
				ignoreUntil(localbuf, srcnode, srcn1);
		}
		srchgs.beforeFirst();

		boolean hasmore = dchgs.next() && srchgs.next();
		while (hasmore) {
			// src - dst
			int diff = compare(srchgs, dchgs);
			// int diff = Nyquence.compareNyq(srchgs.getLong(chgm.nyquence), dchgs.getLong(chgm.nyquence));
			if (diff == 0) {
				// changes propagated to both sides through 3rd parties.
				if (indexOf(srchgs.getString(chgm.subs), this.synode) >= 0) {
					// insertion propagation
					remolog.remove_sub(srchgs, this.synode); // e.g. "I A A:0 1 B"
				}
				// e.g. I A A:0 1 C/D
			}
			else if (diff < -1) {
				// delete propagation
				remolog.remove(srchgs);
			}
			else if (diff < 0) {
				// e.g. I A A:0 1 is missing at B
				localog.append(srchgs);
				hasmore = srchgs.next();
			}
			else if (diff <= 1) {
				// e.g. I B B:0 1 is missing at A
				remolog.append(dchgs);
				hasmore = dchgs.next();
			}
			else // diff == 2
				localog.remove_sub(dchgs, this.synode);

			hasmore = srchgs.next() && dchgs.next();
		}

		while(srchgs.hasnext()) {
			int diff = compare(srchgs, this.n0);
			if (diff >= 0)
				throw new SemanticException("Shouldn't be here");
			else if (diff == -1)
				localog.append(srchgs);
			else // diff == -2
				remolog.remove(srchgs);

			srchgs.next();
		}
		while(dchgs.hasnext()) {
			remolog.append(dchgs);

			int diff = compare(sn1, dchgs);
			if (diff == 0)
				break;
			else if (diff < 0)
				throw new SemanticException("Shouldn't be here");
			else if (diff == -1)
				remolog.append(srchgs);
			else // diff == -2
				remolog.remove(srchgs);

			localog.append(srchgs);
			dchgs.next();
	
		}
		
		// FIXME what happen if local committed and remote lost response?
		// commit(localog);
		localbuf.add(localog);

		return remolog;
	}

	private int compare(long sn0, AnResultset dchgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private int compare(AnResultset srchgs, Nyquence n02) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * Compare source and destination record, using both current row.
	 * @param ini initiator
	 * @param ack acknowledger
	 * @return
	 * -2: ack to ini deletion propagation,
	 * -1: ack to ini appending,
	 * 0: needing to be merged (same record),
	 * 1: ini to ack appending
	 * 2: ini to ack deletion propagation
	 * @throws SQLException 
	 */
	int compare(AnResultset ini, AnResultset ack) throws SQLException {
		long sn = ini.getLong(chgm.nyquence);
		long dn = ack.getLong(chgm.nyquence);
		int diff = Nyquence.compareNyq(sn, dn);
		if (diff == 0)
			return 0;
		
		return (int) Math.min(2, Math.max(-2, (sn - dn)));
	}

	DBSynsactBuilder ignoreUntil(List<ChangeLogs> commitBuff, String srcnode, long srcn1) {
		return this;
	}

	DBSynsactBuilder commitUntil(List<ChangeLogs> commitBuff, String srcnode, long untilN0) {
		List<Statement<?>> stats = new ArrayList<Statement<?>>();
		for (ChangeLogs l : commitBuff) {
			for (Object [] c : l.changes) {
				if (Nyquence.compareNyq(ChangeLogs.parseNyq(c).n, untilN0) <= 0)
					break;
				stats.add(eq((String)c[0], CRUD.C)
					? insert(chgm.tbl)
							.nv(chgm.crud, (String)c[1])
							.nv(chgm.synoder, (String)c[2])
							.nv(chgm.uids, (String)c[3])
					: eq((String)c[0], CRUD.U)
					? update(chgm.tbl)
						.nv(chgm.subs, removele((String)c[2], (String)c[4]))
						.whereEq(chgm.synoder, (String)c[2])
						.whereEq(chgm.uids, (String)c[3])
					: delete(chgm.tbl)
						.whereEq(chgm.synoder, (String)c[2])
						.whereEq(chgm.uids, (String)c[3]));
			}
		}
		return this;
	}

	public static HashMap<String, Nyquence> toNyquect(AnResultset schgs) {
		// TODO Auto-generated method stub
		return null;
	}
}
