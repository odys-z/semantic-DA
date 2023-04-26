package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.split;
import static io.odysz.transact.sql.parts.condition.Funcall.add;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.io.IOException;
import java.sql.SQLException;

import org.xml.sax.SAXException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DASemantics.SemanticHandler;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.meta.NyquenceMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
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

		public static SemanticHandler parseHandler(Transcxt basicTsx, String tabl, smtype semantic,
				String recId, String argstr, boolean ... debug)
				throws SemanticException {
			if (smtype.synChange == semantic)
				return new DBSynmantics.ShSynChange(basicTsx, tabl, recId, split(argstr));
			else
				return SemanticsMap.parseHandler(basicTsx, tabl, semantic, recId, argstr, debug);
		}
	}

//	private static SynmanticsMap initConfigs(String conn, XMLTable xcfg)
//			throws SAXException, IOException, SQLException, SemanticException {
//		xcfg.beforeFirst();
//		if (smtConfigs == null)
//			smtConfigs = new HashMap<String, SemanticsMap>();
//
//		Transcxt trb = null;
//		boolean debug = Connects.getDebug(conn);
//		
//		HashMap<String, DASemantics> m = new HashMap<String, DASemantics>(); 
//
//		xcfg.map(
//			(XMLTable t) -> {
//				String tabl = xcfg.getString("tabl");
//				String pk = xcfg.getString("pk");
//				String smtc = xcfg.getString("smtc");
//				String args = xcfg.getString("args");
//				
//				// because the table is not come with pk = tabl, returned value is useless.
//				if (!m.containsKey(tabl))
//					m.put(tabl, new DASemantics(trb, tabl, pk, debug));
//				m.get(tabl).addHandler(
//					SynmanticsMap.parseHandler(trb, tabl, smtype.parse(smtc), pk, args, debug));
//				return null;
//			});
//
//		SynmanticsMap semanticMap = (SynmanticsMap) new SynmanticsMap(conn).map(m);
//		smtConfigs.put(conn, semanticMap);
//		return (SynmanticsMap) smtConfigs.get(conn);
//	}
	
	protected SynodeMeta synm;
	protected NyquenceMeta nyqm;
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;

	public DBSynsactBuilder(String conn)
			throws SQLException, SAXException, IOException, TransException {
		this(conn,
			new SynSubsMeta(conn),
			new SynChangeMeta(conn),
			new NyquenceMeta(conn));
	}
	
	public DBSynsactBuilder(String conn, SynSubsMeta subm, SynChangeMeta chgm, NyquenceMeta nyqm)
			throws SQLException, SAXException, IOException, TransException {
		super(new DBSyntext(conn, initConfigs(conn, loadSemantics(conn), new SemanticsFactory() {
			@Override
			public SemanticsMap ctor(String conn) { return new SynmanticsMap(conn); }
		}), dummy, runtimepath));

		this.subm = subm != null ? subm : new SynSubsMeta(conn);
		this.chgm = chgm != null ? chgm : new SynChangeMeta(conn);
		this.nyqm = nyqm != null ? nyqm : new NyquenceMeta(conn);
	}
	
	@Override
	public ISemantext instancontxt(String conn, IUser usr) throws TransException {
		try {
			return new DBSyntext(conn,
				initConfigs(conn, loadSemantics(conn), (c) -> new SynmanticsMap(conn)),
				usr, runtimepath);
		} catch (SAXException | IOException | SQLException e) {
			e.printStackTrace();
			throw new TransException(e.getMessage());
		}
	}

	/**
	 * Get DB record change's subscriptions.
	 *
	 * @param uid
	 * @param robot
	 * @throws SQLException
	 * @throws TransException
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
}
