package io.odysz.semantic.syn;

import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.io.IOException;
import java.sql.SQLException;

import org.xml.sax.SAXException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.meta.NyquenceMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.transact.x.TransException;

public class DBSynsactBuilder extends DATranscxt {

	protected SynodeMeta synm;
	protected NyquenceMeta nyqm;
	protected SynSubsMeta subm;
	protected SynChangeMeta chgm;

	public DBSynsactBuilder(String conn)
			throws SQLException, SAXException, IOException, TransException {
		super(conn);
		this.subm = new SynSubsMeta(conn);
		this.chgm = new SynChangeMeta(conn);
		this.nyqm = new NyquenceMeta(conn);
	}

	@Override
	public ISemantext instancontxt(String connId, IUser usr) throws TransException {
		try {
			return new DBSyntext(connId, loadSynmatics(connId),
				Connects.getMeta(connId), usr, runtimepath);
		} catch (SemanticException | SQLException | SAXException | IOException e) {
			e.printStackTrace();
			throw new TransException(e.getMessage());
		}
	}

//	private HashMap<String, DBSynmantics> loadSynmatics(String connId) throws SAXException, IOException {
//		return super.loadSemantics(connId, connId, getSysDebug());
//	}

//	public static HashMap<String, DBSynmantics> loadSynmantics(String connId, String cfgpath, boolean debug)
//			throws SAXException, IOException, SQLException, SemanticException {
//
//		HashMap<String, DBSynmantics> syns = new HashMap<String, DBSynmantics>();
//		return syns;
//	}

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

	public Nyquence nyquence(String conn, String org, String synid, String entity) throws SQLException, TransException {
		return new Nyquence(((AnResultset) select(nyqm.tbl)
				.col(nyqm.nyquence, "n")
				.whereEq(nyqm.entbl, entity)
				.whereEq(nyqm.org, org)
				.whereEq(nyqm.synode, synid)
				.rs(instancontxt(conn, dummy))
				.rs(0))
				.nxt()
				.getInt("n"));
	}

	public void addSynode(String conn, Synode node, IUser robot) throws TransException, SQLException {
		node.insert(synm, insert(synm.tbl, robot))
			.ins(this.instancontxt(conn, robot));
	}

	public SynEntity loadEntity(String eid, String conn, IUser usr, SyntityMeta phm)
			throws TransException, SQLException {
		AnResultset ents = entity(phm, eid);

		AnResultset subs = (AnResultset)select(chgm.tbl, "ch")
				.je("ch", subm.tbl, "sb", chgm.uids, subm.uids, chgm.org, subm.org)
				.whereEq("ch", chgm.entbl, phm.tbl)
				.whereEq("sb", subm.entbl, phm.tbl)
				.whereEq(chgm.entfk, eid)
				.rs(instancontxt(conn, usr))
				.rs(0);

		SynEntity entA = new SynEntity(ents, phm, chgm, subm);
		String skip = entA.synode();
		entA.format(subs);

		return entA;
	}

	protected AnResultset entity(SyntityMeta phm, String eid) {
		return null;
	}
}
