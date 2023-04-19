package io.odysz.semantic.syn;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

import org.xml.sax.SAXException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

public class DBSynsactBuilder extends DATranscxt {

	protected SynodeMeta synm;
	protected SynSubsMeta subm;
	// protected SyntityMeta entm;
	protected SynChangeMeta chgm;

	public DBSynsactBuilder(String conn, SynChangeMeta change, SynSubsMeta subs)
			throws SQLException, SAXException, IOException, SemanticException {
		super(conn);
		this.subm = subs;
		// this.entm = entity;
		this.chgm = change;
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

	private HashMap<String, DBSynmantics> loadSynmatics(String connId) throws SAXException, IOException {
		return null;
	}

	public static HashMap<String, DBSynmantics> loadSynmantics(String connId, String cfgpath, boolean debug)
			throws SAXException, IOException, SQLException, SemanticException {

		HashMap<String, DBSynmantics> syns = new HashMap<String, DBSynmantics>();
		return syns;
	}

	/**
	 * Get DB record change's subscriptions.
	 * 
	 * @param entId
	 * @param entm 
	 * @param robot
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public AnResultset subscripts(String conn, String entId, SyntityMeta entm, IUser robot)
			throws TransException, SQLException {
		return (AnResultset) select(subm.tbl, "ch")
				.cols(subm.cols())
				.col(Funcall.count(subm.subs), "cnt")
				.whereEq(subm.entbl, entm.tbl)
				.whereEq(subm.entId, entId)
				.rs(instancontxt(conn, robot))
				.rs(0);
	}

	public Nyquence nyquence(String conn) {
		return new Nyquence(0);
	}

	public void addSynode(String conn, Synode node, IUser robot) throws TransException, SQLException {
		node.insert(synm, insert(synm.tbl, robot))
			.ins(this.instancontxt(conn, robot));
	}

	public SynEntity loadEntity(String eid, String conn, IUser usr, SyntityMeta phm)
			throws TransException, SQLException {
		AnResultset ents = ((DBSyntext) instancontxt(conn, usr))
			.entities(phm, eid);

		AnResultset subs = (AnResultset)select(chgm.tbl, "ch")
				.je("ch", subm.tbl, "sb", chgm.entfk, subm.entId)
				.whereEq("ch", chgm.entbl, phm.tbl)
				.whereEq("sb", subm.entbl, phm.tbl)
				.whereEq(chgm.entfk, eid)
				.rs(instancontxt(conn, usr))
				.rs(0);

		SynEntity entA = new SynEntity(ents, phm, chgm, subm);
		String skip = entA.synode();
		entA.format(ents);

		return entA;
	}
}
