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
	protected SyntityMeta entm;
	protected SynChangeMeta chgm;

	public DBSynsactBuilder(String conn, SyntityMeta entity, SynChangeMeta change, SynSubsMeta subs)
			throws SQLException, SAXException, IOException, SemanticException {
		super(conn);
		this.subm = subs;
		this.entm = entity;
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
	 * Get entity record's subscriptions.
	 * 
	 * @param uid
	 * @param robot
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public AnResultset subscripts(String conn, String uid, IUser robot) throws TransException, SQLException {
		return (AnResultset) select(subm.tbl, "ch")
				.cols(subm.cols())
				.col(Funcall.count(subm.subs), "cnt")
				.whereEq(subm.entbl, entm.tbl)
				.whereEq(subm.uids, uid)
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

}
