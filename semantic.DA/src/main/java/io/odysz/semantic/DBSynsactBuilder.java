package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

import org.xml.sax.SAXException;

import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

public class DBSynsactBuilder extends DATranscxt {

	public DBSynsactBuilder(String conn) throws SQLException, SAXException, IOException, SemanticException {
		super(conn);
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

}
