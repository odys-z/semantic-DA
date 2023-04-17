package io.odysz.semantic.docsync;

import static io.odysz.semantic.CRUD.*;
import static io.odysz.semantic.docsync.ZSUNodesDA.*;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import io.odysz.anson.x.AnsonException;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DBSynsactBuilder;
import io.odysz.semantic.DBSyntextTest;
import io.odysz.semantic.DBSyntextTest.Ck;
import io.odysz.semantic.DocUtils;
import io.odysz.semantic.T_PhotoMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Update;
import io.odysz.transact.x.TransException;

public class ShareDocTest {
	
	static DBSynsactBuilder trb;

	static SynodeMeta sym;
	static SynChangeMeta chm;
	static SynSubsMeta sbm;
	static T_PhotoMeta phm;

	/**
	 * <p>kyiv local -&gt; kyiv (hub) -&gt; kharkiv</p>
	 * 
	 * Test {@link SyncWorker}' pushing (synchronizing 'pub') with album-jserv as sync hub (8081).
	 * 
	 * @throws TransException 
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws AnsonException 
	 * @throws GeneralSecurityException 
	 * @throws SsException 
	 * @throws SAXException 
	 * @throws ClassNotFoundException 
	 * 
	 */
	@Test
	void testShareBykyiv() throws 
		AnsonException, SQLException, IOException, TransException, SAXException, ClassNotFoundException {

		String clientpath = anDevice.png;

		// 0. clean failed tests
		// clean(worker, clientpath);

		// 1. manage a local file at device
		T_Photo photo = new T_Photo().create(clientpath);

		// 2. synchronize to cloud hub ( hub <- device )
		String pid = createPhoto(kyiv.conn, photo, kyiv.robot, new T_PhotoMeta(trb.getSysConnId()));

		// 2.1 verify the sharing tasks been created
		Ck c3 = new DBSyntextTest.Ck(anDevice.conn, trb, anDevice.worker, anDevice.userId);
		c3.change(C, pid, phm);
		c3.subs(pid, kyiv.nodeId, kharkiv.nodeId);

		// 2.2 device vs kyiv
		DBSyntextTest.sync(anDevice.sid, kyiv.sid);
	
		// 2.3 kyiv vs kharkiv
		DBSyntextTest.sync(anDevice.sid, kyiv.sid);

		// 3. verify changes at kharkiv
		// 3.1 subscriptions cleared
		ArrayList<String> pids = kharkiv.ck.entities(anDevice.nodeId, anDevice.png);
		kharkiv.ck.change(C, pids.get(0), phm);
		kharkiv.ck.subs(pids.get(0), -1);

		// 3.2 verify files
		kharkiv.ck.verifile(anDevice.nodeId, anDevice.png);
	}

	/**
	 * Simulates the processing of Albums.createFile(), creating a stub photo and having syncflag updated.
	 * 
	 * @see DocUtils#createFileB64(String, SyncDoc, IUser, DocTableMeta, DATranscxt, Update)
	 * @see Docsyncer#onDocreate(SyncDoc, DocTableMeta, IUser)
	 * @param conn
	 * @param photo
	 * @param usr
	 * @param meta table meta of h_photes
	 * @return doc id, e.g. pid
	 * @throws TransException
	 * @throws SQLException
	 * @throws IOException 
	 */
	String createPhoto(String conn, T_Photo photo, SyncRobot usr, T_PhotoMeta meta)
			throws TransException, SQLException, IOException {

		if (!DATranscxt.hasSemantics(conn, meta.tbl, smtype.extFilev2))
			throw new SemanticException("Semantics of ext-file2.0 for h_photos.uri can't be found");
		
		// Update post = DBSyncher.onDocreate(photo, meta, usr);
		return DocUtils.createFileB64(conn, photo, usr, meta, trb, null);
	}

}
