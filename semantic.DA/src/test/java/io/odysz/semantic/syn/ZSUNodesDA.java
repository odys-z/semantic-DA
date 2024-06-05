package io.odysz.semantic.syn;

import java.io.IOException;
import java.sql.SQLException;

import org.xml.sax.SAXException;

import io.odysz.semantic.syn.DBSyntextTest.Ck;
import io.odysz.transact.x.TransException;

/**
 * Consts for testing.
 * @author odys-z@github.com
 */
public class ZSUNodesDA {
	public static final String family = "ZSU";
	
	public final String folder;
	public String png;

	public final int sid;
	public final String conn;
	public final SyncRobot robot;
	public final String nodeId;
	public final SynodeMode mode = SynodeMode.hub;

	public final String worker;
	public String userId;
	public final Ck ck;
	
	public static ZSUNodesDA kyiv;
	public static ZSUNodesDA kharkiv;
	public static ZSUNodesDA anDevice;
	
	static void init() throws ClassNotFoundException, SQLException, TransException, IOException, SAXException {
		kyiv = new ZSUNodesDA(0);
		kharkiv = new ZSUNodesDA(1);
		anDevice  = new ZSUNodesDA(3);
		anDevice.png = "src/test/res/zsu-device/ukraine.png";
		anDevice.userId = "user-3";
	}

	public ZSUNodesDA(int sid) throws ClassNotFoundException, SQLException, TransException, IOException, SAXException {
		this.sid = sid;
		folder = "zsu-" + sid;
		conn = DBSyntextTest.conns[sid];
		ck = new Ck(sid, new DBSynsactBuilder(null, "zsu-dev-" + sid, "zsu", 0), "zsu");
		robot = (SyncRobot) ck.robot();
		nodeId = robot.deviceId;
		worker = robot.uid();
	}
}
