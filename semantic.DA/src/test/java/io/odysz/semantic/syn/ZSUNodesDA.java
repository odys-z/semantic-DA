package io.odysz.semantic.syn;

import io.odysz.semantic.syn.DBSyntextTest.Ck;

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
	
	static {
		kyiv = new ZSUNodesDA(0);
		kharkiv = new ZSUNodesDA(1);
		anDevice  = new ZSUNodesDA(3);
		anDevice.png = "src/test/res/zsu-device/ukraine.png";
		anDevice.userId = "user-3";
	}

	public ZSUNodesDA(int sid) {
		this.sid = sid;
		folder = "zsu-" + sid;
		conn = DBSyntextTest.conns[sid];
		ck = DBSyntextTest.c[sid]; 
		robot = (SyncRobot) ck.robot;
		nodeId = robot.deviceId;
		worker = robot.uid();
	}
}
