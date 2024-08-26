//package io.odysz.semantic.syn;
//
///**
// * Consts for testing.
// * @author odys-z@github.com
// */
//public class ZSUNodesDA {
//	/**
//	 * @since 1.4.40, this is the synchronizing domain
//	 */
//	public static final String family = "zsu";
//	
//	public final String folder;
//	public String png;
//
//	public final int sid;
//	public final String conn;
//
//	public String userId;
//	
//	public static ZSUNodesDA kyiv;
//	public static ZSUNodesDA kharkiv;
//	public static ZSUNodesDA anDevice;
//	
//	static void init() throws Exception {
//		kyiv = new ZSUNodesDA(0);
//		kharkiv = new ZSUNodesDA(1);
//		anDevice  = new ZSUNodesDA(3);
//		anDevice.png = "src/test/res/zsu-device/ukraine.png";
//		anDevice.userId = "user-3";
//	}
//
//	public ZSUNodesDA(int sid) throws Exception {
//		this.sid = sid;
//		folder = "zsu-" + sid;
//		conn = DBSyntableTest.conns[sid];
//		// ck = new Docheck(sid, new DBSyntableBuilder(null, "zsu-dev-" + sid, "zsu", SynodeMode.peer));
//		// robot = (SyncRobot) ck.robot();
//		// nodeId = robot.deviceId;
//		// worker = robot.uid();
//	}
//}
