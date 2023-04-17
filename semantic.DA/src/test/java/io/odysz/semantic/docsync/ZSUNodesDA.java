package io.odysz.semantic.docsync;

import io.odysz.semantic.DBSyntextTest;
import io.odysz.semantic.DBSyntextTest.Ck;

/**
 * Consts for testing.
 * @author odys-z@github.com
 */
public class ZSUNodesDA {
	public final String folder;
	public String png;

	public final int sid;
	public final String conn;
	public final SyncRobot robot;
	public final String nodeId;
	public final T_SynodeMode mode = T_SynodeMode.hub;

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

//	public static class Kyiv {
//		public static final String folder = "zsu-kyiv";
//		public static int sid;
//		public static final String conn;
//		public static SyncRobot robot;
//		public static final String nodeId;
//		public static final T_SynodeMode mode = T_SynodeMode.hub;
//
//		public static final String worker;
//		
//		static {
//			sid = 0;
//			conn = DBSyntextTest.conns[sid];
//			robot = (SyncRobot) DBSyntextTest.c[sid].robot;
//			nodeId = robot.deviceId;
//			worker = robot.uid();
//		}
//	}
//
//	public static class Kharkiv {
//		public static final String folder = "zsu-kharkiv";
//		public static int sid;
//		public static final String conn = DBSyntextTest.conns[1];
//		public static final T_SynodeMode mode = T_SynodeMode.main;
//		public static SyncRobot robot;
//		public static final String nodeId;
//		public static final String worker;
//		
//		static {
//			sid = 0;
//			robot = (SyncRobot) DBSyntextTest.c[sid].robot;
//			nodeId = robot.deviceId;
//			worker = robot.uid();
//		}
//	}
//
//	public static class AnDevice {
//		public static final String png = "src/test/res/zsu-device/ukraine.png";
//		public static final String folder = "zsu-dev";
//		public static final String conn = DBSyntextTest.conns[3];
//		public static final T_SynodeMode mode = T_SynodeMode.device;
//		public static SyncRobot robot;
//		public static final String device;
//		public static final String worker;
//		public static final String userId;
//		
//		static {
//			robot = (SyncRobot) DBSyntextTest.c[3].robot;
//			device = robot.deviceId;
//			worker = robot.uid();
//			userId = "syn-3";
//		}
//	}
}
