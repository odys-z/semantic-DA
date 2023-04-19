package io.odysz.semantic;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SessionInf;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * A robot is only used for test.
 * 
 * @author odys-z@github.com
 */
public class SyncTestRobot implements IUser {

	protected long touched;
	protected String userId;
	protected String userName;

	protected String orgId;
	public String orgId() { return orgId; }
	public SyncTestRobot orgId(String org) {
		orgId = org;
		return this;
	}

	protected String deviceId;
	public String deviceId() { return deviceId; }

	protected String ssid;

	protected Set<String> tempDirs;
	public String orgName;
	public SyncTestRobot orgName (String org) {
		orgName = org;
		return this;
	}

	public SyncTestRobot(String userid) {
		this.userId = userid;
	}

	/**
	 * for jserv construction
	 * 
	 * @param userid
	 * @param pswd
	 * @param userName
	 */
	public SyncTestRobot(String userid, String pswd, String userName) {
		this.userId = userid;
		this.userName = userName;
	}
	
	public static class RobotMeta extends TableMeta {
		String device;
		String iv;
		public RobotMeta(String tbl, String... conn) {
			super(tbl, conn);

			iv = "iv";
			device = "device";
		}
	}

	@Override
	public TableMeta meta() { return new RobotMeta("a_users"); }

	@Override
	public ArrayList<String> dbLog(ArrayList<String> sqls) throws TransException { return null; }

	@Override public boolean login(Object request) throws TransException { return true; }

	@Override
	public IUser touch() {
		touched = System.currentTimeMillis();
		return this;
	} 

	@Override public long touchedMs() { return touched; } 

	@Override public String uid() { return userId; }

	@Override public void writeJsonRespValue(Object writer) throws IOException { }

	@Override public IUser logAct(String funcName, String funcId) { return this; }

	@Override public String sessionId() { return ssid; }

	@Override public IUser sessionId(String ssid) { this.ssid = ssid; return this; }

	@Override public IUser notify(Object note) throws TransException { return this; }

	@Override public List<Object> notifies() { return null; }

	/**
	 * Get a temp dir, and have it deleted when logout.
	 * 
	 * @param conn
	 * @param tablPhotos 
	 * @return the dir
	 * @throws SemanticException
	 */
	public String touchTempDir(String conn, String tablPhotos) throws SemanticException {

		String extroot = ((ShExtFilev2) DATranscxt
						.getHandler(conn, tablPhotos, smtype.extFilev2))
						.getFileRoot();

		String tempDir = IUser.tempDir(extroot, userId, "uploading-temp", ssid);
		if (tempDirs == null)
			tempDirs= new HashSet<String>(1);
		tempDirs.add(tempDir);
		return tempDir;
	}

//	public String defaultAlbum() {
//		return "a-001";
//	}

	public SessionInf sessionInf() {
		return new SessionInf().device(deviceId);
	}

	public SyncTestRobot device(String dev) {
		deviceId = dev;
		return this;
	}
}
