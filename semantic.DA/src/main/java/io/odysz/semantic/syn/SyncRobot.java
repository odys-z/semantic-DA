package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.isNull;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import io.odysz.anson.IJsonable;
import io.odysz.common.Utils;
import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.SessionInf;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * A robot is only used for test.
 * 
 * @author odys-z@github.com
 */
public class SyncRobot extends SemanticObject implements IUser, IJsonable {

	protected long touched;
	protected String userId;
	protected String userName;
	protected String pswd;
	protected String iv;

	protected String orgId;
	public String orgId() { return orgId; }

	public SyncRobot orgId(String org) {
		orgId = org;
		return this;
	}


	public SyncRobot domain(String dom) {
		return this;
	}

	protected String deviceId;
	public String deviceId() { return deviceId; }
	public SyncRobot deviceId(String devid) {
		deviceId = devid;
		return this;
	}

	protected String ssid;

	protected Set<String> tempDirs;
	public String orgName;
	public SyncRobot orgName (String org) {
		orgName = org;
		return this;
	}

	private Syndomanager syndomanager;

	public SyncRobot(String userid, String pswd) {
		this.userId = userid;
		this.pswd   = pswd;
	}

	/**
	 * Costructor for jserv construction
	 * 
	 * @param userid
	 * @param pswd
	 * @param userName
	 */
	public SyncRobot(String userid, String pswd, String userName, String device) {
		this.userId = userid;
		this.userName = userName;
		this.deviceId = device;
		this.pswd = pswd;
	}

	/**
	 * @param userid
	 * @param pswd
	 * @param userName
	 */
	public SyncRobot(String userid, String pswd, String userName) {
		this(userid, pswd, userName, null);
	}
	
	public SyncRobot(SessionInf rob, String pswd) {
		this(rob.uid(), rob.userName(), rob.device, pswd);
	}

	public SyncRobot() {
		this.userId = "to be init";
	}

	public static class RobotMeta extends TableMeta {
		public final String device;
		public final String iv;

		public RobotMeta(String tbl, String... conn) {
			super("a_users", conn);
			pk = "userId";
			iv = "iv";
			device = "device";
		}
	}

	@Override
	public TableMeta meta(String ... connId) throws SQLException, TransException {
		return new RobotMeta("a_users")
				.clone(Connects.getMeta(
				isNull(connId) ? null : connId[0], "a_users"));
	}

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

	@Override public String pswd() { return pswd; }

	@Override public void writeJsonRespValue(Object writer) throws IOException { }

	@Override public IUser logAct(String funcName, String funcId) { return this; }

	@Override public String sessionId() { return ssid; }

	@Override public IUser sessionId(String ssid) { this.ssid = ssid; return this; }

	@Override public IUser notify(Object note) throws TransException { return this; }

	@Override public List<Object> notifies() { return null; }

	@Override
	public SemanticObject logout() {
		if (tempDirs != null)
		for (String temp : tempDirs) {
			try {
				Utils.logi("Deleting: %s", temp);
				FileUtils.deleteDirectory(new File(temp));
			} catch (IOException e) {
				Utils.warn("Can not delete folder: %s.\n%s", temp, e.getMessage());
			}
		}

		if (syndomanager != null)
			syndomanager.unlock(deviceId, userId);

		return null;
	}

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

	public String defaultAlbum() {
		return "a-001";
	}

	public SessionInf sessionInf() {
		return new SessionInf().device(deviceId);
	}
}
