package io.oz.syn;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import io.odysz.common.Utils;
import io.odysz.semantic.DASemantics.ShExtFilev2;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.SessionInf;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * Synchronizing user for robot and base class of DocUser.
 * This is not a type for session management.
 * 
 * @author odys-z@github.com
 */
public class SyncUser extends SemanticObject implements IUser {

	SyndomContext domx;

	protected long touched;
	protected String userId;
	protected String userName;
	protected String pswd;
	protected String iv;

	protected String domain;

	protected String org;
	public String orgId() { return org; }
	public SyncUser orgId(String org) {
		this.org = org;
		return this;
	}


	public SyncUser domain(String dom) {
		return this;
	}

	protected String deviceId;
	public String deviceId() { return deviceId; }
	public SyncUser deviceId(String devid) {
		deviceId = devid;
		return this;
	}

	protected String ssid;

	protected Set<String> tempDirs;
	public String orgName;
	public SyncUser orgName (String org) {
		orgName = org;
		return this;
	}

	public SyncUser(String userid, String pswd) {
		this.userId = userid;
		this.pswd   = pswd;
	}

	/**
	 * Constructor for jserv synssion instance.
	 * 
	 * @param userid
	 * @param pswd
	 * @param userName
	 */
	public SyncUser(String userid, String pswd, String userName, String device) {
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
	public SyncUser(String userid, String pswd, String userName) {
		this(userid, pswd, userName, null);
	}
	
	public SyncUser(SessionInf rob, String pswd) {
		this(rob.uid(), rob.userName(), rob.device, pswd);
	}

	public SyncUser() {
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
		throw new SemanticException("This method should be overriden by DocUser.");
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

	

	@Override public IUser logAct(String funcName, String funcId) { return this; }

	@Override public String sessionId() { return ssid; }

	@Override public IUser sessionId(String ssid) { this.ssid = ssid; return this; }

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
		if (domx != null)
		domx.unlockx(this);
		return null;
	}

	/**
	 * Get a temp dir, and have it deleted when logout.
	 * 
	 * @param conn
	 * @param doctbl 
	 * @return the dir
	 * @throws SemanticException No {@link smtype#extFilev2}, used for redirecting file root with
	 * environment variable, found for the table, <i>doctbl</i>.
	 */
	public String touchTempDir(String conn, String doctbl) throws SemanticException {
		if (!DATranscxt.hasSemantics(conn, doctbl, smtype.extFilev2))
			throw new SemanticException(
					"Touching temp dir is failed. No smtype.extFilev handler is configured for conn %s, table %s.",
					conn, doctbl);

		String extroot = ((ShExtFilev2) DATranscxt
						.getHandler(conn, doctbl, smtype.extFilev2))
						.getFileRoot();

		String tempDir = IUser.tempDir(extroot, userId, "uploading-temp", ssid);
		if (tempDirs == null)
			tempDirs= new HashSet<String>(1);
		tempDirs.add(tempDir);
		return tempDir;
	}
	
	public SessionInf sessionInf() {
		return new SessionInf().device(deviceId);
	}

	/** SynssionServ */
	public Object synssion;

	/**
	 * @param synssionServ, instance of SynssionServ
	 */
	public void synssion(Object synssionServ) {
		this.synssion = synssionServ;
	}

	@SuppressWarnings("unchecked")
	public <T> T synssion() { return (T) synssion; }

}
