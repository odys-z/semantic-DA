package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.semantic.util.SQLString;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**This robot handle logs of table a_log()
 * @author odys-z@github.com
 */
public class LoggingUser implements IUser {

	private DATranscxt logSemantic;
	private String uid;
	private SemanticObject action;
	@SuppressWarnings("unused")
	private String sessionKey;

	public static IUser dumbUser;

	/**
	 * @param logConn
	 * @param logCfgPath e.g. "src/test/res/semantic-log.xml"
	 * @param userId
	 * @param action
	 */
	public LoggingUser(String logConn, String logCfgPath, String userId, SemanticObject action) {
		this.uid = userId;
		this.action = action;

		dumbUser = new IUser() {
				@Override public TableMeta meta(String ... connId) { return null; }
				@Override public ArrayList<String> dbLog(ArrayList<String> sqls) { return null; }
				@Override public String uid() { return "dummy"; }
				@Override public IUser logAct(String funcName, String funcId) { return this; }
				@Override public String sessionKey() { return null; }
				@Override public IUser sessionKey(String skey) { return null; }
				@Override public IUser notify(Object note) throws TransException { return this; }
				@Override public List<Object> notifies() { return null; }
				@Override public long touchedMs() { return 0; }
			};

		try {
			DATranscxt.loadSemantics(logConn);

			logSemantic = new DATranscxt(logConn); //, DATranscxt.meta(logConn));
		} catch (SAXException | IOException | SemanticException | SQLException e) {
			e.printStackTrace();
		}
	}

	@Override public TableMeta meta(String ... connId) { return null; }

	@Override
	public String uid() { return uid; }

	@Override
	public ArrayList<String> dbLog(final ArrayList<String> sqls) {
		return genLog(logSemantic, "a_logs", sqls, this,
				action.getString("funcName"),
				action.getString("funcId"));
	}

	/**
	 * Generate sqls for db logging.
	 * No exception can be thrown here, no error message for client if failed.
	 * @param logBuilder
	 * @param logTabl
	 * @param sqls
	 * @param commitUser
	 * @param funcName
	 * @param funcId
	 * @return
	 */
	public static ArrayList<String> genLog(DATranscxt logBuilder, String logTabl,
			final ArrayList<String> sqls, IUser commitUser, String funcName, String funcId) {
		try {
			logBuilder.insert(logTabl, dumbUser) // dummy for stop recursive logging
				.nv("oper", commitUser.uid())
				.nv("funcName", funcName)
				.nv("funcId", funcId)
				.nv("cnt", String.valueOf(sqls.size()))
				.nv("txt", txt(sqls))
				.ins(logBuilder.basictx().clone(null)); // Note: must cloned, otherwise there are resulved values.
		} catch (SQLException e) {
			// failed case must be a bug - commitLog()'s exception already caught.
			Utils.warn("Wrong configuration can leads to this failure. Check includes:\n" +
					"config.xml/k=log-connId value, make sure the connection is the correct for the semantics.xml.");
			e.printStackTrace();
		} catch (TransException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static String txt(ArrayList<String> sqls) {
		return sqls == null ? null :
			sqls.stream().map(e -> SQLString.formatSql(e))
				.collect(Collectors.joining(";"));
	}

	@Override
	public boolean login(Object req) throws TransException { return false; }

	@Override
	public String sessionId() { return null; }

	@Override
	public SemanticObject logout() { return null; }

	@Override
	public void writeJsonRespValue(Object writer) throws IOException { }

	@Override
	public IUser logAct(String funcName, String funcId) { return this; }

	@Override
	public IUser sessionKey(String skey) {
		this.sessionKey = skey;
		return this;
	}

	@Override
	public IUser notify(Object note) throws TransException {
		return null;
	}

	@Override
	public List<Object> notifies() { return null; }

	@Override
	public long touchedMs() { return System.currentTimeMillis(); }

	@Override
	public String sessionKey() { return null; }
}
