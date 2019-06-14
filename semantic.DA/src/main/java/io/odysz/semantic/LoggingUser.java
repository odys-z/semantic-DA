package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.semantic.util.SQLString;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**This robot handle logs of table a_log()
 * @author odys-z@github.com
 */
public class LoggingUser implements IUser {

	private DATranscxt logSemantic;
	private String uid;
	private SemanticObject action;
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
				@Override public ArrayList<String> dbLog(ArrayList<String> sqls) { return null; }
				@Override public String uid() { return "dummy"; }
				@Override public String get(String prop) { return "prop"; }
				@Override public IUser logAct(String funcName, String funcId) { return null; }
			};
		
		try {
			// DATranscxt.initConfigs(logConn, "src/test/res/semantic-log.xml");
			DATranscxt.loadSemantics(logConn, logCfgPath);

			logSemantic = new DATranscxt(logConn); //, DATranscxt.meta(logConn)); 
		} catch (SAXException | IOException | SemanticException | SQLException e) {
			e.printStackTrace();
		} 
	}

	@Override
	public String uid() { return uid; }

	@Override
	public String get(String prop) { return "prop"; }

	@Override
	public IUser set(String prop, Object v) { return this; }

	@Override
	public ArrayList<String> dbLog(ArrayList<String> sqls) {
		return genLog(logSemantic, sqls, this,
				action.getString("funcName"),
				action.getString("funcId"));
	}
	
	public static ArrayList<String> genLog(DATranscxt logBuilder, ArrayList<String> sqls,
			IUser commitUser, String funcName, String funcId) {
		// no exception can be thrown here, no error message for client if failed.
		try {
			// String newId = DASemantext.genId(Connects.defltConn(), "a_logs", "logId", null);
			// String sql = DatasetCfg.getSqlx(Connects.defltConn(), "log-template",
			//	// insert into a_logs(logId, oper, funcName, funcId, cmd, url, operDate, txt)
			//	// values ('%s', '%s', '%s', '%s', null, '%s', sysdate, '%s');
			//	newId, uid, funcName, funcId, cmd, url, String.valueOf(sqls.size()), txt(sqls));

			logBuilder.insert("a_logs", dumbUser) // dummy for stop recursive logging
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

	@Override public void touch() { }

	@Override
	public SemanticObject logout() { return null; }

	@Override
	public void writeJsonRespValue(Object writer) throws IOException { }

	@Override
	public IUser logAct(String funcName, String funcId) {
		set("funcName", funcName);
		set("funcId", funcId);
		return this;
	}

}
