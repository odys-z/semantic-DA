package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.xml.sax.SAXException;

import io.odysz.semantic.util.SQLString;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.transact.x.TransException;

public class LoggingUser implements IUser {

	private DATranscxt logSemantic;
	private String uid;
	private SemanticObject action;
	public static IUser dumbUser;

	public LoggingUser(String userId, SemanticObject action) {
		this.uid = userId;
		this.action = action;

		dumbUser = new IUser() {
				@Override public ArrayList<String> dbLog(ArrayList<String> sqls) { return null; }
				@Override public boolean login(Object req) throws TransException { return false; }
				@Override public String sessionId() { return null; }
				@Override public void touch() { }
				@Override public String uid() { return "dummy"; }
				@Override public String get(String prop) { return "prop"; }
				@Override public IUser set(String prop, Object v) { return this; }
				@Override public SemanticObject logout() { return null; }
				@Override public void writeJsonRespValue(Object writer) throws IOException { }
			};
		
		ISemantext semt;
		try {
			HashMap<String, DASemantics> ss = DATranscxt.initConfigs(DASemantextTest.connId, "src/test/res/semantic-log.xml"); 
			semt = new DASemantext(DASemantextTest.connId, ss, this);
			logSemantic = new DATranscxt(semt); 
		} catch (SAXException | IOException e) {
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
	
	public static ArrayList<String> genLog(DATranscxt logSemantic, ArrayList<String> sqls,
			IUser commitUser, String funcName, String funcId) {
		// no exception can be thrown here, no error message for client if failed.
		try {
			// String newId = DASemantext.genId(Connects.defltConn(), "a_logs", "logId", null);
			// String sql = DatasetCfg.getSqlx(Connects.defltConn(), "log-template",
			//	// insert into a_logs(logId, oper, funcName, funcId, cmd, url, operDate, txt)
			//	// values ('%s', '%s', '%s', '%s', null, '%s', sysdate, '%s');
			//	newId, uid, funcName, funcId, cmd, url, String.valueOf(sqls.size()), txt(sqls));

			logSemantic.insert("a_logs", dumbUser) // dummy for stop recursive logging
				.nv("oper", commitUser.uid())
				.nv("funcName", funcName)
				.nv("funcId", funcId)
//				.nv("funcName", commitUser.action().getString("funcName"))
//				.nv("funcId", commitUser.action().getString("funcId"))
				// also sql count if the column exists
				// .nv("sqlCount", sqls.size())
				.nv("txt", txt(sqls))
				.ins(logSemantic.basictx());
		} catch (SQLException e) {
			// failed case must be a bug - commitLog()'s exception already caught.
			e.printStackTrace();
		} catch (TransException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static String txt(ArrayList<String> sqls) {
		return sqls == null ? null :
			// FIXME performance problem here
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

}
