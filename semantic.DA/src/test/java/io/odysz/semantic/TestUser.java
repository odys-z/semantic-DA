package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.xml.sax.SAXException;

import io.odysz.semantic.DA.DATranscxt;
import io.odysz.semantic.util.SQLString;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

public class TestUser implements IUser {

	private DATranscxt logSemantic;
	private String uid;
	private SemanticObject action;

	public TestUser(String userId, SemanticObject action) {
		this.uid = userId;
		this.action = action;
		
		ISemantext semt;
		try {
			semt = new DASemantext("src/test/res/semantic-log.xml");
			logSemantic = new DATranscxt(semt); 
		} catch (SemanticException | SAXException | IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getUserId() { return uid; }

	@Override
	public ArrayList<String> dbLog(ArrayList<String> sqls) {
		// no exception can be thrown here, no error message for client if failed.
		try {
			// String newId = DASemantext.genId(Connects.defltConn(), "a_logs", "logId", null);
			// String sql = DatasetCfg.getSqlx(Connects.defltConn(), "log-template",
			//	// insert into a_logs(logId, oper, funcName, funcId, cmd, url, operDate, txt)
			//	// values ('%s', '%s', '%s', '%s', null, '%s', sysdate, '%s');
			//	newId, uid, funcName, funcId, cmd, url, String.valueOf(sqls.size()), txt(sqls));
			logSemantic.insert("a_logs", null)
				.nv("oper", uid)
				.nv("funcName", action.getString("funcName"))
				.nv("funcId", action.getString("funcId"))
				.nv("txt", txt(sqls))
				.ins();
			//Connects.commitLog(sql); // reporting commit failed in err console
		} catch (SQLException e) {
			// failed case must be a bug - commitLog()'s exception already caught.
			e.printStackTrace();
		} catch (TransException e) {
			e.printStackTrace();
		}
		return null;
	}

	private String txt(ArrayList<String> sqls) {
		return sqls == null ? null :
			// FIXME performance problem here
			sqls.stream().map(e -> SQLString.formatSql(e))
				.collect(Collectors.joining(";"));
	}

}
