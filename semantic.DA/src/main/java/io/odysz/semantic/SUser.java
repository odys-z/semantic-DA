package io.odysz.semantic;

import java.io.IOException;
import java.sql.SQLException;

import io.odysz.semantics.x.SemanticException;

/**Semantic User used for finger print when asking DA functions.<br>
 * Finger print including Semantic data operater, logger info, etc.
 * @author ody
 *
 */
public interface SUser {
	//JsonStructure logout(Object jheader);
	public SemanticObject logout(Object header);
	public void removed();
	public void touch();
	/**
	 * @param jlogin org.json.simple.JSONObject or javax.json.JsonObject (both will be replaced by Gson)
	 * @param request
	 * @return
	 * @throws IrSessionException
	 * @throws SQLException
	 * @throws IOException
	 */
	public boolean login(Object jlogin) throws SemanticException, SQLException, IOException;
	public String getSessionId();
	public long getLastAccessTime();
	public String getLogId();
	public String getUserId();
	public String getUserName();
	public String getRoleName();
	public String getRoleId();
	public String getOrgName();
	public String getOrgId();
	public boolean isAdmin();
	public String homepage();
}
