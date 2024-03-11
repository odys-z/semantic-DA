package io.odysz.semantic.meta;

import static io.odysz.common.Utils.loadTxt;

import io.odysz.semantics.meta.TableMeta;

/**
 * Hard coded field string of user table information.
 * @deprecated really needed to be moved here?
 * @since 1.4.40
 * @author odys-z@github.com
 */
public class JUserMeta extends TableMeta {

	public JUserMeta(String... conn) {
		super("a_users", conn);
		this.pk      = "userId";
		this.uname   = "userName";
		this.pswd    = "pswd";
		this.iv      = "encAuxiliary";
		this.org     = "orgId";
		this.orgName = "orgName";
		this.role    = "roleId";
		this.roleName= "roleName";

		ddlSqlite = loadTxt(JUserMeta.class, "a_users.sqlite.ddl");
	}

	/**key in config.xml for class name, this class implementing IUser is used as user object's type. */
	// public String pk; // = "userId";
	public String uname; // = "userName";
	public String pswd; // = "pswd";
	public String iv; // = "encAuxiliary";
	/** v1.4.11, column of org id */
	public String org;
	/** v1.4.11, column of org name */
	public String orgName;
	/** v1.4.11, column of role id */
	public String role;
	/** v1.4.11, column of role name */
	public String roleName;

	public String orgTbl = "a_orgs";
	public String roleTbl = "a_roles";

	public JUserMeta userName(String unamefield) {
		uname = unamefield;
		return this;
	}

	public JUserMeta iv(String ivfield) {
		iv = ivfield;
		return this;
	}

	public JUserMeta pswd(String pswdfield) {
		pswd = pswdfield;
		return this;
	}
}