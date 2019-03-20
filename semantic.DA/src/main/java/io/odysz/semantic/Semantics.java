package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.common.Radix64;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DA.DatasetCfg;
import io.odysz.common.AESHelper;
import io.odysz.common.DateFormat;

/**Data structure semantics item description and supporter,
 * e.g. semantics of fullpath is used for depth-first tree traveling.<br>
 * TODO should renamed as DB Table relationships?
 * @author ody
 */
public class Semantics {
	/**Response error code*/
	public static final String ERR_CHK = "err_smtcs";;

	/**<b>1. fullpath</b>: auto update fullpath field according to parent-id and record id;<br>
	 * <b>2. parentChildren</b>: delete children before delete parent;<br>
	 * <b>3. dencrypt</b>: decrypt then encrypt (target col cannot be pk or anything other semantics will updated,<br>
	 * TODO not supported in multiUpdate yet);<br>
	 * <b>4. opTime</b>: oper and operTime that must auto updated when a user updating a record;<br>
	 * <b>5. checkSqlCountOnDel</b>: check is this record a referee of children records - results from sql.select(count, description-args ...). The record(s) can't been deleted if referenced;<br>
	 * <b>6. checkSqlCountOnInsert</b>: check is this record count when inserting - results from sql.select(count, description-args ...). The record(s) can't been inserted if count > 0;<br>
	 * <b>7. checkDsCountOnDel</b>: check is this record a referee of children records - results from detaset.select(count, description-args ...). This is the oracle adaptive version of checkSqlCountOnDel;<br>
	 * <b>8. composingCol</b>: compose a column from other columns;<br>
	 * <b>9. stampUp1ThanDown</b> add 1 more second to down-stamp column and save to up-stamp.<br>
	 * UpdateBatch supporting:<br>
	 * on inserting, up-stamp is the value of increased down stamp, or current time if it's not usable;<br>
	 * on updating, up-stamp is set as down stamp increased if down stamp value not presented in sql, or,
	 * up stamp will be ignored if down stamp presented. (use case of down stamp updating by synchronizer).<br>
	 * <b>x. orclob</b>: the field must saved as clob when driver type is orcl;
	 */
	public enum smtype {
		fullpath,
		parentChildrenOnDel,
		dencrypt,
		opTime,
		checkSqlCountOnDel,
		checkSqlCountOnInsert,
		checkDsCountOnDel,
		composingCol,
		stamp1MoreThanRefee,
		orclClob
		}
	private String target;

	// fullpath
	private String pathSufix;
	private String parentField;
	private String fullPath;
	private String descFullpath;
	
	/** parent-children constrains. All child table referencing this table's fk info.<br>
	 * String[][] of {String[]{[0] child-table, [1] child-fk}, String[]{...}, ...}
	 */
	private ArrayList<String[]> childConstraints;
	// private String childTab;
	// private String childFK;
	/** Readable description */
	private String descPC;
	private String idField;
	
	// de-encrypt
	private String cipherCol;
	private String ivCol;
	private String decryptK;
	private String encryptK;

	private ArrayList<String> lobFields;

	// oper-time
	private String operField;
	private String opTimeField;
	
	// check referee count
	private String checkCountErrFormat_Del;
	private String checkCountSql_Del;
	private String checkCountPkCol_Del;

	private String checkCountErrFormat_Ins;
	private String checkCountSql_Ins;
	private String checkCountValueCol_Ins;
	private String deschkIns;

	/**semantic = fullpath, args["tabl", "rec-id", "parentId-field", "suffix-field", "fullpath-field"]<br>
	 * semantic = parentChildrenOnDel, args["parent-table", "parent-id-field", "child-table", "child-fk-field"]<br>
	 * semantic = dencrypt, args["tabl", "cipher-col", "iv-col", "decrypt-key", "encrypt-key"]<br>
	 * semantic = opTime, args["tabl", "rec-id", "oper-col", "operTime-col"]<br>
	 * semantic = checkCountOnDel(sql), args["tabl", "rec-id", "desc-template", "select count-arg0, recName-arg1, ...", "deleting-pk-col"]<br>
	 * semantic = checkCountOnInsert(sql), args["tabl", "rec-id", "desc-template", "select count-arg0, recName-arg1, ...", "deleting-pk-col"]<br>
	 * semantic = composed-col = concat(composing-cols), args["tabl", "rec-id", "composed-col", "composing-col1", "'const'", ...]<br>
	 * semantic = stampUp1ThanDown, args["tabl", "id-field", "up-stamp", "down-stamp"]<br>
	 * semantic = clob, args["bump-case-tabl", "id-field", "lob-field"]
	 * @param semantic
	 * @param args
	 */
	public Semantics(smtype semantic, String[] args) {
		try { addSemantics(semantic, args);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**@see {@link Semantics2#IrSemantics(smtype, String[])}
	 * @param semantic
	 * @param args
	 * @throws SQLException
	 */
	public void addSemantics(smtype semantic, String[] args) throws SQLException {
		if (target != null && !target.equals(args[0]))
			throw new SQLException(String.format("Adding semetics to different target: ", target, args[0]));
		if (smtype.fullpath == semantic)
			addFullpath(args);
		else if (smtype.parentChildrenOnDel == semantic)
			addParentChildren(args);
		else if (smtype.dencrypt == semantic)
			addDencrypt(args);
		else if (smtype.orclClob == semantic)
			addClob(args);
		else if (smtype.opTime == semantic)
			addOperTime(args);
		else if (smtype.checkSqlCountOnDel == semantic)
			addCheckSqlCountOnDel(args);
		else if (smtype.checkSqlCountOnInsert == semantic)
			addCheckSqlCountOnInsert(args);
		else if (smtype.composingCol == semantic)
			addComposings(args);
		else if (smtype.stamp1MoreThanRefee == semantic)
			addUpDownStamp(args);
		else throw new SQLException("Unsuppported semantics: " + semantic);
	}

	/**Throw exception if args is null or target (table) not correct.
	 * @param args
	 * @throws SQLException
	 */
	private void checkParas(String[] args) throws SQLException {
		if (args == null || args[0] == null || args[1] == null || args[2] == null)
			throw new SQLException(String.format("adding semantics with empty args? %s %s %s", (Object[])args));

		if (target != null && !target.equals(args[0]))
			throw new SQLException(String.format("adding semantics for different target? %s vs. %s", target, args[0]));
		if (idField != null && !idField.equals(args[1]))
			throw new SQLException(String.format("adding semantics for target of diferent id field? %s vs. %s", idField, args[1]));
	}
	
	private void addClob(String[] args) throws SQLException {
		checkParas(args);

		target = args[0];
		idField = args[1];
		if (lobFields == null)
			lobFields = new ArrayList<String>(1);
		lobFields.add(args[2]);
	}

	/**semantic = checkCountOnDel(sql), args["tabl", "rec-id", "desc-template", "select count-arg0, recName-arg1, ...", "deleting-pk-col"]
	 * 
	 * @param args
	 * @throws SQLException
	 */
	private void addCheckSqlCountOnDel(String[] args) throws SQLException {
		checkParas(args);

		target = args[0];
		idField = args[1];
		checkCountErrFormat_Del = args[2];
		checkCountSql_Del = args[3];
		checkCountPkCol_Del = args[4];
	}

	private void addCheckSqlCountOnInsert(String[] args) throws SQLException {
		checkParas(args);

		target = args[0];
		idField = args[1];
		checkCountErrFormat_Ins = args[2];
		checkCountSql_Ins = args[3];
		checkCountValueCol_Ins = args[4];
		
		deschkIns = String.format("Check sql results on table %s, value-col %s", target, checkCountValueCol_Ins);
	}

	private void addOperTime(String[] args) throws SQLException {
		checkParas(args);

		target = args[0];
		idField = args[1];
		operField = args[2];
		opTimeField = args[3];
	}

	private void addDencrypt(String[] args) {
		target = args[0];
		cipherCol = args[1];
		ivCol = args[2];
		decryptK = args[3];
		encryptK = args[4];
	}

	private void addParentChildren(String[] args) {
		if (pathSufix != null) {
			if (!target.equals(args[0]) || !idField.equals(args[1]))
				System.err.println(String.format(
					"WARN - IrSemantics: The adding parent.id(%s.%s) is inconsistant with the exist one(%s.%s).",
					args[0], args[1], target, pathSufix));
		}
		target = args[0];
		idField = args[1];
		if (childConstraints == null)
			childConstraints = new ArrayList<String[]> ();
		childConstraints.add(new String[] {args[2], args[3]});
		//String.format("parent-child rule (%s.%s <-FK- '%s.%s')", target, id, childTab, childFK);
		descPC = formatPcDesc();
	}

	private String formatPcDesc() {
		String s = "";
		for (String[] c : childConstraints)
			s += String.format("\nparent-child rule (%s.%s <-FK- '%s.%s')", target, pathSufix, c[0], c[1]);
		return s;
	}

	private void addFullpath(String[] args) {
		if (pathSufix != null) {
			if (!target.equals(args[0]) || !pathSufix.equals(args[1]))
				System.err.println(String.format(
					"WARN - IrSemantics: The adding parent.id(%s.%s) is inconsistant with the exist one(%s.%s).",
					args[0], args[1], target, pathSufix));
		}
		target = args[0];
		idField = args[1];
		parentField =  args[2];
		pathSufix = args[3];
		fullPath = args[4];
		descFullpath = String.format("fullpath rule (%s = 'parent-%s.%s')", fullPath, fullPath, pathSufix);
	}

	/**Generating fullpath - why not used?
	 * @param conn
	 * @param parentId
	 * @param recId
	 * @param siblingOrder
	 * @return
	 * @throws SQLException
	 */
	public String genFullpath(String conn, Object parentId, Object recId, Object siblingOrder) throws SQLException {
		int order = 0;
		try { order = Integer.valueOf((String) siblingOrder); } catch (Exception e) {}
		String sibling = Radix64.toString(order, 2);
		// No parent, root-fullpath = sibling-id
		if (parentId == null || parentId.equals(recId))
			return String.format("%1$2s %2$s", sibling, recId);
		SResultset rs = Connects.select(conn, String.format( "select %s as parentPath from %s where %s = '%s'",
				fullPath, target, idField, parentId), Connects.flag_nothing);
		// find parent.fullpath
		if (rs.getRowCount() == 0)
			// no parent (path) found
			return String.format("%1$2s %2$s", sibling, recId);

		rs.beforeFirst().next();
		String parentPath = rs.getString("parentPath");
		if (parentPath == null || parentPath.trim().length() == 0)
			return String.format("%1$2s %2$s", sibling, recId);
		else {
			return String.format("%s.%2s %s", parentPath, sibling, recId);
		}
	}

	public boolean isFullpath(String f) {
		return fullPath!=null && fullPath.equals(f);
	}
	
	/**Parent field in tree table
	 * @param p
	 * @return
	 */
	public boolean isParentField(String p) {
		return parentField!=null && parentField.equals(p);
	}

	public boolean isPathSuffixField(String f) {
		return pathSufix != null && pathSufix.equals(f);
	}
	
	public boolean isIdField(String f) {
		return idField != null && idField.equals(f);
	}

	public String getDesc(smtype semantic) {
		if (smtype.parentChildrenOnDel == semantic)
			return descPC;
		else if (smtype.checkSqlCountOnInsert == semantic)
			return deschkIns;
		else return descFullpath;
	}
	
	public String getFullpathField() {
		return fullPath;
	}

	/**Is this table a parent table that a child table has a fk(single column) depends on this table?
	 * @param tabl
	 * @return
	 */
	public boolean isParentTable(String tabl) {
		return target != null && target.equals(tabl) && childConstraints != null && childConstraints.size() > 0;
	}

	public boolean isCheckCountTable(String tabl) {
		return target != null && target.equals(tabl) && checkCountPkCol_Del != null && checkCountSql_Del != null;
	}

	/**If the configured sql has a resulting count (with pk-arg select) > 0,
	 * throw an SemanticException of message add while configuration.
	 * @param pkCol
	 * @param pkv
	 * @throws SemanticException thrown when semantics-checking(IrSemantics.checkSqlCountOnDel, ...) failed.
	 */
	public void checkRefereeCount(String connId, String pkCol, String pkv) throws SemanticException {
		try {
			if (pkCol.equals(checkCountPkCol_Del)) {
				SResultset rs = Connects.select(connId, String.format(checkCountSql_Del, pkv), Connects.flag_nothing);
				rs.beforeFirst();
				if (rs.next()) {
					int cnt = rs.getInt(1);
					int cols = rs.getColumnCount();
					String arg1 = cols >= 2 ? rs.getString(2) : "";
					String arg2 = cols >= 3 ? rs.getString(3) : "";
					String arg3 = cols >= 4 ? rs.getString(4) : "";
					String arg4 = cols >= 5 ? rs.getString(5) : "";
					if (cnt > 0)
						throw new SemanticException(String.format(checkCountErrFormat_Del, cnt, arg1, arg2, arg3, arg4));
					// else return ok
				}
				else
					throw new SemanticException(String.format(checkCountErrFormat_Del, 0, "", "", "", ""));
			} // else check nothing
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**[{tabl: child-1, act: {act-obj}}, <br>
	 *  {tabl: child-2, act: {act-obj}}]
	 * @param parentId
	 * @return
	 */
	public SemanticObject[] getDeleteChildrenCond(String parentId) {
		if (childConstraints == null) return null;
		SemanticObject[] jreqs = new SemanticObject[childConstraints.size()];

		for (int i = 0; i < jreqs.length; i++) {
			String[] childfk = childConstraints.get(i);
			SemanticObject cond = new SemanticObject();
			cond.put("field", childfk[1]);
			cond.put("v", parentId);
			cond.put("logic", "=");
			SemanticObject jconds = new SemanticObject();
			// jconds.addObject(cond);

			SemanticObject req = new SemanticObject();
			SemanticObject act = new SemanticObject();
			act.put("a", "delete");
			act.put("conds", jconds);
			req.put("tabl", childfk[0]);
			req.put("act", act);
			jreqs[i] = req;
		}
		return jreqs;
	}

	public boolean isCipherField(String c) { return c != null && c.equals(cipherCol); }

	public boolean isIvField(String c) { return c != null && c.equals(ivCol); }

	public String getCipherField() { return cipherCol; }

	public String getIvField() { return ivCol; }

	/**
	 * @param pB64 cipher in Base64
	 * @param ivB64
	 * @return [cipher-base64, new-iv-base64]
	 * @throws SQLException
	 */
	public String[] dencrypt(String pB64, String ivB64) throws SQLException {
		try {
			return AESHelper.dencrypt(pB64, decryptK, ivB64, encryptK);
		} catch (Throwable e) {
			e.printStackTrace();
			throw new SQLException (e.getMessage()); 
		}
	}

	public boolean is(smtype type) {
		if (type == smtype.fullpath)
			return fullPath != null;
		else if (type == smtype.parentChildrenOnDel)
			return childConstraints != null && childConstraints.size() > 0;
		else if (type == smtype.checkSqlCountOnInsert)
			return checkCountSql_Ins != null && checkCountValueCol_Ins != null;
		else if (type == smtype.dencrypt)
			return cipherCol != null && decryptK.length() > 0 && encryptK.length() > 0;
		else if (type == smtype.composingCol)
			return composedCol != null && composingCols != null && composingCols.length > 0;
		else if (type == smtype.stamp1MoreThanRefee)
			return upStamp != null && downStamp != null;
		else
			return false;
	}

	public boolean hasOperTime() {
		return operField != null;
	}

	public String getOperField() {
		return operField;
	}

	public String getOpTimeField() {
		return opTimeField;
	}

	public void checkSqlCountIns(String connId, String chkf, Object chkv) throws SemanticException {
		try {
			if (chkf.equals(checkCountValueCol_Ins)) {
				SResultset rs = Connects.select(connId, String.format(checkCountSql_Ins, chkv), Connects.flag_nothing);
				rs.beforeFirst();
				if (rs.next()) {
					int cnt = rs.getInt(1);
					int cols = rs.getColumnCount();
					String arg1 = cols >= 2 ? rs.getString(2) : "";
					String arg2 = cols >= 3 ? rs.getString(3) : "";
					String arg3 = cols >= 4 ? rs.getString(4) : "";
					String arg4 = cols >= 5 ? rs.getString(5) : "";
					if (cnt > 0)
						throw new SemanticException(String.format(checkCountErrFormat_Ins, cnt, arg1, arg2, arg3, arg4));
					// else return ok
				}
				else
					throw new SemanticException(String.format(checkCountErrFormat_Ins, 0, "", "", "", ""));
			} // else check nothing
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

	public boolean isCheckCountInsert(String tabl) {
		return this.target.equals(tabl) && checkCountValueCol_Ins != null;
	}

	public boolean isChkCntInsField(String vcol) {
		return checkCountValueCol_Ins.equals(vcol);
	}

	////////////////////////////////// composing //////////////////////////////////////////////
	private String composedCol;

	private String[] composingCols;

	private void addComposings(String[] args) throws SQLException {
		checkParas(args);
		
		target = args[0];
		idField = args[1];
		composedCol = args[2];
		composingCols = new String[args.length - 3];
		for (int i = 3; i < args.length; i++) {
			composingCols[i - 3] = args[i];
		}
	}

	public boolean isComposedCol(String n) {
		return composedCol != null && composedCol.equals(n);
	}

	public int compondsCount() {
		return composingCols == null ? -1 : composingCols.length;
	}

	public int compondIdx(String n) {
		if (composingCols == null)
			return -1;
		for (int i = 0; i < composingCols.length; i++)
			if (composingCols[i].equals(n))
				return i;
		return -1;
	}

	public String getComposedCol() {
		return composedCol;
	}

	/**Composing componds and newIds into composed column.<br>
	 * Calling UpdateBatch.resolveAuto to resolve reference.
	 * @param newIds
	 * @param componds
	 * @return
	 */
	public String getComposedV(ArrayList<String> newIds, String[] componds) {
		String res = "";
//		for (int ix = 0; ix < composingCols.length; ix++) {
//			String t = composingCols[ix];
//			// constant
//			if (t.startsWith("'") && t.endsWith("'")) {
//				res += t.replaceFirst("^'", "").replaceAll("'$", "");
//			}
//			// variable from client
//			else res += UpdateBatch.resolveAuto(componds[ix], newIds);
//		}
		return res;
	}
	
	//////////////////// stamp up-stamp 1 more than down-stamp ///////////////////
	private String upStamp;
	private boolean supportUpStampOnUpdate;
	private String downStamp;
	private String defltRefeeVal;

	/**
	 * @param args [0] table, [1] id-field, [2] up-stamp, [3] down-stamp, [4] default down stamp, [5] generate up-stamp
	 * @throws SQLException
	 */
	private void addUpDownStamp(String[] args) throws SQLException {
		checkParas(args);
		target = args[0];
		idField = args[1];
		upStamp = args[2];
		downStamp = args[3];
		defltRefeeVal = args.length > 4 ? args[4] : "0";
		supportUpStampOnUpdate = args.length > 5 ? Boolean.valueOf(args[5]) : false;
	}

	public String upstamp() {
		return upStamp;
	}

	public boolean isDownstamp(String n) {
		return downStamp != null && downStamp.equals(n);
	}

	public String defltRefeeVal() { return defltRefeeVal; }

	/**For null, '0', invalid date string, return "1776-07-04 00:00:00", other wise add 1 second to dataStr.
	 * @param conn
	 * @param dateStr
	 * @return
	 */
	public String upstampOnInsert(String conn, Object dateStr) {
		try {
			if (dateStr == null || ((String)dateStr).trim().length() < 6)
				// len(76-7-4) = 6
				return DateFormat.incSeconds(Connects.driverType(conn), "1776-07-04 00:00:00", 0);
			return DateFormat.incSeconds(Connects.driverType(conn), (String) dateStr, 1);
		} catch (Exception e) { return DateFormat.getTimeStampYMDHms(Connects.driverType(conn)); }
	}

	public boolean isUpstamp(String n) {
		return upStamp != null && upStamp.equals(n);
	}

	/**Get up-stamp on update (increase down stamp).<br>
	 * Return null if semantics is specified as no up-stamp generating (like on DB that support timestamp on update)
	 * @param conn
	 * @param tabl
	 * @param recId
	 * @return
	 * @throws SemanticException 
	 */
	public String upstampOnUpdate(String conn, String tabl, String recId) throws SemanticException {
		if (!supportUpStampOnUpdate)
			return null;
		try {
			// <mysql>select Date_add(%s, interval 1 second) upstamp from %s where %s = '%s'</mysql>
			// 
			String sql = DatasetCfg.getSql(conn, "semantics.get-downstamp",
					downStamp, tabl, this.idField, recId);
			SResultset rs = Connects.select(sql, Connects.flag_nothing);
			rs.next();
			return rs.getString("upstamp");
		} catch (SQLException e) {
			return DateFormat.getTimeStampYMDHms(Connects.driverType(conn));
		}
	}

}


