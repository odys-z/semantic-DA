package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.xml.sax.SAXException;

import io.odysz.module.rs.SResultset;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactory;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DA.DATranscxt;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**<h2>The default semantics used by semantic-DA.</h2>
 * The {@link DASemantext} use this to manage semantics configuration for resolving data semantics.
 * <h3>What's DASemantics for?</h3>
 * <p>Well, the word semantics is a computer science term. The author don't want to redefine this word,
 * but here is some explanation what <i>semantic-transact</i> is trying to support.</p>
 * <p>In a typical relational database based application, the main operation of data is CRUD.
 * And the most often such data operation can be abstracted to some operation pattern,
 * and they are always organized as a database transaction/batch operation described in SQL.</p>
 * <p>Take "book-author" relation for example, the author's ID is also the parent referenced by
 * book's author FK. If trying to delete an author in DB, there are 2 typical policies can be applied
 * by the application. The first is delete all books by the author accordingly; the second is warn and
 * deny the operation if some books are referencing the author. Both of this must been organized into
 * a transact/batch operation, with the second transact as check-then-delete.</p>
 * <p>In this case, you will find the FK relationship can be handled in a generalized operation, through
 * parameterizing some variables like table name, child referencing column name and parent ID.</p>
 * <p>Take the {@link DASemantics.stype#parentChildrenOnDel} for example, it's automatically support
 * "deleting all children when deleting parent" semantics. What the user (application developer) need to
 * do is configure a semantics item then delete the parent directly.</p>
 * <p>Now you (a developer) will definitely understand what's the "parentChildrenOnDel" for. Semantic-transact
 * abstract and hide these patterns, wrapped them automatically into a transaction. That's what semantic-
 * transact want to do.</p>
 * <h3>How to Use</h3>
 * <p>To use this function:</p>
 * <p>1. Configure the "semantics.xml". See example in test/resources/semantics.xml.<br>
 * 2. Set the configured semantics as context of {@link io.odysz.transact.sql.Statement}. See example in
 * {@link io.odysz.transact.SemanticsTest}. Then use Statement's subclass's commit() method to generate SQLs</p>
 * <h3>Is this Enough?</h3>
 * <p>The 9 to 10 types of semantics defined in {@link DASemantics.stype} is enough for some enterprise projects.
 * It depends on how abstract the semantics we want to support.</p>
 * </p>Another consideration is that semantic-transact never take supporting all semantics logic as it's goal.
 * It only trying to release burden of daily repeated tasks. Fortunately, such tasks' logic is simple, and the
 * burden is heavy. Let semantic-transact handle these simple logic, that's semantic-transact designed for. If
 * the semantics is complex, use anything you are familiar with.</p>
 * <p>Before doing that, check the semantics-cheapflow workflow engine first, which is based on semantics-transact,
 * and can handle typical - not very cheap if by our define - logics all necessary for enterprise applications.
 * It's a good example illustrating that if the semantics is designed carefully, those semantics supported by
 * this class is enough. </p>
 * <p>But it do need the application developers follow some design conventions. If you need you own semantics
 * implementation, implement the interface {@link ISemantext}, or simply initialize {@link io.odysz.transact.sql.Transcxt}
 * with null semantics, which will disable semantic supporting. 
 
 * @author ody
 *
 */
public class DASemantics {
	/**error code key word*/
	public static final String ERR_CHK = "err_smtcs";;

	/**<b>0. {@link #autoPk} </b> key-word: "auto" | "pk" | "a-k"<br>
	 * <b>1. {@link #fullpath} </b> key-word: "fullpath" | "fp" | "f-p"<br>
	 * <b>2. {@link #parentChildren} </b> key-word: "pc-del-all" | "parent-child-del-all"<br>
	 * <b>3. {@link #dencrypt} </b> key-word: "d-e" | "de-encrypt" <br>
	 * <b>4. {@link #opTime}</b> key-word: "o-t" | "oper-time"<br>
	 * <b>5. {@link #checkSqlCountOnDel} </b> key-word: "ck-cnt-del" | "check-count-del" <br>
	 * <b>6. {@link #checkSqlCountOnInsert} </b> key-word: "ck-cnt-ins" | "check-count-insert" <br>
	 * <b>7. {@link #checkDsCountOnDel} </b> key-word: "ds-cnt-ins" | "ds-count-insert" <br>
	 * <b>8. {@link #composingCol} </b> key-word: "cmp-col" | "compose-col" | "compose-column" <br>
	 * <b>9. {@link #stampUp1ThanDown} </b> key-word: "stamp-up1" <br>
	 * <b>10.{@link #orclob} </b> key-word: "clob"<br>
	 * UpdateBatch supporting:<br>
	 * on inserting, up-stamp is the value of increased down stamp, or current time if it's not usable;<br>
	 * on updating, up-stamp is set as down stamp increased if down stamp value not presented in sql, or,
	 * up stamp will be ignored if down stamp presented. (use case of down stamp updating by synchronizer).<br>
	 * <b>x. orclob</b>: the field must saved as clob when driver type is orcl;
	 */
	public enum smtype {
		/**"auto" | "pk" | "a-k": Generate auto pk for the field when inserting */
		autoPk,
		/** "f-p" | "fp" | "fullpath":
		 * when updating, auto update fullpath field according to parent-id and current record id<br>
		 * Handler: {@link ShFullpath#ShFullpath(String, String, String[])}*/
		fullpath,
		/** "p-c-del-all" | "parent-child-del-all": delete children before delete parent */
		parentChildrenOnDel,
		/** "d-e" | "de-encrypt": decrypt then encrypt (target col cannot be pk or anything other semantics will updated */
		dencrypt,
		/** "o-t" | "oper-time": oper and operTime that must auto updated when a user updating a record*/
		opTime,
		/** "ck-cnt-del" | "check-count-del": check is this record a referee of children records - results from sql.select(count, description-args ...). The record(s) can't been deleted if referenced;*/
		checkSqlCountOnDel,
		/** "ck-cnt-ins" | "ck-cnt-insert": check is this record count when inserting - results from sql.select(count, description-args ...). The record(s) can't been inserted if count > 0;*/
		checkSqlCountOnInsert,
		/** "ds-cnt-del" | "ds-count-del": check is this record a referee of children records - results from detaset.select(count, description-args ...). This is the oracle adaptive version of checkSqlCountOnDel; */
		// checkDsCountOnDel,
		/** "cmp-col" | "compose-col" | "compse-column": compose a column from other columns;*/
		composingCol,
		/** "s-up1" | "stamp-up1": add 1 more second to down-stamp column and save to up-stamp*/
		stamp1MoreThanRefee,
		/** "clob" | "orclob": the column is a CLOB field, semantic-transact will read/write separately in stream and get final results.*/
		orclob;

		public static smtype parse(String type) throws SemanticException {
			if (type == null) throw new SemanticException("semantics is null");
			type = type.toLowerCase().trim();
			if ("auto".equals(type) || "pk".equals(type) || "a-k".equals(type) || "autopk".equals(type))
				return autoPk;
			else if ("fullpath".equals(type) || "f-p".equals(type))
				return fullpath;
			else if ("pc-del-all".equals(type) || "parent-child-del-all".equals(type) || "parentchildondel".equals(type))
				return parentChildrenOnDel;
			else if ("d-e".equals(type) || "de-encrypt".equals(type) || "dencrypt".equals(type))
				return dencrypt;
			else if ("o-t".equals(type) || "oper-time".equals(type) || "optime".equals(type))
				return opTime;
			else if ("ck-cnt-del".equals(type) || "check-count-del".equals(type) || "checksqlcountondel".equals(type))
				return checkSqlCountOnDel;
			else if ("ck-cnt-del".equals(type) || "check-count-del".equals(type) || "checksqlcountoninsert".equals(type))
				return checkSqlCountOnInsert;
//			else if ("ds-cnt-del".equals(type) || "ds-count-del".equals(type) || "checkdscountondel".equals(type))
//				return checkDsCountOnDel;
			else if ("cmp-col".equals(type) || "compose-col".equals(type) || "compse-column".equals(type) || "composingcol".equals(type))
				return composingCol;
			else if ("s-up1".equals(type) || type.startsWith("stamp1"))
				return stamp1MoreThanRefee;
			else if ("clob".equals(type) || "orclob".equals(type))
				return orclob;
			else throw new SemanticException("semantics not known: " + type);
		}
	}

	private static HashMap<String, DASemantics> ss;
	/** raw transact context for DB accessing without semantics support */
	private static DATranscxt rawTsx;

	public static void init(String filepath) throws SAXException, SemanticException {
		if (ss == null) {
			ss = new HashMap<String, DASemantics>();
			XMLTable conn = XMLDataFactory.getTable(new Log4jWrapper("") , "semantics", filepath,
			new IXMLStruct() {
				@Override public String rootTag() { return "semantics"; }
				@Override public String tableTag() { return "t"; }
				@Override public String recordTag() { return "s"; }});

			conn.beforeFirst();
			while (conn.next()) {
				String tabl = conn.getString("tabl");
				DASemantics s = ss.get(tabl);
				if (s == null)
					s = new DASemantics(conn.getString("smtc"), tabl,
							conn.getString("pk"), conn.getString("args"));
				else s.addSemantics(conn.getString("smtc"), tabl,
						conn.getString("pk"), conn.getString("args"));
				ss.put(tabl, s);
			}

			rawTsx = new DATranscxt(null);
		}
	}


	public static DASemantics get(String tabl) {
		return ss == null ? null : ss.get(tabl);
	}

	///////////////////////////////// base class ///////////////////////////////
	private ArrayList<SemanticHandler> handlers;

	public DASemantics(String semantic, String tabl, String recId, String args) throws SemanticException {
		addSemantics(semantic, tabl, recId, args);
	}

	private void addSemantics(String semantic, String tabl, String recId, String args) throws SemanticException {
		addSemantics(smtype.parse(semantic), tabl, recId, args);
	}
	
	/**@see {@link Semantics2#Semantics(smtype, String[])}
	 * @param semantic
	 * @param tabl
	 * @param args
	 * @throws SQLException
	 */
	public void addSemantics(smtype semantic, String tabl, String recId, String args) throws SemanticException {
		SemanticHandler handler = null;

		String[] argss = args.split(",");
		if (smtype.fullpath == semantic)
			// addFullpath(tabl, recId, argss);
			handler = new ShFullpath(tabl, recId, argss);
//		else if (smtype.autoPk == semantic)
//			addAutoPk(tabl, recId, argss);
		else if (smtype.parentChildrenOnDel == semantic)
			// addParentChildren(tabl, recId, argss);
			handler = new ShPCDelAll(tabl, recId, argss);
//		else if (smtype.dencrypt == semantic)
//			addDencrypt(tabl, recId, argss);
//		else if (smtype.orclob == semantic)
//			addClob(tabl, recId, argss);
//		else if (smtype.opTime == semantic)
//			addOperTime(tabl, recId, argss);
		else if (smtype.checkSqlCountOnDel == semantic)
			// addCheckSqlCountOnDel(tabl, recId, argss);
			handler = new ShChkPCDel(tabl, recId, argss);
		else if (smtype.checkSqlCountOnInsert == semantic)
			// addCheckSqlCountOnInsert(tabl, recId, argss);
			handler = new ShChkPCInsert(tabl, recId, argss);
//		else if (smtype.composingCol == semantic)
//			addComposings(tabl, recId, argss);
//		else if (smtype.stamp1MoreThanRefee == semantic)
//			addUpDownStamp(tabl, recId, argss);
		else throw new SemanticException("Unsuppported semantics: " + semantic);
		
		handlers.add(handler);
	}

	public void onInsert(ArrayList<Object[]> row, Map<String, Integer> cols) {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.insert)
					handler.onInsert(row, cols);
	}

	//////////////////////////////// Base Handler //////////////////////////////
	abstract static class SemanticHandler {
		boolean insert = false;
		boolean update = false;
		String target;
		String idField;
		String[] args;

		SemanticHandler(String semantic, String tabl, String pk,
				String[] args) throws SemanticException {
			checkParas(tabl, pk, args);
			target = tabl;
			idField = pk;
		}

		void onInsert(ArrayList<Object[]> row, Map<String, Integer> cols) {}

		SemanticHandler(smtype fullpath, String tabl, String pk,
				String[] args) throws SemanticException {
			checkParas(tabl, pk, args);
			target = tabl;
			idField = pk;
			this.args = args;
		}

		/**Throw exception if args is null or target (table) not correct.
		 * @param tabl
		 * @param pk
		 * @param args
		 * @throws SemanticException sementic configuration not matching the target or lack of args.
		 */
		private void checkParas(String tabl, String pk, String[] args) throws SemanticException {
			if (tabl == null || pk == null || args == null)
				throw new SemanticException(String.format(
						"adding semantics with empty targets? %s %s %s",
						tabl, pk, args));
	
			if (target != null && !target.equals(tabl))
				throw new SemanticException(String.format("adding semantics for different target? %s vs. %s", target, tabl));
			if (idField != null && !idField.equals(pk))
				throw new SemanticException(String.format("adding semantics for target of diferent id field? %s vs. %s", idField, pk));
		}
	}
	
	//////////////////////////////// subclasses ////////////////////////////////
	static class ShFullpath extends SemanticHandler {
		/**
		 * @param tabl
		 * @param recId
		 * @param args 0: parent Id field, 1: sibling/sort field (optional), 2: fullpath field
		 * @throws SemanticException
		 */
		public ShFullpath(String tabl, String recId, String[] args) throws SemanticException {
			super(smtype.fullpath, tabl, recId, args);
			insert = true;
			update = true;
		}

		@Override
		void onInsert(ArrayList<Object[]> row, Map<String, Integer> cols) {
			String sibling = null;
			try { sibling = (String) row.get(cols.get(args[1]))[1];}
			catch (Exception e) {}
			
			String id = (String) row.get(cols.get(idField))[1];

			String pid = (String) row.get(cols.get(args[0]))[1];
			
			// select fullpath where id = $parentId
			String v = null;
			try {
				SResultset rs =
						(SResultset) rawTsx.select(target, "_t0")
						.col(args[2])
						.where("=", idField, "'" + pid + "'")
						.rs(new DASemantext(""));
				rs.beforeFirst().next();
				String parentpath = rs.getString(args[2]);
				v = String.format("%s.%s%s", parentpath,
					sibling == null ? "" : sibling + " ", id);
			} catch (TransException | SQLException e) {
				e.printStackTrace();
			}


			Object[] nv;
			if (cols.containsKey(args[2]))
				nv = row.get(cols.get(args[2]));
			else {
				nv = new String[] {args[2], v};
				cols.put(args[2], row.size());
				row.add(nv);
			}
		}
	}

	static class ShPCDelAll extends SemanticHandler {
		public ShPCDelAll(String tabl, String recId, String[] args) throws SemanticException {
			super(smtype.parentChildrenOnDel, tabl, recId, args);
		}

	}
	
	static class ShChkPCDel extends SemanticHandler {
		public ShChkPCDel(String tabl, String recId, String[] args) throws SemanticException {
			super(smtype.checkSqlCountOnDel, tabl, recId, args);
		}
	}
	
	static class ShChkPCInsert extends SemanticHandler {
		public ShChkPCInsert(String tabl, String recId, String[] args) throws SemanticException {
			super(smtype.checkSqlCountOnInsert, tabl, recId, args);
			insert = true;
		}
	}
}
