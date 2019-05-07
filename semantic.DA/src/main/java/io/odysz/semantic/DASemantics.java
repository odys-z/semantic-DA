package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import io.odysz.common.LangExt;
import io.odysz.common.Utils;
import io.odysz.semantics.IResults;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

/**<h2>The default semantics used by semantic-DA.</h2>
 * <p>The {@link DASemantext} use this to manage semantics configuration for resolving data semantics.</p>
 * DASemantics is basically a {@link SemanticHandler}'s container, with subclass handlers handling different
 * semantics (processing values). </p>
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
 
 * @author odys-z@github.com
 *
 */
public class DASemantics {
	/**error code key word*/
	public static final String ERR_CHK = "err_smtcs";;
	
	public static boolean debug = true; 

	/**<b>0. {@link #autoInc} for xml keywords and args. handler: {@link ShAutoK}<br>
	 * <b>1. {@link #fkIns} fk on insert (resolved when referencing auto key). handler: {@link ShChkPCInsert}<br>
	 * <b>2. {@link #fullpath} tree table fullpath. handler: {@link ShFullpath}<br>
	 * <b>3. {@link #defltVal} default value column. handler: {@link ShDefltVal}<br>
	 * <b>4. {@link #parentChildrenOnDel} delete children before deleting parent. handler: {@link ShPCDelAll}<br>
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
		/**xml/smtc = "auto" | "pk" | "a-k" | "autopk" 
		 * @see {@link ShAutoK} */
		autoInc,
		/**xml/smtc = "fk" | "pkref" | "fk-ins"
		 * args: see {@link ShFkOnIns} */
		fkIns,
		/**xml/smtc = "f-p" | "fp" | "fullpath":
		 * Handler: {@link ShFullpath#ShFullpath(String, String, String[])}*/
		fullpath,
		/**xml/smtc = "dfltVal" | "dv" | "d-v":
		 * default value*/
		defltVal,
		/** "pc-del-all" | "parent-child-del-all" | "parentchildondel"
		 * Handler: {@link ShChkPCDel}*/
		parentChildrenOnDel,
		/** "d-e" | "de-encrypt": decrypt then encrypt (target col cannot be pk or anything other semantics will updated */
		dencrypt,
		/**xml/smtc = "o-t" | "oper-time" | "optime"<br>
		 * Handler: {@link ShOperTime}*/
		opTime,
		/** "ck-cnt-del" | "check-count-del": check is this record a referee of children records - results from sql.select(count, description-args ...). The record(s) can't been deleted if referenced;*/
		checkSqlCountOnDel,
		/** "ck-cnt-ins" | "ck-cnt-insert": check is this record count when inserting - results from sql.select(count, description-args ...). The record(s) can't been inserted if count > 0;*/
		checkSqlCountOnInsert,
		/** "cmp-col" | "compose-col" | "compse-column": compose a column from other columns;*/
		composingCol,
		/** "s-up1" | "stamp-up1": add 1 more second to down-stamp column and save to up-stamp*/
		stamp1MoreThanRefee,
		/** "clob" | "orclob": the column is a CLOB field, semantic-transact will read/write separately in stream and get final results.*/
		orclob;

		/**Note: we don't use enum.valueOf(), because of fault / fuzzy tolerate.
		 * @param type
		 * @return {@link smtype}
		 * @throws SemanticException
		 */
		public static smtype parse(String type) throws SemanticException {
			if (type == null) throw new SemanticException("semantics is null");
			type = type.toLowerCase().trim();
			if ("auto".equals(type) || "pk".equals(type) || "a-k".equals(type) || "autopk".equals(type))
				return autoInc;
			else if ("fk".equals(type) || "pkref".equals(type) || "fk-ins".equals(type))
				return fkIns;
			else if ("fullpath".equals(type) || "f-p".equals(type) || "fp".equals(type))
				return fullpath;
			else if ("dfltVal".equals(type) || "d-v".equals(type) || "dv".equals(type))
				return defltVal;
			else if ("pc-del-all".equals(type) || "parent-child-del-all".equals(type) || "parentchildondel".equals(type))
				return parentChildrenOnDel;
			else if ("d-e".equals(type) || "de-encrypt".equals(type) || "dencrypt".equals(type))
				return dencrypt;
			else if ("o-t".equals(type) || "oper-time".equals(type) || "optime".equals(type))
				return opTime;
			else if ("ck-cnt-del".equals(type) || "check-count-del".equals(type) || "checksqlcountondel".equals(type))
				return checkSqlCountOnDel;
			else if ("ck-cnt-ins".equals(type) || "check-count-ins".equals(type) || "checksqlcountoninsert".equals(type))
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

	private HashMap<String, DASemantics> ss;

	/**Static transact context for DB accessing without semantics support.<br>
	 * Used to generate auto ID. */
	private Transcxt basicTsx;

	public DASemantics get(String tabl) {
		return ss == null ? null : ss.get(tabl);
	}

	///////////////////////////////// container class ///////////////////////////////
	private ArrayList<SemanticHandler> handlers;
	private boolean hasAutopk = false;

	private String tabl;
	private String pk;

	public DASemantics(Transcxt basicTx, String tabl, String recId) {
		this.tabl = tabl;
		this.pk = recId;
		// staticTsx = new DATranscxt();
		basicTsx = basicTx;
		
		handlers = new ArrayList<SemanticHandler>();
	}

	/**@see {@link Semantics2#Semantics(smtype, String[])}
	 * @param semantic
	 * @param tabl
	 * @param recId
	 * @param args
	 * @throws SemanticException
	 */
	public void addHandler(smtype semantic, String tabl, String recId, String[] args) throws SemanticException {
		checkParas(tabl, pk, args);
		checkSmtcs(tabl, semantic);
		SemanticHandler handler = null;

		if (smtype.fullpath == semantic)
			handler = new ShFullpath(basicTsx, tabl, recId, args);
		else if (smtype.autoInc == semantic) {
			handler = new ShAutoK(basicTsx, tabl, recId, args);
			hasAutopk = true;
		}
		else if (smtype.fkIns == semantic)
			handler = new ShFkOnIns(basicTsx, tabl, recId, args);
		else if (smtype.parentChildrenOnDel == semantic)
			handler = new ShPCDelAll(basicTsx, tabl, recId, args);
		else if (smtype.defltVal == semantic)
			handler = new ShDefltVal(basicTsx, tabl, recId, args);
//		else if (smtype.dencrypt == semantic)
//			addDencrypt(tabl, recId, argss);
//		else if (smtype.orclob == semantic)
//			addClob(tabl, recId, argss);
		else if (smtype.opTime == semantic)
			handler = new ShOperTime(basicTsx, tabl, recId, args);
		else if (smtype.checkSqlCountOnDel == semantic)
			handler = new ShChkPCDel(basicTsx, tabl, recId, args);
		else if (smtype.checkSqlCountOnInsert == semantic)
			handler = new ShChkPCInsert(basicTsx, tabl, recId, args);
//		else if (smtype.composingCol == semantic)
//			addComposings(tabl, recId, argss);
//		else if (smtype.stamp1MoreThanRefee == semantic)
//			addUpDownStamp(tabl, recId, argss);
		else throw new SemanticException("Unsuppported semantics: " + semantic);
		
		if (debug)
			handler.logi();
		handlers.add(handler);
	}

	/**Throw exception if args is null or target (table) not correct.
	 * @param tabl
	 * @param pk
	 * @param args
	 * @throws SemanticException sementic configuration not matching the target or lack of args.
	 */
	private void checkParas(String tabl, String pk, String[] args) throws SemanticException {
		if (tabl == null || pk == null || args == null || args.length == 0)
			throw new SemanticException(String.format(
					"adding semantics with empty targets? %s %s %s",
					tabl, pk, args));

		if (this.tabl != null && !this.tabl.equals(tabl))
			throw new SemanticException(String.format("adding semantics for different target? %s vs. %s", this.tabl, tabl));
		if (this.pk != null && !this.pk.equals(pk))
			throw new SemanticException(String.format("adding semantics for target of diferent id field? %s vs. %s", this.pk, pk));
	}
	
	private void checkSmtcs(String tabl, smtype newSmtcs) throws SemanticException {
		if (handlers == null) return;
		for (SemanticHandler handler : handlers)
			if (handler.sm == newSmtcs)
				throw new SemanticException("Found duplicate semantics: %s %s\nDetails: All semantics configuration is merged into 1 static copy. Each table in every connection can only have one instance of the same smtype.",
						tabl, newSmtcs.name());
	}

	public boolean isPrepareInsert() { return hasAutopk; }

	public void onInsPrepare(DASemantext semantx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.insert && handler.insPrepare)
					handler.onPrepare(semantx, row, cols, usr);
	}

	public void onInsert(ISemantext semantx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.insert && !handler.insPrepare)
					handler.onInsert(semantx, row, cols, usr);
	}

	//////////////////////////////// Base Handler //////////////////////////////
	abstract static class SemanticHandler {
		boolean insPrepare = false;
		boolean insert = false;
		boolean update = false;
		String target;
		String idField;
		String[] args;
		protected Transcxt trxt;

		protected smtype sm;

		SemanticHandler(Transcxt trxt, String semantic, String tabl, String pk,
				String[] args) throws SemanticException {
			this.trxt = trxt;
			target = tabl;
			idField = pk;
		}

		public void logi() {
			Utils.logi("Semantics Handler %s\ntabl %s, pk %s, args %s",
					sm.name(), target, idField, LangExt.toString(args));
		}

		void onPrepare(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {}
		void onInsert(ISemantext sxt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {}
		void onUpdate(ISemantext sxt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {}

		SemanticHandler(Transcxt trxt, smtype sm, String tabl, String pk,
				String[] args) throws SemanticException {
			this.trxt = trxt;
			target = tabl;
			idField = pk;
			this.sm = sm;
			this.args = args;
		}
	}
	
	//////////////////////////////// subclasses ////////////////////////////////
	/**
	 *When updating, auto update fullpath field according to parent-id and current record id<br>
	 * args 0: parent Id field, 1: sibling/sort field (optional), 2: fullpath field
	 * @author odys-z@github.com
	 */
	static class ShFullpath extends SemanticHandler {
		/**
		 * @param tabl
		 * @param recId
		 * @param args 0: parent Id field, 1: sibling/sort field (optional), 2: fullpath field
		 * @throws SemanticException
		 */
		public ShFullpath(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.fullpath, tabl, recId, args);
			insert = true;
			update = true;
		}

		@Override
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			String sibling = null;
			try { sibling = (String) row.get(cols.get(args[1]))[1];}
			catch (Exception e) {}
			
			String v = null;
			try {
				if (!cols.containsKey(idField) || row.get(cols.get(idField)) == null)
					throw new TransException("Fullpath configuration wrong: idField = %s, cols: %s",
							idField, LangExt.toString(cols));

				String id = (String) row.get(cols.get(idField))[1];

				String pid = cols.containsKey(args[0]) ?
							  (String) row.get(cols.get(args[0]))[1]
							: null;
			
				if (pid == null || "null".equals(pid)) {
					Utils.warn("Fullpath Handling Error\nTo generate fullpath, parentId must configured.\nFound parent col: %s,\nconfigured args = %s,\nhandling cols = %s\nrows = %s",
							pid, LangExt.toString(args), LangExt.toString(cols), LangExt.toString(row));
					v = id;
				}
				else {
					IResults rs = trxt.select(target, "_t0")
						.col(args[2])
						.where("=", idField, "'" + pid + "'")
						.rs(stx);

					if (rs .beforeFirst().next()) {
						String parentpath = rs.getString(args[2]);
						v = String.format("%s.%s%s", parentpath,
							sibling == null ? "" : sibling + " ", id);
					}
					else
						v = String.format("%s%s",
							sibling == null ? "" : sibling + " ", id);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			Object[] nv;
			if (cols.containsKey(args[2]))
				nv = row.get(cols.get(args[2]));
			else {
				nv = new Object[] {args[2], v};
				cols.put(args[2], row.size());
				row.add(nv);
			}
		}
		
		@Override
		void onUpdate(ISemantext sxt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			onInsert(sxt, row, cols, usr);
		}
	}

	/**Auto key handler.<br>
	 * smtc = "auto" | "a-k" | "pk" | "autopk"<br>
	 * Generate auto increased value for the field when inserting.<br>
	 * on-events: insert<br>
	 * smtc = "auto" | "ai" | "a-i"<br>
	 * args = [0: pk-field] */
	static class ShAutoK extends SemanticHandler {
		/**
		 * @param trxt
		 * @param tabl
		 * @param pk
		 * @param args 0: auto field
		 * @throws SemanticException
		 */
		ShAutoK(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.autoInc, tabl, pk, args);
			if (args == null || args.length == 0 || args[0] == null)
				throw new SemanticException("AUTO pk semantics configuration not correct. tabl = %s, pk = %s, args: %s",
						tabl, pk, LangExt.toString(args));
			insert = true;
			insPrepare = true;
		}
		
		@Override
		void onPrepare(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			Object[] nv;
			if (cols.containsKey(args[0]))
				nv = row.get(cols.get(args[0]));
			else {
				nv = new Object[2];
				cols.put(args[0], row.size());
				row.add(nv);
			}
			nv[0] = args[0];
			try {
				// side effect: generated auto key already been put into autoVals, can be referenced later. 
				nv[1] = stx.genId(target, args[0]);
			} catch (SQLException | TransException e) {
				e.printStackTrace();
			}
		}
	}

	/**Handle fk referencing resolving when inserting children.<br>
	 * Args[0]: referencing col, [1]: target table, [2] target pk col
	 */
	static class ShFkOnIns extends SemanticHandler {
		ShFkOnIns(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.fkIns, tabl, pk, args);
			insert = true;
			// TODO / FIXME not for updating?
		}

		@Override
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			Object[] nv;
			// Debug Note:
			// Don't use pk to find referencing col here, relation table's pk can be null.
			// Use args[0] instead.
			if (cols.containsKey(args[0])) { // referencing col
				nv = row.get(cols.get(args[0]));
			}
			else { // add a semantics required cell if it's absent.
				nv = new Object[] {args[0], null};
				cols.put(args[0], row.size());
				row.add(nv);
			}
//			SemanticObject refs = (SemanticObject) stx.results();
//			if (refs == null || !refs.has(args[1]))
//				Utils.warn("Trying resolve FK failed. fk = %s.%s, parent = %s.%s,\nFK config args:\t%s,\ndata cols:\t%s,\ndata row:\t%s.\nAlso note that in current version, only auto key can be referenced and auto resolved.",
//						target, args[0], args[1], args[2],
//						LangExt.toString(args), LangExt.toString(cols), LangExt.toString(row));
//			else {
//				Object refeev = ((SemanticObject) refs.get(args[1])).get(args[2]);
//				nv[1] = refeev == null ? nv[1] : refeev; // referee can be null?
//			}
			try {
				nv[1] = stx.resulvedVal(args[1], args[2]);
			}catch (Exception e) {
				if (nv[1] != null) {
					if (debug)
						Utils.warn("Trying resolve FK failed, but fk value exists. child-fk(%s.%s) = %s, parent = %s.%s",
								target, args[0], nv[1], args[1], args[2]);
				}
				else Utils.warn("Trying resolve FK failed. child-fk = %s.%s, parent = %s.%s,\n" +
							"FK config args:\t%s,\ndata cols:\t%s,\ndata row:\t%s.\n%s: %s\n" +
							"Also note that in current version, only auto key can be referenced and auto resolved.",
						target, args[0], args[1], args[2],
						LangExt.toString(args), LangExt.toString(cols), LangExt.toString(row),
						e.getClass().getName(), e.getMessage());
			}
		}
	}

	/**Delete childeren before delete parent.<br>
	 * smtype: {@link smtype#parentChildrenOnDel}
	 * 
	 * @author odys-z@github.com
	 */
	static class ShPCDelAll extends SemanticHandler {
		public ShPCDelAll(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.parentChildrenOnDel, tabl, recId, args);
			update = true;
		}

		@Override
		void onUpdate(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
			throw new SemanticException("Sorry TODO...");
		}
		
	}

	/**Handle default value.
	 * args: [0] value-field, [1] default-value
	 * @author odys-z@github.com
	 */
	static class ShDefltVal extends SemanticHandler {
		ShDefltVal(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.defltVal, tabl, recId, args);
			insert = true;
			update = true;
		}

		@Override
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			if (args.length > 1 && args[1] != null) {
				Object[] nv;
				if (cols.containsKey(args[1]))
					nv = row.get(cols.get(args[1]));
				else {
					nv = new Object[2];
					cols.put(args[1], row.size());
					row.add(nv);
				}
				nv[0] =  args[1];
				nv[1] =  args[2];
			}
		}
	}
	
	static class ShChkPCDel extends SemanticHandler {
		public ShChkPCDel(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.checkSqlCountOnDel, tabl, recId, args);
			update = true;
		}

		@Override
		void onUpdate(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
			throw new SemanticException("Sorry TODO...");
		}
		
	}
	
	static class ShChkPCInsert extends SemanticHandler {
		public ShChkPCInsert(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.checkSqlCountOnInsert, tabl, recId, args);
			insert = true;
		}
		
		@Override
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
			throw new SemanticException("fkIns? TODO...");
		}
		
	}
	
	/**semantics: automatic operator / time - time is now(), operator is session user id.<br>
	 * smtyp: {@link smtype#opTime}<br>
	 * args 0: oper-field, 1: oper-time-field (optional)
	 * 
	 * @author odys-z@github.com
	 */
	static class ShOperTime extends SemanticHandler {
		/**
		 * @param trxt
		 * @param tabl
		 * @param recId
		 * @param args 0: oper-field, 1: oper-time-field (optional)
		 * @throws SemanticException
		 */
		public ShOperTime(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.opTime, tabl, recId, args);
			insert = true;
			update = true;
		}

		@Override
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			// operTiem
			if (args.length > 1 && args[1] != null) {
				Object[] nvTime;
				if (cols.containsKey(args[1]))
					nvTime = row.get(cols.get(args[1]));
				else {
					nvTime = new Object[2];
					cols.put(args[1], row.size());
					row.add(nvTime);
				}
				nvTime[0] =  args[1];
				nvTime[1] =  Funcall.now(stx.dbtype());
			}

			// oper
			Object[] nvOper;
			if (cols.containsKey(args[0]))
				nvOper = row.get(cols.get(args[0]));
			else {
				nvOper = new Object[2];
				cols.put(args[0], row.size()); // oper
				row.add(nvOper);
			}
			nvOper[0] = args[0];
			nvOper[1] = usr == null ? "sys" : usr.uid();
		}
		
		void onUpdate(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			onInsert(stx, row, cols, usr);
		}
	}
}
