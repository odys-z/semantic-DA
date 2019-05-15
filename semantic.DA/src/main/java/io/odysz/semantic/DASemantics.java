package io.odysz.semantic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import io.odysz.common.LangExt;
import io.odysz.common.Regex;
import io.odysz.common.Utils;
import io.odysz.module.rs.SResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.parts.Logic;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.sql.parts.condition.Predicate;
import io.odysz.transact.x.TransException;

/**<h2>The default semantics used by semantic-DA.</h2>
 * <p>The {@link DASemantext} use this to manage semantics configuration for resolving data semantics.</p>
 * DASemantics is basically a {@link SemanticHandler}'s container, with subclass handlers handling different
 * semantics (processing values). </p>
 * <h3>What's DASemantics for?</h3>
 * <p>Well, the word semantics is a computer science term. The author don't want to redefine this word,
 * but here is some explanation what <i>semantic-DA</i> with <i>semantic-transact</i> is trying to support.</p>
 * <p>In a typical relational database based application, the main operation of data is CRUD.
 * And the most often such data operation can be abstracted to some operation pattern,
 * and they are always organized as a database transaction/batch operation described in SQL.</p>
 * <p>Take "book-author" relation for example, the author's ID is also the parent referenced by
 * book's author FK. If trying to delete an author in DB, there are 2 typical policies can be applied
 * by the application. The first is delete all books by the author accordingly; the second is warning and
 * denying the operation if some books are referencing the author. Both of this must/can been organized into
 * a transact/batch operation, with the second transact as check-then-delete.</p>
 * <p>In this case, you will find the FK relationship can be handled in a generalized operation, through
 * parameterizing some variables like table name, child referencing column name and parent ID.</p>
 * <p>Take the {@link DASemantics.smtype#parentChildrenOnDel} for example, it's automatically support
 * "deleting all children when deleting parent" semantics. What the user (application developer) need to
 * do is configure a semantics item then delete the parent directly.</p>
 * <p>Now you (a developer) will definitely understand what's the "parentChildrenOnDel" for. Semantic-DA
 * abstract and hide these patterns, wrapped them automatically into a transaction. That's what semantic-
 * DA want to do.</p>
 * <h3>How to Use</h3>
 * <p>To use this function:</p>
 * <p>1. Configure the "semantics.xml". See example in test/resources/semantics.xml.<br>
 * 2. Set the configured semantics as context of {@link io.odysz.transact.sql.Statement}. See example in
 * {@link io.odysz.semantic.DASemantextTest}. Then use Statement's subclass's commit() method to generate SQLs</p>
 * <h3>Is this Enough?</h3>
 * <p>The 9 or 10 types of semantics defined in {@link DASemantics.smtype} is enough for some enterprise projects.
 * It depends on how abstract the semantics we want to support. But it seems enough for us, at least now.</p>
 * </p>Another consideration is that semantic-DA never take supporting all semantics logic as it's goal.
 * It's only trying to release burden of daily repeated tasks. Fortunately, such tasks' logic is simple, and the
 * burden is heavy. Let semantic-* handle these simple logic, that's semantic-* designed for. If
 * the semantics is complex, use anything you are familiar with. But in this case semantic-* are still useful
 * to do this tasks, if users are familiar with the lower level API.</p>
 * <p>Before doing that, check the semantics-cheapflow workflow engine first, which is based on semantics-*,
 * and can handle typical - not very cheap if by our define - logics all necessary for enterprise applications.
 * It's a good example illustrating that if the semantics is designed carefully, those semantics supported by
 * this pattern is enough. </p>
 * <p>But it do needs the application developers follow some design conventions. If you need you own semantics
 * implementation, implement the interface {@link ISemantext}, or simply initialize {@link io.odysz.transact.sql.Transcxt}
 * with null semantics, which will disable semantic supporting. In that way, it's working as a structured sql composing API. 
 
 * @author odys-z@github.com
 */
public class DASemantics {
	/**error code key word*/
	public static final String ERR_CHK = "err_smtcs";;
	
	public static boolean debug = true; 

	/**Semantics type supported by DASemantics.
	 * For each semantics example, see <a href=''>semantic.DA/test/semantics.xml</a> 
	 * For semanticx.xml/s/smtc value, check the individual enum.<br>
	 * <b>0. {@link #autoInc}</b><br>
	 * <b>1. {@link #fkIns}</b><br>
	 * <b>2. {@link #fullpath}</b><br>
	 * <b>3. {@link #defltVal}</b><br>
	 * <b>4. {@link #parentChildrenOnDel}</b><br>
	 * <b>3. {@link #dencrypt}</b><br>
	 * <b>4. {@link #opTime}</b><br>
	 * <b>5. {@link #checkSqlCountOnDel} </b><br>
	 * <b>6. {@link #checkSqlCountOnInsert} </b><br>
	 * <b>7. {@link #checkDsCountOnDel} </b><br>
	 * <b>8. {@link #composingCol} TODO</b><br>
	 * <b>9. {@link #stampUp1ThanDown} TODO</b><br>
	 * <b>10.{@link #orclob} TODO</b><br>
	 * UpdateBatch supporting:<br>
	 * on inserting, up-stamp is the value of increased down stamp, or current time if it's not usable;<br>
	 * on updating, up-stamp is set as down stamp increased if down stamp value not presented in sql, or,
	 * up stamp will be ignored if down stamp presented. (use case of down stamp updating by synchronizer).<br>
	 * <b>x. orclob</b>: the field must saved as clob when driver type is orcl;
	 */
	public enum smtype {
		/**Auto Key<br>
		 * xml/smtc = "auto" | "pk" | "a-k" | "autopk" <br>
		 * Generate auto increased value for the field when inserting.<br>
		 * on-events: insert<br>
		 * smtc = "auto" | "ai" | "a-i"<br>
		 * args = [0]: pk-field <br>
		 * Handler: {@link DASemantics.ShAutoK} */
		autoInc,
		/**xml/smtc = "fk" | "pkref" | "fk-ins"<br>
		 * Handler: {@link DASemantics.ShFkOnIns} */
		fkIns,
		/**xml/smtc = "f-p" | "fp" | "fullpath":<br>
		 * Handler: {@link ShFullpath#ShFullpath(String, String, String[])}*/
		fullpath,
		/**xml/smtc = "dv" | "d-v" | "dfltVal":<br>
		 * Handler: {@link ShDefltVal} */
		defltVal,
		/** "pc-del-all" | "parent-child-del-all" | "parentchildondel"<br>
		 * <pre>args:
 [0] name or child referencing column (a_domain.domainId's value will be used)
 [1] child table
 [2] child pk (or condition column)

 Example: domainId a_orgs orgType, ...

 The sql of the results shall be:
 delete from a_orgs where orgType in (select domainId from a_domain where domainId = '000001')
 where the 'where clause' in select clause is composed from condition of the delete request's where condition.</pre>
		 * Handler: {@link ShChkSqlCntDel}*/
		parentChildrenOnDel,
		/** "d-e" | "de-encrypt" | "dencrypt":<br>
		 * decrypt then encrypt (target col cannot be pk or anything other semantics will updated<br>
		 * Handler: {@link TODO}*/
		dencrypt,
		/**xml/smtc = "o-t" | "oper-time" | "optime"<br>
		 * Finger printing session user's db updating - record operator / oper-time<br>
		 * Handler: {@link ShOperTime}*/
		opTime,
		/** "ck-cnt-del" | "check-count-del" | "checksqlcountondel":<br>
		 * check is this record a referee of children records - results from sql.select(count, description-args ...).
		 * The record(s) can't been deleted if referenced;<br>
		 * <pre> [0] name or child referencing column (a_domain.domainId's value will be used)
 [1] child table
 [2] child pk (or condition column)

 Example: domainId a_orgs orgType, ...

 The sql of the results shall be:
 select count(orgType) from a_orgs where orgType in (select domainId from a_domain where domainId = '000001')
 where the 'where clause' in select clause is composed from condition of the delete request's where condition.</pre>
		 * where args are column name of parent table.</p>
		 * Handler: {@link ShChkSqlCntDel}*/
		checkSqlCountOnDel,
		/** "ck-cnt-ins" | "check-count-ins" | "checksqlcountoninsert":<br>
		 * Check is this record count when inserting - results from sql.select(count-sql, description-args ...).
		 * The record(s) can't been inserted if count > 0;<br>
		 * <p>args: [0] arg1, [1] arg2, ..., [len -1] count-sql with "%s" formatter<br>
		 * where args are column name of parent table.</p>
		 * Handler: {@link ShChkCntInst}*/
		checkSqlCountOnInsert,
		/** "cmp-col" | "compose-col" | "compse-column": compose a column from other columns;<br>
		 * TODO*/
		composingCol,
		/** "s-up1" | "stamp-up1": add 1 more second to down-stamp column and save to up-stamp<br>
		 * TODO*/
		stamp1MoreThanRefee,
		/** "clob" | "orclob": the column is a CLOB field, semantic-transact will read/write separately in stream and get final results.<br>
		 * Handler: TODO? */
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
			else if ("fp".equals(type) || "f-p".equals(type) || "fullpath".equals(type))
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
			else throw new SemanticException("semantics not known, type: " + type);
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
			handler = new ShChkCntDel(basicTsx, tabl, recId, args);
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

	public void onUpdate(ISemantext semantx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.update)
					handler.onUpdate(semantx, row, cols, usr);
	}

	public void onDelete(ISemantext semantx, Statement<? extends Statement<?>> stmt,
			Condit whereCondt, IUser usr) throws TransException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.delete)
					handler.onDelete(semantx, stmt, whereCondt, usr);
	}
	//////////////////////////////// Base Handler //////////////////////////////
	abstract static class SemanticHandler {
		boolean insPrepare = false;
		boolean insert = false;
		boolean update = false;
		boolean delete = false;

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
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {}
		void onUpdate(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {}

		/**Handle onDelete event.
		 * @param stx
		 * @param stmt
		 * @param whereCondt delete statement's condition.
		 * @param usr
		 * @throws TransException
		 */
		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit whereCondt, IUser usr) throws TransException { }

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
					SemanticObject s = trxt.select(target, "_t0")
						.col(args[2])
						.where("=", idField, "'" + pid + "'")
						.rs(stx);

					SResultset rs = (SResultset) s.rs(0);
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

	/**@see smtype#autoInc
	 * @author odys-z@github.com
	 */
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
			if (cols.containsKey(args[0])			// with nv from client
				&& cols.get(args[0]) < row.size())	// with nv must been generated from semantics
				nv = row.get(cols.get(args[0]));
			else {
				nv = new Object[2];
				cols.put(args[0], row.size());
				row.add(nv);
			}
			nv[0] = args[0];

			try {

				Object alreadyResulved = stx.resulvedVal(target, args[0]);
				if (alreadyResulved != null) {
					// When cross fk referencing happened, this branch will reached by handling post inserts.
					Utils.warn("Debug Notes: Found an already resulved value (%s) while handling %s auto-key generation.",
							alreadyResulved, target);
					nv[1] = alreadyResulved;
				}
				else 
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
		}

		@Override
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			Object[] nv;
			// Debug Note:
			// Don't use pk to find referencing col here, related table's pk can be null.
			// Use args[0] instead.
			if (cols.containsKey(args[0])			// with nv from client
				&& cols.get(args[0]) < row.size())	// with nv must been generated from semantics
//			if (cols.containsKey(args[0])) { // referencing col
				nv = row.get(cols.get(args[0]));
//			}
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
	 * args: [0] parent's referee column\s [1] child-table1\s [2] child pk1, // child 1, with comma ending, space separated
	 * 		 {0] parent's referee column\s [1] child-table2\s [2] child pk2  // child 1, without comma ending, space separated
	 * smtype: {@link smtype#parentChildrenOnDel}
	 * 
	 * @author odys-z@github.com
	 */
	static class ShPCDelAll extends SemanticHandler {
		private String[][] argss;

		public ShPCDelAll(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.parentChildrenOnDel, tabl, recId, args);
			delete = true;
			argss = split(args);
		}
		
		public static String[][] split(String[] ss) {
			if (ss == null) return null;
			String[][] argss = new String[ss.length][];
			for (int ix = 0; ix < ss.length; ix++) {
				String[] args = LangExt.split(ss[ix], " ");
				argss[ix] = args;
			}
			return argss;
		}

		@Override
		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr) throws TransException {
			if (argss != null && argss.length > 0)
				for (String[] args : argss)
					if (args != null && args.length > 1 && args[1] != null) {
						stmt.before(delChild(args, stmt, condt, usr));
						// Design Notes: about multi level children deletion:
						// If row can't been retrieved here, cascading children deletion can't been supported 
					}
		}

		/** genterate sql e.g. delete from child where child_pk = parent.referee
		 * @param args
		 * @param stmt
		 * @param condt deleting's condition
		 * @param usr 
		 * @return {@link Delete}
		 * @throws TransException 
		 */
		private Delete delChild(String[] args, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr) throws TransException {
			if (condt == null)
				throw new SemanticException("Parent table %s has a semantics triggering child table (%s) deletion, but the condition is null.",
						target, args[1]);

			Query s = stmt.transc().select(target)
					.col(args[0])
					.where(condt);

			Predicate inCondt = new Predicate(Logic.op.in, args[0], s);
			Delete d = stmt.transc().delete(args[1])
					.where(new Condit(inCondt));

			return d;
//			if (v != null) 
//				return s.where_("=", String.format("%s.%s", args[1], args[2]), v);
//			else 
//				// return an unresolved value for debugging.
//				return s.where("=", args[2], String.format("%s.%s", target, args[0]));
		}
		
	}

	/**Handle default value.
	 * args: [0] value-field, [1] default-value<br>
	 * e.g. pswd,123456 can set pwd = '123456' 
	 * @author odys-z@github.com
	 */
	static class ShDefltVal extends SemanticHandler {
		static Regex regQuot = new Regex("^\\s*'.*'\\s*$");
		ShDefltVal(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.defltVal, tabl, recId, args);
			insert = true;
			update = true;
			
			args[1] = dequote(args[1]);
		}

		@Override
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			if (args.length > 1 && args[1] != null) {
				Object[] nv;
				if (cols.containsKey(args[0])			// with nv from client
					&& cols.get(args[0]) < row.size())	// with nv must been generated from semantics
//				if (cols.containsKey(args[1]))
					nv = row.get(cols.get(args[0]));
				else {
					nv = new Object[2];
					cols.put(args[0], row.size());
					row.add(nv);
				}
				nv[0] =  args[0];
				if (nv[1] == null)
					nv[1] = args[1];
				else if ("".equals(nv[1]) && args[1] != null && !args[1].equals(""))
					// this is not robust but any better way to handle empty json value?
					nv[1] = args[1];
			}
		}

		private String dequote(String dv) {
			if (dv != null && dv instanceof String && regQuot.match((String)dv))
				return ((String)dv).replaceAll("^\\s*'", "").replaceFirst("'\\s*$", "");
			return (String) dv;
		}
	}
	
	/**Check with sql before deleting<br>
	 * @see smtype#checkSqlCountOnDel
	 * @author odys-z@github.com
	 */
	static class ShChkCntDel extends SemanticHandler {
		private String[][] argss;

		public ShChkCntDel(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.checkSqlCountOnDel, tabl, recId, args);
			delete = true;
			argss = ShPCDelAll.split(args);
		}

		@Override
		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr) throws TransException {
			if (argss != null && argss.length > 0)
				for (String[] args : argss)
					if (args != null && args.length > 1 && args[1] != null) {
						// stmt.before(delChild(args, stmt, condt, usr));
						chkCnt(args, stmt, condt);
					}
		}

		private void chkCnt(String[] args, Statement<? extends Statement<?>> stmt, Condit condt) throws TransException {
			SemanticObject s;
			try {
				Query slct = stmt.transc().select(target)
						.col(args[0])
						.where(condt);

				Predicate inCondt = new Predicate(Logic.op.in, args[2], slct);

				s = stmt.transc().select(args[1])
						.col("count("+ args[2] + ")", "cnt")
						.where(inCondt)
						.rs(stmt.transc().basictx());

				SResultset rs = (SResultset) s.rs(0);
				rs.beforeFirst().next();

				if (rs.getInt("cnt") > 0)
					throw new SemanticException("%s.%s: %s", target, sm.name(), rs.getInt("cnt"));
			} catch (SQLException e) {
				e.printStackTrace();
				throw new TransException(e.getMessage());
			}
		}
		
	}
	
	static class ShChkPCInsert extends SemanticHandler {
		public ShChkPCInsert(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.checkSqlCountOnInsert, tabl, recId, args);
			insert = true;
		}
		
		@Override
		void onInsert(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
			if (args.length > 1 && args[1] != null) {
				Object[] nv = new Object[args.length - 1];
				
				// find values
				for (int ix = 0; ix < args.length - 1; ix++) {
					if (cols.containsKey(args[ix])) {
						Object[] nmval = row.get(cols.get(args[ix]));
						if (nmval != null && nmval.length > 1 && nmval[1] != null)
							nv[ix] = nmval[1];
						else nv[ix] = "";
					}
				}
				String sql = String.format(args[args.length - 1], nv);
				try {
					SResultset rs = Connects.select(stx.connId(), sql, Connects.flag_nothing);
					rs.beforeFirst().next();
					if (rs.getInt(1) > 0)
						throw new SemanticException("Checking count on %s.%s (%s = %s ...) failed",
								target, idField, args[0], nv[0]);
				} catch (SQLException e) {
					throw new SemanticException("Can't access db to check count on insertion, check sql configuration: %s", sql);
				}

			}
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
