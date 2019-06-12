package io.odysz.semantic;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

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
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.ExtFile;
import io.odysz.transact.sql.parts.Logic;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.sql.parts.condition.Predicate;
import io.odysz.transact.x.TransException;

/**
 * <h2>The default semantics plugin used by semantic-DA.</h2>
 * <p>
 * The {@link DASemantext} use this to manage semantics configuration for
 * resolving data semantics.
 * </p>
 * DASemantics is basically a {@link SemanticHandler}'s container, with subclass
 * handlers handling different semantics (processing values).
 * </p>
 * <h3>What's DASemantics for?</h3>
 * <p>
 * Well, the word semantics is a computer science term. The author don't want to
 * redefine this word, but here is some explanation what <i>semantic-DA</i> with
 * <i>semantic-transact</i> is trying to support.
 * </p>
 * <p>
 * In a typical relational database based application, the main operation of
 * data is CRUD. And the most often such data operation can be abstracted to
 * some operation pattern, and they are always organized as a database
 * transaction/batch operation described in SQL.
 * </p>
 * <p>
 * Take "book-author" relation for example, the author's ID is also the parent
 * referenced by book's author FK. If trying to delete an author in DB, there
 * are 2 typical policies can be applied by the application. The first is delete
 * all books by the author accordingly; the second is warning and denying the
 * operation if some books are referencing the author. Both of this must/can
 * been organized into a transact/batch operation, with the second transact as
 * check-then-delete.
 * </p>
 * <p>
 * In this case, you will find the FK relationship can be handled in a
 * generalized operation, through parameterizing some variables like table name,
 * child referencing column name and parent ID.
 * </p>
 * <p>
 * Take the {@link DASemantics.smtype#parentChildrenOnDel} for example, it's
 * automatically support "deleting all children when deleting parent" semantics.
 * What the user (application developer) need to do is configure a semantics
 * item then delete the parent directly.
 * </p>
 * <p>
 * Now you (a developer) will definitely understand what's the
 * "parentChildrenOnDel" for. Semantic-DA abstract and hide these patterns,
 * wrapped them automatically into a transaction. That's what semantic- DA want
 * to do.
 * </p>
 * <h3>How to Use</h3>
 * <p>
 * To use this function:
 * </p>
 * <p>
 * 1. Configure the "semantics.xml". See example in
 * test/resources/semantics.xml.<br>
 * 2. Set the configured semantics as context of
 * {@link io.odysz.transact.sql.Statement}. See example in
 * {@link io.odysz.semantic.DASemantextTest}. Then use Statement's subclass's
 * commit() method to generate SQLs
 * </p>
 * <h3>Is this Enough?</h3>
 * <p>
 * The 9 or 10 types of semantics defined in {@link DASemantics.smtype} is
 * enough for some enterprise projects. It depends on how abstract the semantics
 * we want to support. But it seems enough for us, at least now.
 * </p>
 * </p>
 * Another consideration is that semantic-DA never take supporting all semantics
 * logic as it's goal. It's only trying to release burden of daily repeated
 * tasks. Fortunately, such tasks' logic is simple, and the burden is heavy. Let
 * semantic-* handle these simple logic, that's semantic-* designed for. If the
 * semantics is complex, use anything you are familiar with. But in this case
 * semantic-* are still useful to do this tasks, if users are familiar with the
 * lower level API.
 * </p>
 * <p>
 * Before doing that, check the semantics-cheapflow workflow engine first, which
 * is based on semantics-*, and can handle typical - not very cheap if by our
 * define - logics all necessary for enterprise applications. It's a good
 * example illustrating that if the semantics is designed carefully, those
 * semantics supported by this pattern is enough.
 * </p>
 * <p>
 * But it do needs the application developers follow some design conventions. If
 * you need you own semantics implementation, implement the interface
 * {@link ISemantext}, or simply initialize
 * {@link io.odysz.transact.sql.Transcxt} with null semantics, which will
 * disable semantic supporting. In that way, it's working as a structured sql
 * composing API.
 * 
 * @author odys-z@github.com
 */
public class DASemantics {
	/** error code key word */
	public static final String ERR_CHK = "err_smtcs";;

	public static boolean debug = true;

	/**
	 * Semantics type supported by DASemantics. For each semantics example, see
	 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/res/semantics.xml'>
	 * semantic.DA/test/semantics.xml</a><br>
	 * For semanticx.xml/s/smtc value, check the individual enum values:<br>
	 * <b>0. {@link #autoInc}</b><br>
	 * <b>1. {@link #fkIns}</b><br>
	 * <b>2. {@link #fkCateIns}</b><br>
	 * <b>3. {@link #fullpath}</b><br>
	 * <b>4. {@link #defltVal}</b><br>
	 * <b>5. {@link #parentChildrenOnDel}</b><br>
	 * <b>6. {@link #parentChildrenOnDelByTable}</b><br>
	 * <b>7. {@link #dencrypt}</b><br>
	 * <b>8. {@link #opTime}</b><br>
	 * <b>9. {@link #checkSqlCountOnDel} </b><br>
	 * <b>10.{@link #checkSqlCountOnInsert} </b><br>
	 * <b>11.{@link #checkDsCountOnDel} </b><br>
	 * <b>12.{@link #postFk}</b><br>
	 * <b>13.{@link #extFile}</b><br>
	 * <b>14.{@link #composingCol} TODO</b><br>
	 * <b>15. {@link #stampUp1ThanDown} TODO</b><br>
	 * <b>16.{@link #orclob} TODO</b><br>
	 */
	public enum smtype {
		/**
		 * Auto Key<br>
		 * xml/smtc = "auto" | "pk" | "a-k" | "autopk" <br>
		 * Generate auto increased value for the field when inserting.<br>
		 * on-events: insert<br>
		 * <p>
		 * args: [0]: pk-field
		 * </p>
		 * 
		 * Handler: {@link DASemantics.ShAutoK}
		 */
		autoInc,
		/**
		 * xml/smtc = "fk" | "pkref" | "fk-ins"<br>
		 * <p>
		 * Automatically fill fk when inserting. Only referenced auto pk can be
		 * resolved.
		 * </p>
		 * <p>
		 * args: 0 referencing col, 1 parent table, 2 parent pk
		 * </p>
		 * Handler: {@link DASemantics.ShFkOnIns}
		 */
		fkIns,
		/**
		 * xml/smtc = "fkc" | "f-i-c" | "fk-ins-cate"<br>
		 * <p>Automatically fill merged child fk when inserting.
		 * Only referenced auto pk can be resolved.</p>
		 * <p>About Merged Child Table:<br>
		 * Take the <i>attachements</i> table for external file's information for example,
		 * the a_attaches(See
		 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/res'>
		 * sqlite test DB</a>) has a field, named 'busiId', referencing multiple parent table.
		 * The parent table is distinguished with filed busiTbl.
		 * </p>
		 * <p>args: 0 business cate (table name); 1 merged child fk; 2 parent table, 3 parent referee [, ...]</p>
		 * Handler: {@link DASemantics.ShFkOnIns}
		 */
		fkCateIns,
		/**
		 * xml/smtc = "f-p" | "fp" | "fullpath":<br>
		 * <p>
		 * args: 0: parent Id field, 1: sibling/sort field (optional), 2: fullpath field
		 * </p>
		 * Handler: {@link ShFullpath}
		 */
		fullpath,
		/**
		 * xml/smtc = "dv" | "d-v" | "dfltVal":<br>
		 * Handler: {@link ShDefltVal}
		 */
		defltVal,
		/**
		 * "pc-del-all" | "parent-child-del-all" | "parentchildondel"<br>
		 * 
		 * <pre>args: [pc-define, ...], where pc-define is a space sperated strings:
		pc-define[0] name or child referencing column, e.g. domainId for a_domain.domainId
		pc-define[1] child table, e.g. a_orgs
		pc-define[2] child fk (or condition column), e.g. orgType
		
		Example: domainId a_orgs orgType, ...
		
		When deleting a_domain, the sql of the results shall be:
		delete from a_orgs where orgType in (select domainId from a_domain where domainId = '000001')
		where the 'where clause' in select clause is composed from condition of the delete request's where condition.
		 * </pre>
		 * Handler: {@link ShPCDelAll}
		 */
		parentChildrenOnDel,
		/**
		 * "pc-del-tbl" | "pc-del-by-tbl" | "pc-tbl"<br>
		 * 
		 * <pre>args: [pc-define, ...], where pc-define is a space sperated strings:
		pc-define[0] name or child referencing column (a_domain.domainId's value will be used)
		pc-define[1] child table
		pc-define[2] child fk (or condition column)
		pc-define[3] child cate (e.g. table name)
		
		Example: domainId a_orgs orgType, ...
		
		The sql of the results shall be:
		delete from a_orgs where orgType in (select domainId from a_domain where domainId = '000001')
		where the 'where clause' in select clause is composed from condition of the delete request's where condition.
		 * </pre>
		 * 
		 * Handler: {@link ShPCDelByTbl}
		 */
		parentChildrenOnDelByTabl,
		/**
		 * "d-e" | "de-encrypt" | "dencrypt":<br>
		 * decrypt then encrypt (target col cannot be pk or anything other semantics
		 * will updated<br>
		 * Handler: {@link TODO}
		 */
		dencrypt,
		/**
		 * xml/smtc = "o-t" | "oper-time" | "optime"<br>
		 * Finger printing session user's db updating - record operator / oper-time<br>
		 * Handler: {@link ShOperTime}
		 */
		opTime,
		/**
		 * "ck-cnt-del" | "check-count-del" | "checksqlcountondel":<br>
		 * check is this record a referee of children records - results from
		 * sql.select(count, description-args ...). The record(s) can't been deleted if
		 * referenced;<br>
		 * 
		 * <pre>
		 *  [0] name or child referencing column (a_domain.domainId's value will be used)
		[1] child table
		[2] child pk (or condition column)
		
		Example: domainId a_orgs orgType, ...
		
		The sql of the results shall be:
		select count(orgType) from a_orgs where orgType in (select domainId from a_domain where domainId = '000001')
		where the 'where clause' in select clause is composed from condition of the delete request's where condition.
		 * </pre>
		 * 
		 * where args are column name of parent table.
		 * </p>
		 * Handler: {@link ShChkSqlCntDel}
		 */
		checkSqlCountOnDel,
		/**
		 * "ck-cnt-ins" | "check-count-ins" | "checksqlcountoninsert":<br>
		 * Check is this record count when inserting - results from
		 * sql.select(count-sql, description-args ...). The record(s) can't been
		 * inserted if count > 0;<br>
		 * <p>
		 * args: [0] arg1, [1] arg2, ..., [len -1] count-sql with "%s" formatter<br>
		 * where args are column name of parent table.
		 * </p>
		 * Handler: {@link ShChkCntInst}
		 */
		checkSqlCountOnInsert,
		/**
		 * "p-f" | "p-fk" | "post-fk"<br>
		 * <p>
		 * <b>semantics:</b> post fk wire back - parent has an fk to child (only one
		 * child is sensible, like makes cross refs)
		 * </p>
		 * <p>
		 * <b>Note:</b><br>
		 * This semantics works only when previously resolved auto key exists; if the
		 * value doesn't exist, will be ignored.<br>
		 * The former is the case of inserting new child, and parent refer to it; the
		 * later is the case of updating a child, the parent already has it's pk,
		 * nothing should be done.
		 * </p>
		 * <p>
		 * <b>Further Discussion:</b><br>
		 * As cross reference is not a good ideal, this semantics sometimes leads to
		 * trouble. Any suggestion or comments are welcome.
		 * </p>
		 * <p>
		 * <b>args:</b> 0 referencing col, 1 target table, 2 target pk(must be an auto
		 * key)
		 * </p>
		 * <b>Handler:</b> {@link ShPostFk}
		 */
		postFk,
		/**
		 * Attach Attachments to Attaching Table (saving file in file system)<br>
		 * xml/smtc = "ef" | "xf" | "ext-file" | "e-f" | "x-f" <br>
		 * Take the update statement's file field as a separated file clob (base 64
		 * encoded). When updating, save it to file system, then replace the nv's v with
		 * filename<br>
		 * on-events: insert, update<br>
		 * <p>
		 * args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)<br>
		 * Handler: {@link DASemantics.ShExtFile} <br>
		 * 
		 * <h5>About Updating Handling</h5>
		 * <p>On updating external files handler.</p>
		 * <p>This method only throw an exception currently, applying the semantics predefined as:<br>
		 * AS all files are treaded as binary file, no file can be modified, only delete then create it makes sense.</p>
		 * <p>Client should avoid updating an external file will handling business logics.</p>
		 * <p><b>NOTE:</b><br>This can be changed in the future.</p>
		 * 
		 * Attechment info's table sql (mysql)
		 * <pre>CREATE TABLE `a_attaches` (
		`attId` varchar(20) COLLATE utf8mb4_bin NOT NULL,
		`attName` varchar(50) CHARACTER SET utf8mb4 DEFAULT NULL,
		`subPath` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL,
		`busiTbl` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
		`recId` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
		`oper` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
		`optime` datetime DEFAULT NULL,
		PRIMARY KEY (`attId`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
		 * </pre>
		 * 
		 * sqlite:
		 * 
		 * <pre>
		 * CREATE TABLE a_attaches (
		attId TEXT NOT NULL,
		attName TEXT,
		uri TEXT,
		busiTbl TEXT,
		busiId TEXT,
		oper TEXT,
		optime DATETIME,
		CONSTRAINT a_attaches_PK PRIMARY KEY (attId)) ;
		 * </pre>
		 */
		extFile,
		/**
		 * "cmp-col" | "compose-col" | "compse-column": compose a column from other
		 * columns;<br>
		 * TODO
		 */
		composingCol,
		/**
		 * TODO
		 * "s-up1" | "stamp-up1": add 1 more second to down-stamp column and save to
		 * up-stamp<br>
		 * UpdateBatch supporting:<br>
		 * on inserting, up-stamp is the value of increased down stamp, or current time
		 * if it's not usable;<br>
		 * on updating, up-stamp is set as down stamp increased if down stamp value not
		 * presented in sql, or, up stamp will be ignored if down stamp presented. (use
		 * case of down stamp updating by synchronizer).<br>
		 */
		stamp1MoreThanRefee,
		/**
		 * "clob" | "orclob": the column is a CLOB field, semantic-transact will
		 * read/write separately in stream and get final results.<br>
		 * Handler: TODO?
		 */
		orclob;

		/**
		 * Note: we don't use enum.valueOf(), because of fault / fuzzy tolerate.
		 * 
		 * @param type
		 * @return {@link smtype}
		 * @throws SemanticException
		 */
		public static smtype parse(String type) throws SemanticException {
			if (type == null)
				throw new SemanticException("semantics is null");
			type = type.toLowerCase().trim();
			if ("auto".equals(type) || "pk".equals(type) || "a-k".equals(type) || "autopk".equals(type))
				return autoInc;
			else if ("fk".equals(type) || "pkref".equals(type) || "fk-ins".equals(type))
				return fkIns;
			else if ("fk-ins-cate".equals(type) || "f-i-c".equals(type) || "fkc".equals(type))
				return fkCateIns;
			else if ("fp".equals(type) || "f-p".equals(type) || "fullpath".equals(type))
				return fullpath;
			else if ("dfltVal".equals(type) || "d-v".equals(type) || "dv".equals(type))
				return defltVal;
			else if ("pc-del-all".equals(type) || "parent-child-del-all".equals(type)
					|| "parentchildondel".equals(type))
				return parentChildrenOnDel;
			else if ("pc-del-tbl".equals(type) || "pc-del-by-tabl".equals(type)
					|| "pc-tbl".equals(type))
				return parentChildrenOnDelByTabl;
			else if ("d-e".equals(type) || "de-encrypt".equals(type) || "dencrypt".equals(type))
				return dencrypt;
			else if ("o-t".equals(type) || "oper-time".equals(type) || "optime".equals(type))
				return opTime;
			else if ("ck-cnt-del".equals(type) || "check-count-del".equals(type) || "checksqlcountondel".equals(type))
				return checkSqlCountOnDel;
			else if ("ck-cnt-ins".equals(type) || "check-count-ins".equals(type)
					|| "checksqlcountoninsert".equals(type))
				return checkSqlCountOnInsert;
			else if ("p-f".equals(type) || "p-fk".equals(type) || "post-fk".equals(type))
				return postFk;
			else if ("cmp-col".equals(type) || "compose-col".equals(type) || "compse-column".equals(type)
					|| "composingcol".equals(type))
				return composingCol;
			else if ("s-up1".equals(type) || type.startsWith("stamp1"))
				return stamp1MoreThanRefee;
			else if ("clob".equals(type) || "orclob".equals(type))
				return orclob;
			else if ("ef".equals(type) || "e-f".equals(type) || "ext-file".equals(type) || "xf".equals(type)
					|| "x-f".equals(type))
				return extFile;
			else
				throw new SemanticException("semantics not known, type: " + type);
		}
	}

	private HashMap<String, DASemantics> ss;

	/**
	 * Static transact context for DB accessing without semantics support.<br>
	 * Used to generate auto ID.
	 */
	private Transcxt basicTsx;

	public DASemantics get(String tabl) {
		return ss == null ? null : ss.get(tabl);
	}

	///////////////////////////////// container class
	///////////////////////////////// ///////////////////////////////
	private ArrayList<SemanticHandler> handlers;

	private String tabl;
	private String pk;

	public static boolean verbose = true;

	public DASemantics(Transcxt basicTx, String tabl, String recId) {
		this.tabl = tabl;
		this.pk = recId;
		basicTsx = basicTx;

		handlers = new ArrayList<SemanticHandler>();
	}

	public void addHandler(smtype semantic, String tabl, String recId, String[] args) throws SemanticException, SQLException {
		checkParas(tabl, pk, args);
		if (isDuplicate(tabl, semantic))
			return;
		SemanticHandler handler = null;

		if (smtype.fullpath == semantic)
			handler = new ShFullpath(basicTsx, tabl, recId, args);
		else if (smtype.autoInc == semantic)
			handler = new ShAutoK(basicTsx, tabl, recId, args);
		else if (smtype.fkIns == semantic)
			handler = new ShFkOnIns(basicTsx, tabl, recId, args);
		else if (smtype.fkCateIns == semantic)
			handler = new ShFkCates(basicTsx, tabl, recId, args);
		else if (smtype.parentChildrenOnDel == semantic)
			handler = new ShPCDelAll(basicTsx, tabl, recId, args);
		else if (smtype.parentChildrenOnDelByTabl == semantic)
			handler = new ShPCDelByCate(basicTsx, tabl, recId, args);
		else if (smtype.defltVal == semantic)
			handler = new ShDefltVal(basicTsx, tabl, recId, args);
		// else if (smtype.dencrypt == semantic)
		// addDencrypt(tabl, recId, argss);
		// else if (smtype.orclob == semantic)
		// addClob(tabl, recId, argss);
		else if (smtype.opTime == semantic)
			handler = new ShOperTime(basicTsx, tabl, recId, args);
		else if (smtype.checkSqlCountOnDel == semantic)
			handler = new ShChkCntDel(basicTsx, tabl, recId, args);
		else if (smtype.checkSqlCountOnInsert == semantic)
			handler = new ShChkPCInsert(basicTsx, tabl, recId, args);
		else if (smtype.postFk == semantic)
			handler = new ShPostFk(basicTsx, tabl, recId, args);
		else if (smtype.extFile == semantic)
			handler = new ShExtFile(basicTsx, tabl, recId, args);
		// else if (smtype.composingCol == semantic)
		// addComposings(tabl, recId, argss);
		// else if (smtype.stamp1MoreThanRefee == semantic)
		// addUpDownStamp(tabl, recId, argss);
		else
			throw new SemanticException("Unsuppported semantics: " + semantic);

		if (debug)
			handler.logi();
		handlers.add(handler);
	}

	/**
	 * Throw exception if args is null or target (table) not correct.
	 * 
	 * @param tabl
	 * @param pk
	 * @param args
	 * @throws SemanticException
	 *             sementic configuration not matching the target or lack of args.
	 */
	private void checkParas(String tabl, String pk, String[] args) throws SemanticException {
		if (tabl == null || pk == null || args == null || args.length == 0)
			throw new SemanticException(String.format("adding semantics with empty targets? %s %s %s", tabl, pk, args));

		if (this.tabl != null && !this.tabl.equals(tabl))
			throw new SemanticException(
					String.format("adding semantics for different target? %s vs. %s", this.tabl, tabl));
		if (this.pk != null && !this.pk.equals(pk))
			throw new SemanticException(
					String.format("adding semantics for target of diferent id field? %s vs. %s", this.pk, pk));
	}

	/**
	 * Check is the semantics duplicated?
	 * 
	 * @param tabl
	 * @param newSmtcs
	 * @return false no duplicating, true duplicated
	 * @throws SemanticException
	 */
	private boolean isDuplicate(String tabl, smtype newSmtcs) throws SemanticException {
		if (handlers == null)
			return false;
		for (SemanticHandler handler : handlers)
			if (handler.sm == newSmtcs && newSmtcs != smtype.fkIns && newSmtcs != smtype.postFk) {
				Utils.warn("Found duplicate semantics: %s %s\n"
						+ "Details: All semantics configuration is merged into 1 static copy. Each table in every connection can only have one instance of the same smtype.",
						tabl, newSmtcs.name());
				return true;
			}
		return false;
	}

	public boolean has(smtype sm) {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.sm == sm)
					return true;
		return false;
	}

	public void onInsert(ISemantext semantx, Insert statemt, ArrayList<Object[]> row, Map<String, Integer> cols,
			IUser usr) throws SemanticException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.insert)
					handler.onInsert(semantx, statemt, row, cols, usr);
	}

	public void onUpdate(ISemantext semantx, Update satemt, ArrayList<Object[]> row, Map<String, Integer> cols,
			IUser usr) throws SemanticException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.update)
					handler.onUpdate(semantx, satemt, row, cols, usr);
	}

	public void onDelete(ISemantext semantx, Statement<? extends Statement<?>> stmt, Condit whereCondt, IUser usr)
			throws SemanticException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.delete)
					handler.onDelete(semantx, stmt, whereCondt, usr);
	}

	public void onPost(DASemantext sx, Statement<? extends Statement<?>> stmt, ArrayList<Object[]> row,
			Map<String, Integer> cols, IUser usr, ArrayList<String> sqlBuf) throws SemanticException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.post)
					handler.onPost(sx, stmt, row, cols, usr, sqlBuf);
	}

	//////////////////////////////// Base Handler //////////////////////////////
	abstract static class SemanticHandler {
		boolean insert = false;
		boolean update = false;
		boolean delete = false;

		boolean post = false;

		String target;
		String pkField;
		String[] args;
		protected Transcxt trxt;

		protected smtype sm;

		SemanticHandler(Transcxt trxt, String semantic, String tabl, String pk, String[] args)
				throws SemanticException {
			this.trxt = trxt;
			target = tabl;
			pkField = pk;
		}

		public void logi() {
			Utils.logi("Semantics Handler %s\ntabl %s, pk %s, args %s", sm.name(), target, pkField,
					LangExt.toString(args));
		}

		// void onPrepare(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer>
		// cols, IUser usr) {}
		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
		}

		void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
		}

		/**
		 * Handle onDelete event.
		 * 
		 * @param stx
		 * @param stmt
		 * @param whereCondt
		 *            delete statement's condition.
		 * @param usr
		 * @throws SemanticException
		 * @throws SQLException 
		 */
		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit whereCondt, IUser usr)
				throws SemanticException {
		}

		void onPost(DASemantext sm, Statement<? extends Statement<?>> stmt, ArrayList<Object[]> row,
				Map<String, Integer> cols, IUser usr, ArrayList<String> sqlBuf) throws SemanticException {
		}

		SemanticHandler(Transcxt trxt, smtype sm, String tabl, String pk, String[] args) throws SemanticException {
			this.trxt = trxt;
			target = tabl;
			pkField = pk;
			this.sm = sm;
			this.args = args;
		}

		public static String[][] split(String[] ss) {
			if (ss == null)
				return null;
			String[][] argss = new String[ss.length][];
			for (int ix = 0; ix < ss.length; ix++) {
				String[] args = LangExt.split(ss[ix], " ");
				argss[ix] = args;
			}
			return argss;
		}

	}

	//////////////////////////////// subclasses ////////////////////////////////
	/**
	 * When updating, auto update fullpath field according to parent-id and current
	 * record id<br>
	 * args 0: parent Id field, 1: sibling/sort field (optional), 2: fullpath field
	 * 
	 * @author odys-z@github.com
	 */
	static class ShFullpath extends SemanticHandler {
		/**
		 * @param tabl
		 * @param recId
		 * @param args
		 *            see {@link smtype#fullpath}
		 * @throws SemanticException
		 */
		public ShFullpath(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.fullpath, tabl, recId, args);
			insert = true;
			update = true;
		}

		@Override
		void onInsert(ISemantext stx, Insert insert, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			String sibling = null;
			try {
				sibling = (String) row.get(cols.get(args[1]))[1];
			} catch (Exception e) {
			}

			String v = null;
			try {
				if (!cols.containsKey(pkField) || row.get(cols.get(pkField)) == null)
					throw new SemanticException("Fullpath configuration wrong: idField = %s, cols: %s", pkField,
							LangExt.toString(cols));

				String id = (String) row.get(cols.get(pkField))[1];

				String pid = cols.containsKey(args[0]) ? (String) row.get(cols.get(args[0]))[1] : null;

				if (pid == null || "null".equals(pid)) {
					Utils.warn(
							"Fullpath Handling Error\nTo generate fullpath, parentId must configured.\nFound parent col: %s,\nconfigured args = %s,\nhandling cols = %s\nrows = %s",
							pid, LangExt.toString(args), LangExt.toString(cols), LangExt.toString(row));
					v = id;
				} else {
					SemanticObject s = trxt.select(target, "_t0").col(args[2]).where("=", pkField, "'" + pid + "'")
							.rs(stx);

					SResultset rs = (SResultset) s.rs(0);
					if (rs.beforeFirst().next()) {
						String parentpath = rs.getString(args[2]);
						v = String.format("%s.%s%s", parentpath, sibling == null ? "" : sibling + " ", id);
					} else
						v = String.format("%s%s", sibling == null ? "" : sibling + " ", id);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			Object[] nv;
			if (cols.containsKey(args[2]))
				nv = row.get(cols.get(args[2]));
			else {
				nv = new Object[] { args[2], v };
				cols.put(args[2], row.size());
				row.add(nv);
			}
		}

		@Override
		void onUpdate(ISemantext sxt, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			// Design Memo: statement parameter (updt, or insert for onInsert()) is not used
			onInsert(sxt, null, row, cols, usr);
		}
	}

	/**
	 * @see smtype#autoInc
	 * @author odys-z@github.com
	 */
	static class ShAutoK extends SemanticHandler {
		/**
		 * @param trxt
		 * @param tabl
		 * @param pk
		 * @param args
		 *            0: auto field
		 * @throws SemanticException
		 */
		ShAutoK(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.autoInc, tabl, pk, args);
			if (args == null || args.length == 0 || LangExt.isblank(args[0]))
				throw new SemanticException("AUTO pk semantics configuration not correct. tabl = %s, pk = %s, args: %s",
						tabl, pk, LangExt.toString(args));
			insert = true;
			// insPrepare = true;
		}

		@Override
		// void onPrepare(ISemantext stx, ArrayList<Object[]> row, Map<String, Integer>
		// cols, IUser usr) {
		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			Object[] nv;
			if (cols.containsKey(args[0]) // with nv from client
					&& cols.get(args[0]) < row.size()) // with nv must been generated from semantics
				nv = row.get(cols.get(args[0]));
			else {
				nv = new Object[2];
				cols.put(args[0], row.size());
				row.add(nv);
			}
			nv[0] = args[0];

			try {

				Object alreadyResulved = stx.resulvedVal(target, args[0]);
				if (alreadyResulved != null && verbose)
					// {
					// When cross fk referencing happened, this branch will reached by handling post
					// inserts.
					Utils.warn(
							"Debug Notes(verbose): Found an already resulved value (%s) while handling %s auto-key generation. Replacing ...",
							alreadyResulved, target);
				// nv[1] = alreadyResulved;
				// }
				// else
				// side effect: generated auto key already been put into autoVals, can be
				// referenced later.
				nv[1] = stx.genId(target, args[0]);
			} catch (SQLException | TransException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Handle fk referencing resolving when inserting children.<br>
	 * 
	 * @see smtype#fkIns
	 */
	static class ShFkOnIns extends SemanticHandler {
		ShFkOnIns(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.fkIns, tabl, pk, args);
			insert = true;
		}

		@Override
		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			Object[] nv;
			// Debug Note:
			// Don't use pk to find referencing col here, related table's pk can be null.
			// Use args[0] instead.
			if (cols.containsKey(args[0]) // with nv from client
					&& cols.get(args[0]) < row.size()) // with nv must been generated from semantics
				// if (cols.containsKey(args[0])) { // referencing col
				nv = row.get(cols.get(args[0]));
			// }
			else { // add a semantics required cell if it's absent.
				nv = new Object[] { args[0], null };
				cols.put(args[0], row.size());
				row.add(nv);
			}
			try {
				Object v = stx.resulvedVal(args[1], args[2]);
				if (v != null && (nv[1] == null || LangExt.isblank((String) nv[1])))
					nv[1] = v;
			} catch (Exception e) {
				if (nv[1] != null) {
					if (debug)
						Utils.warn(
								"Trying resolve FK failed, but fk value exists. child-fk(%s.%s) = %s, parent = %s.%s",
								target, args[0], nv[1], args[1], args[2]);
				} else
					Utils.warn("Trying resolve FK failed. child-fk = %s.%s, parent = %s.%s,\n"
							+ "FK config args:\t%s,\ndata cols:\t%s,\ndata row:\t%s.\n%s: %s\n"
							+ "Also note that in current version, only auto key can be referenced and auto resolved.",
							target, args[0], args[1], args[2], LangExt.toString(args), LangExt.toString(cols),
							LangExt.toString(row), e.getClass().getName(), e.getMessage());
			}
		}
	}

	/**
	 * Delete childeren before delete parent.<br>
	 * args: [0] parent's referee column\s [1] child-table1\s [2] child pk1, //
	 * child 1, with comma ending, space separated {0] parent's referee column\s [1]
	 * child-table2\s [2] child pk2 // child 1, without comma ending, space
	 * separated smtype: {@link smtype#parentChildrenOnDel}
	 * 
	 * @author odys-z@github.com
	 */
	static class ShPCDelAll extends SemanticHandler {
		protected String[][] argss;

		public ShPCDelAll(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.parentChildrenOnDel, tabl, recId, args);
			delete = true;
			argss = split(args);
		}

		@Override
		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {
			if (argss != null && argss.length > 0)
				for (String[] args : argss)
					if (args != null && args.length > 1 && args[1] != null) {
						stmt.before(delChild(args, stmt, condt, usr));
						// Design Notes: about multi level children deletion:
						// If row can't been retrieved here, cascading children deletion can't been supported
					}
		}

		/**
		 * genterate sql e.g. delete from child where child_pk = parent.referee
		 * 
		 * @param args
		 * @param stmt
		 * @param condt
		 *            deleting's condition
		 * @param usr
		 * @return {@link Delete}
		 * @throws SemanticException
		 */
		protected Delete delChild(String[] args, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {
			if (condt == null)
				throw new SemanticException(
						"Parent table %s has a semantics triggering child table (%s) deletion, but the condition is null.",
						target, args[1]);
			try {
			Query s = stmt.transc().select(target)
					.col(pkField)	// parent's referee
					.where(condt);
				Predicate inCondt = new Predicate(Logic.op.in, args[0], s);
				Delete d = stmt.transc().delete(args[1]).where(new Condit(inCondt));
				return d;
			} catch (TransException e) {
				throw new SemanticException(e.getMessage());
			}
		}

	}

	static class ShPCDelByCate extends ShPCDelAll {

		public ShPCDelByCate(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, tabl, recId, args);
			super.sm = smtype.parentChildrenOnDelByTabl;
		}

		@Override
		protected Delete delChild(String[] args, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {
			return super.delChild(args, stmt, condt, usr)
						.whereEq(args[3], target);
		}
	}
	
	static class ShFkCates extends SemanticHandler {
		protected String[][] argss;

		/** configured field of busi-tbl, e.g. busiTbl for a_attaches */
		private int ixbusiTbl = 0;
		private int ixbusiId = 1;
		private int ixparentbl = 2;
		private int ixparentpk = 3;

		public ShFkCates(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.fkCateIns, tabl, recId, args);
			argss = split(args);
			insert = true;
		}

		@Override
		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			for (String[] argus : argss) {
				// busiTbl for fk-ins-cate must known
				if (cols == null ||
						!cols.containsKey(argus[ixbusiTbl]) || cols.get(argus[ixbusiTbl]) == null) {
					Utils.warn("Can't handle fk-busi without column %s", argus[ixbusiTbl]);
					continue;
				}
				// <!-- 0 business cate (table name); 1 merged child fk; 2 parent table, 3 parent referee [, ...]  -->
				Object[] nvBusiTbl = row.get(cols.get(argus[ixbusiTbl]));
				if (nvBusiTbl == null || LangExt.isblank((String)nvBusiTbl[1])) {
					Utils.warn("Can't generate value of %s.%s without business cate, the value of %s not provided",
							target, argus[ixbusiId], argus[ixbusiTbl]);
				}
				else if (!nvBusiTbl[1].equals(argus[ixparentbl])){
					// if the value is not for this cate (busiTbl), ignore it
					continue;
				}
	
				Object bid; 
				bid = stx.resulvedVal(argus[ixparentbl], argus[ixparentpk]);
				Object[] rowBid; 

				String fBusiId = argus[ixbusiId]; // field name, e.g. (a_attaches.)busiId

				if (cols.containsKey(fBusiId)
						&& cols.get(fBusiId) >= 0 && cols.get(fBusiId) < row.size()) {
					rowBid = row.get(cols.get(fBusiId));
					if (rowBid != null) {
						// already provided by client, override it if possible
						if (bid != null)
							rowBid[1] = bid;
						continue;
					}
					// otherwise it may be an Resulving()
				}
	
				// add a semantics required cell if it's absent.
				String vbusiTbl = (String) nvBusiTbl[1];
				Object[] rowBusiTbl = row.get(cols.get(argus[ixbusiTbl]));

				if (rowBusiTbl == null) {
					Utils.warn("%s is a semantics that is intend to use a table name as business cate, but date to handled doesn't provide the business cate (by %s) .\n" +
							sm.name(), argus[ixbusiTbl], vbusiTbl, target);
					continue;
				}

				if (LangExt.isblank(vbusiTbl, "'\\s*'")
					|| vbusiTbl.equals(rowBusiTbl[0]))
					// not for this semantics
					continue;

				if (stx.colType(vbusiTbl) == null)
					Utils.warn("%s is a semantics that is intend to use a table name as business cate, but table %s can't been found.\n" +
							"Deleting the records of table %s or %s will result in logical error.",
							sm.name(), argus[ixbusiTbl], vbusiTbl, target);
					
//				// bid = stx.resulvedVal(argus[ixparentbl], argus[ixparentpk]);
//				if (bid == null) {
//					// bid is null - can't resulve, not providen
//					utils.warn("%s is a semantics that is intend create an fk to %s.%s, but resulve the value.\n" +
//						"it must be an auto-key field, or the client must provide the actual value.",
//						sm.name(), vbusitbl, target);
//					continue;
//				}
//				else {
					// bid is not provided
					cols.put(fBusiId, row.size());
					row.add(new Object[] {argus[ixbusiId], bid});
//				}
			}
		}
	}

	/**
	 * Handle default value. args: [0] value-field, [1] default-value<br>
	 * e.g. pswd,123456 can set pwd = '123456'
	 * 
	 * @author odys-z@github.com
	 */
	static class ShDefltVal extends SemanticHandler {
		static Regex regQuot = new Regex("^\\s*'.*'\\s*$");

		ShDefltVal(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.defltVal, tabl, recId, args);
			insert = true;
			// update = true;

			args[1] = dequote(args[1]);
		}

		@Override
		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			if (args.length > 1 && args[1] != null) {
				Object[] nv;
				if (cols.containsKey(args[0]) // with nv from client
						&& cols.get(args[0]) < row.size()) // with nv must been generated from semantics
					// if (cols.containsKey(args[1]))
					nv = row.get(cols.get(args[0]));
				else {
					nv = new Object[2];
					cols.put(args[0], row.size());
					row.add(nv);
				}
				nv[0] = args[0];
				if (nv[1] == null)
					nv[1] = args[1];
				else if ("".equals(nv[1]) && args[1] != null && !args[1].equals(""))
					// this is not robust but any better way to handle empty json value?
					nv[1] = args[1];
			}
		}

		private String dequote(String dv) {
			if (dv != null && dv instanceof String && regQuot.match((String) dv))
				return ((String) dv).replaceAll("^\\s*'", "").replaceFirst("'\\s*$", "");
			return (String) dv;
		}
	}

	/**
	 * Save configured nv as file.<br>
	 * args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)
	 * 
	 * @author odys-z@github.com
	 */
	static class ShExtFile extends SemanticHandler {
		/** Saving root.<br>
		 * The path rooted from return of {@link ISemantext#relativpath(String...)}. */
		static final int ixRoot = 0;
		/** Index of Path field */
		static final int ixUri = 1;
		static final int ixBusiTbl = 2;
		static final int ixBusiId = 3;
		/** Index of client file name */
		static final int ixClientName = 4;

		String rootpath = "";

		ShExtFile(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException, SQLException {
			super(trxt, smtype.extFile, tabl, pk, args);
			delete = true;
			insert = true;
			// update = true;

			rootpath = args[ixRoot];

//			try {
				if (LangExt.isblank(args[2]))
					Utils.warn("ShExtFile handling special attachment table semantics, which is needing a business category filed in the table.\n" +
						"But the configuration on the target table (%s) doesn't provide the semantics (business table name field not specified)",
						target);
//			}catch(TransException e) {
//					throw new SemanticException("Can't find table %s, which defined by semantics on table %s.\n" +
//						"It's is required to delete the external file's records.",
////						target, args[2]);
//			}
		}

		@Override
		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			if (args.length > 1 && args[1] != null) {
				Object[] nv;
				// args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)
				if (cols.containsKey(args[ixUri])) {
					// save file, replace v
					nv = row.get(cols.get(args[ixUri]));
					if (nv != null && nv[1] != null
						&& nv[1] instanceof String && ((String) nv[1]).length() > 0) {

						// find business category
						String busi = (String) row.get(cols.get(args[ixBusiTbl]))[1];
						try {
							// save to WEB-INF/uploads/[busiTbl]/[uri]
							String relatvpth = stx.relativpath(args[0], busi);

							// can be a string or an auto resulving
							Object fn = row.get(cols.get(pkField))[1];

							ExtFile f;
							if (fn instanceof Resulving)
								f = new ExtFile((Resulving) fn);
							else // must be a string
								f = new ExtFile(new ExprPart((String) fn));
							
							if (args.length >= ixClientName) {
								String clientname = args[ixClientName];
								if (cols.containsKey(clientname)) {
									clientname = (String) row.get(cols.get(clientname))[1];
									if (clientname != null)
										f.filename(clientname);
								}
							}

							f.prefixPath(relatvpth)
								.absPath(stx.containerRoot())
								.b64((String) nv[1]);
							// nv[1] = f;
							nv = new Object[] {nv[0], f};
							row.set(cols.get(args[ixUri]), nv);
						} catch (TransException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}

		/**<p>On updating external files handler.</p>
		 * <p>This method only throw an exception currently, applying the semantics predefined as:<br>
		 * AS all files are treaded as binary file, no file can be modified, only delete then create it makes sense.</p>
		 * <p>Client should avoid updating an external file will handling business logics.</p>
		 * <p><b>NOTE:</b><br>This can be changed in the future.</p>
		 * @see io.odysz.semantic.DASemantics.SemanticHandler#onUpdate(io.odysz.semantics.ISemantext, io.odysz.transact.sql.Update, java.util.ArrayList, java.util.Map, io.odysz.semantics.IUser)
		 */
		@Override
		void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
			// onInsert(stx, null, row, cols, usr);
			if (args.length > 1 && args[1] != null) {
				Object[] nv;
				// args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)
				if (cols.containsKey(args[ixUri])) {
					// save file, replace v
					nv = row.get(cols.get(args[ixUri]));
					if (nv != null && nv[1] != null
						&& nv[1] instanceof String && ((String) nv[1]).length() > 0) {
						throw new SemanticException("Found the extFile value presented in %s, but updating is not supported by extFile. See:\n" +
								"https://odys-z.github.io/javadoc/semantic.DA/io/odysz/semantic/DASemantics.smtype.html#extFile\n" +
								"About Updating Handling",
								args[ixUri]);
					}
				}
			}
		}

		@Override
		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {

				// delete external files when sqls committed
				// args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)
				SResultset rs;
				try {
//					if (trxt.tableMeta(args[2]) == null)
//						throw new SemanticException("Can't find table %s, which defined by semantics on table %s.\n" +
//							"It's is required to delete the external file's records.",
//							target, args[2]);

					rs = (SResultset) stmt.transc()
							.select(target)
							.col(args[ixUri])
							.where(condt)
							// This is a special condition guards that only records for attached table's are deleted
							// .whereEq(args[2], target) - wrong: this only triggered when a_attaches is being deleted
							.rs(stmt.transc().basictx())
							.rs(0);
					rs.beforeFirst();

					while (rs.next()) {
						try {
							String uri = rs.getString(args[ixUri]);
							if (LangExt.isblank(uri, "\\.*", "\\**", "\\s*"))
								continue;

							uri = FilenameUtils.concat(stx.containerRoot(), uri);

							if (verbose)
								Utils.warn("deleting %s", uri);

							final String v = uri;
							stx.addOnOkOperate((st, sqls) -> {
								File f = new File(v);
								if (!f.isDirectory())
									f.delete();
								else 
									Utils.warn("ShExtHandler#onDelete(): Ignoring deleting directory %s", v);

								return null;
							});
						}
						catch (Exception ex) {
							ex.printStackTrace();
						}
					}
				} catch (SQLException e) {
					e.printStackTrace();
				} catch (TransException e) {
					throw new SemanticException(e.getMessage());
				}
		}
	}

	/**
	 * Check with sql before deleting<br>
	 * 
	 * @see smtype#checkSqlCountOnDel
	 * @author odys-z@github.com
	 */
	static class ShChkCntDel extends SemanticHandler {
		private String[][] argss;

		public ShChkCntDel(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.checkSqlCountOnDel, tabl, recId, args);
			delete = true;
			argss = split(args);
		}

		@Override
		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {
			if (argss != null && argss.length > 0)
				for (String[] args : argss)
					if (args != null && args.length > 1 && args[1] != null) {
						// stmt.before(delChild(args, stmt, condt, usr));
						chkCnt(args, stmt, condt);
					}
		}

		private void chkCnt(String[] args, Statement<? extends Statement<?>> stmt, Condit condt) throws SemanticException {
			SemanticObject s;
			try {
				Query slct = stmt.transc().select(target).col(args[0]).where(condt);

				Predicate inCondt = new Predicate(Logic.op.in, args[2], slct);

				s = stmt.transc().select(args[1]).col("count(" + args[2] + ")", "cnt").where(inCondt)
						.rs(stmt.transc().basictx());

				SResultset rs = (SResultset) s.rs(0);
				rs.beforeFirst().next();

				if (rs.getInt("cnt") > 0)
					throw new SemanticException("%s.%s: %s", target, sm.name(), rs.getInt("cnt"));
			} catch (SQLException | TransException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}
	}

	static class ShChkPCInsert extends SemanticHandler {
		public ShChkPCInsert(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.checkSqlCountOnInsert, tabl, recId, args);
			insert = true;
		}

		@Override
		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			if (args.length > 1 && args[1] != null) {
				Object[] nv = new Object[args.length - 1];

				// find values
				for (int ix = 0; ix < args.length - 1; ix++) {
					if (cols.containsKey(args[ix])) {
						Object[] nmval = row.get(cols.get(args[ix]));
						if (nmval != null && nmval.length > 1 && nmval[1] != null)
							nv[ix] = nmval[1];
						else
							nv[ix] = "";
					}
				}
				String sql = String.format(args[args.length - 1], nv);
				try {
					SResultset rs = Connects.select(stx.connId(), sql, Connects.flag_nothing);
					rs.beforeFirst().next();
					if (rs.getInt(1) > 0)
						throw new SemanticException("Checking count on %s.%s (%s = %s ...) failed", target, pkField,
								args[0], nv[0]);
				} catch (SQLException e) {
					throw new SemanticException(
							"Can't access db to check count on insertion, check sql configuration: %s", sql);
				}

			}
		}
	}

	/**
	 * semantics: automatic operator / time - time is now(), operator is session
	 * user id.<br>
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
		 * @param args
		 *            0: oper-field, 1: oper-time-field (optional)
		 * @throws SemanticException
		 */
		public ShOperTime(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.opTime, tabl, recId, args);
			insert = true;
			update = true;
		}

		@Override
		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			// operTiem
			if (args.length > 1 && args[1] != null) {
				Object[] nvTime;
				if (cols.containsKey(args[1])) {
					int ix = cols.get(args[1]);
					nvTime = row.get(ix);
					try {
						if (debug)
							Utils.warn("ShOperTime#onInsert(): Found o-t value(%s, %s) exists, replacing...", nvTime[0],
									nvTime[1]);
					} catch (Exception e) {
						e.printStackTrace();
					}
					nvTime = new Object[2];
				} else {
					nvTime = new Object[2];
					cols.put(args[1], row.size());
					row.add(nvTime);
				}
				nvTime[0] = args[1];
				nvTime[1] = Funcall.now(stx.dbtype());
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

		void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			// Design Memo: insrt is not used in onInsert
			onInsert(stx, null, row, cols, usr);
		}
	}

	static class ShPostFk extends SemanticHandler {
		ShPostFk(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.postFk, tabl, pk, args);
			post = true;
		}

		@Override
		public void onPost(DASemantext stx, Statement<?> stmt, ArrayList<Object[]> row, Map<String, Integer> cols,
				IUser usr, ArrayList<String> sqlBuff) throws SemanticException {
			Object[] nv;
			Object resulved = null;
			try {
				resulved = stx.resulvedVal(args[1], args[2]);
			} catch (Exception e) {
				// throw new SemanticException("Post FK can not resulved: %s, table: %s",
				// smtype.postFk.name(), target);
			}

			if (resulved == null)
				return; // a post wire up can do nothing

			// Debug Note:
			// Don't use pk to find referencing col here, related table's pk can be null.
			// Use args[0] instead.
			if (cols.containsKey(args[0]) // with nv from client
					&& cols.get(args[0]) < row.size()) // with nv must been generated from semantics
				nv = row.get(cols.get(args[0]));
			else { // add a semantics required cell if it's absent.
				nv = new Object[] { args[0], null };
			}
			nv[1] = resulved;
			// try {
			// // nv[1] = stx.resulvedVal(args[1], args[2]);
			// nv[1] = resulved;
			// }catch (Exception e) {
			// throw new SemanticException("Post FK can not resulved: %s, table: %s",
			// smtype.postFk.name(), target);
			// }
			// if (nv[1] == null)
			// throw new SemanticException("Post FK can not resulved: %s, table: %s",
			// smtype.postFk.name(), target);
			// append a sql
			Object pk = row.get(cols.get(pkField))[1];
			if (pk instanceof String)
				try {
					((DATranscxt) stmt.transc()).update(target, usr).nv((String) nv[0], nv[1])
							.where_("=", pkField, (String) pk)
							// Debug Notes: using null Semantext for no more semantics handling iterating
							.commit(null, sqlBuff);
				} catch (TransException e) {
					e.printStackTrace();
					throw new SemanticException(e.getMessage());
				}
			else
				throw new SemanticException(
						"Currently DASemantics.ShPostFk can only handle string type id for post update fk. (%s.%s = %s)",
						target, pkField, pk);
		}
	}
}