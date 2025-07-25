package io.odysz.semantic;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.f;
import static io.odysz.common.LangExt.ifnull;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.split;
import static io.odysz.common.LangExt.str;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import io.odysz.anson.Anson;
import io.odysz.common.AESHelper;
import io.odysz.common.EnvPath;
import io.odysz.common.FilenameUtils;
import io.odysz.common.LangExt;
import io.odysz.common.Regex;
import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.module.xtable.XMLTable.IMapValue;
import io.odysz.semantic.DATranscxt.SemanticsMap;
import io.odysz.semantic.DA.AbsConnect;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.AbsPart;
import io.odysz.transact.sql.parts.ExtFileInsert;
import io.odysz.transact.sql.parts.ExtFileInsertv2;
import io.odysz.transact.sql.parts.ExtFilePaths;
import io.odysz.transact.sql.parts.ExtFileUpdate;
import io.odysz.transact.sql.parts.ExtFileUpdatev2;
import io.odysz.transact.sql.parts.Logic;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.sql.parts.condition.Predicate;
import io.odysz.transact.x.TransException;

/**
 * <h2>The default table semantics plugin used by semantic-DA.</h2>
 * <p>
 * The {@link DASemantext} use this to manage semantics configuration for
 * resolving data semantics.
 * </p>
 * DASemantics is basically a semantics handler, {@link SemanticHandler} 's container,
 * with subclass handlers handling different semantics (processing values).
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
 * <h5><a href='https://odys-z.github.io/dev/topics/index.html'>Documents</a></h5>
 * 
 * @author odys-z@github.com
 */
@SuppressWarnings("deprecation")
public class DASemantics {
	/** error code key word */
	public static final String ERR_CHK = "err_smtcs";;

	public boolean verbose = true;

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
	 * <b>6. {@link #parentChildrenOnDelByCate}</b><br>
	 * <b>7. {@link #dencrypt}</b><br>
	 * <b>8. {@link #opTime}</b><br>
	 * <b>9. {@link #checkSqlCountOnDel} </b><br>
	 * <b>10.{@link #checkSqlCountOnInsert} </b><br>
	 * <b>11.{@link #postFk}</b><br>
	 * <b>12.{@link #extFile}</b><br>
	 * <b>13.{@link #extFilev2}</b><br>
	 * <b>14.{@link #synChange}</b><br>
	 */
	public enum smtype {
		/**
		 * Auto Key<br>
		 * xml/smtc = "auto" | "pk" | "a-k" | "autopk" <br>
		 * Generate auto increased value for the field when inserting.<br>
		 * on-events: insert<br>
		 * <p>
		 * args: [0]: pk-field; [1]: optional, pk-prefix (since 1.4.35)
		 * </p>
		 * 
		 * Handler: {@link DASemantics.ShAutoKPrefix}
		 * @since 1.4.35, add pk-prefix, args[1], can be a field name or string consts.
		 * @since 1.4.45, will automatically insert sequence number to oz_autoseq.
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
		 * the a_attaShExtFileches(See
		 * <a href='https://github.com/odys-z/semantic-DA/blob/master/semantic.DA/src/test/res'>
		 * sqlite test DB</a>) has a field, named 'busiId', referencing multiple parent table.
		 * The parent table is distinguished with filed busiTbl.
		 * </p>
		 * <p>args: 0 business cate (table name); 1 merged child fk; 2 parent table, 3 parent referee [, ...]</p>
		 * Handler: {@link DASemantics.ShFkInsCates}
		 */
		fkCateIns,
		/**
		 * xml/smtc = "f-p" | "fp" | "fullpath":<br>
		 * <p>args: 0: parent Id field, 1: sibling/sort field (optional), 2: fullpath field, 3: sort size (optional, default 2)
		 * <br>where sort size is the digital length for formatting fullpath string.</p>
		 * Handler: {@link DASemantics.ShFullpath}
		 */
		fullpath,
		/**
		 * xml/smtc = "dv" | "d-v" | "dfltVal":<br>
		 * Handler: {@link DASemantics.ShDefltVal}
		 */
		defltVal,
		/**
		 * "pc-del-all" | "parent-child-del-all" | "parentchildondel"<br>
		 * 
		 * <pre>args: [pc-define, ...], where pc-define is a space sperated strings:
		pc-define[0] name or child referencing column, e.g. domainId for a_domain.domainId
		pc-define[1] child table, e.g. a_orgs
		pc-define[2] child fk (or condition columnr), e.g. orgType
		
		Example: domainId a_orgs orgType, ...
		
		When deleting a_domain, the sql of the results shall be:
		delete from a_orgs where orgType in (select domainId from a_domain where domainId = '000001')
		where the 'where clause' in select clause is composed from condition of the delete request's where condition.
		 * </pre>
		 * Handler: {@link DASemantics.ShPCDelAll}
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
		 * Handler: {@link DASemantics.ShPCDelByCate}
		 */
		parentChildrenOnDelByCate,
		/**
		 * "d-e" | "de-encrypt" | "dencrypt":<br>
		 * decrypt then encrypt (target col cannot be pk or anything other semantics
		 * will updated<br>
		 * Handler: {@link DASemantics.ShDencrypt}
		 */
		dencrypt,

		/**
		 * "d-e.r" | "de-encrypt.r" | "dencrypt.r":<br>
		 * @deprecated to be implemented: new semantics handler: de-encrypt field on reading (in case client
		 * requiring read plain password etc. by accident.
		 */
		dencryptOnRead,
		
		/**
		 * xml/smtc = "o-t" | "oper-time" | "optime"<br>
		 * Finger printing session user's db updating - record operator / oper-time<br>
		 * Handler: {@link DASemantics.ShOperTime}
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
		 * Handler: {@link DASemantics.ShChkCntDel}
		 */
		checkSqlCountOnDel,
		/**
		 * "ck-cnt-ins" | "check-count-ins" | "checksqlcountoninsert":<br>
		 * Check is this record count when inserting - results from
		 * sql.select(count-sql, description-args ...). The record(s) can't been
		 * inserted if count &gt; 0;<br>
		 * <p>
		 * args: [0] arg1, [1] arg2, ..., [len -1] count-sql with "%s" formatter<br>
		 * where args are column name of parent table.
		 * </p>
		 * Handler: {@link DASemantics.ShChkCntDel}
		 */
		checkSqlCountOnInsert,
		/**
		 * "p-f" | "p-fk" | "post-fk"<br>
		 * <p>
		 * <b>semantics:</b><br>
		 * post fk wire back - parent has an fk to child (only one
		 * child makes sense, like making cross refs)
		 * </p>
		 * <p>
		 * <b>Note:</b><br>
		 * This semantics works only when previously resolved auto key exists; if the
		 * value doesn't exist, will be ignored.<br>
		 * The former is the case of inserting new child, and parent refer to it;<br>
		 * the later is the case of updating a child, the parent already has it's pk,
		 * nothing should be done.
		 * </p>
		 * <p>r
		 * <b>Further Discussion:</b><br>
		 * As cross reference is not a good ideal, this semantics sometimes leads to
		 * trouble. Any suggestion or comments are welcome.
		 * </p>
		 * <p>
		 * <b>args:</b> 0 referencing col, 1 target table, 2 target pk(must be an auto
		 * key)
		 * </p>
		 * <b>Handler:</b> {@link DASemantics.ShPostFk}
		 */
		postFk,

		/**
		 * xml/smtc = "ef" | "xf" | "ext-file" | "e-f" | "x-f" <br>
		 * @deprecated this is the same with {@link #extFilev2} 
		 */
		extFile,
		
		/**
		 * xml/smtc = "ef2.0" | "xf2.0" | "ext-file2.0" | "e-f2.0" | "x-f2.0"
		 * <p>Save and load a special field as file of file system.
		 * Can handle more subfolder (configered in xml as field name of data table).</p>
		 * <p>The file content should be a Base 64 encoded block.</p>
		 * <p>This semantics only used for handling small files.
		 * If the file is large, there are an example in Semantic.jserv which
		 * uses a block sequence for uploading files.</p>
		 * <p>Tips</p>
		 * <ul><li>Multiple nodes writing into the same file path can causing the file locking exception.</li></ul>
		 * <p>args:</br>
		 * 0: rec-id<br>
		 * 1: uri, the Base64 content<br>
		 * 2: subfolder<br>
		 * 3: ...<br>
		 *-1: file name<br>
		 * </p> 
		 * <p>args<br>
		 * 0: uploads,<br>
		 * 1: uri - uri field,<br>
		 * 2: sub-folder level 0,<br>
		 * 3: sub-folder level 1,<br>
		 * ... ,<br>
		 *-1: client-name for saving readable file name<br></p>
		 * At least one level of subfolder is recommended.
		 * @since 1.4.25
		 * @since 1.5.18 This handler won't replace an Anson Envelope,
		 * see {@link Anson#startEnvelope(String)},
		 * and can be read by both {@link Funcall#extfile(String...)}
		 * and {@link Funcall#refile(io.odysz.transact.sql.parts.AnDbField)}.
		 * 
		 */
		extFilev2,
		
		/**
		 * xml/smtc = "syn-change" | "s-c" <br>
		 * Logging table changes for DB synchronizing.
		 * 
		 * <p>
		 * args<br> 
		 * 0: full qualified name of table meta<br>
		 * 1: global identity fields, separated with space, e.g. "synoder clientpath"<br>
		 * 2: [deprecated] fields to be cleared (optional with empty value), separated with space<br>
		 * </p>
		 * Example: &lt;args&gt;io.odysz.semantic.meta.SynodeMeta,crud,org synid,remarks&lt;/args&gt;
		 */
		synChange
		;

		/**
		 * "cmp-col" | "compose-col" | "compse-column": compose a column from other
		 * columns;<br>
		 * TODO
		composingCol
		 */

		/**
		 * Convert string key to {@link smtype}.
		 * 
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
			else if ("dfltval".equals(type) || "d-v".equals(type) || "dv".equals(type))
				return defltVal;
			else if ("pc-del-all".equals(type) || "parent-child-del-all".equals(type)
					|| "parentchildondel".equals(type))
				return parentChildrenOnDel;
			else if ("pc-del-tbl".equals(type) || "pc-del-by-tabl".equals(type)
					|| "pc-tbl".equals(type))
				return parentChildrenOnDelByCate;
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
			else if ("ef2.0".equals(type) || "e-f2.0".equals(type) || "ext-file2.0".equals(type)
					|| "xf2.0".equals(type) || "x-f2.0".equals(type))
				return extFilev2;
			else if ("s-c".equals(type) || "syn-change".equals(type))
				return synChange;
			else if ("s-up1".equals(type) || type.startsWith("stamp1"))
				throw new SemanticException("Semantic type stamp1MoreThanRefee is deprecated.");
			else if ("clob".equals(type) || "orclob".equals(type))
				// return orclob;
				throw new SemanticException("Since v1.4.12, orclob is no longer supported.");
			else if ("ef".equals(type) || "e-f".equals(type) || "ext-file".equals(type)
					|| "xf".equals(type) || "x-f".equals(type))
				// return extFile;
				return extFilev2;
			else
				throw new SemanticException("semantics not known, type: " + type);
		}
	}

	/**
	 * Static transact context for DB accessing without semantics support.<br>
	 * Used to generate auto ID.
	 */
	protected Transcxt basicTsx;

	/**
	 * Use this to replace metas from DB for semantics extension.
	 * 
	 * @deprecated since 2.0.0, to have a meta be the type of
	 * {@link io.odysz.semantic.meta.SemanticTableMeta SemanticTableMeta}, configure
	 * the class name in semantics.xml/t[id=metas], instead of calling this method.
	 * 
	 * @since 1.4.25
	 * @param tbl
	 * @param m
	 * @param connId
	 * @return replaced meta
	 * @throws TransException 
	 * @throws SQLException
	 */
	static public TableMeta replaceMeta(String tbl, TableMeta m, String ... connId)
			throws TransException, SQLException {
		String conn = isNull(connId) ? Connects.defltConn() : connId[0];
		TableMeta mdb = Connects.getMeta(conn, m.tbl);
		if (mdb == null)
			throw new TransException("Can't find table %s from DB connection %s.", m.tbl, conn);
		Connects.setMeta(conn, m.clone(mdb));
		return mdb;
	}

	///////////////////////////////// container class
	///////////////////////////////// ///////////////////////////////
	protected ArrayList<SemanticHandler> handlers;

	public final String tabl;
	private String pk;

	/**
	 * Note: basicTx is already created per the connection, i. e. connect id is known. 
	 * @param basicTx
	 * @param tabl
	 * @param recId
	 * @param verbose
	 */
	public DASemantics(Transcxt basicTx, String tabl, String recId, boolean ... verbose) {
		this.tabl = tabl;
		this.pk = recId;
		basicTsx = basicTx;
		
		this.verbose = isNull(verbose) ? false : verbose[0];

		handlers = new ArrayList<SemanticHandler>();
	}

	public DASemantics addHandler(SemanticHandler h) {
		if (verbose)
			h.logi();
		handlers.add(h);
		return this;
	}

	public void addHandler(smtype semantic, String tabl, String recId, String[] args)
			throws Exception {
		checkParas(tabl, pk, args);
		if (isDuplicate(tabl, semantic))
			return;
		SemanticHandler handler = null;
		handler = parseHandler(basicTsx, tabl, semantic, recId, args);

		if (verbose)
			handler.logi();
		handlers.add(handler);
	}
	
	public SemanticHandler handler(smtype sm) {
		if (handlers == null)
			return null;
		for (SemanticHandler h : handlers) {
			if (h.is(sm))
				return h;
		}
		return null;
	}

	public SemanticsMap createSMap(String conn) {
		return new SemanticsMap(conn);
	}


	public SemanticHandler parseHandler(Transcxt trb, String tabl, smtype semantic, String recId, String[] args)
			throws Exception {
		if (smtype.fullpath == semantic)
			return new ShFullpath(basicTsx, tabl, recId, args);
		else if (smtype.autoInc == semantic)
			return new ShAutoKPrefix(basicTsx, tabl, recId, args);
		else if (smtype.fkIns == semantic)
			return new ShFkOnIns(basicTsx, tabl, recId, args);
		else if (smtype.fkCateIns == semantic)
			return new ShFkInsCates(basicTsx, tabl, recId, args);
		else if (smtype.parentChildrenOnDel == semantic)
			return new ShPCDelAll(basicTsx, tabl, recId, args);
		else if (smtype.parentChildrenOnDelByCate == semantic)
			return new ShPCDelByCate(basicTsx, tabl, recId, args);
		else if (smtype.defltVal == semantic)
			return new ShDefltVal(basicTsx, tabl, recId, args);
		else if (smtype.dencrypt == semantic)
			return new ShDencrypt(basicTsx, tabl, recId, args);
		// else if (smtype.orclob == semantic)
		// addClob(tabl, recId, argss);
		else if (smtype.opTime == semantic)
			return new ShOperTime(basicTsx, tabl, recId, args);
		else if (smtype.checkSqlCountOnDel == semantic)
			return new ShChkCntDel(basicTsx, tabl, recId, args);
		else if (smtype.checkSqlCountOnInsert == semantic)
			return new ShChkPCInsert(basicTsx, tabl, recId, args);
		else if (smtype.postFk == semantic)
			return new ShPostFk(basicTsx, tabl, recId, args);
		else if (smtype.extFile == semantic)
			// throw new SemanticException("Since 1.5.0, smtype.extFile is replaced by extFilev2!");
			return new ShExtFilev2(basicTsx, tabl, recId, args);
		else if (smtype.extFilev2 == semantic)
			return new ShExtFilev2(basicTsx, tabl, recId, args);
		else
			throw new SemanticException("Cannot load configured semantics of key: %s, with trans-builder: %s, on basic connection %s.",
				semantic, trb.getClass().getName(), trb.basictx().connId());
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
				Utils.warn("Found duplicate semantics: %s %s\\n"
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

	public void onInsert(ISemantext semantx, Insert statemt, ArrayList<Object[]> row,
			Map<String, Integer> cols, IUser usr) throws TransException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.insert)
					handler.onInsert(semantx, statemt, row, cols, usr);
	}

	public void onUpdate(ISemantext semantx, Update satemt, ArrayList<Object[]> row,
			Map<String, Integer> cols, IUser usr) throws TransException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.update)
					handler.onUpdate(semantx, satemt, row, cols, usr);
	}

	public void onDelete(ISemantext semantx,
			// Statement<? extends Statement<?>> stmt,
			Delete stmt,
			Condit whereCondt, IUser usr) throws TransException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.delete)
					handler.onDelete(semantx, stmt, whereCondt, usr);
	}

	public void onPost(ISemantext sx, Statement<? extends Statement<?>> stmt, ArrayList<Object[]> row,
			Map<String, Integer> cols, IUser usr, ArrayList<String> sqlBuf) throws SemanticException {
		if (handlers != null)
			for (SemanticHandler handler : handlers)
				if (handler.post)
					handler.onPost(sx, stmt, row, cols, usr, sqlBuf);
	}

	//////////////////////////////// Base Handler //////////////////////////////
	public abstract static class SemanticHandler implements IMapValue {
		protected boolean insert = false;
		protected boolean update = false;
		protected boolean delete = false;

		protected boolean post = false;

		protected String target;
		@Override
		public String mapKey() { return target; }
		
		protected String pkField;
		protected String[] args;
		protected Transcxt trxt;

		protected smtype sm;

		protected boolean verbose;

		SemanticHandler(Transcxt trxt, String semantic, String tabl, String pk,
				String[] args, boolean verbose) throws SemanticException {
			this.trxt = trxt;
			target = tabl;
			pkField = pk;
			this.verbose = verbose;
		}

		public void logi() {
			Utils.logi("Semantics Handler [%s]\ntabl: %s,\tpk: %s,\targs: %s",
						sm.name(), target, pkField, LangExt.toString(args));
		}

		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws TransException {
		}

		protected void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws TransException  {
		}

		/**
		 * Handle onDelete event.
		 * 
		 * @param stx
		 * @param del
		 * @param whereCondt
		 *            delete statement's condition.
		 * @param usr
		 * @throws TransException
		 * @throws SQLException 
		 */
		protected void onDelete(ISemantext stx,
				// Statement<? extends Statement<?>> stmt,
				Delete del,
				Condit whereCondt, IUser usr)
				throws TransException {
		}

		protected void onPost(ISemantext sm, Statement<? extends Statement<?>> stmt, ArrayList<Object[]> row,
				Map<String, Integer> cols, IUser usr, ArrayList<String> sqlBuf) throws SemanticException {
		}

		protected SemanticHandler(Transcxt trxt, smtype sm, String tabl, String pk, String[] args) throws SemanticException {
			this.trxt = trxt;
			target = tabl;
			pkField = pk;
			this.sm = sm;
			this.args = args;
		}

		/**Expand the row to the size of cols - in case the cols expanded by semantics handling
		 * @param row row to expand
		 * @param cols column index
		 * @return the row expanded
		 */
		static ArrayList<Object[]> expandRow(ArrayList<Object[]> row, Map<String, Integer> cols) {
			if (row == null || cols == null || row.size() >= cols.size())
				return row;
			
			int size0 = row.size();
			
			for (int cx = size0; cx < cols.size(); cx ++)
				row.add(new Object[] { null, null});

			for (String col : cols.keySet())
				if (cols.get(col) >= size0)
					row.set(cols.get(col), new Object[] { col, null});
			return row;
		}

		public boolean is(smtype sm) {
			return this.sm == sm;
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
		private int siblingSize;

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
			
			siblingSize = 3;
			if (args.length >= 4)
				try {siblingSize = Integer.valueOf(args[3]);}
				catch (Exception e) {}
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insert, ArrayList<Object[]> row,
				Map<String, Integer> cols, IUser usr) throws SemanticException {
			String sibling;
			try {
				String s = String.valueOf(row.get(cols.get(args[1]))[1]);
				sibling = LangExt.leftPad(s, siblingSize, '0');
			} catch (Exception e) {
				sibling = LangExt.leftPad("", siblingSize, "0");
			}

			Object v = null;
			try {
				Object pid = cols.containsKey(args[0]) ? row.get(cols.get(args[0]))[1] : null;

				if (isblank(pid, "null")) {
					Utils.warnT(new Object() {},
						"Fullpath Handling Error. To generate fullpath, the parentId must be configured, and parent value must be providen.\n" +
						"table  : %1$s,\n" +
						"parent : %2$s,\n" +
						"args   : %3$s,\n" +
						"cols   : %4$s\n" +
						"row    : %5$s",
						target, pid, LangExt.toString(args), str(cols), LangExt.str(row));
					// v1.3.0 v = id;
				} else {
					SemanticObject s = trxt.select(target, "_t0").col(args[2]).where("=", pkField, "'" + pid + "'")
							.rs(stx);

					AnResultset rs = (AnResultset) s.rs(0);
					if (rs.beforeFirst().next()) {
						String parentpath = rs.getString(args[2]);
						/* v1.3.0
						v = String.format("%s.%s %s",
								LangExt.isblank(parentpath, "null") ? "" : parentpath,
								sibling, id);
						*/
						v = String.format("%s.%s", parentpath, sibling);
					} else
						/* v1.3.0
						v = String.format("%s %s", sibling, id);
						*/
						v = sibling;
				}
			} catch (Exception e) {
				if ( !(e instanceof TransException) )
					e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}

			Object[] nv;
			// When second row is arriving, the col's index exists, but nv can not been got.
			// Try multi row insertion for fullpath.
			if (cols.containsKey(args[2])) {
				// in the second row, cols contains the name, but row is not the same size
				// if (row.size() < cols.size()) -- triggerring as necessary
				if (row.size() <= cols.get(args[2]))
					row = expandRow(row, cols);

				nv = row.get(cols.get(args[2]));
			}
			else {
				// nv = new Object[] { args[2], stx.composeVal(v, target, args[2]) };
				nv = new Object[] { args[2], trxt.quotation(v, stx.connId(), target, args[2]) };
				cols.put(args[2], row.size());
				row.add(nv);
			}
		}

		@Override
		protected void onUpdate(ISemantext sxt, Update updt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
						throws SemanticException {
			// Design Memo: statement parameter (updt, or insert for onInsert()) is not used
			onInsert(sxt, null, row, cols, usr);
		}
	}

//	/**
//	 * Auto Pk Handler.<br>
//	 * Generate a radix 64, 6 bit of string representation of integer.
//	 * @see smtype#autoInc
//	 * @deprecated replaced by {@link ShAutoKPrefix}
//	 */
//	static class ShAutoK extends SemanticHandler {
//		/**
//		 * @param trxt
//		 * @param tabl
//		 * @param pk
//		 * @param args 0: auto field
//		 * @throws SemanticException
//		 */
//		ShAutoK(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
//			super(trxt, smtype.autoInc, tabl, pk, args);
//			if (args == null || args.length == 0 || isblank(args[0]))
//				throw new SemanticException("AUTO pk semantics' configuration is not correct. tabl = %s, pk = %s, args: %s",
//						tabl, pk, LangExt.toString(args));
//			insert = true;
//		}
//
//		@Override
//		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
//			Object[] nv;
//			if (cols.containsKey(args[0]) // with nv from client
//					&& cols.get(args[0]) < row.size()) // with nv must been generated from semantics
//				nv = row.get(cols.get(args[0]));
//			else {
//				nv = new Object[2];
//				cols.put(args[0], row.size());
//				row.add(nv);
//			}
//			nv[0] = args[0];
//
//			try {
//
//				Object alreadyResulved = stx.resulvedVal(target, args[0], -1);
//				if (verbose && alreadyResulved != null)
//					// 1. When cross fk referencing happened, this branch will reached by handling post inserts.
//					// 2. When multiple children inserting, this happens
//					Utils.warn(
//							"Debug Notes(verbose): Found an already resulved value (%s) while handling %s auto-key generation. Replacing ...",
//							alreadyResulved, target);
//				// side effect: generated auto key already been put into autoVals,
//				// which can be referenced later.
//				// nv[1] = stx.composeVal(stx.genId(target, args[0]), target, args[0]);
//				nv[1] = trxt.quotation(stx.genId(stx.connId(), target, args[0]), stx.connId(), target, args[0]);
//			} catch (SQLException | TransException e) {
//				e.printStackTrace();
//			}
//		}
//	}

	/**
	 * Auto Pk Handler.<br>
	 * Generate a radix 64, 6 bit of string representation of integer.
	 * @see smtype#autoInc
	 */
	static class ShAutoKPrefix extends SemanticHandler {
		String[] prefixCols;

		ShAutoKPrefix(Transcxt trxt, String tabl, String pk, String[] args) throws SQLException, TransException {
			super(trxt, smtype.autoInc, tabl, pk, args);

			long start0 = 0;

			if (args == null || args.length < 2 || isblank(args[0]) || isblank(args[1]))
//				throw new SemanticException(
//						"Since Semantic.DA 1.4.45, AUTO pk's configuration format is:\n"
//						+ "<tabl>*,<start-long>*,<prefix-0>,... <prefix-i>.\n"
//						+ "Some fields are missing in auto-key configuration: conn = %s, tabl = %s, pk = %s, args = %s",
//						trxt.basictx().connId(), tabl, pk, LangExt.toString(args));


				Utils.warn(
						"Since Semantic.DA 1.4.45, AUTO pk's configuration format is:\n"
						+ "<tabl>*,<start-long>*,<prefix-0>,... <prefix-i>.\n"
						+ "Some fields are missing in auto-key configuration: conn = %s, tabl = %s, pk = %s, args = %s",
						trxt.basictx().connId(), tabl, pk, LangExt.toString(args));

			// 1.4.45: insert start0 to oz_autoseq 
			try {
				start0 = Long.valueOf(ifnull(args[args.length - 1], "0"));
			} catch (Exception e) {}

			String sql = f("select count(sid) c from oz_autoseq where sid = '%1$s.%2$s'", target, args[0]);
			AnResultset rs = Connects.select(trxt.basictx().connId(), sql);
			if (!rs.next())
				throw new SemanticException("Something wrong: " + sql); // ?
			else if (0 == rs.getInt("c"))
				// TODO to be tested in DB other than sqlite.
				Connects.commit(trxt.basictx().connId(), DATranscxt.dummyUser(),
					f("insert into oz_autoseq (sid, seq, remarks) values\r\n"
					+ "('%1$s.%2$s', %3$s, 'by ShAutoKPrefix');", target, pk, start0));

			if (args.length >= 3)
				prefixCols = Arrays.copyOfRange(args, 2, args.length);

			insert = true;
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {

			Object[] autonv;
			if (cols.containsKey(args[0]) // with nv from client
					&& cols.get(args[0]) < row.size()) { // with nv must been generated from semantics
				autonv = row.get(cols.get(args[0]));
				
				try {
					if (autonv[1] instanceof String && eq("AUTO", (String)autonv[1]))
						Utils.warnT(new Object() {},
							"Using AUTO for auto pk is no longer supported since 1.4.40. Please use Resulving object instead, or leave the field empty.\n" +
							"table: %s, field: %s",
							target, args[0]);
				} catch(Exception e) {
					Utils.warnT(new Object() {}, e.getMessage());
				}
			}
			else {
				autonv = new Object[2];
				cols.put(args[0], row.size());
				row.add(autonv);
			}
			autonv[0] = args[0];

			// prefix
			String prefix = "";
			if (prefixCols != null)
				for (String precol : prefixCols) {
					if (prefix.length() > 0)
						prefix += ".";
					if (cols.containsKey(precol)) {
						Object[] v = row.get(cols.get(precol));
						if (v[1] instanceof AbsPart)
							try {
								prefix += unquote((AbsPart)v[1]);
							} catch (TransException e) {
								e.printStackTrace();
								prefix += precol;
							}
						else prefix += v[1] == null ? "" : v[1].toString();
					}
					else
						prefix += precol;
				}
			try {
				/*
				Object alreadyResulved = stx.resulvedVal(target, args[0]);
				if (verbose && alreadyResulved != null && isNull(prefixCols))
					// 1. When cross fk referencing happened, this branch will reached by handling post inserts.
					// 2. When multiple children inserting, this happens
					Utils.warn(
						"Debug Notes(verbose): Found an already resulved value (%s) while handling %s auto-key generation. Replacing ...",
						alreadyResulved, target);
				
				if (alreadyResulved == null && isblank(autonv[1])) {
					String ak = isblank(prefix)
						? stx.genId(stx.connId(), target, args[0])
						: stx.genId(stx.connId(), target, args[0], prefix);
					autonv[1] = trxt.quotation(ak, stx.connId(), target, args[0]);
				}
				else if (alreadyResulved != null && isblank(autonv[1]))
					autonv[1] = alreadyResulved;
				*/
				if (isblank(autonv[1])) {
					String ak = isblank(prefix)
						? stx.genId(stx.connId(), target, args[0])
						: stx.genId(stx.connId(), target, args[0], prefix);
					autonv[1] = trxt.quotation(ak, stx.connId(), target, args[0]);
				}
			} catch (SQLException | TransException e) {
				e.printStackTrace();
			}
		}

		public static String unquote(AbsPart prefixVal) throws TransException {
			return prefixVal == null ? ""
					: prefixVal.sql(null)
					.replaceAll("^\\s*'", "")
					.replaceAll("'\\s*$", "");
		}
	}

	/**
	 * Resolve fk reference when inserting children.<br>
	 * 
	 * TODO to be tested: multi-level offspring's insertion should be triggered. This is essential to log changes.
	 * 
	 * @see smtype#fkIns
	 */
	static class ShFkOnIns extends SemanticHandler {
		ShFkOnIns(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.fkIns, tabl, pk, args);
			insert = true;
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			Object[] nv;
			// Debug Note:
			// Don't use pk to find referencing col here, related table's pk can be null.
			// Use args[0] instead.
			if (cols.containsKey(args[0]) // with nv from client
					&& cols.get(args[0]) < row.size()) // with nv must been generated from semantics
				nv = row.get(cols.get(args[0]));
			else {
				// add a semantics required cell if it's absent.
				nv = new Object[] { args[0], null };
				cols.put(args[0], row.size());
				row.add(nv);
			}
			try {
				Object v = stx.resulvedVal(args[1], args[2], -1);
				if (v != null && (nv[1] == null || isblank(nv[1])
						|| nv[1] instanceof ExprPart && ((ExprPart)nv[1]).isNull()))
					nv[1] = trxt.quotation(v, stx.connId(), target, (String)nv[0]);
			} catch (Exception e) {
				if (nv[1] != null) {
					if (verbose)
						Utils.warn(
						"Trying to resolve FK failed, but fk value exists. child-fk(%s.%s) = %s, parent = %s.%s",
						target, args[0], nv[1], args[1], args[2]);
				} else
					Utils.warn("Trying to resolve FK failed. child-fk = %s.%s, parent = %s.%s,\n" +
						"FK config args:\t%s,\n" +
						"data cols:\t%s,\n" +
						"data row:\t%s.\n" +
						"%s: %s\n" +
						"Also note that in current version, only auto key can be referenced and auto resolved.",
						target, args[0], args[1], args[2], LangExt.toString(args), str(cols),
						str(row), e.getClass().getName(), e.getMessage());
			}
		}
	}

	/**
	 * Delete children before delete parent.<br>
	 * args: [0] parent's referee column\s [1] child-table1\s [2] child pk1, //
	 * child 1, with comma ending, space separated {0] parent's referee column\s [1]
	 * child-table2\s [2] child pk2 // child 1, without comma ending, space
	 * separated smtype: {@link smtype#parentChildrenOnDel}
	 * 
	 * TODO to be tested: multi-level offspring's deletion should be triggered. This is essential to log changes.
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
		protected void onDelete(ISemantext stx,
				// Statement<? extends Statement<?>> stmt,
				Delete stmt,
				Condit condt, IUser usr)
				throws SemanticException {
			if (argss != null && argss.length > 0)
				for (String[] args : argss)
					if (args != null && args.length > 1 && args[1] != null) {
						stmt.before(delChild(args, stmt, condt, usr));
						// Design Notes: about multi level children deletion:
						// If a row can't been retrieved here, the cascading children deletion can't been supported
					}
		}

		/**
		 * genterate sql e.g. delete from child where child_pk = parent.referee
		 * 
		 * @param args
		 * @param stmt
		 * @param condt deleting's condition
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
			super.sm = smtype.parentChildrenOnDelByCate;
		}

		@Override
		protected Delete delChild(String[] args, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {
			return super.delChild(args, stmt, condt, usr)
						.whereEq(args[3], target);
		}
	}
	
	static class ShFkInsCates extends SemanticHandler {
		protected String[][] argss;

		/** configured field of busi-tbl, e.g. busiTbl for a_attaches */
		private int ixbusiTbl = 0;
		private int ixbusiId = 1;
		private int ixparentbl = 2;
		private int ixparentpk = 3;

		public ShFkInsCates(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.fkCateIns, tabl, recId, args);
			argss = split(args);
			insert = true;
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			for (String[] argus : argss) {
				// busiTbl for fk-ins-cate must be known
				if (cols == null ||
						!cols.containsKey(argus[ixbusiTbl]) || cols.get(argus[ixbusiTbl]) == null) {
					Utils.warn("Can't handle fk-busi without column %s", argus[ixbusiTbl]);
					continue;
				}
				// <!-- 0 business cate (table name); 1 merged child fk; 2 parent table, 3 parent referee [, ...]  -->
				Object[] nvBusiTbl = row.get(cols.get(argus[ixbusiTbl]));
				if (nvBusiTbl == null || isblank(nvBusiTbl[1])) {
					Utils.warn("Can't generate value of %s.%s without business cate, the value of %s not provided",
							target, argus[ixbusiId], argus[ixbusiTbl]);
				}
				else if (!nvBusiTbl[1].toString().equals(argus[ixparentbl])){
					// if the value is not for this cate (busiTbl), ignore it
					continue;
				}
	
				Object bid; 
				bid = stx.resulvedVal(argus[ixparentbl], argus[ixparentpk], -1);
				if (isblank(bid, "''")) {
					// can't resulve, try if client provided
					if (cols.containsKey(argus[ixbusiId]))
						bid = row.get(cols.get(argus[ixbusiId]))[1];
				}

				if (isblank(bid, "''"))
					throw new SemanticException("Semantics %s can't been handled without business record Id - resulving failed: %s.%s",
							sm.name(), argus[ixparentbl], argus[ixparentpk]);
				Object[] rowBid; 

				String fBusiId = argus[ixbusiId]; // field name, e.g. (a_attaches.)busiId

				// A client provided busi-id
				if (cols.containsKey(fBusiId)
						&& cols.get(fBusiId) >= 0 && cols.get(fBusiId) < row.size()) {
					rowBid = row.get(cols.get(fBusiId));
					if (rowBid != null) {
						// already provided by client, override it if possible
						if (bid != null)
							// rowBid[1] = bid;
							// rowBid[1] = stx.composeVal(bid, target, fBusiId);
							rowBid[1] = trxt.quotation(bid, stx.connId(), target, fBusiId);
					}
					// otherwise it may be a Resulving()
				}
				// B busi-id is not provided, create it
				else {
					// add a semantics required cell if it's absent.
					Object vbusiTbl = nvBusiTbl[1];
					Object[] rowBusiTbl = row.get(cols.get(argus[ixbusiTbl]));
	
					// e.g. not "a_users" presented in row
					if (rowBusiTbl == null) {
						Utils.warn("%s is a semantics that is intend to use a table name as business cate, but date to handled doesn't provide the business cate (by %s) .\\n" +
								sm.name(), argus[ixbusiTbl], vbusiTbl, target);
						continue;
					}
	
					// e.g. this handler only handling records for a_users's children.
					if (isblank(vbusiTbl, "'\\s*'")
						|| vbusiTbl.equals(rowBusiTbl[0]))
						// not for this semantics
						continue;
	
					// e.g. no table "a_user2" exists as appointed by row's data.
					if (stx.tablType(vbusiTbl.toString()) == null)
						Utils.warn("%s is a semantics that is intend to use a table name as business cate, but table %s can't been found.\\n" +
								"Deleting the records of table %s or %s will result in logical error.",
								sm.name(), argus[ixbusiTbl], vbusiTbl, target);
						
					// e.g recId = user001
					cols.put(fBusiId, row.size());
					row.add(new Object[] {argus[ixbusiId],
							// stx.composeVal(bid, target, argus[ixbusiId])
							trxt.quotation(bid, stx.connId(), target, argus[ixbusiId])
							});
				}
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

			args[1] = dequote(args[1]);
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			/*
			if (args.length > 1 && args[1] != null) {
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
				if (nv[1] == null)
					nv[1] = stx.composeVal(args[1], target, (String)nv[0]);
				else if ("".equals(nv[1]) && args[1] != null && !args[1].equals(""))
					// this is not robust but any better way to handle empty json value?
					nv[1] = stx.composeVal(args[1], target, (String)nv[0]);
				else if ((nv[1] instanceof ExprPart) && ((ExprPart)nv[1]).isNull())
					nv[1] = stx.composeVal(args[1], target, (String)nv[0]);
			}*/

			if (args.length > 1 && args[1] != null) {
				requiredNv(args[0],
					trxt.quotation(args[1], stx.connId(), target, args[0]),
					cols, row, target, usr);
			}
		}

		private String dequote(String dv) {
			if (dv != null && dv instanceof String && regQuot.match((String) dv))
				return ((String) dv).replaceAll("^\\s*'", "").replaceFirst("'\\s*$", "");
			return (String) dv;
		}
	}

	/**
	 * @deprecated replaced with ShExtFilev2
	 * Save configured nv as file.<br>
	 * args<br>
	 * 0: uploads,<br>
	 * 1: uri - uri field,<br>
	 * 2: busiTbl - sub-folder,<br>
	 * 3: busiId - not used,<br>
	 * 4: client-name (optional) for saving readable file name<br>
	 * 
	 * <h5>Note</h5>
	 * <p>For large file, use stream asynchronous mode, otherwise it's performance problem here.</p>
	 * <p>Whether uses or not a stream mode file up down loading is a business tier decision by semantic-jserv.
	 * See Anclient.jave/album test for example.</p>
	 * 
	 * @author odys-z@github.com
	 */
	static class ShExtFile extends SemanticHandler {
		/** Saving root.<br>
		 * The path rooted from return of {@link ISemantext#relativpath(String...)}. */
		public static final int ixExtRoot = 0;
		/** Index of Path field */
		static final int ixUri = 1;
		static final int ixBusiCate = 2;
		static final int ixSubCate = 3;
		/** Index of client file name */
		static final int ixClientName = 4;

		public ShExtFile(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException, SQLException {
			super(trxt, smtype.extFile, tabl, pk, args);
			delete = true;
			insert = true;
			update = true;


			if (isblank(args[ixBusiCate]))
				Utils.warn("ShExtFile handling special attachment table semantics, which is needing a business category filed in the table.\\n" +
					"But the configuration on the target table (%s) doesn't provide the semantics (business table name field not specified)",
					target);
		}

		public String getFileRoot() {
			return args[ixExtRoot];
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			// if (args.length > 1 && args[1] != null) {
			if (args.length > 1 && args[1] != null && cols != null && cols.containsKey(args[ixUri])) {
				Object[] nv;
				// args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)
				if (cols.containsKey(args[ixUri])) {
					// save file, replace v
					nv = row.get(cols.get(args[ixUri]));
					if (nv != null && nv[1] != null
						&& (nv[1] instanceof String && !isblank(nv[1]) || nv[1] instanceof AbsPart)) {

						// find business category - this arg can not be is optional, every file has it's unique uri
						Object busicat = row.get(cols.get(args[ixBusiCate]))[1];
						String subpath = busicat.toString();
						
						// TODO to be tested everywhere: sub-cate is optional
						String subpath2 = "";
						if (cols.containsKey(args[ixSubCate]))
							subpath2 = row.get(cols.get(args[ixSubCate]))[1].toString();


						// can be a string or an auto resulving (fk is handled before extfile)
						Object fn = row.get(cols.get(pkField))[1];

						ExtFileInsert f;
						if (fn instanceof Resulving)
							// f = new ExtFile((Resulving) fn, arargs[ixRoot]gs[ixRoot], stx.containerRoot());
							f = new ExtFileInsert((Resulving) fn, getFileRoot(), stx);
						else
							// f = new ExtFile(new ExprPart(fn.toString()), args[ixRoot], stx.containerRoot());
							f = new ExtFileInsert(new ExprPart(fn.toString()), getFileRoot(), stx);
						
						if (args.length >= ixClientName) {
							String clientname = args[ixClientName];
							if (cols.containsKey(clientname)) {
								clientname = row.get(cols.get(clientname))[1].toString();
								if (clientname != null)
									f.filename(clientname);
							}
						}

						f.prefixPath(subpath, subpath2) // e.g. "a_users", "ody"
							.b64(nv[1].toString());
						
						if (verbose)
							try {
								Utils.logi("[io.odysz.semantic.DASemantics.debug] :\n\t%s", f.absolutePath(stx));
							} catch (TransException e) {
								e.printStackTrace();
							}

						nv = new Object[] {nv[0], f};
						row.set(cols.get(args[ixUri]), nv);
					}
				}
			}
		}

		/**<p>On updating external files handler.</p>
		 * <p>This method only moves the file with new uri &amp; client name, applying the semantics predefined as:<br>
		 * AS all files are treated as binary file, no file can be modified, only delete then create it makes sense.</p>
		 * <p>Client should avoid updating an external file while handling business logics.</p>
		 * <p><b>NOTE:</b><br>This can be changed in the future.</p>
		 * @see io.odysz.semantic.DASemantics.SemanticHandler#onUpdate(io.odysz.semantics.ISemantext, io.odysz.transact.sql.Update, java.util.ArrayList, java.util.Map, io.odysz.semantics.IUser)
		 */
		@Override
		protected void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
			// onInsert(stx, null, row, cols, usr);
			if (args.length > 1 && args[1] != null && cols != null && cols.containsKey(args[ixUri])) {
			// if (args.length > 1 && args[1] != null) {
				Object[] nv;
				if (cols.containsKey(args[ixSubCate]) || cols.containsKey(args[ixBusiCate]) || cols.containsKey(args[ixClientName])) {
					if ( !cols.containsKey(args[ixUri]) || !cols.containsKey(args[ixSubCate])
					  || !cols.containsKey(args[ixBusiCate]) || !cols.containsKey(args[ixClientName]) )
						throw new SemanticException("[ExtFile 1.0] To update (move file) %s.%s, all fields' values must be provided by user. Old uri value is required, and reqired fields are: %s, %s, %s.",
									target, args[ixUri], args[ixBusiCate], args[ixSubCate], args[ixClientName]);
					
					String folder = row.get(cols.get(args[ixBusiCate]))[1].toString();
					String subpath2 = row.get(cols.get(args[ixSubCate]))[1].toString();
					String oldUri = row.get(cols.get(args[ixUri]))[1].toString();
					String oldName = FilenameUtils.getName(oldUri);

					ExtFileUpdate f = new ExtFileUpdate(oldName, getFileRoot(), stx)
							.oldUri(oldUri)
							.prefixPath(folder, subpath2);
					nv = row.get(cols.get(args[ixUri]));
					nv = new Object[] { nv[0], f };
					row.set(cols.get(args[ixUri]), nv);
				}
				else {
					// save file, replace v - throw exception
					nv = row.get(cols.get(args[ixUri]));
					if (nv != null && nv[1] != null &&
						(  nv[1] instanceof String && ((String) nv[1]).length() > 0
						|| nv[1] instanceof ExprPart && !((ExprPart) nv[1]).isNull() )) {
						throw new SemanticException("Found the extFile value presented in %s.%s, but updating is configured as semantics.",
									target, args[ixUri]);
					}
				}
			}
		}

		@Override
		protected void onDelete(ISemantext stx,
				// Statement<? extends Statement<?>> stmt,
				Delete stmt,
				Condit condt, IUser usr)
				throws SemanticException {

				// delete external files when sqls committed
				// args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)
				AnResultset rs;
				try {
					rs = (AnResultset) stmt.transc()
							.select(target)
							.col(args[ixUri])
							.where(condt)
							// v1.3.0: basictx() created a connection not the same as the visiting one 
							// .rs(stmt.transc().basictx())
							.rs(stmt.transc().instancontxt(stx.connId(), usr))
							.rs(0);
					rs.beforeFirst();

					while (rs.next()) {
						try {
							String uri = rs.getString(args[ixUri]);
							if (isblank(uri, "\\.*", "\\**", "\\s*"))
								continue;

							uri = EnvPath.decodeUri(stx, uri);

							if (verbose)
								Utils.warn("deleting %s", uri);

							final String v = uri;
							stx.addOnRowsCommitted((st, sqls) -> {
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
	 * <h5>Note</h5>
	 * <p>For large file, use stream asynchronous mode, otherwise it's a performance problem here.</p>
	 * <p>Whether uses or not a stream mode file up/down loading is a business tier decision by semantic-jserv.
	 * See Anclient.jave/album test for example.</p>
	 * 
	 * <p>This semantics won't process reading events. To load the file content at the field,
	 * use {@link Funcall#extfile(String...)}.</p>
	 * 
	 * @see smtype#extFilev2
	 * @author odys-z@github.com
	 * @since 1.4.40
	 */
	static public class ShExtFilev2 extends SemanticHandler {
		/** Saving root.<br>
		 * The path rooted from return of {@link ISemantext#relativpath(String...)}. */
		public static final int ixExtRoot = 0;
		/** Index of URI field */
		static final int ixUri = 1;

		public ShExtFilev2(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.extFilev2, tabl, pk, args);
			
			delete = true;
			insert = true;
			update = true;

			if (isblank(args[ixUri + 1]))
				Utils.warn("ShExtFile v2.0 handling special attachment table semantics, which needs multiple sub folder fileds in the table.\\n" +
					"But the configuration on the target table (%s) doesn't provide the semantics (business table name field not specified)",
					target);
		}

		/** 
		 * @return this.args[{@link #ixExtRoot}], is usually the configured volume variable.
		 */
 		public String getFileRoot() {
			return args[ixExtRoot];
		}

		/*
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
			if (args.length > 1 && args[1] != null && cols != null && cols.containsKey(args[ixUri])) {
				Object[] nv;
				// args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)
				if (cols.containsKey(args[ixUri])) {
					// save file, replace v
					nv = row.get(cols.get(args[ixUri]));
					if (nv != null && nv[1] != null
						&& (nv[1] instanceof String && !isblank(nv[1]) && !Anson.startEnvelope((String)nv[1])
							|| nv[1] instanceof AbsPart && !startEnvelope((AbsPart)nv[1]))) {

						// can be a string or an auto resulving (fk is handled before extfile)
						Object fid = row.get(cols.get(pkField))[1];

						ExtFileInsert f;
						if (fid instanceof Resulving)
							f = new ExtFileInsert((Resulving) fid, getFileRoot(), stx);
						else
							f = new ExtFileInsert(new ExprPart(fid.toString()), getFileRoot(), stx);
						
						String clientname = args[args.length - 1];
						if (cols.containsKey(clientname)) {
							clientname = row.get(cols.get(clientname))[1].toString();
							if (clientname != null)
								f.filename(clientname);
						}

						f.b64(nv[1]);
						
						for (int i = ixUri + 1; i < args.length - 1; i++) {
							if (!cols.containsKey(args[i])) 
								throw new SemanticException("To insert (create file) %s.%s, all required fields must be provided by user (missing %s).\nConfigured fields: %s.\nGot cols: %s",
										target, args[ixUri], args[i],
										Stream.of(args).skip(2).collect(Collectors.joining(", ")),
										cols.keySet().stream().collect(Collectors.joining(", ")));
							else
								f.appendSubFolder(row.get(cols.get(args[i]))[1]);
						}

						if (verbose)
							try {
								Utils.logi("[io.odysz.semantic.DASemantics.SemanticHandler.verbose] :\n\t%s", f.absolutePath(stx));
							} catch (TransException e) {
								e.printStackTrace();
							}

						nv = new Object[] {nv[0], f};
						row.set(cols.get(args[ixUri]), nv);
					}
				}
			}
		}
		*/
		
		@Override
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row,
				Map<String, Integer> cols, IUser usr) throws TransException {

			if (args.length > 1 && args[1] != null && cols != null && cols.containsKey(args[ixUri])) {
				Object[] nv;
				// args 0: uploads, 1: uri, 2: busiTbl, 3: busiId, 4: client-name (optional)
				if (cols.containsKey(args[ixUri])) {
					// save file, replace v
					nv = row.get(cols.get(args[ixUri]));
					if (nv != null && nv[1] != null
						&& (nv[1] instanceof String && !isblank(nv[1]) && !Anson.startEnvelope((String)nv[1])
							|| nv[1] instanceof AbsPart && !startEnvelope((AbsPart)nv[1]))) {

						// can be a string or an auto resulving (fk is handled before extfile)
						Object fid = row.get(cols.get(pkField))[1];

						ExtFileInsertv2 f;
						if (fid instanceof Resulving)
							// f = new ExtFileInsertv2(getFileRoot(), (Resulving) fid, stx);
							f = new ExtFileInsertv2(new ExtFilePaths(getFileRoot(), ((Resulving)fid).sql(stx), null));
						else
							// f = new ExtFileInsertv2(getFileRoot(), new ExprPart(fid.toString()), stx);
							f = new ExtFileInsertv2(getExtPaths(fid.toString(), null));
						
						
						String clientname = args[args.length - 1];
						if (cols.containsKey(clientname)) {
							clientname = row.get(cols.get(clientname))[1].toString();
							if (clientname != null)
								f.filename(clientname);
						}

						f.b64(nv[1])
						 // .subpaths(ixUri, args, cols, row);
						 .subpaths(Arrays.copyOfRange(args, ixUri + 1, args.length - 1), cols, row);

						if (verbose)
							try {
								Utils.logi("[io.odysz.semantic.DASemantics.SemanticHandler.verbose] :\n\t%s",
										f.absolutePath(stx));
							} catch (TransException e) {
								e.printStackTrace();
							}

						nv = new Object[] {nv[0], f};
						row.set(cols.get(args[ixUri]), nv);
					}
				}
			}
		}


		public ExtFilePaths getExtPaths(String fid, String filename) {
			return new ExtFilePaths(getFileRoot(), fid, filename);
		}

		public static boolean startEnvelope(AbsPart expr) {
			return expr instanceof ExprPart && ((ExprPart)expr).isEvelope();
		}

		/**
		 * <p>On updating external files' handler.</p>
		 * <p>This method only moves the file with new uri &amp; client name, applying the semantics predefined as:<br>
		 * AS all files are treated as binary file, no file can be modified, only delete then create it makes sense.</p>
		 * <p>Client should avoid updating an external file while handling business logics.</p>
		 * <p><b>NOTE:</b><br>This can be changed in the future.</p>
		 * @throws TransException 
		 * @see SemanticHandler#onUpdate(ISemantext, Update, ArrayList, Map, IUser)
		 */
		@Override
		protected void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws TransException {
			if (cols.containsKey(args[ixUri])) {
				throw new SemanticException(
					"Found ext-file's contenet is updated.\n" +
					"Currently update ExtFile (%s.%s) is not supported (and moving file shouldn't updating content). Use delete & insert instead.",
					target, args[ixUri]);
			}
			else if (args.length > 1 && args[ixUri] != null && cols != null && !cols.containsKey(args[ixUri])) {
				boolean touch = false;
				for (int i = ixUri + 1; i < args.length - 1; i++)
					if (cols.containsKey(args[i])) {
						touch = true;
						break;
					}
				if (touch) {
					// String oldUri = row.get(cols.get(args[ixUri]))[1].toString();
					String[] oldUriPk = selectUriPk(stx, updt, updt.where(), usr);
					String oldName = FilenameUtils.getName(oldUriPk[0]);

					ExtFileUpdatev2 f = new ExtFileUpdatev2(oldUriPk[1], oldUriPk[1], oldName)
										.oldUri(oldUriPk[0]);

					f.subpaths(Arrays.copyOfRange(args, ixUri + 1, args.length - 1), cols, row);
					Object[] nv = new Object[] { args[ixUri], f };
					cols.put(args[ixUri], nv.length);
					row.add(nv);
				}
			}
		}

		@SuppressWarnings("unused")
		private String selectUri(ISemantext stx, Statement<?> stmt, Condit pk, IUser usr) throws SemanticException {
			AnResultset rs;
			try {
				rs = (AnResultset) stmt
						.transc()
						.select(target)
						.col(args[ixUri], "uri")
						.where(pk)
						.rs(stmt.transc().instancontxt(stx.connId(), usr))
						.rs(0);
				
				if (rs.total() > 1) {
					throw new SemanticException("Semantics handler, ExtFilev2, can only handling moving one file for each statement. Multiple records moving is not supported. Statement condition: %s",
							pk.sql(stx));
				}
				rs.beforeFirst().next();
				
				return rs.getString("uri");
			} catch (SQLException | TransException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}

		/**
		 * @param stx
		 * @param stmt
		 * @param pk
		 * @param usr
		 * @return [uri, pk]
		 * @throws SemanticException
		 */
		private String[] selectUriPk(ISemantext stx, Statement<?> stmt, Condit pk, IUser usr) throws SemanticException {
			AnResultset rs;
			try {
				rs = (AnResultset) stmt
						.transc()
						.select(target)
						.col(args[ixUri], "uri").col(this.pkField, "pk")
						.where(pk)
						.rs(stmt.transc().instancontxt(stx.connId(), usr))
						.rs(0);
				
				if (rs.total() > 1) {
					throw new SemanticException("Semantics handler, ExtFilev2, can only handling moving one file for each statement. Multiple records moving is not supported. Statement condition: %s",
							pk.sql(stx));
				}
				rs.beforeFirst().next();
				
				return new String[] {rs.getString("uri"), rs.getString("pk")};
			} catch (SQLException | TransException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}

		@Override
		protected void onDelete(ISemantext stx,
				Delete stmt,
				Condit condt, IUser usr)
				throws SemanticException {

			try {
				AnResultset rs = (AnResultset) stmt
						.transc()
						.select(target)
						.col(args[ixUri], "uri")
						.where(condt)
						.rs(stmt.transc().instancontxt(stx.connId(), usr))
						.rs(0);
				rs.beforeFirst();

				while (rs.next()) {
					try {
						String dburi = rs.getString("uri");
						if (isblank(dburi, "\\.*", "\\**", "\\s*")
							|| Regex.startsEvelope(dburi))
							continue;

						// 2025-07-25
						// FIXME TODO to be tested
						// uri = EnvPath.decodeUri(stx, uri);
						final String uri = ExtFileUpdatev2.decodeUriPath(dburi);
						Utils.warn("*** ---- ======== verified %s ?", uri);
						// end

						if (verbose)
							Utils.warn("deleting %s [%s.uri: %s]", uri, target, dburi);

						// final String v = uri;
						stx.addOnRowsCommitted((st, sqls) -> {
							File file = new File(uri);
							if (!file.isDirectory())
								file.delete();
							else 
								Utils.warn("ShExtHandler#onDelete(): Ignoring deleting %s", uri);

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
		protected void onDelete(ISemantext stx,
				// Statement<? extends Statement<?>> stmt,
				Delete stmt,
				Condit condt, IUser usr)
				throws SemanticException {
			if (argss != null && argss.length > 0)
				for (String[] args : argss)
					if (args != null && args.length > 1 && args[1] != null) {
						chkCnt(stx, args, stmt, condt, usr);
					}
		}

		private void chkCnt(ISemantext stx, String[] args, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {
			SemanticObject s;
			try {
				Query slct = stmt.transc().select(target).col(args[0]).where(condt);

				Predicate inCondt = new Predicate(Logic.op.in, args[2], slct);

				s = stmt.transc().select(args[1])
						.col("count(" + args[2] + ")", "cnt")
						.where(inCondt)
						// v1.3.0: basictx() created a connection not the same as the visiting one 
						// .rs(stmt.transc().basictx());
						.rs(stmt.transc().instancontxt(stx.connId(), usr));

				AnResultset rs = (AnResultset) s.rs(0);
				rs.beforeFirst().next();

				if (rs.getInt("cnt") > 0)
					throw new SemanticException("%s.%s: %s %s",
							target, sm.name(), args[1], rs.getInt("cnt"))
							.ex(new SemanticObject()
								.put("sm", sm.name())
								.put("tbl", target)
								.put("childTbl", args[1])
								.put("cnt", rs.getInt("cnt")));
			} catch (SQLException | TransException e) {
				if (e instanceof SemanticException)
					throw (SemanticException)e;
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}
	}

	/**
	 * TODO to be tested: multi-level offspring's checking is not be triggered. This is not essential to log changes?
	 * 
	 * @author odys-z@github.com
	 *
	 */
	static class ShChkPCInsert extends SemanticHandler {
		public ShChkPCInsert(Transcxt trxt, String tabl, String recId, String[] args) throws SemanticException {
			super(trxt, smtype.checkSqlCountOnInsert, tabl, recId, args);
			insert = true;
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
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
					AnResultset rs = Connects.select(stx.connId(), sql, AbsConnect.flag_nothing);
					rs.beforeFirst().next();
					if (rs.getInt(1) > 0)
						throw new SemanticException("Checking count on %s.%s (%s = %s ...) failed",
								target, pkField, args[0], nv[0])
								.ex(new SemanticObject()
									.put("sm", sm.name())
									.put("tbl", target)
									.put("childTbl", args[0])
									.put("childField", nv[0]));
				} catch (SQLException e) {
					throw new SemanticException(
							"Can't access db to check count on insertion, check sql configuration: %s", sql);
				}
			}
		}
	}

	static class ShDencrypt extends SemanticHandler {
		String colIv;
		String colCipher;
		
		ShDencrypt(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.dencrypt, tabl, pk, args);
			insert = true;
			update = true;
			colIv = args[1];
			colCipher = args[0];
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols,
				IUser usr) throws SemanticException {
			if (cols.containsKey(colCipher)) {
				if (!cols.containsKey(colIv)) {
					if (verbose)
						Utils.warn("Can't find IV columm: %s (for encrypt column %s)\n\t- possible 1. wrong iv config;\n2. configured default encrypted column value.\n3.reset user password to plain text as intial tocken",
							colIv, colCipher);
				}

				else {
					Object[] ivB64 = row.get(cols.get(colIv));
					Object[] cipherB64 = row.get(cols.get(colCipher));
					if (ivB64 != null && !AbsPart.isblank(ivB64[1])) {
						// cipher col
						String decryptK = usr.sessionId();
						Object[] civ = dencrypt(insrt, cipherB64[1].toString(), ivB64[1].toString(), decryptK);
						// [0] cipher, [1] iv
						if (civ != null) {
							// cipherB64[1] = stx.composeVal(civ[0], target, colCipher);
							cipherB64[1] = trxt.quotation(civ[0], stx.connId(), target, colCipher);
							// ivB64[1] = stx.composeVal(civ[1], target, colIv);
							ivB64[1] = trxt.quotation(civ[1], stx.connId(), target, colIv);
						}
						else Utils.warn("Decrypt then ecrypt failed. target %s, col: %s, (client) decipher key: %s",
								target, colCipher, decryptK);
					}
				}
			}
		}

		/**Decrypt with decryptK, then protect with my root key.
		 * @param stx
		 * @param pB64
		 * @param ivB64
		 * @param decryptK
		 * @return cipher of null for failed
		 * @throws SemanticException
		 */
		private Object[] dencrypt(Statement<?> stx, String pB64, String ivB64, String decryptK) throws SemanticException {
			try {
				// String decryptK = (String) usr.sessionKey();
				if (decryptK != null) {
					// mvn test -Drootkey=********
					String rootK = DATranscxt.key("user-pswd");
					return AESHelper.dencrypt(pB64, decryptK, ivB64, rootK);
				}
				return null;
			} catch (Throwable e) {
				e.printStackTrace();
				throw new SemanticException (e.getMessage()); 
			}
		}

		protected void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols,
				IUser usr) throws SemanticException {
			onInsert(stx, null, row, cols, usr);
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
		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			/* v1.3.0 change log:
			 * since multiple rows are expanded one by one, once the first rows have cols expended,
			 * following rows should expanded automatically.
			 * operTiem
			*/
			if (args.length > 1 && args[1] != null) {
				Object[] nvTime;
				if (cols.containsKey(args[1])) {
					int ix = cols.get(args[1]);
					if (row.size() <= ix) {
						// this row need to be expanded - happens when handling following rows after 1st row expanded cols with oper & optime.
						nvTime = new Object[2];
						int adding = ix - row.size() + 1;
						while (adding > 0) {
							adding--;
							row.add(new Object[2]);
						}
						row.set(ix, nvTime);
					}
					else
						nvTime = row.get(ix);
				} else {
					// this happens in the first row
					nvTime = new Object[2];
					cols.put(args[1], row.size());
					row.add(nvTime);
				}
				nvTime[0] = args[1];
				nvTime[1] = Funcall.now();
			}

			// oper
			requiredNv(args[0],
					// stx.composeVal(usr == null ? "sys" : usr.uid(), target, args[0]),
					trxt.quotation(usr == null ? "sys" : usr.uid(), stx.connId(), target, args[0]),
					cols, row, target, usr);
			/*
			Object[] nvOper;
			if (cols.containsKey(args[0])) {
				int jx = cols.get(args[0]);

				if (row.size() <= jx) {
					// this row need to be expanded - happens when handling following rows after 1st row expanded cols with oper & optime.
					nvOper = new Object[2];
					int adding = jx - row.size() + 1;
					while (adding > 0) {
						adding--;
						row.add(new Object[2]);
					}
					row.set(jx, nvOper);
				}
				else
					nvOper = row.get(jx);
			}
			else {
				nvOper = new Object[2];
				cols.put(args[0], row.size()); // oper
				row.add(nvOper);
			}
			nvOper[0] = args[0];
			nvOper[1] = stx.composeVal(usr == null ? "sys" : usr.uid(), target, args[0]);
			*/
		}

		protected void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) {
			// Design Memo: insrt is not used in onInsert
			onInsert(stx, null, row, cols, usr);
		}
	}

	/**
	 * Force to set or extend a nv pair into row
	 * 
	 * @param n forced field
	 * @param v forced value
	 * @param cols
	 * @param row row to be expanded
	 * @param target for the table
	 * @param usr operator
	 * @return the extended or forced value's nv pair
	 */
	public static Object[] requiredNv(String n, AbsPart v,
			Map<String, Integer> cols, ArrayList<Object[]> row, String target, IUser usr) {
		Object[] nv;
		if (cols.containsKey(n)) {
			int jx = cols.get(n);

			if (row.size() <= jx) {
				// this row need to be expanded - happens when handling following rows after 1st row expanded cols with oper & optime.
				nv = new Object[2];
				int adding = jx - row.size() + 1;
				while (adding > 0) {
					adding--;
					row.add(new Object[2]);
				}
				row.set(jx, nv);
			}
			else
				nv = row.get(jx);
		}
		else {
			nv = new Object[2];
			cols.put(n, row.size());
			row.add(nv);
		}
		nv[0] = n;
		nv[1] = v;

		return nv;
	}

	static class ShPostFk extends SemanticHandler {
		ShPostFk(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.postFk, tabl, pk, args);
			post = true;
		}

		@Override
		public void onPost(ISemantext stx, Statement<?> stmt, ArrayList<Object[]> row, Map<String, Integer> cols,
				IUser usr, ArrayList<String> sqlBuff) throws SemanticException {
			Object[] nv;
			Object resulved = null;
			try {
				resulved = stx.resulvedVal(args[1], args[2], -1);
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
			// nv[1] = stx.composeVal(resulved, target, args[0]);
			nv[1] = trxt.quotation(resulved, stx.connId(), target, args[0]);

			// append a sql
			Object pk = row.get(cols.get(pkField))[1];
				try {
					Update u = ((DATranscxt) stmt.transc()).update(target, usr)
							//.nv((String) nv[0], nv[1])
							.whereEq(pkField, pk);

					if (nv[1] instanceof AbsPart)
						u.nv((String)nv[0], (AbsPart)nv[1]);
					else
						u.nv((String)nv[0], (String)nv[1]);

					// Debug Notes: using null Semantext for no more semantics handling iterating
					u.commit(null, sqlBuff);
				} catch (TransException e) {
					e.printStackTrace();
					throw new SemanticException(e.getMessage());
				}
		}
	}
	
	/**
	 * Shallow copy, with new list of handlers, with each elements referring to the original one.
	 */
	public DASemantics clone() {
		DASemantics clone = new DASemantics(basicTsx, tabl, pk, verbose);
		clone.handlers = new ArrayList<SemanticHandler>(handlers.size());
		
		for (SemanticHandler sh : handlers)
			clone.handlers.add(sh);
		
		return clone;
	}
}