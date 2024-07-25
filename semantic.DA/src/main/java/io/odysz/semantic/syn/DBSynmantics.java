package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Query;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.Resulving;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.x.TransException;

/**
 * See the topic of <a href='https://odys-z.github.io/dev/topics/semantics/semantypes.html#extending-default-handler-plugin'>
 * Extending default handler plugin</a>.
 * 
 * @author odys-z@github.com
 */
public class DBSynmantics extends DASemantics {

	final String synode;

	public DBSynmantics(Transcxt basicTx, String synode, String tabl, String pk, boolean... verbose) {
		super(basicTx, tabl, pk, verbose);
		this.synode = synode;
	}

	@Override
	public SemanticHandler parseHandler(Transcxt tsx, String tabl, smtype smtp,
			String pk, String[] args) throws SemanticException {
		if (smtype.synChange == smtp)
			try {
				return new DBSynmantics.ShSynChange(tsx, synode, tabl, pk, args);
			} catch (TransException | SQLException | SAXException | IOException e) {
				e.printStackTrace();
				return null;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		else
			return super.parseHandler(tsx, tabl, smtp, pk, args);
	}
	
	@Override
	public DBSyntableBuilder.SynmanticsMap createSMap(String conn) {
		return new DBSyntableBuilder.SynmanticsMap(synode, conn);
	}
	
	public static Insert logChange(DBSyntableBuilder b, Insert inst,
			SynodeMeta synm, SynChangeMeta chgm, SynSubsMeta subm, SyntityMeta entm,
			// String synode, Object pid) throws TransException {
			String synode) throws TransException {
		Update u = b.update(entm.tbl);
		Resulving pid = new Resulving(entm.tbl, entm.pk);
		if (pid instanceof Resulving)
			u.nv(entm.synuid, SynChangeMeta.uids(synode, (Resulving)pid));
		else
			u.nv(entm.synuid, SynChangeMeta.uids(synode,  pid.toString()));

		Insert insc = b.insert(chgm.tbl)
				.nv(chgm.entbl, entm.tbl)
				.nv(chgm.crud, CRUD.C)
				.nv(chgm.synoder, b.synode())
				// .nv(chgm.uids, SynChangeMeta.uids(b.synode(), pid))
				.nv(chgm.nyquence, b.stamp())
				.nv(chgm.seq, b.incSeq())
				.nv(chgm.domain, b.domain())
				.post(b.insert(subm.tbl)
					.cols(subm.insertCols())
					.select((Query) b.select(synm.tbl)
						.col(new Resulving(chgm.tbl, chgm.pk))
						.col(synm.synoder)
						.where(op.ne, synm.synoder, constr(b.synode()))
						.whereEq(synm.domain, b.domain())));
		if (pid instanceof Resulving)
			insc.nv(chgm.uids, SynChangeMeta.uids(b.synode(), (Resulving)pid));
		else
			insc.nv(entm.synuid, SynChangeMeta.uids(synode,  pid.toString()));

		return inst
			.post(u.whereEq(entm.pk, pid))
			.post(insc);
	}

	public static Update logChange(DBSyntableBuilder b, Update updt,
			SynodeMeta synm, SynChangeMeta chgm, SynSubsMeta subm, SyntityMeta entm,
			String synoder, List<String> synuids, Iterable<String> updcols)
				throws TransException, SQLException {

		for (String synuid : synuids)
			updt.post(b
					.insert(chgm.tbl)
					.nv(chgm.entbl, entm.tbl)
					.nv(chgm.crud, CRUD.U)
					.nv(chgm.synoder, synoder)
					.nv(chgm.uids, synuid)
					.nv(chgm.nyquence, b.stamp())
					.nv(chgm.seq, b.incSeq())
					.nv(chgm.domain, b.domain())
					.nv(chgm.updcols, updcols)
					.post(b.insert(subm.tbl)
						.cols(subm.insertCols())
						.select((Query)b.select(synm.tbl)
							.col(new Resulving(chgm.tbl, chgm.pk))
							.col(synm.synoder)
							.where(op.ne, synm.synoder, constr(synoder))
							.whereEq(synm.domain, b.domain()))))
			;
	
		return updt;
	}

	public static class ShSynChange extends SemanticHandler {
		static String apidoc = "TODO ...";
		protected final SynChangeMeta chm;
		protected final SynodeMeta snm;
		protected final SynSubsMeta sbm;

		/**
		 * Target synchronzed table meta, e.g. PhotoMeta.
		 */
		public final SyntityMeta entm;
		protected final Resulving entId;
		protected final String synode;
		
		protected final DATranscxt st;

		ShSynChange(Transcxt trxt, String synode, String tabl, String pk, String[] args)
				throws SQLException, SAXException, IOException, TransException, Exception {
			super(trxt, smtype.synChange, tabl, pk, args);
			insert = true;
			update = true;
			delete = true;

			this.synode = synode;
			st = new DATranscxt(null);
			
			try {
				snm = new SynodeMeta(trxt.basictx().connId());
			} catch (TransException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
			chm = new SynChangeMeta();
			sbm = new SynSubsMeta(chm);
			
			TableMeta m = trxt.tableMeta(tabl);
			if (!eq(args[0], m.getClass().getName())) {
				Class<?> cls = Class.forName(args[0]);
				Constructor<?> constructor = cls.getConstructor(String.class);
				entm = (SyntityMeta) constructor.newInstance(trxt.basictx().connId());
				entm.replace();
			}
			else entm = (SyntityMeta) m;
			entId = new Resulving(entm.tbl, entm.pk);
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws TransException {
			if (!checkBuilder(stx)) return;

			if (verbose) Utils.logi("synChange: onInsert ...");

			verifyRequiredNvs(entm.globalIds(), cols, row);

			DBSyntableBuilder synb = ((ISyncontext)stx).synbuilder();
			logChange(synb, insrt, snm, chm, sbm, entm, synode);
//			insrt
//			  .post(st.update(entm.tbl)
//				.nv(entm.synuid, SynChangeMeta.uids(synode, entId))
//				.whereEq(entm.pk, entId))
//			  .post(st.insert(chm.tbl)
//				.nv(chm.entbl, entm.tbl)
//				.nv(chm.crud, CRUD.C)
//				.nv(chm.synoder, synode)
//				.nv(chm.uids, SynChangeMeta.uids(synode, entId))
//				.nv(chm.nyquence, stx.stamp().n)
//				.nv(chm.seq, stx.incSeq())
//				.nv(chm.domain, stx.domain())
//				.post(st.insert(sbm.tbl)
//					.cols(sbm.insertCols())
//					.select((Query) st.select(snm.tbl)
//						.col(new Resulving(chm.tbl, chm.pk))
//						.col(snm.synoder)
//						.where(op.ne, snm.synoder, constr(synode))
//						.whereEq(snm.domain, stx.domain()))))
			;
		}
		
		protected boolean checkBuilder(ISemantext stx) {
			if (stx instanceof ISyncontext &&
				((ISyncontext)stx).synbuilder() instanceof DBSyntableBuilder) 
				return true;
			
			try { Utils.warnT(new Object() {},
				"Syn-change's handler needs a builder of type DBSyntableBuilder, but got semantext %s on table %s.\n" +
				"Semantics handling is ignored.",
				stx.getClass().getName(), target);
			} catch (Exception e) { e.printStackTrace(); }
			return false;
		}

		protected void onUpdate(ISyncontext stx, Update updt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws TransException, SQLException {
			AnResultset hittings = isHit(stx, updt, row, cols, usr);
			if (hittings.getRowCount() > 0)
				updt = 
				logChange(stx.synbuilder(), updt, snm, chm, sbm, entm, synode, null, cols.keySet());
		}

		private AnResultset isHit(ISyncontext stx, Update updt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws TransException, SQLException {
			return ((AnResultset) trxt.select(target)
				.col(entm.synuid)
				.where(updt.where())
				.rs(trxt.instancontxt(stx.connId(), usr))
				.rs(0));
		}

		protected void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt,
				Condit condt, IUser usr) throws SemanticException {
			
			AnResultset row;
			try {
				row = (AnResultset) trxt
						.select(target)
						.where(condt)
						.groupby(pkField)
						.rs(trxt.instancontxt(stx.connId(), usr))
						.rs(0);

				while (row.next()) {
					Delete delChg = trxt.delete(chm.tbl);
					Delete delSub = trxt.delete(sbm.tbl);
					for (String id : ((SyntityMeta) stx.getTableMeta(target)).globalIds()) {
						delChg.whereEq(id, row.getString(id));
						delSub.whereEq(id, row.getString(id)); // TODO sub tabel changed
					}

					stmt.post(delChg);
				}
			} catch (TransException | SQLException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}
		
		static void verifyRequiredNvs(Set<String> required, Map<String, Integer> cols, ArrayList<Object[]> row)
				throws SemanticException {
		}
		
	}

//	public static class ShSynChange_del extends SemanticHandler {
//		static String apidoc = "TODO ...";
//		protected final SynChangeMeta chm;
//		protected final SynodeMeta snm;
//		protected final SynSubsMeta sbm;
//
//		// protected final DBSynsactBuilder syb;
//
//		/** Ultra high frequency mode, the data frequency need to be reduced with some cost */
//		// private final boolean UHF;
//		private final String entflag;
//		// private final ExprPart entGID;
//		private final Resulving entChangeId;
//
//		/**
//		 * Target synchronzed table meta's name, configured in xml. E.g. io.oz.album.PhotoMeta.
//		 */
//		private final String metacls;
//
//		/**
//		 * Target synchronzed table meta, e.g. PhotoMeta.
//		 */
//		private final SyntityMeta entm;
//
//		ShSynChange_del (Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
//			super(trxt, smtype.synChange, tabl, pk, args);
//			insert = true;
//			update = true;
//			delete = true;
//
//			try {
//				snm = new SynodeMeta(trxt.basictx().connId());
//			} catch (TransException e) {
//				e.printStackTrace();
//				throw new SemanticException(e.getMessage());
//			}
//			chm = new SynChangeMeta();
//			sbm = new SynSubsMeta(chm);
//			
//			this.metacls = args[0];
//			
//			TableMeta m = trxt.tableMeta(tabl);
//			if (!eq(metacls, m.getClass().getName())) {
//				try {
//					Class<?> cls = Class.forName(metacls);
//					Constructor<?> constructor = cls.getConstructor(String.class);
//					entm = (SyntityMeta) constructor.newInstance(trxt.basictx().connId());
//					entm.replace();
//				} catch (ReflectiveOperationException | TransException | SQLException e) {
//					Utils.warn("Error to create instance of table meta: %s", metacls);
//					e.printStackTrace();
//					throw new SemanticException(e.getMessage());
//				}
//			}
//			else entm = (SyntityMeta) m;
//
//			entflag = trim(args[1]);
//			
//			// entGID = compound(split(args[2], " "));
//			entChangeId = new Resulving(sbm.tbl, sbm.pk);
//		}
//
//		protected void onInsert(ISyncontext stx, Insert insrt,
//				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
//			Utils.logi("synChange: onInsert ...\n\n");
//			requiredNv(entflag, new ExprPart(CRUD.C), cols, row, target, usr);
//			logChange(stx, insrt, row, cols, usr);
//		}
//
//		protected void onUpdate(ISyncontext stx, Update updt,
//				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
//				throws SemanticException {
//			requiredNv(entflag, new ExprPart(CRUD.U), cols, row, target, usr);
//			if (isHit(stx, updt, row, cols, usr))
//				updt = (Update) logChange(stx, updt, row, cols, usr);
//		}
//
//		private boolean isHit(ISyncontext stx, Update updt,
//				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
//				throws SemanticException {
//
//			try {
//				return ((AnResultset) trxt.select(target)
//					.col(count(pkField), "c")
//					.where(updt.where())
//					.rs(trxt.instancontxt(stx.connId(), usr))
//					.rs(0))
//					.nxt()
//					.getInt("c") > 0;
//			} catch (TransException | SQLException e) {
//				e.printStackTrace();
//				throw new SemanticException(e.getMessage());
//			}
//		}
//
//		/**
//		 * Log changes when synchronized entities updated.
//		 * @param <T>
//		 * @param stx
//		 * @param stmt
//		 * @param row
//		 * @param cols
//		 * @param usr
//		 * @return statements, i. e. stmt
//		 * @throws SemanticException
//		 */
//		private <T extends Statement<?>> T logChange(ISyncontext stx, T stmt,
//				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
//				throws SemanticException {
//
//			// args: pk, crud-flag, n pk2 ...,         [col-value-on-del, ...]
//			// e.g.: pid,crud,      synoder clientpath,exif,uri "",clientpath
//
//			verifyRequiredCols(entm.globalIds(), cols.keySet());
//
//			Delete delChg = trxt.delete(chm.tbl);
//			for (String c : cols.keySet())
//				// all cols now exists in row (extended by verifyRequiredCols
//				// if (entm.globalIds().contains(c))
//				delChg.whereEq(c, row.get(cols.get(c))[1]);
//
//			
//			Insert insChg = trxt.insert(chm.tbl);
//
//			String pid = (String) stx.resulvedVal(args[1], args[2], -1);
//			String synoder = (String) row.get(cols.get(chm.synoder))[1];
//			try {
//				insChg.select(trxt
//						.select(snm.tbl, "s")
//						// .je("s", nyqm.tbl, "ny", snm.org(), nyqm.org(), snm.synode, nyqm.synode)
//						.col(constr(pid))
//						.col(CRUD.C, chm.crud)
//						.col(snm.nyquence)
//						// .whereEq(nyqm.entbl, target)
//						.whereEq(snm.domain, usr.orgId())
//						.whereEq(snm.synoder, synoder));
//
//				// and insert subscriptions, or merge syn_change & syn_subscribe?
//				Insert insubs = trxt.insert(sbm.tbl)
//						.select(trxt
//							.select(snm.tbl)
//							.col(snm.synoder, sbm.synodee)
//							//.col(entGID, sbm.uids)
//							.col(entChangeId, sbm.changeId)
//							.where(op.ne, snm.synoder, usr.uid())
//							.whereEq(snm.domain, usr.orgId()));
//
//				insChg.post(trxt
//						.delete(sbm.tbl)
//						// .whereEq(sbm.domain, usr.orgId())
//						// .whereEq(sbm.uids, usr)
//						.whereEq(sbm.changeId, entChangeId)
//						.whereEq(sbm.pk, usr)
//						.post(insubs));
//			} catch (TransException e) {
//				e.printStackTrace();
//				throw new SemanticException(e.getMessage());
//			}
//
//			stmt.post(delChg);
//			
//			return stmt;
//		}
//
//		protected void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
//				throws SemanticException {
//			
//			AnResultset row;
//			try {
//				row = (AnResultset) trxt
//						.select(target)
//						.where(condt)
//						.groupby(pkField)
//						.rs(trxt.instancontxt(stx.connId(), usr))
//						.rs(0);
//
//				while (row.next()) {
//					Delete delChg = trxt.delete(chm.tbl);
//					Delete delSub = trxt.delete(sbm.tbl);
//					for (String id : ((SyntityMeta) stx.getTableMeta(target)).globalIds()) {
//						delChg.whereEq(id, row.getString(id));
//						delSub.whereEq(id, row.getString(id)); // TODO sub tabel changed
//					}
//
//					stmt.post(delChg);
//				}
//			} catch (TransException | SQLException e) {
//				e.printStackTrace();
//				throw new SemanticException(e.getMessage());
//			}
//		}
//
//		private void verifyRequiredCols(Set<String> pkcols, Set<String> cols)
//				throws SemanticException {
//			int idCols = 0;
//			if (isNull(pkcols) && isNull(cols))
//				return;
//
//			if (!isNull(pkcols) && !isNull(cols))
//				for (String pkcol : pkcols)
//					if (cols.contains(pkcol))
//						idCols++;
//					else break;
//
//			if (idCols > 0 && idCols <= cols.size() && idCols != len(pkcols))
//				// FIXME let's force extend the cols 
//				// TODO doc
//				throw new SemanticException(
//					"To update a synchronized table, all cols' values for identifying a global record are needed to generate syn-change log.\n"
//					+ "Columns in field values recieved are: %s\n"
//					+ "Required columns: %s, %s.\n"
//					+ "For how to configue See javadoc: %s",
//					str((AbstractSet<String>)cols), this.pkField, str((HashSet<String>)pkcols), apidoc);
//		}
//	}

}
