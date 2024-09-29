package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 * See the topic of <a href='https://odys-z.github.io/dev/topics/semantics/3plugin.html#extending-default-semantics-plugin'>
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
			String pk, String[] args) throws Exception {
		if (smtype.synChange == smtp)
			return new DBSynmantics.ShSynChange(tsx, synode, tabl, pk, args);
		else
			return super.parseHandler(tsx, tabl, smtp, pk, args);
	}
	
	@Override
	public DBSyntableBuilder.SynmanticsMap createSMap(String conn) {
		return new DBSyntableBuilder.SynmanticsMap(synode, conn);
	}
	
	public static Insert logChange(DBSyntableBuilder b, Insert inst,
			SyntityMeta entm, String synode) throws TransException {
		Update u = b.update(entm.tbl);
		Resulving pid = new Resulving(entm.tbl, entm.pk);
		if (pid instanceof Resulving)
			u.nv(entm.synuid, SynChangeMeta.uids(synode, (Resulving)pid));
		else
			u.nv(entm.synuid, SynChangeMeta.uids(synode,  pid.toString()));

		Insert insc = b.insert(b.chgm.tbl)
				.nv(b.chgm.entbl, entm.tbl)
				.nv(b.chgm.crud, CRUD.C)
				.nv(b.chgm.synoder, b.synode())
				.nv(b.chgm.nyquence, b.stamp())
				.nv(b.chgm.seq, b.incSeq())
				.nv(b.chgm.domain, b.domain())
				.post(b.insert(b.subm.tbl)
					.cols(b.subm.insertCols())
					.select((Query) b.select(b.synm.tbl)
						.col(new Resulving(b.chgm.tbl, b.chgm.pk))
						.col(b.synm.synoder)
						.where(op.ne, b.synm.synoder, constr(b.synode()))
						.whereEq(b.synm.domain, b.domain())));
		if (pid instanceof Resulving)
			// insc.nv(b.chgm.uids, SynChangeMeta.uids(b.synode(), (Resulving)pid));
			insc.nv(b.chgm.uids, SynChangeMeta.uids(synode, (Resulving)pid));
		else
			insc.nv(entm.synuid, SynChangeMeta.uids(synode,  pid.toString()));

		return inst
			.post(u.whereEq(entm.pk, pid))
			.post(insc);
	}

	public static Update logChange(DBSyntableBuilder b, Update updt,
			SyntityMeta entm, String synoder, List<String> synuids, Iterable<String> updcols)
				throws TransException, SQLException {
		for (String synuid : synuids)
			updt.post(b
					.insert(b.chgm.tbl)
					.nv(b.chgm.entbl, entm.tbl)
					.nv(b.chgm.crud, CRUD.U)
					.nv(b.chgm.synoder, synoder)
					.nv(b.chgm.uids, synuid)
					.nv(b.chgm.nyquence, b.stamp())
					.nv(b.chgm.seq, b.incSeq())
					.nv(b.chgm.domain, b.domain())
					.nv(b.chgm.updcols, updcols)
					.post(b.insert(b.subm.tbl)
						.cols(b.subm.insertCols())
						.select((Query)b.select(b.synm.tbl)
							.col(new Resulving(b.chgm.tbl, b.chgm.pk))
							.col(b.synm.synoder)
							.where(op.ne, b.synm.synoder, constr(synoder))
							.whereEq(b.synm.domain, b.domain()))))
			;
	
		return updt;
	}
	
	public static Delete logChange(DBSyntableBuilder b, Delete delt,
			SyntityMeta entm, String synuid) throws TransException, SQLException {
		return delt.post(b
				.insert(b.chgm.tbl)
				.nv(b.chgm.entbl, entm.tbl)
				.nv(b.chgm.crud, CRUD.D)
				.nv(b.chgm.synoder, b.synode())
				.nv(b.chgm.uids, synuid)
				.nv(b.chgm.nyquence, b.stamp())
				.nv(b.chgm.seq, b.incSeq())
				.nv(b.chgm.domain, b.domain())
				.post(b
					.insert(b.subm.tbl)
					.cols(b.subm.insertCols())
					.select((Query)b
						.select(b.synm.tbl)
						.col(new Resulving(b.chgm.tbl, b.chgm.pk))
						.col(b.synm.synoder)
						.where(op.ne, b.synm.synoder, constr(b.synode()))
						.whereEq(b.synm.domain, b.domain())))
		);
	}	

	public static class ShSynChange extends SemanticHandler {

		// static String apidoc = "TODO ...";
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
				throws SemanticException {
			super(trxt, smtype.synChange, tabl, pk, args);
			insert = true;
			update = true;
			delete = true;

			this.synode = synode;
			try {
				st = new DATranscxt(null);
				snm = new SynodeMeta(trxt.basictx().connId());
				chm = new SynChangeMeta();
				sbm = new SynSubsMeta(chm);
			
				TableMeta m = trxt.tableMeta(tabl);
				if (!eq(args[0], m.getClass().getName())) {
					Class<?> cls;
						cls = Class.forName(args[0]);
						Constructor<?> constructor = cls.getConstructor(String.class);
						entm = (SyntityMeta) constructor.newInstance(trxt.basictx().connId());
						entm.replace();
				}
				else entm = (SyntityMeta) m;
				entId = new Resulving(entm.tbl, entm.pk);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}

		@Override
		protected void onInsert(ISemantext stx, Insert insrt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws TransException {
			if (!checkBuilder(stx)) return;

			if (verbose) Utils.logi("synChange: onInsert ...");

			DBSyntableBuilder synb = ((ISyncontext)stx).synbuilder();
			logChange(synb, insrt, entm, synode);
		}
		
		protected boolean checkBuilder(ISemantext stx) {
			if (stx instanceof ISyncontext &&
				((ISyncontext)stx).synbuilder() instanceof DBSyntableBuilder) 
				return true;
			
			try { Utils.warnT(new Object() {},
				"\nSyn-change's handler needs a builder of type DBSyntableBuilder.\n" +
				"Semantext: %s.\nTransaction Builder: %s\n" +
				"Table: %s, Synode: %s\n" +
				"Semantics handling is ignored.",
					stx.getClass().getName(),
					((ISyncontext)stx).synbuilder() instanceof DBSyntableBuilder
					? ((ISyncontext)stx).synbuilder() : null,
					target, synode);
			} catch (Exception e) { e.printStackTrace(); }
			return false;
		}

		@Override
		protected void onUpdate(ISemantext stx, Update updt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws TransException {
			try {
				AnResultset hittings = hits(stx, updt, usr);
				if (hittings.getRowCount() > 0)
					updt = logChange(((ISyncontext)stx).synbuilder(),
									updt, entm, synode, null, cols.keySet());
			} catch (TransException | SQLException e) {
				e.printStackTrace();
				throw new TransException(e.getMessage());
			}
		}

		private AnResultset hits(ISemantext stx, Statement<?> updt, IUser usr)
				throws TransException, SQLException {
			return ((AnResultset) trxt.select(target)
				.col(entm.synuid)
				.where(updt.where())
				.rs(trxt.instancontxt(stx.connId(), usr))
				.rs(0))
				.beforeFirst();
		}

		@Override
		protected void onDelete(ISemantext stx, Delete stmt, Condit condt, IUser usr) throws TransException {
			try {
				AnResultset hittings = hits(stx, stmt, usr);
				while (hittings.next())
					stmt = logChange(((ISyncontext)stx).synbuilder(),
								stmt, entm, hittings.getString(entm.synuid));
			} catch (SQLException e) {
				e.printStackTrace();
				throw new TransException(e.getMessage());
			}
			// throw new SemanticException("tested?");

			/*
			try {
				AnResultset row = (AnResultset) trxt
						.select(target)
						.where(condt)
						.groupby(pkField)
						.rs(trxt.instancontxt(stx.connId(), usr))
						.rs(0);

				while (row.next()) {
					Delete delChg = trxt.delete(chm.tbl);
					Delete delSub = trxt.delete(sbm.tbl);
					// Not correct as chm.tbl doesn't have col of syntity's.
					// Haven't reached here yet?
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
			*/
		}
		
	}
}
