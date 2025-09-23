package io.oz.syn;

import static io.odysz.common.LangExt._0;
import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.ArrayList;
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
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.x.TransException;

/**
 * See the topic of
 * <a href='https://odys-z.github.io/dev/topics/semantics/3plugin.html#extending-default-semantics-plugin'>
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
			String pk, String[] args) throws SQLException, TransException {
		if (smtype.synChange == smtp)
			// relic for docs 
			Utils.warn("The syn-change semantics is silenced as a newer design decision");
		return super.parseHandler(tsx, tabl, smtp, pk, args);
	}
	
	@Override
	public DBSynTransBuilder.SynmanticsMap createSMap(String conn) {
		return new DBSynTransBuilder.SynmanticsMap(synode, conn);
	}
	
	/**
	 * 
	 * @param b
	 * @param inst
	 * @param entm
	 * @param synode
	 * @param entitypk, required if the entity's id is a not {@link smptyp.autoInc}.
	 * This is resolved with {@link Resulving}, and overriden by a {@link Resulving} argument.
	 * @return inst
	 * @throws TransException
	 */
	public static Insert logChange(SyndomContext x, DBSyntableBuilder b, Insert inst,
			SyntityMeta entm, String synode, Object... entitypk) throws TransException {

		Insert insc = b.insert(x.chgm.tbl)
			.nv(x.chgm.entbl, entm.tbl)
			.nv(x.chgm.crud, CRUD.C)
			.nv(x.chgm.synoder, x.synode)
			.nv(x.chgm.nyquence, x.stamp.n)
			.nv(x.chgm.seq, b.incSeq())
			.nv(x.chgm.domain, x.domain)
			.post(b.insert(x.subm.tbl)
				.cols(x.subm.insertCols())
				.select((Query) b.select(x.synm.tbl)
					.col(new Resulving(x.chgm.tbl, x.chgm.pk))
					.col(x.synm.synoder)
					.where(op.ne, x.synm.synoder, constr(x.synode))
					.whereEq(x.synm.domain, x.domain)));

		boolean resulveAutokey = DATranscxt.hasSemantics(x.synconn, entm.tbl, smtype.autoInc)
							&& isNull(entitypk);

		Object epk = _0(entitypk);
		ExprPart pid = resulveAutokey
					? new Resulving(entm.tbl, entm.pk)
					: epk instanceof String
					? ExprPart.constr((String)epk)
					: (ExprPart)epk;

		Update upe =  b.update(entm.tbl);

		upe.nv(entm.io_oz_synuid, SynChangeMeta.uids(synode, pid));
		insc.nv(x.chgm.uids, SynChangeMeta.uids(synode, pid));

		inst.post(upe.whereEq(entm.pk, pid))
			.post(insc);

		return inst;
	}

	public static Update logChange(SyndomContext x, DBSyntableBuilder b, Update updt,
			SyntityMeta entm, String synoder, AnResultset hittings, Iterable<String> updcols)
				throws TransException, SQLException {
		if (hittings != null) {
			hittings.beforeFirst();

			while (hittings.next())
				updt.post(b
					.insert(x.chgm.tbl)
					.nv(x.chgm.entbl, entm.tbl)
					.nv(x.chgm.crud, CRUD.U)
					.nv(x.chgm.synoder, synoder)
					.nv(x.chgm.uids, hittings.getString(entm.io_oz_synuid))
					.nv(x.chgm.nyquence, x.stamp.n)
					.nv(x.chgm.seq, b.incSeq())
					.nv(x.chgm.domain, x.domain)
					.nv(x.chgm.updcols, updcols)
					.post(b.insert(x.subm.tbl)
						.cols(x.subm.insertCols())
						.select((Query)b.select(x.synm.tbl)
							.col(new Resulving(x.chgm.tbl, x.chgm.pk))
							.col(x.synm.synoder)
							.where(op.ne, x.synm.synoder, constr(synoder))
							.whereEq(x.synm.domain, x.domain))))
			;
		}
	
		return updt;
	}
	
	public static Delete logChange(SyndomContext x, DBSyntableBuilder b, Delete delt,
			SyntityMeta entm, AnResultset hittings) throws TransException, SQLException {

		if (hittings != null) {
			hittings.beforeFirst();

			while (hittings.next()) {
				String synuid = hittings.getString(entm.io_oz_synuid);

				return delt.post(b
					.insert(x.chgm.tbl)
					.nv(x.chgm.entbl, entm.tbl)
					.nv(x.chgm.crud, CRUD.D)
					.nv(x.chgm.synoder, x.synode)
					.nv(x.chgm.uids, synuid)
					.nv(x.chgm.nyquence, x.stamp.n)
					.nv(x.chgm.seq, b.incSeq())
					.nv(x.chgm.domain, x.domain)
					.post(b
						.insert(x.subm.tbl)
						.cols(x.subm.insertCols())
						.select((Query)b
							.select(x.synm.tbl)
							.col(new Resulving(x.chgm.tbl, x.chgm.pk))
							.col(x.synm.synoder)
							.where(op.ne, x.synm.synoder, constr(x.synode))
							.whereEq(x.synm.domain, x.domain)))
					);
			}
		}
		return delt;
	}	

	/**
	 * Semantics handler of syn-change logs.
	 * 
	 * This handler should not be configured via semantics.xml, but via dictionary.json's syntities field.
	 */
	public static class ShSynChange extends SemanticHandler {

		protected final SynChangeMeta chm;
		protected final SynodeMeta snm;
		protected final SynSubsMeta sbm;

		/**
		 * Target synchronzed table meta, e.g. PhotoMeta.
		 */
		public final SyntityMeta entm;
		protected final String synode;
		
		protected final DATranscxt st;

		ShSynChange(Transcxt trxt, String synode, SyntityMeta m) throws TransException {
			super(trxt, smtype.synChange, m.tbl, m.pk,
				new String[] {ShSynChange.class.getName()});

			insert = true;
			update = true;
			delete = true;

			this.entm = m;
			this.synode = synode;
			this.st = (DATranscxt) trxt;
			this.snm = new SynodeMeta(trxt.basictx().connId());
			this.chm = new SynChangeMeta();
			this.sbm = new SynSubsMeta(chm);
		}

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

			DBSyntableBuilder synb = (DBSyntableBuilder) ((ISyncontext) stx).synbuilder();
			
			Object pk = null;
			try {
				if (cols.containsKey(entm.pk))
					pk = row.get(cols.get(entm.pk))[1];
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			logChange(((ISyncontext) stx).syndomContext(), synb, insrt, entm, synode, pk);
		}
		
		protected boolean checkBuilder(ISemantext stx) {
			if (stx instanceof ISyncontext &&
				((ISyncontext)stx).synbuilder() instanceof DBSyntableBuilder) 
				return true;
			
			try { Utils.warnT(new Object() {},
				"\nSyn-change's handler needs a builder of type DBSyntableBuilder.\n" +
				"Semantext: %s.\nSynTransaction Builder: %s\n" +
				"Table: %s, Synode: %s\n" +
				"Semantics handling is ignored.",
					stx.getClass().getName(),
					stx instanceof ISyncontext
					? ((ISyncontext)stx).synbuilder() : "Not an ISyncontext",
					target, synode);
			} catch (Exception e) { e.printStackTrace(); }
			return false;
		}

		@Override
		protected void onUpdate(ISemantext stx, Update updt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws TransException {
			try {
				AnResultset hittings = hits(stx, updt);
				if (hittings.getRowCount() > 0)
					updt = logChange(((ISyncontext)stx).syndomContext(),
								(DBSyntableBuilder) ((ISyncontext)stx).synbuilder(), updt, entm, synode,
								hittings, cols.keySet());
			} catch (TransException | SQLException e) {
				e.printStackTrace();
				throw new TransException(e.getMessage());
			}
		}

		private AnResultset hits(ISemantext stx, Statement<?> updt)
				throws TransException, SQLException {
			return ((AnResultset) trxt.select(target)
				.col(entm.io_oz_synuid)
				.where(updt.where())
				.rs(stx)
				.rs(0))
				.beforeFirst();
		}

		@Override
		protected void onDelete(ISemantext stx, Delete stmt, Condit condt, IUser usr) throws TransException {
			try {
				AnResultset hittings = hits(stx, stmt);
				while (hittings.next())
					stmt = logChange(
							((ISyncontext)stx).syndomContext(),
							(DBSyntableBuilder) ((ISyncontext)stx).synbuilder(),
							stmt, entm, hittings);
			} catch (SQLException e) {
				e.printStackTrace();
				throw new TransException(e.getMessage());
			}
		}
		
	}
}
