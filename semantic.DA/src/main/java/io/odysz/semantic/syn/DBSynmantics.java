package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.eq;
import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.len;
import static io.odysz.common.LangExt.split;
import static io.odysz.common.LangExt.str;
import static io.odysz.common.LangExt.trim;
import static io.odysz.transact.sql.parts.condition.ExprPart.constr;
import static io.odysz.transact.sql.parts.condition.Funcall.compound;
import static io.odysz.transact.sql.parts.condition.Funcall.count;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.syn.DBSynsactBuilder.SynmanticsMap;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.x.TransException;

public class DBSynmantics extends DASemantics {

	public DBSynmantics(Transcxt basicTx, String tabl, String pk, boolean... verbose) {
		super(basicTx, tabl, pk, verbose);
	}

	@Override
	public SemanticHandler parseHandler(Transcxt tsx, String tabl, smtype smtp,
			String pk, String[] args) throws SemanticException {
		if (smtype.synChange == smtp)
			try {
				return new DBSynmantics.ShSynChange(
						new DBSynsactBuilder(tsx.basictx().connId(), "dummy-loader"),
						tabl, pk, args);
			} catch (TransException | SQLException | SAXException | IOException e) {
				e.printStackTrace();
				return null;
			}
		else
			return super.parseHandler(tsx, tabl, smtp, pk, args);
	}
	
	@Override
	public SynmanticsMap createSMap(String conn) {
		return new SynmanticsMap(conn);
	}

	public static class ShSynChange extends SemanticHandler {
		static String apidoc = "TODO ...";
		protected final SynChangeMeta chm;
		protected final SynodeMeta snm;
		protected final SynSubsMeta sbm;

		protected final DBSynsactBuilder syb;

		/** Ultra high frequency mode, the data frequency need to be reduced with some cost */
		// private final boolean UHF;
		private final String entflag;
		private final ExprPart entGID;

		/**
		 * Target synchronzed table meta's name, configured in xml. E.g. io.oz.album.PhotoMeta.
		 */
		private final String metacls;

		/**
		 * Target synchronzed table meta, e.g. PhotoMeta.
		 */
		private final SyntityMeta entm;

		ShSynChange(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.synChange, tabl, pk, args);
			insert = true;
			update = true;
			delete = true;

			// UHF = true;
			
			if (trxt instanceof DBSynsactBuilder)
				syb = (DBSynsactBuilder) trxt;
			else
				throw new SemanticException("ShSynChange (xml/smtype=s-c) requires instance of DBSynsactBuilder as the default builder.");

			try {
				snm = new SynodeMeta(trxt.basictx().connId(), null);
			} catch (TransException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
			chm = new SynChangeMeta();
			sbm = new SynSubsMeta();
			
			this.metacls = args[0];
			
			TableMeta m = trxt.tableMeta(tabl);
			if (!eq(metacls, m.getClass().getName())) {
				try {
					Class<?> cls = Class.forName(metacls);
					Constructor<?> constructor = cls.getConstructor(String.class);
					entm = (SyntityMeta) constructor.newInstance(trxt.basictx().connId());
					entm.replace();
				} catch (ReflectiveOperationException | TransException | SQLException e) {
					Utils.warn("Error to create instance of table meta: %s", metacls);
					e.printStackTrace();
					throw new SemanticException(e.getMessage());
				}
			}
			else entm = (SyntityMeta) m;

			entflag = trim(args[1]);
			
			entGID = compound(split(args[2], " "));
		}

		protected void onInsert(ISyncontext stx, Insert insrt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {
			Utils.logi("synChange: onInsert ...\n\n");
			requiredNv(entflag, new ExprPart(CRUD.C), cols, row, target, usr);
			logChange(stx, insrt, row, cols, usr);
		}

		protected void onUpdate(ISyncontext stx, Update updt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			requiredNv(entflag, new ExprPart(CRUD.U), cols, row, target, usr);
			if (isHit(stx, updt, row, cols, usr))
				updt = (Update) logChange(stx, updt, row, cols, usr);
		}

		private boolean isHit(ISyncontext stx, Update updt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {

			try {
				return ((AnResultset) syb.select(target)
					.col(count(pkField), "c")
					.where(updt.where())
					.rs(syb.instancontxt(stx.connId(), usr))
					.rs(0))
					.nxt()
					.getInt("c") > 0;
			} catch (TransException | SQLException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}

		/**
		 * Log changes when synchronized entities updated.
		 * @param <T>
		 * @param stx
		 * @param stmt
		 * @param row
		 * @param cols
		 * @param usr
		 * @return statements, i. e. stmt
		 * @throws SemanticException
		 */
		private <T extends Statement<?>> T logChange(ISyncontext stx, T stmt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {

			// args: pk, crud-flag, n pk2 ...,         [col-value-on-del, ...]
			// e.g.: pid,crud,      synoder clientpath,exif,uri "",clientpath

			verifyRequiredCols(entm.globalIds(), cols.keySet());

			Delete delChg = syb.delete(chm.tbl);
			for (String c : cols.keySet())
				// all cols now exists in row (extended by verifyRequiredCols
				// if (entm.globalIds().contains(c))
				delChg.whereEq(c, row.get(cols.get(c))[1]);

			
			Insert insChg = syb.insert(chm.tbl);

			String pid = (String) stx.resulvedVal(args[1], args[2]);
			String synoder = (String) row.get(cols.get(chm.synoder))[1];
			try {
				insChg.select(syb
						.select(snm.tbl, "s")
						// .je("s", nyqm.tbl, "ny", snm.org(), nyqm.org(), snm.synode, nyqm.synode)
						.col(constr(pid))
						.col(CRUD.C, chm.crud)
						.col(String.valueOf(stx.nyquence()), snm.nyquence)
						// .whereEq(nyqm.entbl, target)
						.whereEq(snm.domain, usr.orgId())
						.whereEq(snm.synoder, synoder));

				// and insert subscriptions, or merge syn_change & syn_subscribe?
				Insert insubs = syb.insert(sbm.tbl)
						.select(syb
							.select(snm.tbl)
							.col(snm.synoder, sbm.synodee).col(entGID, sbm.uids)
							.where(op.ne, snm.synoder, usr.uid())
							.whereEq(snm.domain, usr.orgId()));

				insChg.post(syb
						.delete(sbm.tbl)
						.whereEq(sbm.org, usr.orgId())
						.whereEq(sbm.uids, usr)
						.whereEq(sbm.pk, usr)
						.post(insubs));
			} catch (TransException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}

			stmt.post(delChg);
			
			return stmt;
		}

		protected void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {
			
			AnResultset row;
			try {
				row = (AnResultset) syb
						.select(target)
						.where(condt)
						.groupby(pkField)
						.rs(syb.instancontxt(stx.connId(), usr))
						.rs(0);

				while (row.next()) {
					Delete delChg = syb.delete(chm.tbl);
					Delete delSub = syb.delete(sbm.tbl);
					for (String id : ((SyntityMeta) stx.getTableMeta(target)).globalIds()) {
						delChg.whereEq(id, row.getString(id));
						delSub.whereEq(id, row.getString(id));
					}

					stmt.post(delChg);
				}
			} catch (TransException | SQLException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}

		private void verifyRequiredCols(Set<String> pkcols, Set<String> cols)
				throws SemanticException {
			int idCols = 0;
			if (isNull(pkcols) && isNull(cols))
				return;

			if (!isNull(pkcols) && !isNull(cols))
				for (String pkcol : pkcols)
					if (cols.contains(pkcol))
						idCols++;
					else break;

			if (idCols > 0 && idCols <= cols.size() && idCols != len(pkcols))
				// FIXME let's force extend the cols 
				// TODO doc
				throw new SemanticException(
					"To update a synchronized table, all cols' values for identifying a global record are needed to generate syn-change log.\n"
					+ "Columns in field values recieved are: %s\n"
					+ "Required columns: %s, %s.\n"
					+ "For how to configue See javadoc: %s",
					str((AbstractSet<String>)cols), this.pkField, str((HashSet<String>)pkcols), apidoc);
		}
	}

	/**
	 * 
	 * <p>args<br>
	 * see {@link smtype#synChange}
	 * 
	 * @deprecated keep for awhile for tests can be passed
	 * 
	 * @author odys-z@github.com
	 */
//	public static class ShStampByNode extends SemanticHandler {
//		/** 0. conn-id of database of syn_stamp */
//		public static final int ixConn = 0;
//		/** 1. stamp table name, e.g. syn_node */
//		public static final int ixLogTabl = 1;
//		/** 2. target table field, e.g. 'tabl' */
//		public static final int ixSynTbl = 2;
//		/** 3. remote node id field, e.g. synode */
//		public static final int ixSynode = 3;
//		/** 4. crud */
//		public static final int ixCrud = 4;
//		/** 5. rec-count */
//		public static final int ixRecount = 5;
//		/** 6. stamp, optional. If no presented, will use Funcall.now(). */
//		public static final int ixStamp = 6;
//
//		private int cnt;
//		private DATranscxt shSt;
//		/** conn-id of database of syn_stamp */
//		private String conn;
//
//		static private Set<String> devices;
//		static {
//			devices = new HashSet<String>();
//		}
//
//		public ShStampByNode(Transcxt basicTsx, String tabl, String recId, String[] args) throws SemanticException {
//			super(basicTsx, smtype.synChange, tabl, recId, args);
//
//			insert = true;
//			delete = true;
//			update = true;
//			
//			cnt = 0;
//			try {
//				conn = args[ixConn];
//				if (isblank(conn))
//					conn = Connects.defltConn();
//					
//				shSt = new DATranscxt(conn);
//				
//				IUser shUser = new IUser() {
//					@Override public TableMeta meta(String ... c) { return null; }
//					@Override public ArrayList<String> dbLog(ArrayList<String> sqls) { return null; }
//					@Override public String uid() { return "ShStampByNode"; }
//					@Override public IUser logAct(String funcName, String funcId) { return this; }
//					@Override public String sessionKey() { return null; }
//					@Override public IUser sessionKey(String skey) { return null; }
//					@Override public IUser notify(Object note) throws TransException { return this; }
//					@Override public List<Object> notifies() { return null; }
//					@Override public long touchedMs() { return 0; }
//				};
//
//				AnResultset rs = (AnResultset) shSt
//					.select(args[ixLogTabl], "s")
//					.col(args[ixSynode], "n")
//					.whereEq(args[ixSynTbl], target)
//					.groupby(args[ixSynode])
//					.rs(basicTsx.instancontxt(null, shUser))
//					.rs(0);
//
//				while (rs.next()) {
//					devices.add(rs.getString("n"));
//				}
//
//			} catch (SQLException | SAXException | IOException | TransException e) {
//				// Fatal Error
//				e.printStackTrace();
//				throw new SemanticException(e.getMessage());
//			}
//		}
//
//		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
//				throws SemanticException {
//			
//			if (isblank(usr.deviceId()))
//				throw new SemanticException(
//						"Table %s's stampByNode semantics requires user provide device id. But it is empty (user-id = %s).",
//						target, usr.uid());
//
//			IPostOptn op = stx.onTableCommittedHandler(target);
//			ShStampByNode that = this;
//
//			if (op == null) {
//				cnt = 1;
//				op = devices.contains(usr.deviceId()) ?
//					(ctx, sqls) -> {
//						Update u = shSt.update(args[ixLogTabl], usr)
//							.nv(args[ixRecount], that.cnt)
//							.nv(args[ixCrud], CRUD.C)
//							.whereEq(args[ixSynTbl], target)
//							.whereEq(args[ixSynode], usr.deviceId());
//						
//						if (ixStamp <= args.length && !isblank(args[ixStamp]))
//							u.nv(args[ixStamp], now());
//						u.u(ctx);
//						return null;
//					}
//					: (ctx, sqls) -> {
//						// not likely a concurrency problem as the device won't update with multiple threads.
//						ShStampByNode.devices.add(usr.deviceId());
//
//						Insert i = shSt.insert(args[ixLogTabl], usr)
//							.nv(args[ixRecount], that.cnt)
//							.nv(args[ixCrud], CRUD.C)
//							.nv(args[ixSynTbl], target)
//							.nv(args[ixSynode], usr.deviceId());
//						
//						if (ixStamp <= args.length && !isblank(args[ixStamp]))
//							i.nv(args[ixStamp], now());
//						i.ins(shSt.instancontxt(conn, usr));
//						
//						return null;
//					};
//				stx.addOnTableCommitted(target, op);
//			}
//			else cnt++;
//		}
//
//		protected void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
//				throws SemanticException {
//			onInsert(stx, null, row, cols, usr);
//		}
//
//		protected void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
//				throws SemanticException {
//			if (isblank(usr.deviceId()))
//				throw new SemanticException(
//						"Table %s's stampByNode semantics requires user provide device id. But it is empty (user-id = %s, delete).",
//						target, usr.uid());
//
//			IPostOptn op = stx.onTableCommittedHandler(target);
//			ShStampByNode that = this;
//
//			if (op == null) {
//				that.cnt = 1;
//				op = (st, sqls) -> {
//					that.cnt = 1;
//					Update u = shSt.update(args[ixLogTabl], usr)
//						.nv(args[ixRecount], that.cnt)
//						.nv(args[ixCrud], CRUD.C)
//						.whereEq(args[ixSynTbl], target)
//						.whereEq(args[ixSynode], usr.deviceId());
//					
//					if (ixStamp <= args.length && !isblank(args[ixStamp]))
//						u.nv(args[ixStamp], now());
//						
//					u.u(shSt.instancontxt(conn, usr));
//					return null;
//				};
//			}
//			else cnt++;
//
//			stx.addOnTableCommitted(target, op);
//		}
//	}
}
