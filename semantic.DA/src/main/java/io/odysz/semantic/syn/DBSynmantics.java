package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.str;
import static io.odysz.transact.sql.parts.condition.Funcall.add;
import static io.odysz.transact.sql.parts.condition.Funcall.count;
import static io.odysz.transact.sql.parts.condition.Funcall.now;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xml.sax.SAXException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
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
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Statement.IPostOptn;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.Logic.op;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.x.TransException;

public class DBSynmantics extends DASemantics {

	@Override
	public void addHandler(smtype semantic, String tabl, String recId, String[] args)
			throws SemanticException, SQLException {

		if (smtype.synChange == semantic)
			handlers.add(new ShSynChange(basicTsx, tabl, recId, args));
		else super.addHandler(semantic, tabl, recId, args);
	}

	public DBSynmantics(Transcxt basicTx, String tabl, String recId, boolean verbose) {
		super(basicTx, tabl, recId, verbose);
	}
	
	public static class ShSynChange extends SemanticHandler {
		static String apidoc = "TODO ...";
		protected final SynChangeMeta chm;
		protected final SynodeMeta snm;
		protected final SynSubsMeta sbm;

		protected final DBSynsactBuilder syb;

		/** Ultra high frequency mode, the data frequency need to be reduced with some cost */
		protected final boolean UHF;
		private String entflag;

		ShSynChange(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.synChange, tabl, pk, args);
			if (trxt instanceof DBSynsactBuilder)
				// builder
				syb = (DBSynsactBuilder) trxt;
			else
				throw new SemanticException("ShSynChange (xml/smtype=s-c) requires instance of DBSynsactBuilder as the default builder.");

			snm = new SynodeMeta();
			chm = new SynChangeMeta();
			sbm = new SynSubsMeta();
			UHF = true;
		}

		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			// target-table.flag = crud
			requiredNv(entflag, new ExprPart(CRUD.C), cols, row, target, usr);
			logChange(stx, insrt, row, cols, usr);
		}

		protected void onUpdate(ISemantext stx, Update updt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			requiredNv(entflag, new ExprPart(CRUD.U), cols, row, target, usr);
			if (isHit(stx, updt, row, cols, usr))
				updt = (Update) logChange(stx, updt, row, cols, usr);
		}

		private boolean isHit(ISemantext stx, Update updt,
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

		private <T extends Statement<?>> T logChange(ISemantext stx, T stmt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {

			// args: pk,n pk2 ...,[col-value-on-del ...]
			// args: pid,synoder clientpath,exif,uri "",clientpath
			SyntityMeta entm = (SyntityMeta) stx.getTableMeta(target);

			verifyRequiredCols(entm.globalIds(), cols.keySet());

			Delete delChg = syb.delete(chm.tbl);
			for (String c : cols.keySet())
				if (entm.globalIds().contains(c))
					delChg.whereEq(c, row.get(cols.get(c))[1]);

			
			Insert insChg = syb.insert(chm.tbl);

			String pid = (String) stx.resulvedVal(args[1], args[2]);
			String synoder = (String) row.get(cols.get(chm.synoder))[1];
			try {
				insChg.select(syb
						.select(snm.tbl, "s")
						.col(pid, chm.entfk)
						.col(CRUD.C, chm.crud)
						.col(UHF ? add(snm.nyquence, snm.inc) : add(snm.nyquence, 1), chm.nyquence)
						.whereEq(snm.entbl, target)
						.whereEq(snm.synoder, synoder));

				// and insert subscriptions, or merge syn_change & syn_subscribe?
				Insert insubs = syb.insert(sbm.tbl)
						.select(syb
							.select(snm.tbl)
							.col(snm.synode).col(snm.synode)
							.where(op.ne, snm.synode, usr.uid())
							.whereEq(snm.org, usr.orgId()));

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
			
			if (this.UHF)
				stmt.post(clearInc(synoder, usr));
			else stmt.post(inc(synoder, usr));

			return stmt;
		}

		private Statement<?> inc(String synid, IUser usr) {
			return syb.update(snm.tbl, usr)
				.nv(snm.nyquence, add(snm.nyquence, 1))
				.whereEq(snm.synoder, synid)
				.whereEq(snm.entbl, target);
		}

		private Update clearInc(String synid, IUser usr) {
			return syb.update(snm.tbl, usr)
				.nv(snm.nyquence, add(snm.nyquence, snm.inc))
				.nv(snm.inc, 0)
				.whereEq(snm.synoder, synid)
				.whereEq(snm.entbl, target);
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

			if (idCols > 0 && idCols < cols.size())
				// TODO doc
				throw new SemanticException(
					"To update a synchronized table, all cols' values for identifying a global record are needed to generate syn-change log.\n"
					+ "Columns in field values recieved are: %s\n"
					+ "For how to configue See javadoc: %s",
					str((HashSet<String>)cols), apidoc);
		}
	}

	/**
	 * 
	 * <p>args<br>
	 * see {@link smtype#synChange}
	 * 
	 * @deprecated keep for awhile for tests passed
	 * 
	 * @author odys-z@github.com
	 */
	public static class ShStampByNode extends SemanticHandler {
		/** 0. conn-id of database of syn_stamp */
		public static final int ixConn = 0;
		/** 1. stamp table name, e.g. syn_node */
		public static final int ixLogTabl = 1;
		/** 2. target table field, e.g. 'tabl' */
		public static final int ixSynTbl = 2;
		/** 3. remote node id field, e.g. synode */
		public static final int ixSynode = 3;
		/** 4. crud */
		public static final int ixCrud = 4;
		/** 5. rec-count */
		public static final int ixRecount = 5;
		/** 6. stamp, optional. If no presented, will use Funcall.now(). */
		public static final int ixStamp = 6;

		private int cnt;
		private DATranscxt shSt;
		/** conn-id of database of syn_stamp */
		private String conn;

		static private Set<String> devices;
		static {
			devices = new HashSet<String>();
		}

		public ShStampByNode(Transcxt basicTsx, String tabl, String recId, String[] args) throws SemanticException {
			super(basicTsx, smtype.synChange, tabl, recId, args);

			insert = true;
			delete = true;
			update = true;
			
			cnt = 0;
			try {
				conn = args[ixConn];
				if (isblank(conn))
					conn = Connects.defltConn();
					
				shSt = new DATranscxt(conn);
				
				IUser shUser = new IUser() {
					@Override public TableMeta meta() { return null; }
					@Override public ArrayList<String> dbLog(ArrayList<String> sqls) { return null; }
					@Override public String uid() { return "ShStampByNode"; }
					@Override public IUser logAct(String funcName, String funcId) { return this; }
					@Override public String sessionKey() { return null; }
					@Override public IUser sessionKey(String skey) { return null; }
					@Override public IUser notify(Object note) throws TransException { return this; }
					@Override public List<Object> notifies() { return null; }
					@Override public long touchedMs() { return 0; }
				};

				AnResultset rs = (AnResultset) shSt
					.select(args[ixLogTabl], "s")
					.col(args[ixSynode], "n")
					.whereEq(args[ixSynTbl], target)
					.groupby(args[ixSynode])
					.rs(basicTsx.instancontxt(null, shUser))
					.rs(0);

				while (rs.next()) {
					devices.add(rs.getString("n"));
				}

			} catch (SQLException | SAXException | IOException | TransException e) {
				// Fatal Error
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}
		}

		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			
			if (isblank(usr.deviceId()))
				throw new SemanticException(
						"Table %s's stampByNode semantics requires user provide device id. But it is empty (user-id = %s).",
						target, usr.uid());

			IPostOptn op = stx.onTableCommittedHandler(target);
			ShStampByNode that = this;

			if (op == null) {
				cnt = 1;
				op = devices.contains(usr.deviceId()) ?
					(ctx, sqls) -> {
						Update u = shSt.update(args[ixLogTabl], usr)
							.nv(args[ixRecount], that.cnt)
							.nv(args[ixCrud], CRUD.C)
							.whereEq(args[ixSynTbl], target)
							.whereEq(args[ixSynode], usr.deviceId());
						
						if (ixStamp <= args.length && !isblank(args[ixStamp]))
							u.nv(args[ixStamp], now());
						u.u(ctx);
						return null;
					}
					: (ctx, sqls) -> {
						// not likely a concurrency problem as the device won't update with multiple threads.
						ShStampByNode.devices.add(usr.deviceId());

						Insert i = shSt.insert(args[ixLogTabl], usr)
							.nv(args[ixRecount], that.cnt)
							.nv(args[ixCrud], CRUD.C)
							.nv(args[ixSynTbl], target)
							.nv(args[ixSynode], usr.deviceId());
						
						if (ixStamp <= args.length && !isblank(args[ixStamp]))
							i.nv(args[ixStamp], now());
						i.ins(shSt.instancontxt(conn, usr));
						
						return null;
					};
				stx.addOnTableCommitted(target, op);
			}
			else cnt++;
		}

		protected void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			onInsert(stx, null, row, cols, usr);
		}

		protected void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {
			if (isblank(usr.deviceId()))
				throw new SemanticException(
						"Table %s's stampByNode semantics requires user provide device id. But it is empty (user-id = %s, delete).",
						target, usr.uid());

			IPostOptn op = stx.onTableCommittedHandler(target);
			ShStampByNode that = this;

			if (op == null) {
				that.cnt = 1;
				op = (st, sqls) -> {
					that.cnt = 1;
					Update u = shSt.update(args[ixLogTabl], usr)
						.nv(args[ixRecount], that.cnt)
						.nv(args[ixCrud], CRUD.C)
						.whereEq(args[ixSynTbl], target)
						.whereEq(args[ixSynode], usr.deviceId());
					
					if (ixStamp <= args.length && !isblank(args[ixStamp]))
						u.nv(args[ixStamp], now());
						
					u.u(shSt.instancontxt(conn, usr));
					return null;
				};
			}
			else cnt++;

			stx.addOnTableCommitted(target, op);
		}
	}


}
