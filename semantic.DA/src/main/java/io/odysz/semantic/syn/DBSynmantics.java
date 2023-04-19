package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.isblank;
import static io.odysz.transact.sql.parts.condition.Funcall.*;

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
import io.odysz.semantic.DASemantics.SemanticHandler;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.Statement.IPostOptn;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.x.TransException;

public class DBSynmantics extends DASemantics {

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
		final SynChangeMeta chm;
		final SynodeMeta sym;
		final SynSubsMeta sbm;

		Set<String> pkcols = new HashSet<String>();
		Set<String> subpks = new HashSet<String>();

		protected DBSynsactBuilder syb;

		/** Ultra high frequency mode, the data frequency need to be reduced with some cost */
		protected final boolean UHF;

		ShSynChange(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.synChange, tabl, pk, args);
			if (trxt instanceof DBSynsactBuilder)
				// builder
				syb = (DBSynsactBuilder) trxt;
			else
				throw new SemanticException("ShSynChange (xml/smtype=s-c) requires instance of DBSynsactBuilder as default builder.");

			sym = new SynodeMeta();
			chm = new SynChangeMeta();
			sbm = new SynSubsMeta();
			UHF = true;
		}

		protected void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {

			setFlag(CRUD.C, stx, insrt, row, cols, usr)
				.logChange(stx, insrt, row, cols, usr);
		}

		private ShSynChange setFlag(String crud, ISemantext stx, Insert insrt, ArrayList<Object[]> row,
				Map<String, Integer> cols, IUser usr) {
			// phm.tbl.flag = crud
			return this;
		}

		protected void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			updt = (Update) logChange(stx, updt, row, cols, usr);
		}

		private <T extends Statement<?>> T logChange(ISemantext stx, T stmt,
				ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr) throws SemanticException {

			// args: pk,n pk2 ...,[col-value-on-del ...]
			// args: pid,synoder clientpath,exif,uri "",clientpath
			Delete del = null;
			Insert insChg = syb.insert(chm.tbl, usr);

			String pid = (String) stx.resulvedVal(args[1], args[2]);
			String synoder = (String) row.get(cols.get(chm.synoder))[1];
			try {
				insChg.select(syb
						.select(sym.tbl, "s")
						.col(pid, chm.entfk)
						.col(CRUD.C, chm.crud)
						.col(UHF ? add(sym.nyquence, sym.inc) : add(sym.nyquence, 1), chm.nyquence)
						.whereEq(sym.entbl, target)
						.whereEq(sym.synoder, synoder));

				// and insert subscriptions, or merge syn_change & syn_subscribe?
				Insert insubs = syb.insert(sbm.tbl)
						.select(syb
							.select(sym.tbl, args)
							.col(sym.synode)
							.whereEq(sbm.org, usr.orgId())
							.whereEq(sbm.entbl, target)
							.whereEq(sbm.entId, pid));

				Delete delSb = syb.delete(sbm.tbl);
				int c = formatSubsWhere(delSb, subpks, cols, usr);
				if (c > 0)
					insChg.post(delSb.post(insubs));
				else insChg.post(insubs);
			} catch (TransException e) {
				e.printStackTrace();
				throw new SemanticException(e.getMessage());
			}

			int cnt = formatChangeWhere(del, pkcols, cols, row, usr);

			if (cnt > 0 && cnt < pkcols.size())
				// TODO doc
				throw new SemanticException("To update a synchronized table, all cols' values for identifying a global record are needed to generate syn-change log.\n"
						+ "Columns in field values recieved are: %s\n"
						+ "For how to configue See javadoc: %s",
						cols.keySet().toString(), apidoc);
			else if (cnt > 0)
				stmt.post(del.post(insChg));
			else stmt.post(insChg);
			
			if (this.UHF)
				stmt.post(clearInc(synoder, usr));
			else stmt.post(inc(synoder, usr));

			return stmt;
		}

		private int formatSubsWhere(Delete delSb, Set<String> subpks2,
				Map<String, Integer> cols, IUser usr) {
			return 0;
		}

		private int formatChangeWhere(Delete del, Set<String> pkcols,
				Map<String, Integer> cols, ArrayList<Object[]> row, IUser usr) {
			int cnt = 0;
			for (String c : cols.keySet()) {
				if (pkcols.contains(c)) {
					cnt++;
					if (del == null)
						del = syb.delete(chm.tbl, usr);
					del.whereEq(c, row.get(cols.get(c))[1]);
				}
			}
			return cnt;
		}

		private Statement<?> inc(String synid, IUser usr) {
			return syb.update(sym.tbl, usr)
				.nv(sym.nyquence, add(sym.nyquence, 1))
				.whereEq(sym.synoder, synid)
				.whereEq(sym.entbl, target);
		}

		private Update clearInc(String synid, IUser usr) {
			return syb.update(sym.tbl, usr)
				.nv(sym.nyquence, add(sym.nyquence, sym.inc))
				.nv(sym.inc, 0)
				.whereEq(sym.synoder, synid)
				.whereEq(sym.entbl, target);
		}

		protected void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
				throws SemanticException {

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
