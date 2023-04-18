package io.odysz.semantic;

import static io.odysz.common.LangExt.isblank;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xml.sax.SAXException;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.Statement.IPostOptn;
import io.odysz.transact.sql.parts.condition.Condit;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

public class DBSynmantics extends DASemantics {

	public void addHandler(smtype semantic, String tabl, String recId, String[] args)
			throws SemanticException, SQLException {

		if (smtype.synChange == semantic)
			// handlers.add(new ShStampByNode(basicTsx, tabl, recId, args));
			handlers.add(new ShSynChange(basicTsx, tabl, recId, args));
		else super.addHandler(semantic, tabl, recId, args);
	}

	public DBSynmantics(Transcxt basicTx, String tabl, String recId, boolean verbose) {
		super(basicTx, tabl, recId, verbose);
	}

	public static class ShSynChange extends SemanticHandler {

		ShSynChange(Transcxt trxt, String tabl, String pk, String[] args) throws SemanticException {
			super(trxt, smtype.synChange, tabl, pk, args);
		}

		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
		}

		void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			onInsert(stx, null, row, cols, usr);
		}

		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
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

		void onInsert(ISemantext stx, Insert insrt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
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
							u.nv(args[ixStamp], Funcall.now());
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
							i.nv(args[ixStamp], Funcall.now());
						i.ins(shSt.instancontxt(conn, usr));
						
						return null;
					};
				stx.addOnTableCommitted(target, op);
			}
			else cnt++;
		}

		void onUpdate(ISemantext stx, Update updt, ArrayList<Object[]> row, Map<String, Integer> cols, IUser usr)
				throws SemanticException {
			onInsert(stx, null, row, cols, usr);
		}

		void onDelete(ISemantext stx, Statement<? extends Statement<?>> stmt, Condit condt, IUser usr)
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
						u.nv(args[ixStamp], Funcall.now());
						
					u.u(shSt.instancontxt(conn, usr));
					return null;
				};
			}
			else cnt++;

			stx.addOnTableCommitted(target, op);
		}
	}
}
