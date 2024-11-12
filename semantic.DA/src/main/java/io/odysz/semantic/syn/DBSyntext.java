//package io.odysz.semantic.syn;
//
//import static io.odysz.semantic.syn.DBSynTransBuilder.SynmanticsMap;
//
//import java.sql.SQLException;
//
//import io.odysz.semantic.DASemantext;
//import io.odysz.semantics.ISemantext;
//import io.odysz.semantics.IUser;
//import io.odysz.semantics.x.SemanticException;
//
///**
// * Used by {@link DBSyntableBuilder}.
// * 
// * @author odys-z@github.com
// */
//public class DBSyntext extends DASemantext {
//
//	public final String synode;
//
//	protected DBSyntext(String connId, String synodeId, SynmanticsMap metas, IUser usr, String rtPath)
//			throws SemanticException, SQLException {
//		super(connId, metas, usr, rtPath);
//		this.synode = synodeId;
//	}
//
//	
//	public IUser usr() { return super.usr; }
//
//	@Override
//	public ISemantext clone(IUser usr) {
//		try {
//			return new DBSyntext(connId, synode, (DBSynTransBuilder.SynmanticsMap) super.semants, usr, basePath);
//		} catch (SQLException | SemanticException e) {
//			e.printStackTrace();
//			return null; // meta is null? how could it be?
//		}
//	}
//	
//	@Override
//	protected ISemantext clone(DASemantext srctx, IUser usr) {
//		try {
//			DASemantext newInst = new DBSyntext(connId, synode, (DBSynTransBuilder.SynmanticsMap) semants, usr, basePath);
//			return newInst;
//		} catch (SemanticException | SQLException e) {
//			e.printStackTrace();
//			return null; // meta is null? how could it be?
//		}
//	}
//}
