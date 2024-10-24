package io.odysz.semantic.syn;

import java.sql.SQLException;

import io.odysz.semantic.DASemantext;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.syn.DBSynTransBuilder.SynmanticsMap;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Transcxt;

public class DBSynmantext extends DASemantext implements ISyncontext { 

	public final String synode;
	private DBSyntableBuilder creator;

	protected DBSynmantext(String connId, String synodeId, SynmanticsMap metas, IUser usr, String rtPath)
			throws SemanticException, SQLException {
		super(connId, metas, usr, rtPath);
		this.synode = synodeId;
	}
	
	public IUser usr() { return super.usr; }

	@Override
	public ISemantext clone(IUser usr) {
		try {
			return new DBSynmantext(connId, synode, (SynmanticsMap) super.semants, usr, basePath);

		} catch (SQLException | SemanticException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}
	
	@Override
	protected ISemantext clone(DASemantext srctx, IUser usr) {
		try {
			DASemantext newInst = new DBSynmantext(connId, synode, (SynmanticsMap) semants, usr, basePath);

			return newInst;
		} catch (SemanticException | SQLException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public DATranscxt synbuilder() {
		return creator;
	}

	@Override
	public <B extends Transcxt> ISemantext creator(B semantext) {
		creator = (DBSyntableBuilder) semantext;
		return this;
	}

	@Override
	public SyndomContext syndomContext() {
		return creator.syndomx;
	}
}
