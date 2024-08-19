package io.odysz.semantic.syn;

import java.sql.SQLException;

import io.odysz.semantic.DASemantext;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.syn.DBSyntableBuilder.SynmanticsMap;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Transcxt;

/**
 * An experimental for handling semantics for DB replication.
 * Configuration is runtime-root/syntext.xml.
 *
 * @author odys-z@github.com
 */
public class DBSyntext extends DASemantext implements ISyncontext {

	public String synode;
	private DATranscxt creator;

	protected DBSyntext(String connId, SynmanticsMap metas, IUser usr, String rtPath)
			throws SemanticException, SQLException {
		super(connId, metas, usr, rtPath);
		// this.xp = xp;
	}
	
	public IUser usr() { return super.usr; }

	@Override
	public ISemantext clone(IUser usr) {
		try {
			return new DBSyntext(connId, (SynmanticsMap) super.semants, usr, basePath);
		} catch (SQLException | SemanticException e) {
			e.printStackTrace();
			return null; // meta is null? how could it be?
		}
	}
	
	@Override
	protected ISemantext clone(DASemantext srctx, IUser usr) {
		try {
			DASemantext newInst = new DBSyntext(connId, (SynmanticsMap) semants, usr, basePath);
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
		creator = (DATranscxt) semantext;
		return this;
	}

	// final ExessionPersist xp;
	// @Override
	// public ExessionPersist xpersist() { return xp; }
}
