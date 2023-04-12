package io.odysz.semantic;

import static io.odysz.common.LangExt.isNull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.meta.TableMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * A synchronizable entity managed by the package, also a server side
 * and jprotocol oriented data record, used for record synchronizing
 * in docsync.jserv. 
 * 
 * @author Ody
 */
public class SynEntity extends Anson {
	protected static String[] synpageCols;

	public String recId;
	public String recId() { return recId; }
	public SynEntity recId(String did) {
		recId = did;
		return this;
	}

	public String clientpath;
	public String fullpath() { return clientpath; }
	public String clientpath2;
	public String clientpath2() { return clientpath2; }

	/** Non-public: doc' device id is managed by session. */
	protected String synode;
	public String synode() { return synode; }
	public SynEntity synode(String synode) {
		this.synode = synode;
		return this;
	}

	@AnsonField(ignoreTo=true)
	protected SyntityMeta entMeta;

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	ISemantext semantxt;

	protected ArrayList<String[]> subs;

	protected String oper;
	protected String nyquence;
	
	public SynEntity() {}
	
	public SynEntity(AnResultset rs, SyntityMeta meta) throws SQLException {
		this.entMeta = meta;
		this.recId = rs.getString(meta.pk);
		
		this.clientpath =  rs.getString(meta.clientpath);
		this.clientpath2 =  rs.getString(meta.clientpath2);
		this.synode =  rs.getString(meta.synoder);
		
		// this.syncFlag = rs.getString(meta.syncflag);
	}

	public SynEntity clientpath(String p) {
		this.clientpath = p;
		return this;
	}

	/**
	 * @param meta TODO change to {@link SyntityMeta} after refactor
	 * @return this
	 */
	public SynEntity onPush(TableMeta meta) {
		return null;
	}

	public SynEntity check(String conn, DBSynsactBuilder tr0, ArrayList<String[]> subs) {
		this.subs = subs;
		return this;
	}

	/**
	 * Format entity synchronization task
	 * @param ents
	 * @return this
	 */
	public SynEntity format(AnResultset ents) {
		// TODO Auto-generated method stub
		return this;
	}

	/**
	 * Commit synchronizations
	 * @param conn
	 * @param tb default transaction builder
	 * @param subs
	 * @param skip 
	 * @param robot 
	 * @return this
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public SynEntity sync(String conn, DBSynsactBuilder tb, AnResultset subs, String skip, IUser robot) throws TransException, SQLException {
		AnResultset e = (AnResultset) tb.select(entMeta.tbl, "ent")
				.whereEq(entMeta.synoder, synode)
				.whereEq(entMeta.clientpath, clientpath)
				.whereEq(entMeta.clientpath2, clientpath2)
				.rs(tb.instancontxt(conn, robot))
				.rs(0);
		
		if (e.getString(entMeta.oper).equals(oper)) {
			// compare ch.n with s.nyq
			int nc = Nyquence.compare64(nyquence, e.getString(entMeta.nyquence));
			if (nc > 0) {
				// write subs to this DB
			}
			// else (nc <= 0) ignore incoming subscriptions
		}
		else {
			// conflict
		}

		return this;
	}


}