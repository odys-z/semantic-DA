package io.odysz.semantic.syn;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.semantics.IUser;
import io.odysz.semantics.meta.TableMeta;
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

	public String uids;

	/** Non-public: doc' device id is managed by session. */
	protected String synode;
	public String synode() { return synode; }
	public SynEntity synode(String synode) {
		this.synode = synode;
		return this;
	}

	@AnsonField(ignoreTo=true)
	protected SyntityMeta entMeta;

	@AnsonField(ignoreTo=true)
	protected SynSubsMeta subMeta;

	@AnsonField(ignoreTo=true)
	protected SynChangeMeta chgMeta;

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	ISemantext semantxt;

	protected ArrayList<String[]> subs;

	protected String synoder;
	protected Nyquence nyquence;
	
	public SynEntity(AnResultset rs, SyntityMeta entity, SynChangeMeta change, SynSubsMeta subs) throws SQLException {
		this.entMeta = entity;
		this.subMeta = subs;
		this.chgMeta = change;

		format(rs);
	}

	public SynEntity(SyntityMeta entm) {
		this.entMeta = entm;
		this.subMeta = new SynSubsMeta();
		this.chgMeta = new SynChangeMeta();
	}

	public SynEntity(AnResultset rs, SyntityMeta meta) throws SQLException {
		this(rs, meta, new SynChangeMeta(), new SynSubsMeta());
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
	 * @return this
	 * @throws SQLException 
	 */
	public SynEntity format(AnResultset rs) throws SQLException {
		this.recId = rs.getString(entMeta.pk);
		this.synode =  rs.getString(chgMeta.synoder);
		return this;
	}

	/**
	 * Commit synchronizations.
	 * 
	 * <p>TODO the performance can be optimized with buffer writing.</p>
	 * @param conn
	 * @param trsb default transaction builder
	 * @param subs subscriptions may or may not override this values
	 * @param skips ignoring subscriptions
	 * @param robot 
	 * @return this
	 * @throws SQLException 
	 * @throws TransException 
	 */
	public SynEntity syncInto(String conn, DBSynsactBuilder trsb, AnResultset subs, Set<String> skips, IUser robot)
			throws TransException, SQLException {
		AnResultset ch = (AnResultset) trsb
				.select(entMeta.tbl, "ent")
				.je("ent", chgMeta.tbl, "ch", entMeta.pk, chgMeta.entfk)
				.cols(chgMeta.cols()).cols(subMeta.cols())
				.whereEq(chgMeta.synoder, synode)
				.whereEq(chgMeta.uids, uids)
				.rs(trsb.instancontxt(conn, robot))
				.rs(0);
		
		if (ch.getString(chgMeta.synoder).equals(synoder)) {
			// compare ch.n with s.nyq
			int nc = Nyquence.compareNyq(nyquence.n, ch.getLong(chgMeta.nyquence));
			if (nc > 0) {
				// write subs to conn.subm.tbl
				// DESIGN NOTES: There is no R/D/E in subscriptions, that's an attribute of Doc sharing relationship 
				trsb.delete(subMeta.tbl, robot)
					.whereEq(subMeta.org, ch.getString(entMeta.org()))
					.whereEq(subMeta.entbl, ch.getString(chgMeta.entbl))
					.whereEq(subMeta.uids, ch.getString(chgMeta.uids))
					.post(trsb
						.insert(subMeta.tbl, robot)
						.values(subMeta.insubVals(subs, skips))
					);
			}
			// else (nc <= 0) ignore incoming subscriptions
		}
		else {
			// conflict
		}

		return this;
	}
	public AnResultset subs() {
		// TODO Auto-generated method stub
		return null;
	}


}