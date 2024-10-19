package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.isblank;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.IUser;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.sql.Transcxt;
import io.odysz.transact.x.TransException;

public class DBSynTransBuilder extends DATranscxt {

	public static class SynmanticsMap extends SemanticsMap {
		String synode;
	
		public SynmanticsMap(String synode, String conn) {
			super(conn);
			this.synode = synode;
		}
	
		@Override
		public DASemantics createSemantics(Transcxt trb, String tabl, String pk, boolean debug) {
			return new DBSynmantics(trb, synode, tabl, pk, debug);
		}
	}
	
	String synconn() { return basictx.connId(); }

	static HashMap<String, HashMap<String, SyntityMeta>> entityRegists;
	public SyntityMeta getSyntityMeta(String tbl) {
		return entityRegists != null
			&& entityRegists.containsKey(synconn())
				? entityRegists.get(synconn()).get(tbl)
				: null;
	} 

	/**
	 * Shouldn't be called as docm is loaded from semantic-syn.xml, except for tests.
	 * @param conn
	 * @param m
	 * @throws SemanticException
	 * @throws TransException
	 * @throws SQLException
	 */
	@Deprecated
	public static void registerEntity(String conn, SyntityMeta m)
			throws SemanticException, TransException, SQLException {
		if (entityRegists == null)
			entityRegists = new HashMap<String, HashMap<String, SyntityMeta>>();
		if (!entityRegists.containsKey(conn))
			entityRegists.put(conn, new HashMap<String, SyntityMeta>());

		entityRegists.get(conn).put(m.tbl, (SyntityMeta) m.clone(Connects.getMeta(conn, m.tbl)));
	}
	
	public static SyntityMeta getEntityMeta(String synconn, String entbl) throws SemanticException {
		if (entityRegists == null || !entityRegists.containsKey(synconn)
			|| !entityRegists.get(synconn).containsKey(entbl))
			throw new SemanticException("Register %s first.", entbl);
			
		return entityRegists.get(synconn).get(entbl);
	}
	
	final boolean debug;

	public final SynodeMeta synm;
	protected final PeersMeta pnvm;
	protected final SynSubsMeta subm;
	protected final SynChangeMeta chgm;
	protected final SynchangeBuffMeta exbm;
	protected final String perdomain;

	final SynodeMode synmode;

	private final boolean force_clean_subs;

	/* */
	private Nyquence stamp;
	public long stamp() { return stamp.n; }
	public Nyquence stampN() { return stamp; }
	
	public DBSynTransBuilder (String domain, String conn, String mynid,
				SynodeMode mode)
				throws SemanticException, SQLException, SAXException, IOException, Exception {

		super ( new DBSyntext(conn, mynid,
			    	initConfigs(conn, loadSemantics(conn), (c) -> new DBSynTransBuilder.SynmanticsMap(mynid, c)),
			    	(IUser) new SyncRobot(mynid, mynid, "rob@" + mynid, mynid),
			    	runtimepath));
		
		debug    = Connects.getDebug(conn);
		perdomain= domain;
		synmode  = mode;

		this.chgm = new SynChangeMeta(conn);
		this.chgm.replace();
		this.subm = new SynSubsMeta(chgm, conn);
		this.subm.replace();
		this.synm = (SynodeMeta) new SynodeMeta(conn).autopk(false);
		this.synm.replace();
		this.exbm = new SynchangeBuffMeta(chgm, conn);
		this.exbm.replace();
		this.pnvm = new PeersMeta(conn);
		this.pnvm.replace();
		
		// seq = 0;
		force_clean_subs = true;

		if (mode != SynodeMode.nonsyn) {
			if (DAHelper.count(this, conn, synm.tbl,
						synm.synoder, mynid, synm.domain, perdomain) <= 0) {
				if (debug) Utils
					.warnT(new Object() {},
						  "\nThis syntable builder is being built for node %s which doesn't exists in domain %s." +
						  "\nThis instence can only be useful if is used to initialize the domain for the node",
						  mynid, perdomain);
			}
			else
				stamp = DAHelper.getNyquence(this, conn, synm, synm.nyquence,
						synm.synoder, mynid, synm.domain, perdomain);

			registerEntity(conn, synm);
		}
		else if (isblank(perdomain))
			Utils.warn("[%s] Synchrnizer builder (id %s) created without domain specified",
				this.getClass().getName(), mynid);

		if (debug && force_clean_subs) Utils
			.logT(new Object() {}, "Transaction builder created with forcing cleaning stale subscriptions.");

	}

	public String domain() {
		// TODO Auto-generated method stub
		return null;
	}

	public DBSynTransBuilder loadNstamp() {
		// TODO Auto-generated method stub
		return null;
	}

}
