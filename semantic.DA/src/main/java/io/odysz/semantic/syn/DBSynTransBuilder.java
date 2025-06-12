package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.isblank;
import static io.odysz.common.LangExt.notNull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

import org.xml.sax.SAXException;

import io.odysz.common.Utils;
import io.odysz.module.xtable.XMLTable;
import io.odysz.semantic.DASemantics;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantic.DA.Connects;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantic.syn.DBSynmantics.ShSynChange;
import io.odysz.semantic.syn.registry.Syntities;
import io.odysz.semantic.util.DAHelper;
import io.odysz.semantics.ISemantext;
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

		public static SemanticsMap clone(String synode, SemanticsMap sm) {
			SynmanticsMap m = new SynmanticsMap(synode, sm.conn);
			for (DASemantics s : sm.ss.values())
				m.ss.put(s.tabl, s.clone());
			
			return m;
		}

		public void addSynchangeHandlers(HashMap<String, SyntityMeta> metas, Transcxt trb) throws TransException {
			if (metas != null)
			for (SyntityMeta m : metas.values()) {
				if (!ss.containsKey(m.tbl))
					ss.put(m.tbl, new DASemantics(trb, m.tbl, m.pk));
			
				ss.get(m.tbl).addHandler(new ShSynChange(trb, synode, m));
				
			}
		}
	}
	
	String synconn() { return basictx.connId(); }

	final boolean debug;

	public final String perdomain;
	public final SynodeMeta synm;

	protected final PeersMeta pnvm;
	protected final SynSubsMeta subm;
	protected final SynChangeMeta chgm;
	protected final SynchangeBuffMeta exbm;

	final SynodeMode synmode;

	final String synode;
	public String synode() { return synode; }

	private final boolean force_clean_subs;

	private DBSyntableBuilder changelogBuilder;

	public DBSynTransBuilder (SyndomContext x, DBSyntableBuilder logger)
			throws SemanticException, SQLException, SAXException, IOException, Exception {

		super(x.synconn);
		
		notNull(x);
		notNull(logger);
		
		debug    = Connects.getDebug(x.synconn);
		perdomain= x.domain;
		synmode  = x.mode;
		synode   = x.synode;

		this.changelogBuilder = logger;
		
		this.chgm = new SynChangeMeta(x.synconn).replace();
		this.subm = new SynSubsMeta(chgm, x.synconn).replace();
		this.synm = (SynodeMeta) new SynodeMeta(x.synconn).autopk(false).replace();
		this.exbm = new SynchangeBuffMeta(chgm, x.synconn).replace();
		this.pnvm = new PeersMeta(x.synconn).replace();
		
		// seq = 0;
		force_clean_subs = true;

		if (x.mode != SynodeMode.nonsyn) {
			if (DAHelper.count(new DATranscxt(x.synconn), x.synconn, synm.tbl,
						synm.synoder, x.synode, synm.domain, perdomain) <= 0) {
				if (debug) Utils
					.warnT(new Object() {},
						  "\nThis syntable builder is being built for node %s which doesn't exists in domain %s." +
						  "\nThis instence can only be useful if is used to initialize the domain for the node",
						  x.synode, perdomain);
			}
		}
		else if (isblank(perdomain))
			Utils.warn("[%s] Synchrnizer builder (id %s) created without domain specified",
				this.getClass().getName(), x.synode);

		if (debug && force_clean_subs) Utils
			.logT(new Object() {}, "Transaction builder created with forcing cleaning stale subscriptions.");

	}

	/** 
	 * A helper version of {@link #DBSynTransBuilder(SyndomContext, DBSyntableBuilder)},
	 * ignoring loading {@link SynmanticsMap}.
	 * 
	 * @param domx
	 * @throws Exception 
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws SQLException 
	 * @throws SemanticException 
	 */
	public DBSynTransBuilder(SyndomContext domx)
			throws SemanticException, SQLException, SAXException, IOException, Exception {
		this(domx, new DBSyntableBuilder(domx));
		notNull(synmanticMaps, "DBSynTransBuilder(SyndomContext domx) must be called only after synSemantics(...) has been called.");
	}

	protected static HashMap<String, SemanticsMap> synmanticMaps;

	/**
	 * Registering syntities, adding semantics handler, {@link ShSynChange},
	 * to the Semantics map.
	 *  
	 * Equivalent to {@link DATranscxt#initConfigs(String, XMLTable, SmapFactory)}.
	 * 
	 * @param <M>
	 * @param <S>
	 * @param trb
	 * @param synode
	 * @param syntities 
	 * @return semantics map of connection {@code conn}
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static <M extends SemanticsMap, S extends DASemantics> M synSemantics(
			DATranscxt trb, String conn, String synode, Syntities syntities) throws Exception {

		if (synmanticMaps == null)
			synmanticMaps = new HashMap<String, SemanticsMap>(); 
		
		DATranscxt.initConfigs(conn,// loadSemanticsXml(conn),
						(c) -> new SemanticsMap(c));

		if (!smtMaps.containsKey(conn))
			throw new SemanticException("Basic semantics map for connection %s is empty.", conn);
		else {
			SemanticsMap m = SynmanticsMap.clone(synode, smtMaps.get(conn));
			synmanticMaps.put(conn, m);
			
			((SynmanticsMap)m).addSynchangeHandlers(syntities.metas, trb);

			return (M) synmanticMaps.get(conn);
		}
	}

	public ISemantext instancontxt(String connId, IUser usr) throws TransException {
		try {
			return new DBSynmantext(connId, synode,
					(SynmanticsMap) synmanticMaps.get(connId), usr, runtimepath)
					.creator(changelogBuilder);
		} catch (Exception e) {
			e.printStackTrace();
			throw new TransException(e.getMessage());
		}
	}
	
	public static SyntityMeta getEntityMeta(String synconn, String tbl)
			throws TransException, SQLException {
		return Syntities.get(synconn).meta(tbl).replace();
	}

	public ISemantext instancontxt() throws TransException {
		return instancontxt(basictx.connId(), dummy);
	}
}
