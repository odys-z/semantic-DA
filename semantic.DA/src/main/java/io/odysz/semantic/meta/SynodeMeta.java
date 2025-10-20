package io.odysz.semantic.meta;

import static io.odysz.common.LangExt._0;
import static io.odysz.common.LangExt.isNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import io.odysz.module.rs.AnResultset;
import io.odysz.module.rs.AnResultset.ObjFilter;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
 * 
 * Synodes in a domain won't care any data across markets, and the table has no market id.
 * But Cynodes in central service does care.
 * 
 * @author odys-z@github.com
 */
public class SynodeMeta extends SyntityMeta {

	public final String org;

	public final String domain;

	public final String remarks;

	/** Nyquence for synchronizing */
	public final String nyquence;

	/** Nyquence for stamping change logs */
	public final String nstamp;

	public final String synoder;

	public final String jserv;

	public final String oper;
	public final String jserv_utc;

	/**
	 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
	 * 
	 * Using 'mac' as device id field.
	 * 
	 * @param conn
	 * @throws SemanticException 
	 */
	public SynodeMeta(String conn) throws TransException {
		super("syn_node", "synid", "mac", conn);

		nyquence = "nyq";
		nstamp   = "nstamp";
		org      = "org";
		domain   = "domain";
		remarks  = "remarks";
		jserv    = "jserv";
		oper     = "oper";
		jserv_utc= "optime";
		synoder  = pk;

		ddlSqlite = loadSqlite(SyntityMeta.class, "syn_node.sqlite.ddl");

		autopk = DATranscxt.hasSemantics(conn, tbl, smtype.autoInc);
	}

	@Override
	public ArrayList<Object[]> updateEntNvs(SynChangeMeta chgm, String entid, AnResultset entities, AnResultset challenges) {
		return null;
	}

	/**
	 * Load valid jservs, by ignoring invalid urls and path roots.
	 * @return {node-id: [jserv, timestamp]}
	 * @since 0.7.6
	 */
	public HashMap<String, String[]> loadJservs(DATranscxt tb, String orgId, String domain, ObjFilter... filter) throws SQLException, TransException {
		return ((AnResultset) tb.select(tbl)
		  .cols(jserv, synoder, jserv_utc, oper)
		  .whereEq(this.org, orgId)
		  .whereEq(this.domain, domain)
		  .rs(tb.instancontxt(conn(), DATranscxt.dummyUser()))
		  .rs(0))
		  .map(synoder,
			  (rs) -> new String[] {rs.getString(jserv), rs.getString(jserv_utc), rs.getString(oper)},
			  (rs) -> isNull(filter) ? true : _0(filter).filter(rs));
	}
}
