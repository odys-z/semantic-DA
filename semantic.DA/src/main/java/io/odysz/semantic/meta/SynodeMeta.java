package io.odysz.semantic.meta;

import java.util.ArrayList;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.DASemantics.smtype;
import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

/**
 * <a href='./syn_node.sqlite.ddl'>syn_node.ddl</a>
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
	public final String optime;

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

		// mac     = "mac";
		nyquence= "nyq";
		nstamp  = "nstamp";
		org     = "org";
		domain  = "domain";
		remarks = "remarks";
		jserv   = "jserv";
		oper    = "oper";
		optime  = "optime";
		synoder = pk;

		ddlSqlite = loadSqlite(SyntityMeta.class, "syn_node.sqlite.ddl");

		autopk = DATranscxt.hasSemantics(conn, tbl, smtype.autoInc);
	}

	@Override
	public ArrayList<Object[]> updateEntNvs(SynChangeMeta chgm, String entid, AnResultset entities, AnResultset challenges) {
		return null;
	}
}
