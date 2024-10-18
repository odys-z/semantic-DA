package io.odysz.semantic.syn.registry;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io_odysz.FilenameUtils;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.anson.x.AnsonException;
import io.odysz.common.EnvPath;
import io.odysz.semantic.meta.SynodeMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.x.SemanticException;
import io.odysz.transact.x.TransException;

public class Syntities extends Anson {
	static HashMap<String, Syntities> registries;

	SynodeMeta synodeMeta;
	
	static {
		registries = new HashMap<String, Syntities>();
	}
	
	@FunctionalInterface
	public interface MetaFactory {
		public SyntityMeta create(SyntityReg synt) throws TransException;
	}

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	HashMap<String, SyntityMeta> metas;

	String conn;
	ArrayList<SyntityReg> syntities;
	
	public Syntities() { 
		metas = new HashMap<String, SyntityMeta>();
	}

	public static Syntities load(String runtimepath, String json, MetaFactory mf)
			throws AnsonException, TransException, IOException {
		String p = FilenameUtils.concat(
				EnvPath.replaceEnv(runtimepath),
				EnvPath.replaceEnv(json));

		Syntities reg =  (Syntities) Anson.fromJson(new FileInputStream(p));

		if (reg.conn == null)
			throw new SemanticException("Syntitier registration without connection specified: %s", p);
			
		reg.synodeMeta = new SynodeMeta(reg.conn);
		reg.metas.put(reg.synodeMeta.tbl, reg.synodeMeta);

		for (SyntityReg synt : reg.syntities) {
			if (!registries.containsKey(reg.conn))
				registries.put(reg.conn, reg);

			reg.metas.put(synt.name, mf.create(synt));
		}
		return reg;
	}

	@SuppressWarnings("unchecked")
	public <T extends SyntityMeta> T meta(String tbl) {
		return metas == null ? null : (T) metas.get(tbl);
	}

	public static Syntities get(String conn) {
		return registries.get(conn);
	}

	public static SynodeMeta getSynodeMeta(String conn) {
		return registries.get(conn).synodeMeta;
	}
}
