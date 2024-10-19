package io.odysz.semantic.syn.registry;

import static io.odysz.common.LangExt.isblank;

import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io_odysz.FilenameUtils;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.common.EnvPath;
import io.odysz.common.Utils;
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
	public
	HashMap<String, SyntityMeta> metas;

	String conn;
	ArrayList<SyntityReg> syntities;
	
	public boolean debug;
	
	public Syntities() { 
		metas = new HashMap<String, SyntityMeta>();
	}

	public static Syntities load(String runtimepath, String json, MetaFactory mf) throws Exception {
		String p = FilenameUtils.concat(
				EnvPath.replaceEnv(runtimepath),
				EnvPath.replaceEnv(json));
		
		Utils.logi("Loading syntities registry: %s", p);

		Syntities reg =  (Syntities) Anson.fromJson(new FileInputStream(p));

		if (reg.conn == null)
			throw new SemanticException("Syntitier registration without connection specified: %s", p);
			
		reg.synodeMeta = new SynodeMeta(reg.conn);
		reg.metas.put(reg.synodeMeta.tbl, reg.synodeMeta);

		for (SyntityReg synt : reg.syntities) {
			if (!registries.containsKey(reg.conn))
				registries.put(reg.conn, reg);

			reg.metas.put(synt.table, !isblank(synt.meta)
					? instance4name(reg.conn, synt)
					: mf == null
					? null
					: mf.create(synt));
		}
		return reg;
	}

	private static SyntityMeta instance4name(String conn, SyntityReg synt) throws Exception {

		@SuppressWarnings("unchecked")
		Class<SyntityMeta> cls = (Class<SyntityMeta>) Class.forName(synt.meta);
		Constructor<SyntityMeta> constructor = null;

		constructor = cls.getConstructor(String.class);

		return (SyntityMeta) constructor.newInstance(conn).replace();
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
