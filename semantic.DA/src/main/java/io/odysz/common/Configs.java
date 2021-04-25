package io.odysz.common;

import java.util.HashMap;

import org.apache.commons.io_odysz.FilenameUtils;
import org.xml.sax.SAXException;

import io.odysz.module.xtable.ILogger;
import io.odysz.module.xtable.IXMLStruct;
import io.odysz.module.xtable.Log4jWrapper;
import io.odysz.module.xtable.XMLDataFactory;
import io.odysz.module.xtable.XMLTable;

/**Load path/config.xml, use {@link #getCfg(String)} to access the configured value (String).<br>
 * A servlet constext must been registed by LeisureFactory before Messages is inited */
public class Configs {
	protected static ILogger log;
	protected static final String tag = "CFG";
	protected static String cfgFile = "config.xml";
	protected static final String deftId = "default";
	protected static HashMap<String, HashMap<String, String>> cfgs;

	static {
		log = new Log4jWrapper("Configs");

		cfgs = new HashMap<String, HashMap<String, String>>(3);
	}

	/**For redirect path of config.xml
	 * @param xmlDir
	 */
	public static void init(String xmlDir) {
		cfgFile = FilenameUtils.concat(xmlDir, cfgFile);
		load(cfgs, deftId);
	}

	protected static void load(HashMap<String, HashMap<String, String>> cfgs, String tid) {
		// String messageFile = null;
		// String fullpath = HelperFactory.getRealPath(cfgFile);
		Utils.logi("config file : %s", cfgFile);

		XMLTable deft = XMLDataFactory.getTable("config.xml", log, tid, cfgFile, new IXMLStruct(){
			@Override public String rootTag() { return "configs"; }
			@Override public String tableTag() { return "t"; }
			@Override public String recordTag() { return "c"; }
		});

		if (deft != null) {
			try {
				HashMap<String, String> defaults = new HashMap<String, String>(deft.getRowCount());
				deft.beforeFirst();
				while (deft.next()) {
					String k = deft.getString("k");
					if (defaults.containsKey(k))
						log.e(tag, "duplicate key found: " + k);
					defaults.put(k, deft.getString("v"));
				}
				cfgs.put(tid, defaults);
			} catch (SAXException e) {
				e.printStackTrace();
			}
		}
	}

	public static String getCfg(String key) {
		return cfgs.get(deftId).get(key);
	}

	public static String getCfg(String tid, String k) {
		if (!cfgs.containsKey(tid)) load(cfgs, tid);
		return cfgs.get(tid).get(k);
	}

	public static boolean getBoolean(String key) {
		String isTrue = Configs.getCfg(key);
		if (isTrue == null) return false;
		isTrue = isTrue.trim().toLowerCase();
		return "true".equals(isTrue) || "1".equals(isTrue) || "y".equals(isTrue) || "yes".equals(isTrue);
	}

	public static int getInt(String key, int deflt) {
		String str = Configs.getCfg(key);
		if (str == null) return deflt;
		str = str.trim().toLowerCase();

		try {
			return Integer.valueOf(str);
		} catch (Exception ex) {
			System.err.println(String.format("Config %s = %s is not an integer (%s)", key, str, ex.getMessage()));
			return deflt;
		}
	}

	public static boolean hasCfg(String key) {
		return hasCfg(deftId, key);
	}

	public static boolean hasCfg(String tid, String key) {
		return getCfg(tid, key) != null;
	}
}
