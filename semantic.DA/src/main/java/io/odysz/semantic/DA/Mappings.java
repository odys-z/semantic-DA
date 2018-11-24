package io.odysz.semantic.DA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.xml.sax.SAXException;

import io.ic.frame.DA.cp.CpDriver;
import io.ic.frame.xtable.IXMLStruct;
import io.ic.frame.xtable.XMLTable;

/**Helper class for managing mapping file data structure.
 * A common module used by both {@link CpDriver} and {@link com.infochange.frame.DA.drvmnger.DmDriver}
 * @author ody
 *
 */
public class Mappings {
	/**xml file struct for db name mapping configuration file, e.g. dc.xml */
	public static final IXMLStruct mapXStruct = new IXMLStruct() {
				@Override public String rootTag() { return "map"; }
				@Override public String tableTag() { return "t"; }
				@Override public String recordTag() { return "r"; }};

//	public enum DriverType {deflt(0), mysql(0), ms2k(1), oracle(2), sqlite(3), postGIS(4);
//		private final int value;
//    	private DriverType(int value) { this.value = value; }
//    	public int getValue() { return value; }
//	};
//
	/**Load map file
	 * @return
	 */
	public static LinkedHashMap<String, XMLTable> loadMapfile(String path) {
		return null;
	}

	/**Convert table's mapping configuration to
	 * map[table-id(logic/bump name), [upper-case-col, bump-case-col]]
	 * @param xmappings
	 * @return
	 * @throws SAXException
	 */
	public static HashMap<String, HashMap<String, String>> convertMap(LinkedHashMap<String, XMLTable> xmappings) throws SAXException {
		if (xmappings == null) return null;
		HashMap<String, HashMap<String, String>> maps = new HashMap<String, HashMap<String, String>>(xmappings.size());
		for (String tid : xmappings.keySet()) {
			if ("tabls".equals(tid)) continue; // main table
			XMLTable xt = xmappings.get(tid);
			HashMap<String, String> map = new HashMap<String, String>(xt.getRowCount());
			xt.beforeFirst();
			while (xt.next()) {
				map.put(xt.getString("f"), xt.getString("b"));
			}
			maps.put(tid, map);
		}
		// replace uppercase key with bump case key (key is logical)
		XMLTable xmain = xmappings.get("tabls");
		xmain.beforeFirst();
		while (xmain.next()) {
			String dbtabl = xmain.getString("u");
			String logicTabl = xmain.getString("b");
			HashMap<String, String> m = maps.remove(dbtabl);
			maps.put(logicTabl, m);
		}
		return maps;
	}
	
	/**Get mapping list for specified tables.
	 * @param mappings mappings defined in mapping file.
	 * @param btid only these tables' mapping will be converted.
	 * @return list[map[upper-case, bump-case]]
	 * @throws SAXException 
	 */
	public static ArrayList<HashMap<String, String>> getMappings4Tabl(
			HashMap<String,HashMap<String,String>> mappings, String... btid) throws SAXException {
		if (btid == null || mappings == null) return null;
		ArrayList<HashMap<String, String>> maplist = new ArrayList<HashMap<String, String>>(btid.length);
		for (String tabl : btid) {
			maplist.add(mappings.get(tabl));
		}
		return maplist;
	}
}
