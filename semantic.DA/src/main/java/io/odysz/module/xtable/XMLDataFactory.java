package io.odysz.module.xtable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Factory of flat xml data.</br>
 * Is this not necessary? Or a generic flat xml data factory is needed?
 * @author Odys
 */
public class XMLDataFactory {
	/** Take as data source ID. Set here for the future expansion. */
	private static String XMLSrcDefault = "com.infochage.frame.xtable.default";
	
	protected static HashMap<String, XMLFlatData> xmldata = new HashMap<String, XMLFlatData>();

	protected static XMLFlatData getXMLData (String dataName, InputStream istream, IXMLStruct xmlStruct, boolean forceReload) {
		if (xmldata.containsKey(dataName) && !forceReload)
			return xmldata.get(dataName);
		try { xmldata.put(dataName, new XMLFlatData(istream, xmlStruct, getLogger())); } 
		catch (Exception e) { e.printStackTrace(); }
		return xmldata.get(dataName);
	}

	/**
	 * Construct a new table.<br/>
	 * Copy data structure from 'fromTableID', copy data from 'fromList'
	 * @param sourceID
	 * @param androidLogger
	 * @param newTableID
	 * @param fromStructureOfTableID
	 * @param fromList
	 * @param targetFullpath
	 * @param xmlStruct
	 * @return new xml table
	 */
	public static XMLTable buildTable(String sourceID, ILogger androidLogger, String newTableID, String fromStructureOfTableID,
			ArrayList<String[]> fromList, String targetFullpath, IXMLStruct xmlStruct) {
		logger = androidLogger;
		try {
			FileInputStream istream = new FileInputStream(targetFullpath);
			IXMLData d = XMLDataFactory.getXMLData(sourceID, istream, xmlStruct, false);
			XMLTable t = d.getTable(fromStructureOfTableID);
			return new XMLTable(newTableID, t.getColumns(), t.getPKs(), fromList);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/** Usage ex.:<br/>
		InputStream istr = res.getAssets().open(configName + ".xml");<br/>
	  	IXMLStruct xmlStruct = new IXMLStruct() {<br/>
			{@literal @}Override public String rootTag() { return "xtables"; }<br/>
			{@literal @}Override public String tableTag() { return "table"; }<br/>
			{@literal @}Override public String recordTag() { return "record"; }<br/>
		};<br/>
		ILogger logger = new Logger();<br/>
		skintable = XMLDataFactory.getTable(logger, configTablename, istr, xmlStruct);<br/>
	 * @param logger
	 * @param tableID
	 * @param fromFullpath
	 * @param xmlStruct
	 * @return target table
	 */
	public static XMLTable getTable(ILogger logger, String tableID, String fromFullpath, IXMLStruct xmlStruct) {
		return getTable(XMLSrcDefault, logger, tableID, fromFullpath, xmlStruct);
	}
	
	public static XMLTable getTable(String sourceID, ILogger androidLogger, String tableID, String fromFullpath, IXMLStruct xmlStruct) {
		logger = androidLogger;
		try {
			FileInputStream istream = new FileInputStream(fromFullpath);
			return getTable(sourceID, androidLogger, tableID, istream, xmlStruct);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/** @see XMLDataFactory#getTable(ILogger, String, String, IXMLStruct) 
	 * @param androidLogger
	 * @param tableID
	 * @param istream
	 * @param xmlStruct
	 * @return target table
	 */
	public static XMLTable getTable(ILogger androidLogger, String tableID, InputStream istream, IXMLStruct xmlStruct) {
		return getTable(XMLSrcDefault, androidLogger, tableID, istream, xmlStruct);
	}
	
	/**Get a table, don't reload. This is for caller avoiding reopen file repeatedly.
	 * @param sourceID
	 * @param androidLogger
	 * @param tableID
	 * @param xmlStruct
	 * @return xml table
	 */
	public static XMLTable getTableReusing(String sourceID, ILogger androidLogger, String tableID, IXMLStruct xmlStruct) {
		logger = androidLogger;
		return getXMLData(sourceID, null, xmlStruct, false).getTable(tableID);
	}
	
	public static XMLTable getTable(String sourceID, ILogger androidLogger, String tableID, InputStream istream, IXMLStruct xmlStruct) {
//		logger = androidLogger;
//		return getXMLData(sourceID, istream, xmlStruct).getTable(tableID);
		return getTable(sourceID, androidLogger, tableID, istream, xmlStruct, false);
	}
	
	public static XMLTable getTable(String sourceID, ILogger androidLogger, String tableID, InputStream istream, IXMLStruct xmlStruct, boolean reload) {
		logger = androidLogger;
		return getXMLData(sourceID, istream, xmlStruct, reload).getTable(tableID);
	}
	
	public static XMLTable getTable(String sourceID, ILogger androidLogger, String tableID, String fromFullpath, IXMLStruct xmlStruct, boolean reload) {
		logger = androidLogger;
		try {
			FileInputStream istream = new FileInputStream(fromFullpath);
			return getTable(sourceID, androidLogger, tableID, istream, xmlStruct, reload);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void writeTables(IXMLStruct xmlStrct, String fullpath, XMLTable[] tables) throws Exception {
		FileOutputStream fo = null;
		File f = new File(fullpath);
		if (f.exists()) f.delete();
		f.createNewFile();
		fo = new FileOutputStream(f);
		// <rootTag>
		fo.write(("<" + xmlStrct.rootTag() + ">\r").getBytes());
		
		for (XMLTable t : tables) {
			try {
				// <table id="tid" columns="c1,c2,c3" pk="c1">
				String cols = "";
				for (String c : t.getColumns().keySet()) {
					cols += c + ",";
				}
				cols = cols.trim();
				if (cols.length() > 1) {
					cols = cols.substring(0, cols.length() - 1);
					cols = cols.trim();
				}
				
				String pks = "";
				for (String pk : t.getPKs().keySet()) {
					pks += pk + ",";
				}
				pks = pks.trim();
				if (pks.length() > 1) {
					pks = pks.substring(0, pks.length() - 1);
					pks = pks.trim();
				}
				
				fo.write(String.format("\t<%1$s id=\"%2$s\" columns=\"%3$s\" pk=\"%4$s\">\r",
						xmlStrct.tableTag(), t.getTableID(), cols, pks).getBytes());
				t.beforeFirst();
				while (t.next()) {
					fo.write(("\t\t<" + xmlStrct.recordTag() + ">\r").getBytes());
					for (String col : t.getColumns().keySet()) {
						fo.write(String.format("\t\t\t<%1$s>%2$s</%1$s>\r", col, t.getString(col)).getBytes());
					}
					fo.write(("\t\t</" + xmlStrct.recordTag() + ">\r").getBytes());
				}
				fo.write(("\t</" + xmlStrct.tableTag() + ">\r").getBytes());
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
			
		fo.write(("</" + xmlStrct.rootTag() + ">").getBytes());
		fo.flush();
		fo.close();
	}
	
	private static ILogger logger;
	protected static ILogger getLogger() throws Exception {
		if (logger == null) throw new Exception("logger not set correctly");
		return logger;
	}
}
