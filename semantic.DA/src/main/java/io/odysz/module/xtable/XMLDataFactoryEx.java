package io.odysz.module.xtable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.xml.sax.SAXException;

/**
 * Factory of flat xml data.</br>
 * All tables are not managed - release memory for performance.
 * @author Odys
 */
public class XMLDataFactoryEx {
	/**
	 * Construct a new table.<br/>
	 * Not xml table data are buffered - for release memory.
	 * @param logger
	 * @param targetFullpath
	 * @param xmlStruct
	 * @return
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static LinkedHashMap<String, XMLTable> getXtables(ILogger logger,
			String targetFullpath, IXMLStruct xmlStruct) throws SAXException, IOException {
		//logger = androidLogger;
		try {
			FileInputStream istream = new FileInputStream(targetFullpath);
			//IXMLData d = XMLDataFactoryEx.getXMLData(sourceID, istream, xmlStruct, false);
			IXMLData d = new XMLFlatData(istream, xmlStruct, logger);
			// XMLTable t = d.getTable(fromStructureOfTableID);
			// return new XMLTable(newTableID, t.getColumns(), t.getPKs(), fromList);
			return d.getTables();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new SAXException(e.getMessage());
		}
	}
	
//	public static void writeXML(IXMLStruct xmlStrct, String fullpath, XMLTable[] tables) throws SAXException, IOException {
//		FileOutputStream fo = null;
//		File f = new File(fullpath);
//		if (f.exists()) f.delete();
//		f.createNewFile();
//		fo = new FileOutputStream(f);
//		// <rootTag>
//		fo.write(("<" + xmlStrct.rootTag() + ">\r").getBytes());
//		
//		for (XMLTable t : tables) {
//			try {
//				// <table id="tid" columns="c1,c2,c3" pk="c1">
//				String cols = "";
//				for (String c : t.getColumns().keySet()) {
//					cols += c + ",";
//				}
//				cols = cols.trim();
//				if (cols.length() > 1) {
//					cols = cols.substring(0, cols.length() - 1);
//					cols = cols.trim();
//				}
//				
//				String pks = "";
//				for (String pk : t.getPKs().keySet()) {
//					pks += pk + ",";
//				}
//				pks = pks.trim();
//				if (pks.length() > 1) {
//					pks = pks.substring(0, pks.length() - 1);
//					pks = pks.trim();
//				}
//				
//				fo.write(String.format("\t<%1$s id=\"%2$s\" columns=\"%3$s\" pk=\"%4$s\">\r",
//						xmlStrct.tableTag(), t.getTableID(), cols, pks).getBytes());
//				t.beforeFirst();
//				while (t.next()) {
//					fo.write(("\t\t<" + xmlStrct.recordTag() + ">\r").getBytes());
//					for (String col : t.getColumns().keySet()) {
//						fo.write(String.format("\t\t\t<%1$s>%2$s</%1$s>\r", col, t.getString(col)).getBytes());
//					}
//					fo.write(("\t\t</" + xmlStrct.recordTag() + ">\r").getBytes());
//				}
//				fo.write(("\t</" + xmlStrct.tableTag() + ">\r").getBytes());
//			} catch (Exception e) {
//				e.printStackTrace();
//				continue;
//			}
//		}
//			
//		fo.write(("</" + xmlStrct.rootTag() + ">").getBytes());
//		fo.flush();
//		fo.close();
//	}

	/**Write xtables (mapping info) into mapping file (fullpath).
	 * @param xmlStrct
	 * @param fullpath
	 * @param tables
	 * @throws IOException 
	 * @throws Exception
	 */
	public static void writeTables(IXMLStruct xmlStrct, String fullpath, XMLTable[] tables) throws SAXException, IOException {
		FileOutputStream fo = null;
		File f = new File(fullpath);
		if (f.exists()) f.delete();
		f.createNewFile();
		fo = new FileOutputStream(f);
		// <rootTag>
		fo.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n".getBytes());
		fo.write(("<" + xmlStrct.rootTag() + ">\n").getBytes());
		
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
				if (t.getPKs() != null) {
					for (String pk : t.getPKs().keySet()) {
						pks += pk + ",";
					}
					pks = pks.trim();
				
					if (pks.length() > 1) {
						pks = pks.substring(0, pks.length() - 1);
						pks = pks.trim();
					}
				}
				
				fo.write(String.format("\t<%1$s id=\"%2$s\" columns=\"%3$s\" pk=\"%4$s\">\n",
						xmlStrct.tableTag(), t.getTableID(), cols, pks).getBytes());
				t.beforeFirst();
				while (t.next()) {
					fo.write(("\t\t<" + xmlStrct.recordTag() + ">").getBytes());
					for (String col : t.getColumns().keySet()) {
						String v = t.getString(col);
						if (v != null)
							fo.write(String.format("<%1$s>%2$s</%1$s>", col, v).getBytes());
					}
					fo.write(("</" + xmlStrct.recordTag() + ">\n").getBytes());
				}
				fo.write(("\t</" + xmlStrct.tableTag() + ">\n").getBytes());
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
			
		fo.write(("</" + xmlStrct.rootTag() + ">").getBytes());
		fo.flush();
		fo.close();
	}
}
