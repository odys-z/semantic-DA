package io.odysz.module.xtable;

import java.io.InputStream;
import java.util.LinkedHashMap;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;

/**
 * Generic flat xml data manager.</br>
 * When inited, kname is treated as reacord id, vname is as value. But this class do nothing for any other data structure.
 * Value can be fieldized by XMLDataFactory.getFLResultset().
 * @author Odys
 */
@SuppressWarnings("deprecation")
public class XMLFlatData implements IXMLData {
	private static final String TAG = "XMLFlatData";

	protected XMLFlatReader handler;

	/** load xml file at "<path>", parse into map 
	 * @throws Exception */
	public XMLFlatData(InputStream istream, IXMLStruct xmlStruct, ILogger logger) throws Exception {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		try {
			handler = new XMLFlatReader(logger, xmlStruct);
			SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(new InputSource(istream), handler);
			logger.i(TAG, "XML file processed.");
		} catch (Exception e) {
			logger.e(TAG, "Error - Loading xml file failed. Check tags in file and xmlStruct...");
			e.printStackTrace();
		}
	}
	
	@Override
	public LinkedHashMap<String, XMLTable> getTables() {
		return handler.getTables();
	}

	@Override
	public XMLTable getTable(String tableID) {
		return handler.getTable(tableID);
	}

	@Override
	public String getTableAttribute(String tableID, String attrName) {
		return null;
	}
}
