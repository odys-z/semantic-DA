package io.odysz.module.xtable;

import java.util.LinkedHashMap;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** 
 * Flat XML data structure handler. 
 * @author Odysseus Zhou
 */
public class XMLFlatReader extends DefaultHandler {
	public static final String Tag = "XMLFlatReader";
	private ILogger logger;
//	public class TableStruct { }

	protected String rootTag;
	protected String tableTag;
	protected String recordTag;
	
	public XMLFlatReader(ILogger logger, IXMLStruct xmlStruct) {
		this.logger = logger;
		rootTag = xmlStruct.rootTag();
		tableTag = xmlStruct.tableTag();
		recordTag = xmlStruct.recordTag();
	}
	
	/** [key = tableID, value = XMLTable[qName, chars]] */
	protected LinkedHashMap<String, XMLTable> tables = new LinkedHashMap<String, XMLTable>();
	public LinkedHashMap<String, XMLTable> getTables() { return tables; }
	
	protected String currV;					// current characters
	protected boolean maybeMore = false;	// may be more characters not received by characters()

	private XMLTable currentTable;
	
	@Override
	public void startElement(String namespaceURI, String localName, String qname,
			Attributes attributes) throws SAXException {
		if (rootTag.equals(qname)) {
			// ignore
		}
		else if (tableTag.equals(qname)) {
			// construct a table
			try {
				currentTable = new XMLTable(attributes.getValue("id"),
						attributes.getValue("columns"),
						attributes.getValue("pk"), logger);
				currentTable.setXmlAttrs(attributes);
				currentTable.startTablePush();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else if (recordTag.equals(qname)) {
			// to build a row
			currentTable.startRecordPush();
		}
		else {
			currV = "";
		}
	}

	public void endElement(String namespaceURI,
            String sName,	// simple name
            String qName)	// qualified name
		throws SAXException	{
		if (rootTag.equals(qName)) {
		}
		else if (tableTag.equals(qName)) {
			// push table
			currentTable.endTablePush();
			tables.put(currentTable.getTableID(), currentTable);
			currentTable = null;
		}
		else if (recordTag.equals(qName)) {
			// push record
			currentTable.endRecordPush(true);
		}
		else {
			if (currentTable == null)
				logger.e(Tag, String.format("Can't put value (%s) from tag <%s> into field. Check the XML struct.", currV, qName));
			else
				currentTable.appendFieldValue(qName, currV.trim());
		}
		maybeMore = false;
	}

	public void characters(char buf[], int offset, int len) throws SAXException	{
		if (maybeMore == true)
		currV += new String(buf, offset, len);
		else currV = new String(buf, offset, len);
		maybeMore = true;	// may be more character chuncks followed
	}

	public XMLTable getTable(String tableID) {
		if (tables == null) return null;
		return tables.get(tableID);
	}
}
