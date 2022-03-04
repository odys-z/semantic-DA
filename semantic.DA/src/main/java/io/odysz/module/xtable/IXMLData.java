package io.odysz.module.xtable;

import java.util.LinkedHashMap;

public interface IXMLData {
	public LinkedHashMap<String, XMLTable> getTables();
	public XMLTable getTable(String tableID);
	public String getTableAttribute(String tableID, String attrName);
}
