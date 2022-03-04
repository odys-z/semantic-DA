package io.odysz.module.xtable;

import java.util.LinkedHashMap;

public interface ITableStruct {
	/** Get columns defining attributes name
	 * @return col specification string
	 */
	public String attrCols();
	public LinkedHashMap<String, Integer> colIdx();
	public String colDefs();
	
	public String attrTableID();
	
	public String attrPks();
	public LinkedHashMap<String, Integer> pkIdx();
	public String pkDefs();
}
