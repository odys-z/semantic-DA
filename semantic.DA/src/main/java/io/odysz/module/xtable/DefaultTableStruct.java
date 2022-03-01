package io.odysz.module.xtable;

import java.util.LinkedHashMap;

public class DefaultTableStruct implements ITableStruct {
	protected static final String attrNameTableID = "tableID";
	protected static final String attrNameColumns = "columns";
	protected static final String attrNamePks = "pk";
	
	protected LinkedHashMap<String, Integer> pkIdx;
	protected String pkDef;
	protected LinkedHashMap<String, Integer> colIdx;
	protected String colDef;

	public DefaultTableStruct(String colDefs, String pkDefs) {
		pkDef = pkDefs;
		colDef = colDefs;
	}
	
	@Override
	public LinkedHashMap<String, Integer> pkIdx() {
		if (pkIdx == null) {
			pkIdx = buildIdx(pkDef);
		}
		return pkIdx;
	}
	
	@Override
	public String pkDefs() {
		return pkDef;
	}
	
	@Override
	public LinkedHashMap<String, Integer> colIdx() {
		if (colIdx == null) {
			colIdx = buildIdx(colDef);
		}
		return colIdx;
	}
	
	protected static LinkedHashMap<String, Integer> buildIdx(String defs) {
		if (defs == null) return null;
		LinkedHashMap<String, Integer> idx = new LinkedHashMap<String, Integer>();
		String[] fns = defs.split(",");
		if (fns == null) return null;
		for (int i = 0; i < fns.length; i++)
			idx.put(fns[i].trim(), i);
		return idx;
	}

	@Override
	public String colDefs() {
		return colDef;
	}
	
	@Override
	public String attrTableID() {
		return attrNameTableID;
	}
	
	@Override
	public String attrPks() {
		return attrNamePks;
	}
	
	@Override
	public String attrCols() {
		return attrNameColumns;
	}
}
