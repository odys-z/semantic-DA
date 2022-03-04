package io.odysz.module.xtable;

public class XMLDefaultStruct implements IXMLStruct {
	protected final String rootQName;
	protected final String tableQName;
	protected final String recordQName;

	public XMLDefaultStruct(String rootTag, String tableTag, String recordTag) {
		rootQName = rootTag;
		tableQName = tableTag;
		recordQName = recordTag;
	}
	
	public XMLDefaultStruct() {
		rootQName = "lyn";
		tableQName = "table";
		recordQName = "record";
	}

	@Override
	public String rootTag() {
		return rootQName;
	}

	@Override
	public String tableTag() {
		return tableQName;
	}

	@Override
	public String recordTag() {
		return recordQName;
	}
}
