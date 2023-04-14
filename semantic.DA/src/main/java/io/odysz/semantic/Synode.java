package io.odysz.semantic;

import static io.odysz.common.LangExt.isNull;

import io.odysz.semantic.meta.SynodeMeta;

public class Synode extends SynEntity {

	public Synode(String synid) {
		super(new SynodeMeta());
		
		this.recId = synid;
	}
	
	public Synode clientpath(String path, String... path2) {
		clientpath = path;
		clientpath2 = isNull(path2) ? null :path2[0];
		return this;
	}
	
}
