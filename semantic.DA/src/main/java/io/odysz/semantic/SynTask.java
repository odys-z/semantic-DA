package io.odysz.semantic;

import io.odysz.anson.Anson;
import io.odysz.semantic.DBSyntext.syntype;

public class SynTask extends Anson {

	protected syntype syn;

	public SynTask(syntype pull) {
		this.syn = pull;
	}

}
