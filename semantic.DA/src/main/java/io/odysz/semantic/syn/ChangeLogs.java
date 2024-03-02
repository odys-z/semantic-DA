package io.odysz.semantic.syn;

import java.sql.SQLException;
import java.util.List;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;

public class ChangeLogs extends Anson {

	@AnsonField(ignoreTo=true)
	SynChangeMeta met;
	
	String entble;
	List<String[]> inserts;
	List<String[]> removes;
	

	public ChangeLogs(SynChangeMeta changemeta) {
		this.met = changemeta;
	}

	public void remove_sub(AnResultset chgs, String synode) throws SQLException {
		removes.add(new String[] {
			chgs.getString(met.crud),
			chgs.getString(met.synoder),
			chgs.getString(met.uids),
			chgs.getString(met.nyquence),
			synode
		});
	}

	public void remove(AnResultset srchgs) {
		
	}

	public void append(AnResultset dchgs) throws SQLException {
		inserts.add(new String[] {
			dchgs.getString(met.crud),
			dchgs.getString(met.synoder),
			dchgs.getString(met.uids),
			dchgs.getString(met.subs) });
	}
}
