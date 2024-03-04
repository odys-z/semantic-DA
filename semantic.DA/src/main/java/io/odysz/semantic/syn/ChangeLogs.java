package io.odysz.semantic.syn;

import java.sql.SQLException;
import java.util.List;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.CRUD;
import io.odysz.semantic.meta.SynChangeMeta;

public class ChangeLogs extends Anson {

	@AnsonField(ignoreTo=true)
	SynChangeMeta met;
	
	String entble;
	/**
	 * 0: change statement, CRUD.C: insert, CRUD.U: remove-subs, CRUD.D: remove-log),<br>
	 * 1: change-crud,<br> 2: synoder,<br> 3: uids,<br> 4: nyquence<br> 
	 */
	List<Object[]> changes;

	public ChangeLogs(SynChangeMeta changemeta) {
		this.met = changemeta;
	}

	public void remove_sub(AnResultset chgs, String synode) throws SQLException {
		changes.add(new Object[] {
			CRUD.U, // insert change log
			chgs.getString(met.crud),
			chgs.getString(met.synoder),
			chgs.getString(met.uids),
			chgs.getLong(met.nyquence),
			synode
		});
	}

	public void remove(AnResultset chgs) throws SQLException {
		changes.add(new Object[] {
			CRUD.D, // delete change log
			chgs.getString(met.crud),
			chgs.getString(met.synoder),
			chgs.getString(met.uids),
			chgs.getLong(met.nyquence)
		});
	}

	public void append(AnResultset dchgs) throws SQLException {
		changes.add(new String[] {
			CRUD.C, // remove change subscriptions
			dchgs.getString(met.crud),
			dchgs.getString(met.synoder),
			dchgs.getString(met.uids),
			dchgs.getString(met.subs) });
	}

	public static Nyquence parseNyq(Object[] c) {
		return new Nyquence((long)c[4]);
	}
}
