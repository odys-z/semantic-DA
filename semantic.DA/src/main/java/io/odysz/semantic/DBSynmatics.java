package io.odysz.semantic;

import java.util.ArrayList;
import java.util.Map;

import io.odysz.semantics.IUser;
import io.odysz.transact.sql.Delete;
import io.odysz.transact.sql.Insert;
import io.odysz.transact.sql.Statement;
import io.odysz.transact.sql.Update;
import io.odysz.transact.sql.parts.condition.Condit;

public class DBSynmatics {

	public void onInsert(DBSyntext dbSyntext, Insert insert, ArrayList<Object[]> row, Map<String, Integer> cols,
			IUser usr) {
		// TODO Auto-generated method stub
		
	}

	public void onUpdate(DBSyntext dbSyntext, Update update, ArrayList<Object[]> nvs, Map<String, Integer> cols,
			IUser usr) {
		// TODO Auto-generated method stub
		
	}

	public void onDelete(DBSyntext dbSyntext, Delete delete, Condit whereCondt, IUser usr) {
		// TODO Auto-generated method stub
		
	}

	public void onPost(DBSyntext dbSyntext, Statement<?> stmt, ArrayList<Object[]> row, Map<String, Integer> cols,
			IUser usr, ArrayList<String> sqls) {
		// TODO Auto-generated method stub
		
	}

}
