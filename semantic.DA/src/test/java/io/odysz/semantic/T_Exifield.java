package io.odysz.semantic;

import java.util.HashMap;

import io.odysz.transact.sql.parts.AnDbField;

public class T_Exifield extends AnDbField {
	HashMap<String, String> exif;

	public T_Exifield add(String name, String v) {
		if (exif == null)
			exif = new HashMap<String, String>();
		exif.put(name, v);
		return this;
	}
}