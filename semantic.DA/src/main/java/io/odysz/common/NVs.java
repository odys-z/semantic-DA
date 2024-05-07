package io.odysz.common;

import java.util.ArrayList;
import io.odysz.anson.Anson;

public class NVs extends Anson {
	protected String name;
	protected ArrayList<Object> values;

	public NVs name(String n) {
		name = n;
		return this;
	}

	public NVs value(Object v) {
		if (values == null)
			values = new ArrayList<Object>();
		values.add(v);
		return this;
	}
}
