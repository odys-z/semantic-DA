package io.odysz.common;

import io.odysz.anson.Anson;

public class NV extends Anson {
	public String name;
	public Object value;

	public NV nv(String n, Object v) {
		this.name = n;
		this.value = v;
		return this;
	}
}
