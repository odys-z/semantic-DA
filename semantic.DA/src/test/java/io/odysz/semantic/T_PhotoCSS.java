package io.odysz.semantic;

import io.odysz.transact.sql.parts.AnDbField;

public class T_PhotoCSS extends AnDbField {
	int size[];
	
	public T_PhotoCSS() {
		size = new int[] {0, 0};
	}

	public T_PhotoCSS(int w, int h) {
		size = new int[] {w, h};
	}
	
	public int w() {
		return size[0];
	}

	public int h() {
		return size[1];
	}

//	@Override
//	public Integer get(String col) {
//		return eq(col, "w") ? w() : eq(col, "h") ? h() : 0;
//	}
}
