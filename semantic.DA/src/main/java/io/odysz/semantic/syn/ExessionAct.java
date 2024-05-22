package io.odysz.semantic.syn;

import java.lang.reflect.Field;

import io.odysz.common.Utils;

public class ExessionAct {
	public static final int restore = -2;
	public static final int unexpected = -1;
	public static final int ready = 0;
	public static final int init = 1;
	public static final int exchange = 2;
	public static final int close = 3;
	
	public static final int mode_server = 0;
	public static final int mode_client = 1;

	public int state;
	int mode;

	public ExessionAct(int serv_client, int ini) {
		mode  = serv_client;
		state = ini;
	}

	public void go(int stat) {
		state = stat;
	}

	public static String nameOf(int s) {
		Field[] fs = ExessionAct.class.getFields();
		for (Field f : fs) {
			try {
				Class<?> t = f.getType();
				if(t == int.class) {
					if (f.getInt(null) == s)
						return f.getName();
				}
			}catch (Exception ex) {
				Utils.warn("ExessionAct#name(): Can't find name of %d.", s);
			}
		}
		return "NA";
	}

	public void onclose() {
		this.state = ready;
	}

}
