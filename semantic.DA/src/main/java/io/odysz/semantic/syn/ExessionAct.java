package io.odysz.semantic.syn;

public class ExessionAct {
	public static final int restore = -2;
	public static final int unexpected = -1;
	public static final int ready = 0;
	public static final int init = 1;
	public static final int exchange = 2;
	
	public static final int mode_server = 0;
	public static final int mode_client = 1;

	public int state;
	int mode;

	public ExessionAct(int serv_client, int ini) {
		mode  = serv_client;
		state = ini;
	}

}
