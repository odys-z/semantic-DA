package io.odysz.semantic.syn;

import io.odysz.anson.Anson;

/**
 *<pre>  server                      client
 * ready                       ready
 *                             exchanging [{@link #initexchange()}]
 * exchanging [{@link #onExchange()}]
 *                             confirming [{@link #confirm()}]
 * confirming [{@link #onAck()}]
 *                             ready [{@link #closexchange()}]
 * ready [{@link #onclose()}]
 * </pre>

 * @author Ody
 *
 */
public class Exchanging extends Anson {
	public static final int init = 1;
	public static final int exchanging = 2;
	public static final int confirming = 3;
	public static final int ready = 4;
	
	public static final int mode_server = 0;
	public static final int mode_client = 1;

	int state;
	int mode;
	
	public Exchanging (int server0_client1) {
		state = ready;
		mode  = server0_client1 == mode_server ? mode_server : mode_client;
	}

	public int initexchange() {
		if (mode == mode_client && state == ready)
			state = init;
		return state;
	}

	public int onExchange () {
		if (mode == mode_server && state == ready)
			state = exchanging;
		return state;
	}

	public int confirm () {
		if (mode == mode_client && state == init)
			state = confirming;
		return state;
	}

	public int onconfirm () {
		if (mode == mode_server && state == exchanging)
			state = confirming;
		return state;
	}

	public int close () {
		if (mode == mode_client && state == confirming)
			state = ready;
		return state;
	}

	public int onclose () {
		if (mode == mode_server && state == confirming)
			state = ready;
		return state;
	}

	public String name() {
		return name(state);
	}

	public static String name(int s) {
		return s == init
			? "init"
			: s == exchanging
			? "exchanging"
			: s == confirming
			? "confirming"
			: "ready";
	}

}
