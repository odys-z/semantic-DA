package io.odysz.semantic.syn;

import io.odysz.anson.Anson;
import io.odysz.semantics.x.ExchangeException;

/**
 *<pre>  server                      client
 * ready                       ready
 *                             exchanging [{@link #initexchange()}]
 * exchanging [{@link #onExchange()}]
 *                             confirming [{@link #ack()}]
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

	public int state;
	int mode;
	
	public Exchanging (int server0_client1) {
		state = ready;
		mode  = server0_client1 == mode_server ? mode_server : mode_client;
	}

	public int initexchange() {
		if (mode == mode_client && (state == ready || state == confirming))
			state = init;
		return state;
	}

	public int onExchange () {
		if (mode == mode_server && (state == ready || state == init))
			state = exchanging;
		return state;
	}

	public int ack () {
		if (mode == mode_client && state == init)
			state = confirming;
		return state;
	}

	public int onAck () {
		if (mode == mode_server && state == exchanging)
			state = ready;
		return state;
	}

	public int close () {
		if (mode == mode_client && (state == confirming || state == init)
		 || mode == mode_server && state == ready)
			state = ready;
		return state;
	}

	public int onclose () {
		if (mode == mode_server && state == ready)
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

	@Override
	public String toString() { return name(); }

	/**
	 * Test the {@code exp} state is the right next state.
	 * @param exp
	 * @return current state
	 * @throws ExchangeException expecting state if {@code exp} is not the right one.
	 */
	public int can(int exp) throws ExchangeException {
		// deprecated, keep for DBSyansactBuilder
		return can(exp, null);
	}

	public int can(int exp, ExessionPersist xp) throws ExchangeException {
		if (mode == mode_client) {
			if (state == ready)
				if (exp != init)
					throw new ExchangeException(init, xp, "Expecting init, but was %s", name(exp));
			if (state == exchanging)
				if (exp != confirming)
					throw new ExchangeException(confirming, xp, "Expecting confirming, but was %s", name(exp));
			if (state == confirming)
				if (exp != ready && exp != init)
					throw new ExchangeException(ready, xp, "Expecting ready/init, but was %s", name(exp));
		}
		else { // (mode == mode_server)
			if (state == ready)
				if (exp != exchanging && exp != init)
					throw new ExchangeException(exchanging, xp, "Expecting exchanging/init, but was %s", name(exp));
			if (state == exchanging)
				if (exp != confirming)
					throw new ExchangeException(confirming, xp, "Expecting confirming, but was %s", name(exp));
			if (state == confirming)
				if (exp != ready)
					throw new ExchangeException(ready, xp, "Expecting ready, but was %s", name(exp));
		}
		return state;
	}
}
