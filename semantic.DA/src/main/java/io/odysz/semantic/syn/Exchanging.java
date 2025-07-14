package io.odysz.semantic.syn;

import static io.odysz.semantic.syn.ExessionAct.*;

import io.odysz.anson.Anson;
import io.odysz.semantics.x.ExchangeException;

/**
 *<pre>  server                      client
 * ready                       ready
 *                             exchanging [{@link #initexchange()}]
 * exchanging [{@link #onExchange()}]
 *                             exchange 
 * close [{@link #close()}]
 *                             ready [{@link #onclose()}]
 * </pre>

 * @author Ody
 *
 */
public class Exchanging extends Anson {

	public int state;
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
		if (mode == mode_server && (state == ready || state == init))
			state = exchange;
		return state;
	}

	public int close () {
		if (mode == mode_client && state == init
		 || mode == mode_server)
			state = ready;
		return state;
	}

	public int onclose () {
		if (mode == mode_server)
			state = ready;
		return state;
	}

	public String name() {
		return name(state);
	}

	public static String name(int s) {
		return s == init
			? "init"
			: s == exchange
			? "exchanging"
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
//	public int can(int exp) throws ExchangeException {
//		// deprecated, keep for DBSyansactBuilder
//		return can(exp, null);
//	}

	public int can(int exp, ExessionPersist xp) throws ExchangeException {
		if (mode == mode_client) {
			if (state == ready)
				if (exp != init)
					throw new ExchangeException(init, xp, "Expecting init, but was %s", name(exp));
			if (state == exchange)
				if (exp != exchange && exp != init && exp != close && exp != restore)
					throw new ExchangeException(exchange, xp, "Expecting ready/init/close/restore while exhcnaging, but was %s", name(exp));
		}
		else { // (mode == mode_server)
			if (state == init)
				if (exp != exchange && exp != init)
					throw new ExchangeException(init, xp, "Expecting exchange/init, but was %s", name(exp));
			if (state == exchange)
				if (exp != exchange && exp != init && exp != close && exp != restore)
					throw new ExchangeException(exchange, xp, "Expecting ready/init/close/restore while exhcnaging, but was %s", name(exp));
		}
		throw new ExchangeException(unexpect, xp, "Unexpected state: %s", name(exp));
	}
}
