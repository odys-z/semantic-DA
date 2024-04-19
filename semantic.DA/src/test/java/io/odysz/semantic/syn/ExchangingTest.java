package io.odysz.semantic.syn;

import static io.odysz.semantic.syn.Exchanging.*;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ExchangingTest {

	@Test
	void test() {
		Exchanging srv = new Exchanging(mode_server);
		Exchanging clt = new Exchanging(mode_client);
		
		assertEquals(ready, srv.state);
		assertEquals(ready, clt.state);

		clt.onExchange();
		assertEquals(ready, clt.state);

		clt.ack();
		assertEquals(ready, clt.state);

		clt.onAck();
		assertEquals(ready, clt.state);

		clt.close();
		assertEquals(ready, clt.state);

		clt.onclose();
		assertEquals(ready, clt.state);

		srv.initexchange();
		assertEquals(ready, clt.state);

		srv.ack();
		assertEquals(ready, clt.state);

		clt.initexchange();
		assertEquals(init, clt.state);

		// 
		srv.onAck();
		assertEquals(ready, srv.state);

		srv.close();
		assertEquals(ready, srv.state);

		srv.onclose();
		assertEquals(ready, srv.state);

		srv.onExchange();
		assertEquals(exchanging, srv.state);

		//
		clt.ack();
		assertEquals(confirming, clt.state);

		srv.onAck();
		assertEquals(confirming, srv.state);

		clt.close();
		assertEquals(ready, clt.state);

		srv.onclose();
		assertEquals(ready, srv.state);
	}

}
