package io.odysz.semantic.syn;

import static io.odysz.semantic.syn.Exchanging.confirming;
import static io.odysz.semantic.syn.Exchanging.exchanging;
import static io.odysz.semantic.syn.Exchanging.init;
import static io.odysz.semantic.syn.Exchanging.mode_client;
import static io.odysz.semantic.syn.Exchanging.mode_server;
import static io.odysz.semantic.syn.Exchanging.ready;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.transact.x.TransException;

class ExchangingTest {

	@Disabled
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

	@Test
	void testRestore() throws TransException, SQLException {
		String conn = "syn.00";
		// SynodeMeta snm = new SynodeMeta(conn);
		SynChangeMeta chm = new SynChangeMeta(conn);
		SynchangeBuffMeta xbm = new SynchangeBuffMeta(chm);
		SynSubsMeta sbm = new SynSubsMeta(chm, conn);
		// T_PhotoMeta phm = new T_PhotoMeta(conn);

		String client = "client";
		String server = "server";
		ExessionPersist cp = new ExessionPersist(null, chm, sbm, xbm, server);
		ExchangeBlock req = cp.init();

		ExessionPersist sp = new ExessionPersist(null, chm, sbm, xbm, client, req);
		ExchangeBlock rep = sp.onInit(req);
		
		// ch: 1, ans: 0
		req = cp.exchange(server, rep);
		assertEquals(1, req.challengeId);
		assertEquals(1, req.answerId);
		assertEquals(1, cp.expAnswerSeq);

		// ch: 2, ans: 1
		rep = sp.onExchange(client, req);
		assertEquals(2, rep.challengeId);
		assertEquals(1, rep.answerId);
		
		// IOException: rep lost
		req = cp.retry(server);
		// requires: ch: 1, ans: 1
		assertEquals(ExessionAct.restore, req.act);
		assertEquals(1, req.challengeId);
		assertEquals(1, req.answerId);

		rep = sp.onRetry(server, req);
		assertEquals(ExessionAct.restore, rep.act);
		assertEquals(2, rep.challengeId);
		assertEquals(1, rep.answerId);
		
		req = cp.exchange(server, rep);
		assertEquals(2, req.challengeId);
		assertEquals(2, req.answerId);

		// ch: 2, ans: 2
		rep = sp.onExchange(client, req);
		assertEquals(3, rep.challengeId);
		assertEquals(2, rep.answerId);
		
		// ch: 3, ans: 3
		req = cp.exchange(server, rep);
		assertEquals(3, req.challengeId);
		assertEquals(3, req.answerId);

		// IOException: req lost
		// ch: 3, ans: 2
		req = cp.retry(server);
		assertEquals(3, req.challengeId);
		assertEquals(3, req.answerId);

		// ch: 3, ans: 3
		rep = sp.onRetry(client, req);
		assertEquals(3, rep.challengeId);
		assertEquals(3, rep.answerId);
		
		// ch: -, ans: 3
		req = cp.exchange(server, rep);
		// ch: 4, ans: -
		rep = sp.onExchange(client, req);

		// ch: -, ans: 4
		req = cp.closexchange(server, rep);
		sp.onclose(client, req);
	}
}
