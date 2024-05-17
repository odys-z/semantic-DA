package io.odysz.semantic.syn;

import static io.odysz.semantic.syn.Exchanging.confirming;
import static io.odysz.semantic.syn.Exchanging.exchanging;
import static io.odysz.semantic.syn.Exchanging.init;
import static io.odysz.semantic.syn.Exchanging.mode_client;
import static io.odysz.semantic.syn.Exchanging.mode_server;
import static io.odysz.semantic.syn.Exchanging.ready;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

										// -1 : -1
										ExchangeBlock req = cp.init();
										cp.forcetest(182, 50);
										req.totalChallenges(cp.totalChallenges, cp.chsize);

										assertEquals(182, req.totalChallenges);
										assertEquals(4, cp.pages());
										assertEquals(50, req.chpagesize);

										assertEquals(ExessionAct.init, req.act);
										assertEquals(-1, req.challengeSeq);
										assertEquals(-1, req.answerSeq);
										assertEquals(-1, cp.expAnswerSeq);

		ExessionPersist sp = new ExessionPersist(null, chm, sbm, xbm, client, req);
		
		// -1 : -1
		ExchangeBlock rep = sp.onInit(req);
		sp.forcetest(245);
		assertEquals(50, sp.chsize);
		rep.totalChallenges(sp.totalChallenges);

		assertEquals(245, rep.totalChallenges);
		assertEquals(5, sp.pages());
		assertEquals(50, rep.chpagesize);

		assertEquals(ExessionAct.init, req.act);
		assertEquals(-1, rep.challengeSeq);
		assertEquals(-1, rep.answerSeq);
		assertEquals(-1, sp.expAnswerSeq);

		// ch   ans   exp           |     ch   ans   exp 
		// -1   -1    -1            |     -1   -1    -1
										assertTrue(cp.expect(rep).nextChpage());
										req = cp.exchange(server, rep);
										assertEquals(ExessionAct.exchange, req.act);
										assertEquals(182, req.totalChallenges);
										assertEquals(0, req.challengeSeq);
										assertEquals(-1, req.answerSeq);
										assertEquals(0, cp.expAnswerSeq);

		// -1   -1    -1            |      0   -1     0

		assertTrue(sp.expect(req).nextChpage());
		rep = sp.onExchange(client, req);
		assertEquals(ExessionAct.exchange, req.act);
		assertEquals(245, rep.totalChallenges);
		assertEquals(0, rep.challengeSeq);
		assertEquals(0, rep.answerSeq);
		assertEquals(0, sp.expAnswerSeq);

		//  0    0     0            |      0   -1     0
										assertTrue(cp.expect(rep).nextChpage());
										req = cp.exchange(server, rep);
										assertEquals(ExessionAct.exchange, req.act);
										assertEquals(1, req.challengeSeq);
										assertEquals(0, req.answerSeq);
										assertEquals(1, cp.expAnswerSeq);

		//  0    0     0            |      1    0     1
										
		assertTrue(sp.expect(req).nextChpage());
		rep = sp.onExchange(client, req);
		assertEquals(ExessionAct.exchange, rep.act);
		assertEquals(1, rep.challengeSeq);
		assertEquals(1, rep.answerSeq);
		assertEquals(1, sp.expAnswerSeq);
	
		//  1    1     1            |      1    0     1
		
										// IOException: rep lost
										req = cp.retryLast(server);
										assertEquals(ExessionAct.restore, req.act);
										assertEquals(1, req.challengeSeq);
										assertEquals(0, req.answerSeq);
										assertEquals(1, cp.expAnswerSeq);

		//  0    0     0            |      1    0     1
		assertEquals(ExessionAct.restore, req.act);

		rep = sp.onRetryLast(client, req);
		assertEquals(ExessionAct.restore, rep.act);
		assertEquals(1, rep.challengeSeq);
		assertEquals(1, rep.answerSeq);
		assertEquals(1, sp.expAnswerSeq);
		
		//  0    0     0            |      0   -1     0
										cp.expect(rep);
										assertEquals(ExessionAct.restore, rep.act);
										cp.exstat().go(ExessionAct.exchange);

										assertTrue(cp.expect(rep).nextChpage());
										req = cp.exchange(server, rep);
										assertEquals(ExessionAct.exchange, req.act);
										assertEquals(2, req.challengeSeq);
										assertEquals(1, req.answerSeq);
										assertEquals(2, cp.expAnswerSeq);

		// 
		assertEquals(ExessionAct.restore, sp.exstate());
		assertEquals(ExessionAct.exchange, req.act);
		sp.exstat().go(ExessionAct.exchange);
		assertTrue(sp.expect(req).nextChpage());

		rep = sp.onExchange(client, req);
		assertEquals(ExessionAct.exchange, rep.act);
		assertEquals(2, rep.challengeSeq);
		assertEquals(2, rep.answerSeq);
		assertEquals(2, sp.expAnswerSeq);
		
										//
										assertTrue(cp.expect(rep).nextChpage());
										req = cp.exchange(server, rep);
										assertEquals(ExessionAct.exchange, req.act);
										assertEquals(3, req.challengeSeq);
										assertEquals(2, req.answerSeq);
										assertEquals(3, cp.expAnswerSeq);

										// IOException: req lost
										req = cp.retryLast(server);
										assertNull(req.srcnode);
										req.srcnode = client;

										assertEquals(ExessionAct.restore, req.act);
										assertEquals(3, req.challengeSeq);
										assertEquals(2, req.answerSeq);
										assertEquals(3, cp.expAnswerSeq);

		// 
		assertTrue(sp.expect(req).nextChpage());
		rep = sp.exchange(client, req);
		assertEquals(ExessionAct.exchange, rep.act);
		assertEquals(3, rep.challengeSeq);
		assertEquals(3, rep.answerSeq);
		assertEquals(3, sp.expAnswerSeq);
		
										// 
										assertFalse(cp.expect(rep).nextChpage());
										assertTrue(rep.moreChallenge());
										req = cp.exchange(server, rep);
										assertEquals(ExessionAct.exchange, req.act);
										assertEquals(-1, req.challengeSeq);
										assertEquals(3, req.answerSeq);
										assertEquals(-1, cp.expAnswerSeq);

		//
		assertTrue(sp.expect(req).nextChpage());
		rep = sp.onExchange(client, req);
		assertEquals(ExessionAct.exchange, rep.act);
		assertEquals(4, rep.challengeSeq);
		assertEquals(-1, rep.answerSeq);
		assertEquals(4, sp.expAnswerSeq);

										//
										assertFalse(cp.expect(rep).nextChpage());
										req = cp.exchange(server, rep);
										assertEquals(ExessionAct.exchange, req.act);
										assertEquals(-1, req.challengeSeq);
										assertEquals(4, req.answerSeq);
										assertEquals(-1, cp.expAnswerSeq);

		//
		assertFalse(sp.expect(req).nextChpage());
		rep = sp.onExchange(client, req);
		assertEquals(ExessionAct.exchange, rep.act);
		assertEquals(-1, rep.challengeSeq);
		assertEquals(-1, rep.answerSeq);
		assertEquals(-1, sp.expAnswerSeq);

										//
										assertFalse(cp.expect(rep).nextChpage());
										assertFalse(rep.moreChallenge());
										req = cp.closexchange(server, rep);
										assertEquals(ExessionAct.close, req.act);
										assertEquals(ExessionAct.ready, cp.exstate());
										assertEquals(-1, req.challengeSeq);
										assertEquals(-1, req.answerSeq);
										assertEquals(-1, cp.expAnswerSeq);

		// 
		assertFalse(sp.expect(req).nextChpage());
		assertFalse(req.moreChallenge());
		sp.onclose(client, req);
		assertEquals(ExessionAct.ready, sp.exstate());
	}
}
