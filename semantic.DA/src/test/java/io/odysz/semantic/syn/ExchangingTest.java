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

import io.odysz.common.Utils;
import io.odysz.semantic.meta.PeersMeta;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.semantic.meta.SynSubsMeta;
import io.odysz.semantic.meta.SynchangeBuffMeta;
import io.odysz.semantic.meta.SynodeMeta;
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
		int no = 0;
		String conn = "syn.00";
		SynodeMeta        snm = new SynodeMeta(conn);
		SynChangeMeta     chm = new SynChangeMeta(conn);
		SynSubsMeta       sbm = new SynSubsMeta(chm, conn);
		SynchangeBuffMeta xbm = new SynchangeBuffMeta(chm, conn);
		PeersMeta         prm = new PeersMeta();
		// T_PhotoMeta       phm = new T_PhotoMeta(conn);

		String client = "client";
		String server = "server";

		Utils.logrst("client initate", ++no);
		ExessionPersist cp = new ExessionPersist(null, chm, sbm, xbm, snm, prm, server);
		ExchangeBlock req = cp.init();
		int ch_c = -1;
		req.print(System.out);

		Utils.logrst("server initate", ++no);
		ExessionPersist sp = new ExessionPersist(null, chm, sbm, xbm, snm, prm, client, req);
		ExchangeBlock rep = sp.onInit(req);
		int ch_s = -1;
		rep.print(System.out);
		
		// ch: 1, ans: 0
		Utils.logrst("client exchange", ++no);
		req = cp.nextExchange(rep);
		req.print(System.out); ch_c = 0;
		assertEquals(ch_c, req.challengeSeq);
		assertEquals(ch_s, req.answerSeq);
		assertEquals(ch_c, cp.expAnswerSeq);

		// ch: 2, ans: 1
		Utils.logrst("server on-exchange", ++no);
		rep = sp.onextExchange(client, req);
		rep.print(System.out); ch_s = 0;
		assertEquals(ch_s, rep.challengeSeq);
		assertEquals(ch_c, rep.answerSeq);
		assertEquals(ch_s, sp.expAnswerSeq);

		// IOException: rep lost
		Utils.logrst("req lost", no, 1);

		Utils.logrst("client retry exchange", ++no);
		req = cp.retryLast(server);
		req.print(System.out);
		// requires: ch: 1, ans: 1
		assertEquals(ExessionAct.restore, req.act);
		assertEquals(1, req.challengeSeq);
		assertEquals(1, req.answerSeq);

		Utils.logrst("server on-retry last", ++no);
		rep = sp.onRetryLast(server, req);
		rep.print(System.out);
		assertEquals(ExessionAct.restore, rep.act);
		assertEquals(2, rep.challengeSeq);
		assertEquals(1, rep.answerSeq);
		
		Utils.logrst("server exchange", ++no);
		req = cp.exchange(server, rep);
		req.print(System.out);
		assertEquals(2, req.challengeSeq);
		assertEquals(2, req.answerSeq);

		Utils.logrst("server on-exchange", ++no);
		rep = sp.onExchange(client, req);
		rep.print(System.out);
		assertEquals(3, rep.challengeSeq);
		assertEquals(2, rep.answerSeq);
		
		Utils.logrst("client exchange", ++no);
		req = cp.exchange(server, rep);
		req.print(System.out);
		assertEquals(3, req.challengeSeq);
		assertEquals(3, req.answerSeq);

		// IOException: req lost
		Utils.logrst("req lost", no, 1);

		Utils.logrst("client retry last", ++no);
		req = cp.retryLast(server);
		req.print(System.out);
		assertEquals(3, req.challengeSeq);
		assertEquals(3, req.answerSeq);

		Utils.logrst("server on-retry last", ++no);
		rep = sp.onRetryLast(client, req);
		rep.print(System.out);
		assertEquals(3, rep.challengeSeq);
		assertEquals(3, rep.answerSeq);
		
		Utils.logrst("client exchange", ++no);
		req = cp.exchange(server, rep);
		req.print(System.out);

		Utils.logrst("server on-exchange", ++no);
		rep = sp.onExchange(client, req);
		rep.print(System.out);

		Utils.logrst("client close", ++no);
		req = cp.closexchange(rep);
		req.print(System.out);

		Utils.logrst("server close", ++no);
		rep = sp.closexchange(req);
		rep.print(System.out);
	}
}
