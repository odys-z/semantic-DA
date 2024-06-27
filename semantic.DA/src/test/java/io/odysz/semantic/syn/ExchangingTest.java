package io.odysz.semantic.syn;

import static io.odysz.semantic.syn.ExessionAct.*;
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
		assertEquals(exchange, srv.state);

		//
		clt.ack();
		assertEquals(0, clt.state);

		srv.onAck();
		assertEquals(0, srv.state);

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
		SynSessionMeta    ssm = new SynSessionMeta(conn);
		PeersMeta         prm = new PeersMeta();
		// T_PhotoMeta       phm = new T_PhotoMeta(conn);

						String client = "client";
		String server = "server";

						Utils.logrst("client initate", ++no);
						ExessionPersist cp = new ExessionPersist(null, chm, sbm, xbm, snm, ssm, prm, server)
								.forcetest(16, 5);
						ExchangeBlock req = cp.init();
						int ch_c = -1;
						req.print(System.out);

		Utils.logrst("server initate", ++no);
		ExessionPersist sp = new ExessionPersist(null, chm, sbm, xbm, snm, ssm, prm, client, req)
				.forcetest(12, 4);
		ExchangeBlock rep = sp.onInit(req);
		int ch_s = -1;
		rep.print(System.out);
		
						// client ch: 0
						Utils.logrst("client exchange", ++no);
						req = cp.nextExchange(rep); ch_c = 0;
						req.print(System.out);
						assertEquals(server, req.peer);
						assertEquals(client, req.srcnode);
						assertEquals(exchange, req.act);
						assertEquals(exchange, cp.exstate());
						assertEquals(ch_c, req.challengeSeq);
						assertEquals(ch_s, req.answerSeq);
						assertEquals(ch_c, cp.expAnswerSeq);

		// server ch: 0
		Utils.logrst("server on-exchange", ++no);
		rep = sp.onextExchange(client, req);
		rep.print(System.out); ch_s = 0;
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(ExessionAct.exchange, rep.act);
		assertEquals(ch_s, rep.challengeSeq);
		assertEquals(ch_c, rep.answerSeq);
		assertEquals(ch_s, sp.expAnswerSeq);

		// IOException: rep lost
		Utils.logrst("reply lost", no, 1);
		
						// TODO client reboot

						// client ch: 0
						Utils.logrst("client retry exchange", ++no);
						req = cp.retryLast(server);
						req.srcnode = client; // test only
						req.print(System.out);
						assertEquals(server, req.peer);
						assertEquals(client, req.srcnode);
						assertEquals(ExessionAct.restore, req.act);
						assertEquals(ch_c, req.challengeSeq);
						assertEquals(  -1, req.answerSeq);  // ch.answer - 1
						assertEquals(ch_c, cp.expAnswerSeq);

		Utils.logrst("server on-retry last", ++no);
		// rep = sp.onRetryLast(client, req);
		rep = sp.pageback().onextExchange(client, req);
		rep.print(System.out);
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(ExessionAct.exchange, rep.act);
		assertEquals(ch_s, rep.challengeSeq);
		assertEquals(ch_c, rep.answerSeq);
		assertEquals(ch_s, sp.expAnswerSeq);
		
						Utils.logrst("client exchange", ++no);
						req = cp.nextExchange(rep); ch_c++;
						req.print(System.out);
						assertEquals(server, req.peer);
						assertEquals(client, req.srcnode);
						assertEquals(ch_c, req.challengeSeq);
						assertEquals(ch_s, req.answerSeq);
						assertEquals(ch_c, cp.expAnswerSeq);

		Utils.logrst("server on-exchange", ++no);
		rep = sp.nextExchange(req); ch_s++;
		rep.print(System.out);
		assertEquals(exchange, sp.exstate());
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(ch_s, rep.challengeSeq);
		assertEquals(ch_c, rep.answerSeq);
		assertEquals(ch_s, sp.expAnswerSeq);
	
						Utils.logrst("client exchange", ++no);
						req = cp.nextExchange(rep); ch_c++;
						req.print(System.out);
						assertEquals(server, req.peer);
						assertEquals(client, req.srcnode);
						assertEquals(exchange, cp.exstate());
						assertEquals(ch_c, req.challengeSeq);
						assertEquals(ch_s, req.answerSeq);
						assertEquals(ch_c, cp.expAnswerSeq);

		// IOException: req lost
		Utils.logrst("req lost", no, 1);

						Utils.logrst("client retry last", ++no);
						req = cp.retryLast(server);
						req.srcnode = client; // test only
						req.print(System.out);
						assertEquals(restore, cp.exstate());
						assertEquals(server, req.peer);
						assertEquals(client, req.srcnode);
						assertEquals(ch_c, req.challengeSeq);
						assertEquals(ch_s, req.answerSeq);
						assertEquals(ch_c, cp.expAnswerSeq);

		Utils.logrst("server on-retry last", ++no);
		rep = sp.onextExchange(client, req); ch_s++;
		rep.print(System.out);
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(ch_s, rep.challengeSeq);
		assertEquals(ch_c, rep.answerSeq);
		assertEquals(ch_s, sp.expAnswerSeq);
		
						Utils.logrst("client exchange", ++no);
						req = cp.nextExchange(rep);
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
