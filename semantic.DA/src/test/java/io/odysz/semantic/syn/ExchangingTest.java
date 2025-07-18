package io.odysz.semantic.syn;

import static io.odysz.semantic.syn.ExessionAct.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.odysz.common.Utils;

class ExchangingTest {

	@Test
	void test() {
		Exchanging srv = new Exchanging(mode_server);
		Exchanging clt = new Exchanging(mode_client);
		
		assertEquals(ready, srv.state);
		assertEquals(ready, clt.state);

		clt.onExchange();
		assertEquals(ready, clt.state);

		clt.close();
		assertEquals(ready, clt.state);

		clt.onclose();
		assertEquals(ready, clt.state);

		srv.initexchange();
		assertEquals(ready, clt.state);

		clt.initexchange();
		assertEquals(init, clt.state);

		// 
		srv.close();
		assertEquals(ready, srv.state);

		srv.onclose();
		assertEquals(ready, srv.state);

		srv.onExchange();
		assertEquals(exchange, srv.state);

		clt.close();
		assertEquals(ready, clt.state);

		srv.onclose();
		assertEquals(ready, srv.state);
	}

	@SuppressWarnings("deprecation")
	@Disabled
	@Test
	void testRestore() throws Exception {
		int no = 0;

		String server = "X";
		String client = "Y";
		String conn_srv = DBSyntableTest.conns[0];
		String conn_clt = DBSyntableTest.conns[1];

		SyndomContext domx_srv = new SyndomContext(SynodeMode.peer, 16, DBSyn2tableTest.zsu, server, conn_srv, true);
		DBSyntableBuilder tb_srv = new DBSyntableBuilder(domx_srv);

		SyndomContext domx_clt = new SyndomContext(SynodeMode.peer, 16, DBSyn2tableTest.zsu, client, conn_clt, true);
		DBSyntableBuilder tb_clt = new DBSyntableBuilder(domx_clt);

						Utils.logrst("client initate", ++no);
						ExessionPersist cp = new ExessionPersist(tb_clt, server)
								.forcetest(16, 5);

						ExchangeBlock req = cp.init();
						int ch_c = -1;
						req.print(System.out);
						assertEquals(server, req.peer);
						assertEquals(client, req.srcnode);
						assertEquals(init, req.act);
						assertEquals(init, cp.exstate());
						assertEquals(ch_c, req.challengeSeq);
						assertEquals(-1, req.answerSeq);
						assertEquals(ch_c, cp.expAnswerSeq);

		Utils.logrst("server initate", ++no);
		ExessionPersist sp = new ExessionPersist(tb_srv, client, req)
				.forcetest(12, 4);
		ExchangeBlock rep = sp.onInit(req);
		int ch_s = -1;
		rep.print(System.out);
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(init, rep.act);
		assertEquals(ch_s, rep.challengeSeq);
		assertEquals(ch_c, rep.answerSeq);
		assertEquals(ch_s, sp.expAnswerSeq);
		
						// client ch: 0
						Utils.logrst("client exchange", ++no);
						req = cp.nextExchange(rep);
						ch_c = 0;
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
		rep = sp.nextExchange(req);
		rep.print(System.out);
		ch_s = 0;
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(exchange, rep.act);
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
						assertEquals(restore, req.act);
						assertEquals(ch_c, req.challengeSeq);
						assertEquals(  -1, req.answerSeq);  // ch.answer - 1
						assertEquals(ch_c, cp.expAnswerSeq);

		Utils.logrst("server on-retry last", ++no);
		rep = sp.pageback().nextExchange(req);
		rep.print(System.out);
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(exchange, rep.act);
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
		rep = sp.nextExchange(req); ch_s++;
		rep.print(System.out);
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(ch_s, rep.challengeSeq);
		assertEquals(ch_c, rep.answerSeq);
		assertEquals(ch_s, sp.expAnswerSeq);
		
						Utils.logrst("client exchange", ++no);
						req = cp.nextExchange(rep); ch_c++;
						req.print(System.out);
						assertEquals(exchange, cp.exstate());
						assertEquals(server, req.peer);
						assertEquals(client, req.srcnode);
						assertEquals(ch_c, req.challengeSeq);
						assertEquals(ch_s, req.answerSeq);
						assertEquals(ch_c, cp.expAnswerSeq);

		Utils.logrst("server on-exchange - no more challenge pages", ++no);
		rep = sp.nextExchange(req); ch_s = -1;// no more challenges
		rep.print(System.out);
		assertEquals(exchange, sp.exstate());
		assertEquals(client, rep.peer);
		assertEquals(server, rep.srcnode);
		assertEquals(ch_s, rep.challengeSeq);
		assertEquals(ch_c, rep.answerSeq);
		assertEquals(ch_s, sp.expAnswerSeq);

						Utils.logrst("client close", ++no);
						req = cp.closexchange(rep);
						req.print(System.out);

		Utils.logrst("server close", ++no);
		rep = sp.closexchange(req);
		rep.print(System.out);
	}
}
