-- drop table if exists syn_sessions;
CREATE TABLE if not exists syn_sessions (
	peer      varchar2(64) NOT NULL,
	chpage    int NOT NULL default -1,
	answerx   int NOT NULL default -1,
	expansx   int NOT NULL default -1,
	mode      int, -- ExessionAct.mode  :: ExessionPersist#mode
	state     int, -- ExessionAct.state :: ExessionPersist#state
	PRIMARY KEY (peer)
);
