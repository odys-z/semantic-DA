-- drop table if exists syn_peers;
-- peer nvs
CREATE TABLE if not exists syn_peers (
	peer      varchar2(64) NOT NULL, -- synssion peer, the exchange target
	synid     varchar2(64) NOT NULL, -- nv.key, known to this synode
	nyq       long NOT NULL DEFAULT 0,
	domain    varchar2(64),          -- helper filter
	PRIMARY KEY (synid, peer)
);
