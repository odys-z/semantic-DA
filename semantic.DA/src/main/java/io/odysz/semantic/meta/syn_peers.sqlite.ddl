-- drop table if exists syn_peers;
-- peer nvs
CREATE TABLE if not exists syn_peers (
	synid     varchar2(64) NOT NULL,
	nyq       long NOT NULL DEFAULT 0,
	peer      varchar2(64) NOT NULL,
	domain    varchar2(64), -- helper filter
	PRIMARY KEY (synid, peer)
);
