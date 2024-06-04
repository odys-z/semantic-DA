-- drop table if exists syn_peers;
-- peer nvs
CREATE TABLE if not exists syn_peers (
	peer      varchar2(64) NOT NULL,
	synid     varchar2(64) NOT NULL,
	nyq       long NOT NULL DEFAULT 0,
	domain    varchar2(64), -- helper filter
	PRIMARY KEY (synid, peer)
);
