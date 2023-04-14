drop table if exists syn_node;
CREATE TABLE syn_node (
	synid     varchar2(64) NOT NULL,-- user input
	org       varchar2(12) NOT NULL,
	mac       varchar2(256),        -- if possible
	snycstamp datetime,             -- timestamp for last successfully synchronizing (both up & down). So each time will merge records between last-stamp & got-stamp (now - 1s)
	remarks   varchar2(256),        -- empty?
	up        datetime,             -- last bring up time
	os        varchar2(256),        -- android, browser, etc.
	anclient  varchar2(64),         -- docsync.jserv version
	extra     varchar2(512),
	oper      varchar2(12) NOT NULL,
	optime    datetime NOT NULL,
	PRIMARY KEY (synid)
);
