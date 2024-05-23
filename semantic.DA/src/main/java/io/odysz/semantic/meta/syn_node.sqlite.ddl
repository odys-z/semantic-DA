-- drop table if exists syn_node;
CREATE TABLE if not exists syn_node (
	org       varchar2(12) NOT NULL,
	synid     varchar2(64) NOT NULL,   -- user input
	nyq       long NOT NULL DEFAULT 0, -- for synchronizing
	nstamp    long NOT NULL DEFAULT 0, -- for stamping new change logs
	domain    varchar2(12) NOT NULL,   -- usually org id
	remarks   varchar2(256),
	mac       varchar2(64),
	oper      varchar2(12),
	optime    datetime,
	-- this commound pk means the table can not maintained as a parent table by Semantics.DA
	PRIMARY KEY (org, domain, synid)
);
