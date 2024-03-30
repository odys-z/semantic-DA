-- drop table if exists syn_node;
CREATE TABLE if not exists syn_node (
	org       varchar2(12) NOT NULL,
	synid     varchar2(64) NOT NULL, -- user input
	nyq       long NOT NULL DEFAULT 0,
	domain    varchar2(12) NOT NULL, -- usually org id
	remarks   varchar2(256),
	mac       varchar2(64),
	oper      varchar2(12),
	optime    datetime,
	PRIMARY KEY (org, domain, synid) -- this means the table can not maintained as parent table by Semantics.DA
);
