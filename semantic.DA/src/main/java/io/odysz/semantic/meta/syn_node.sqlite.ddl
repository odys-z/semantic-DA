-- drop table if exists syn_node;
CREATE TABLE if not exists syn_node (
	org       varchar2(12) NOT NULL,
	synid     varchar2(64) NOT NULL,-- user input
	remarks   varchar2(256),
	crud      char(1)      NOT NULL,
	oper      varchar2(12),
	optime    datetime,
	PRIMARY KEY (org, synid)        -- this means the table can not maintained as parent table by Semantics.DA
);
