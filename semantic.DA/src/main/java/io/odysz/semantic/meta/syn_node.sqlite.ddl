drop table if exists syn_node;
CREATE TABLE syn_node (
	org       varchar2(12) NOT NULL,
	synid     varchar2(64) NOT NULL,-- user input
	nyquence  long,
	PRIMARY KEY (org, synid)        -- this means the table can not maintained as parent table by Semantics.DA
);
