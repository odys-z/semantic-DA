-- drop table if exists a_attaches;
CREATE TABLE a_attaches (
	attId   TEXT NOT NULL,
	attName TEXT,
	uri     TEXT,
	busiTbl TEXT,
	busiId  TEXT,
	oper    TEXT,
	optime  DATETIME,
	CONSTRAINT a_attaches_PK PRIMARY KEY (attId)
);
