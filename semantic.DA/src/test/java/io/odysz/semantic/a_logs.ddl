
-- drop table if exists a_logs;

CREATE TABLE a_logs (
  logId    text(20),
  funcId   text(20),
  funcName text(50),
  oper     text(20),
  logTime  text(20),
  cnt      int,
  txt      text(4000),
  CONSTRAINT oz_logs_pk PRIMARY KEY (logId)
);
