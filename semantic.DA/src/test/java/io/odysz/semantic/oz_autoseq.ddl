
-- drop table if exists oz_autoseq;

CREATE TABLE if not exists oz_autoseq (
  sid text(50),
  seq INTEGER,
  remarks text(200),
  CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid)
);
