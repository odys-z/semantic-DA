-- drop table if exists a_domains;

CREATE TABLE a_domain (
  domainId    text(40) NOT NULL,
  parentId    text(40),
  domainName  text(50),
  domainValue text(50),
  sort        text(11),
  others      text(20),
  fullpath    text(80),
  stamp       text(20),
  upstamp     text(20) NOT NULL DEFAULT '0',
  PRIMARY KEY ("domainId")
);
