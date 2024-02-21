CREATE TABLE h_photos (
  pid varchar(12) NOT NULL,
  family varchar2(12) NOT NULL,
  folder varchar(256) NOT NULL,
  pname varchar(256),
  uri varchar(512) NOT NULL,   -- storage/userId/folder/recId-clientname
  pdate datetime,              -- picture taken time

  device varchar(12),          -- 'original device ID',
  clientpath TEXT DEFAULT '/' NOT NULL, -- shall we support 'moveTo' laterly?

  syncstamp DATETIME DEFAULT CURRENT_TIMESTAMP not NULL,

  shareby varchar(12),         -- 'shared by / creator',
  sharedate datetime,          -- 'shared date time',

  shareflag varchar2(12) default 'prv' not null,
  sync varchar2(4),

  PRIMARY KEY (pid)
);
