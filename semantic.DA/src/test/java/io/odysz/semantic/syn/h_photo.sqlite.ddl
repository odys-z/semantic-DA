drop table if exists h_photos;

CREATE TABLE "h_photos" (
  pid        varchar(12) NOT NULL,
  family     varchar2(12) NOT NULL,
  folder     varchar2(256) NOT NULL,
  pname      varchar2(256),
  uri        varchar2(512) NOT NULL,    -- storage/userId/folder/recId-clientname
  pdate      datetime,                  -- picture taken time
  
  device     varchar(12),               -- 'original device ID',
  clientpath text DEFAULT '/' NOT NULL, -- shall we support 'moveTo' laterly?

  shareby    varchar(12),               -- 'shared by / creator',
  sharedate  datetime not null,         -- 'shared date time',
  exif       text default null,
  mime       varchar2(64), 
  filesize   integer, 
  shareflag  varchar2(12) default 'prv' not null, 
  crud       char(1),
  oper       varchar(12) not null,
  opertime   datetime not null,

  PRIMARY KEY (pid)
);
