drop table if exists h_photos;

CREATE TABLE "h_photos" (
  pid varchar(12) NOT NULL,
  family varchar2(12) NOT NULL,
  folder varchar(256) NOT NULL,
  pname varchar(256),
  uri varchar(512) NOT NULL,   -- storage/userId/folder/recId-clientname
  pdate datetime,              -- picture taken time
  
  device varchar(12),          -- 'original device ID',
  clientpath TEXT DEFAULT '/' NOT NULL, -- shall we support 'moveTo' laterly?
  oper varchar(12) not null,
  opertime datetime not null,  -- this is the timestamp
  syncstamp DATETIME DEFAULT CURRENT_TIMESTAMP not NULL,

  shareby varchar(12),         -- 'shared by / creator',
  sharedate datetime not null, -- 'shared date time',
  tags varchar(512) DEFAULT NULL ,
  geox double DEFAULT 0,
  geoy double DEFAULT 0,
  exif text default null,
  mime TEXT(64), 
  filesize INTEGER, 
  css text,                    -- e.g. {"type":"io.oz.album.tier.PhotoCSS", "size":[3147,1461,1049,487]}
  shareflag varchar2(12) default 'prv' not null, 
  sync varchar2(4),

  PRIMARY KEY (pid)
);
