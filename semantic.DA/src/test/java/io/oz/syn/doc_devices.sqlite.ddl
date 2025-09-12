-- since jserv-album 0.6.5
CREATE TABLE doc_devices (
  synode0 varchar(12)  NOT NULL, -- initial node a device is registered
  device  varchar(25)  NOT NULL, -- prefix synode0 + ak, generated when registering, but is used together with synode-0 for file identity.
  devname varchar(256) NOT NULL, -- set by user, warn on duplicate, use old device id if user confirmed, otherwise generate a new one.
  mac     varchar(512),          -- an anciliary identity for recognize a device if there are supporting ways to automatically find out a device mac
  org     varchar(12)  NOT NULL, -- fk-del, usually won't happen
  owner   varchar(12),           -- or current user, not permenatly bound
  cdate   datetime,
  io_oz_synuid varchar2(25),
  PRIMARY KEY (synode0, device)
);