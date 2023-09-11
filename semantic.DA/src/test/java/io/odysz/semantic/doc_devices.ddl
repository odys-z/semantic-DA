-- drop table if exists doc_devices;
CREATE TABLE doc_devices (
  synode0 varchar(6),            -- nullable for test
  device  varchar(12)  NOT NULL, -- prefix synode0 + ak, as 6.5, generated when registering. 64 ^ 5 = 1GiB.
  devname varchar(256) NOT NULL, -- set by user, warn on duplicate, use old device id if user confirmed, otherwise generate a new one.
  mac     varchar(512),          -- an anciliary identity for recognize a device if there are supporting ways to automatically find out a device mac
  org     varchar(12)  NOT NULL, -- fk-del, usually won't happen
  owner   varchar(12),           -- or current user, not permenatly bound
  cdate   datetime,
  PRIMARY KEY (synode0, device)
); -- registered device names. Name is set by user, prompt if he's device names are duplicated
