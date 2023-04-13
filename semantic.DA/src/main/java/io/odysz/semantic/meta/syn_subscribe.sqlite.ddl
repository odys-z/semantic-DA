drop table if exists syn_subscribe;
create table syn_subscribe (
	tabl        varchar2(64) not null, -- e.g. 'h_photos'
	entFk       varchar2(12) not null, -- entity record Id
	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id, not null?
	clientpath2 text,                  -- support max 3 fields of composed PK, TODO any better patterns?
	synodee     varchar2(12) not null, -- subscriber, fk-on-del, synode id device to finish cleaning task

	-- There is no R/D/E in subscriptions, that's an attribute of doc-sharing relationship
	del         char(1)      not null  -- deletion acknowledgement: R/E
);
