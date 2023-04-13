drop table if exists syn_change;
create table syn_change (
	tabl        varchar2(64) not null, -- e.g. 'h_photos'
	recId       varchar2(12) not null, -- entity record Id
	clientpath  text         not null, -- for h_photos.fullpath, or composed PK for resouce's id, not null?
	clientpath2 text,                  -- support max 3 fields of composed PK, TODO any better patterns?
	oper        varchar2(12) not null, -- stamper
	nyquence    long,                  -- radix64?
	cud         char(1)       not null -- I/U/D
);
