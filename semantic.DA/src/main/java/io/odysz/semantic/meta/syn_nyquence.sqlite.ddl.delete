-- drop table if exists syn_nyqeunce;
create table if not exists syn_nyqeunce (
	org      varchar2(12)  not null,
	synode   varchar2(64)  not null, -- source node (global pk)
	tabl     varchar2(64)  not null, -- e.g. 'h_photos'
	nyquence long          not null,
	inc      int default 0 not null, -- 0 or 1 for UHF
	PRIMARY KEY (org, synode, tabl)
);
