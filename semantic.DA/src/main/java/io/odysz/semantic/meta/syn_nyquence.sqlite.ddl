drop table if exists syn_nyqeunce;
create table syn_nyqeunce (
	org      varchar2(12) not null,
	synode   varchar2(64) not null, -- source node (global pk)
	tabl     varchar2(64) not null, -- e.g. 'h_photos'
	-- oper     varchar2(12) not null, -- stamper
	nyquence long         not null,
	PRIMARY KEY (org, synode, tabl)
);
