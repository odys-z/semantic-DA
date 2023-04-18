drop table if exists syn_nyqeunce;
create table syn_nyqeunce (
	tabl     varchar2(64) not null, -- e.g. 'h_photos'
	synode   varchar2(12) not null, -- source node (global pk)
	oper     varchar2(12) not null, -- stamper
	nyquence long         not null
);
