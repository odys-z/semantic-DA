-- ShFullpath2 (smtc fullpath2), shaped like semantic-doc's s_docnodes: nid given by the caller
create table a_docnodes (
	nid      varchar(40) not null,
	parentId varchar(40),
	sort     integer default 0,
	fullpath varchar(512),
	ptext    varchar(200),
	constraint pk_docnodes primary key (nid)
);
