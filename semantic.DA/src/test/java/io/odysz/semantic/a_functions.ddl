-- drop table if exists a_functions;

create table a_functions (
	funcid   text(20) not null,
	funcname text(200),
	flags    text(2),
	fullpath text(50),
	parentid text(20),
	sibling  integer default 1,
	constraint pk_functions primary key (funcid)
);
