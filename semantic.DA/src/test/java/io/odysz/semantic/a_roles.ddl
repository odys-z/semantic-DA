-- drop table if exists a_roles;

create table a_roles(
  roleid   text(20) not null,
  rolename text(50),
  constraint a_roles_pk primary key (roleid)
);
