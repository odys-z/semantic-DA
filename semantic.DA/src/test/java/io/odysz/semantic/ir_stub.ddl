-- drop table if exists ir_stub;

create table ir_stub (
  stubid  text(50),
  ttxt    text(200),
  upstamp text(20) not null default '0',
  constraint "ir_stub_pk" primary key ("stubid")
);
