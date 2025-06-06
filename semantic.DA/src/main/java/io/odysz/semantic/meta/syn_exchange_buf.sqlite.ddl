-- drop table if exists syn_exchange_buf;
create table if not exists syn_exchange_buf (
    peer        varchar2(12) not null, -- session peer
    changeId    varchar2(12) not null, -- change.cid, domain (?) unique change ID, not auto key (copied) ?
    pagex       int          not null default 0,
    -- why pk includes pagex?
    constraint  syn_change_pk PRIMARY KEY (peer, changeId, pagex)
);
