-- drop table if exists syn_change_buf;
create table if not exists syn_exchange_buf (
    peer        varchar2(12) not null, -- session peer
    changeId    varchar2(12) not null, -- chage.cid, domain unique change ID, not auto key (copied)
    pagex       int          not null default 0,
    -- constraint  syn_change_pk PRIMARY KEY (peer, entfk, domain, tabl, crud, synoder, nyquence)
    constraint  syn_change_pk PRIMARY KEY (peer, changeId, pagex)
);
