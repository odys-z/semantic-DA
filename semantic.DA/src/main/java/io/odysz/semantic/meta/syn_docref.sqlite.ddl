-- drop table if exists syn_docref;
create table if not exists syn_docref (
    syntabl     varchar(32)  not null, -- sychronizing entity table name
    fromPeer    varchar2(12) not null, -- session peer
    uids        varchar2(12) not null, -- change.cid, domain (?) unique change ID, not auto key (copied) ?
    tried       int          not null default 0,
    excludeTag  varchar2(24),          -- current ssid or random number for exclude retry in current thread
    constraint  syn_syntabl_pk PRIMARY KEY (syntabl, fromPeer, uids)
);
