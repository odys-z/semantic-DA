-- drop table if exists syn_change;
create table if not exists syn_change (
    entfk       varchar2(12) not null, -- entity fk, redundant for convenient, not for synchronizing
    domain      varchar2(12) not null,
    tabl        varchar2(64) not null, -- e.g. 'h_photos'
    crud        char(1)      NOT NULL,
    synoder     varchar2(64) not null, -- changer
    uids        text         not null, -- for h_photos.device:fullpath, or composed PK for resouce's id, not null?
    nyquence    long         not null  -- radix64?
);
