-- drop table if exists syn_change;
create table if not exists syn_change (
    cid         varchar2(12) not null, -- change ID, auto key
    -- entfk       varchar2(12) not null, -- entity fk, redundant for convenient, not for synchronizing
    domain      varchar2(12) not null,
    tabl        varchar2(64) not null, -- e.g. 'h_photos'
    crud        char(1)      not null,
    synoder     varchar2(64) not null, -- changer
    uids        text         not null, -- for h_photos.device:fullpath, or composed PK for resouce's id, not null?
    nyquence    long         not null,
    updcols     varchar2(256),
    seq         long         not null default 0, -- order of changes, cleared after nyquence is stepped
    -- A typical sqlite database can hold no more than approximately 2e+13, which far less than 2^63
    -- See 13. Maximum Number Of Rows In A Table, https://sqlite.org/limits.html

    constraint  syn_change_pk PRIMARY KEY (cid)
);
