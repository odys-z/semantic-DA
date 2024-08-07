-- drop table if exists syn_subscribe;
create table if not exists syn_subscribe (
    changeId  varchar2(12) not null, -- fk
    -- domain    varchar2(12) not null,
    -- tabl      varchar2(64) not null, -- e.g. 'h_photos'
    -- uids      text         not null, -- global id. For h_photos.device:fullpath, or composed PK for resouce's id, not null?
    synodee   varchar2(12) not null  -- subscriber, fk-on-del, synode id device to finish cleaning task
    -- PRIMARY KEY (changeId, synodee)
);
