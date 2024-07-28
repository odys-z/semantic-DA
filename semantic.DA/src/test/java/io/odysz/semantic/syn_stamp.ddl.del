-- drop table if exists syn_stamp;

CREATE TABLE syn_stamp
-- table-wise last updating stamps by remote nodes
-- 'D' is actually triggered a cleaning task; 'D' -> 'C';
-- for 'U', uri is not updated
(
	tabl      varchar2(64) NOT NULL, -- e.g. 'h_photos'
	synode    varchar2(12) NOT NULL, -- fk-on-del
	crud	  char(1)      NOT NULL, -- 'R' is ignored; 'D' is actually triggered a cleaning task; 'D' -> 'C'; for 'U', only h_photos.sync & h_photos.clientpath is handled

	recount   INTEGER,
	-- not correct if multiple updated and not used as Semantics.DA can't access context for commitment results.
	-- as photos are uploaded file by file, this is probably only useful for debugging.

	syncstamp DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
	xmlstamp  DATETIME NOT NULL      -- stamp handled by semantics for test
);
