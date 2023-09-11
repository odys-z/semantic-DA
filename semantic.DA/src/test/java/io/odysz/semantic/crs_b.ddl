
-- drop table if exists crs_b;

CREATE TABLE crs_b (
	bid     TEXT NOT NULL,
	remarkb TEXT,
	bfk     TEXT,
	CONSTRAINT crs_b_PK PRIMARY KEY (bid)
);
