
-- drop table if exists crs_a;

CREATE TABLE crs_a (
	aid     TEXT NOT NULL,
	remarka TEXT,
	afk     TEXT,
    testInt INTEGER,
    fundDate NUMERIC,
	CONSTRAINT crs_a_PK PRIMARY KEY (aid)
);
