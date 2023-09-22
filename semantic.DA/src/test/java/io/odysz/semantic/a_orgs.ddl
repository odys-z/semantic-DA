-- drop table if exists a_orgs;

CREATE TABLE a_orgs (
	orgId text(20) NOT NULL,
	orgName text(50),
	orgType text(40) NOT NULL,  -- a reference to a_domain.domainId (parent = '02')
	sort INTEGER,
	fullpath text(256), parent text(20),
	PRIMARY KEY (orgId)
);
