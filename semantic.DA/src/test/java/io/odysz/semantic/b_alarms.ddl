-- drop table if exists b_alarms;
-- changed v 1.4.45
CREATE TABLE b_alarms (
	alarmId text(20) NOT NULL,
	remarks text(200),
	typeId  text(40) NOT NULL, -- a reference to a_domain.domainId (parent = '02')
	PRIMARY KEY (alarmId)
);
