
-- drop table if exists a_alarm_logic;

CREATE TABLE b_alarm_logic (
-- one a_alarm - multiple b_alarm_logic
-- child table of a_alarm
-- business semantics: some devices reported an alarm based on some logic
	logicId text(20) NOT NULL, -- auto key
	alarmId text(20) NOT NULL, -- fk to a_alarm
	remarks text(200),
	updating DATETIME,
	PRIMARY KEY (logicId)
);
