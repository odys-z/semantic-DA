-- drop table if exists b_logic_device;

CREATE TABLE b_logic_device (
-- one a_alarm_logic - multiple a_logic_device
-- child table of a_alarm_logic
-- business logic: devices that formed up a logic report
	deviceLogId text(20) NOT NULL, -- auto key
	logicId text(20) NOT NULL, -- fk to a_alarm_logic
	alarmId text(20), -- fk to ancenstor
	updating DATETIME, remarks TEXT(20),
	PRIMARY KEY (deviceLogId)
);
