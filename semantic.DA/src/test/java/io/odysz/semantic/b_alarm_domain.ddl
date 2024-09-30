-- Sqlite thread buffering test

-- drop table if exists b_alarm_domain;

CREATE TABLE b_alarm_domain(
  aid varchar2(12) NOT NULL, -- fk: b_alrms.alarmId
  did varchar2(12) NOT NULL  -- fk: a_functions.funcId
);
