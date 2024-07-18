--
insert into oz_autoseq (sid, seq, remarks) values
('h_photos.pid', 0, 'photo'),
('a_attaches.attId', 0, 'attachements'),
('a_functions.funcId', 0, 'test'),
('a_logs.logId', 0, 'test'),
('a_orgs.orgId', 0, 'test'),
('a_roles.roleId', 0, 'test'),
('a_users.userId', 0, 'test'),
('b_alarm_logic.logicId', 64 * 4, 'cascade-parent'),
('b_alarms.alarmId', 0, 'cascade-ancestor'),
('b_logic_device.deviceLogId', 64 * 64, 'cascade-child'),
('crs_a.aid', 0, 'test'),
('crs_b.bid', 128 * 64, 'test'),
('doc_devices.device', 64 * 64 * 4, 'device'),
('syn_change.cid', 0, 'change-log id');
