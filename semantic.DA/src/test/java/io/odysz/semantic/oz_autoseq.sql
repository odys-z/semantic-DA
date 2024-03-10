delete from oz_autoseq where sid in (
 'h_photos.pid', 'a_attaches.attId', 'a_functions.funcId', 'a_logs.logId', 'a_orgs.orgId', 'a_roles.roleId', 'a_users.userId',
 'b_alarm_logic.logicId', 'b_alarms.alarmId', 'b_logic_device.deviceLogId', 'crs_a.aid', 'crs_b.bid', 'doc_devices.device');

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
('doc_devices.device', 64 * 64 * 4, 'device');
