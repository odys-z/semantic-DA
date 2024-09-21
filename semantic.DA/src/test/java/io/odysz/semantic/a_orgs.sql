INSERT INTO a_orgs
(orgId,    orgName,      orgType,sort,fullpath,              parent) VALUES
('000000','province A',    '00', 1,  '000000',               NULL),
('00000N','food security', '01', 1,  '000000.00000N',        '000000'),
('00000O','special equi',  '01', 2,  '000000.00000O',        '000000'),
('00000c','33',            '01', 2,  '000000.00000O.00000c', '00000O'),
('00000W','Dali County',   '01', 3,  '000000.00000W',        '000000'),
('00000Y','Yunlong County','01', 1,  '000000.00000W.00000Y', '00000W'),
('00000b','test',          '01', 2,  '000000.00000W.00000b', '00000W');