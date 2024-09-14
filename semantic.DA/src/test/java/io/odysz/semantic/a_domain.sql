INSERT INTO a_domain
(domainId,    parentId, domainName,       domainValue, sort, "others", fullpath,      stamp, upstamp) VALUES
('02-fault',    NULL,   'testChkOnDel()', NULL,        '99', NULL,     '99 02-fault', NULL, '0'),
('0x-thread-0', NULL,   'test: x0',       '0',         '10', 'creator','99 x0',       NULL, '0'),
('0x-thread-1', NULL,   'test: x1',       '1',         '30', 'creator','99 x1',       NULL, '0'),
('0x-thread-2', NULL,   'test: x2',       '2',         '40', 'creator','99 x2',       NULL, '0'),
('0x-thread-3', NULL,   'test: x0',       '3',         '20', 'creator','99 x3',       NULL, '0'),
('0y-thread-0', NULL,   'test: y0',       '0',         '50', 'creator','99 y0',       NULL, '0'),
('0y-thread-1', NULL,   'test: y1',       '1',         '60', 'creator','99 y1',       NULL, '0'),
('0y-thread-2', NULL,   'test: y2',       '2',         '70', 'creator','99 y2',       NULL, '0')
;
