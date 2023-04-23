delete from h_photos;

insert into h_photos
(pid,   family, folder, pname ,            uri, device,   clientpath,                     shareby, sharedate, filesize, shareflag, oper, opertime) values
('pA0', 'URA',  'ura',  'Sun Yet-sen.jpg', '',  'syn.00', 'src/test/res/Sun Yet-sen.jpg', 'ody.z', datetime(),'123',    'pub',     'ody', datetime())
;