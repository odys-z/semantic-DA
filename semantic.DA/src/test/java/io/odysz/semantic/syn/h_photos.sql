delete from h_photos;

insert into h_photos
(pid,   family, folder, pname ,            uri, device,   clientpath,                     shareby, sharedate,  filesize, shareflag, oper, opertime) values
('pA0', 'URA',  'ura',  'Sun Yet-sen.jpg', '',  'syn.00', 'src/test/res/Sun Yet-sen.jpg', 'ody.z', datetime(), '123',    'pub',     'ody', datetime())
;


select u.*, orgName, roleName from a_users u join a_roles r on u.roleId = r.roleId join a_orgs o on u.orgId = o.orgId where u.userId = 'ody';
select * from a_users;
select * from a_orgs;