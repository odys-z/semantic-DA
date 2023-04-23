CREATE TABLE if not exists oz_autoseq (
	sid text(50),
	seq INTEGER,
	remarks text(200),
	CONSTRAINT oz_autoseq_pk PRIMARY KEY (sid)
);

delete from oz_autoseq;

insert into oz_autoseq (sid, seq, remarks) values
('h_photos.pid', 0, 'photo'),
('a_logs.logId', 0, 'test');


