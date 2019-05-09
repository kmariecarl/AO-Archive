--alter table ttmatrices.path_cost
--alter column deptime type char(4);
--select * from ttmatrices.path_cost limit 10;


--alter table ttmatrices.path_cost
--add column deptime_sec integer;
--update ttmatrices.path_cost set deptime_sec = cast(substring(deptime,1,2) as integer) * 3600 + cast(substring(deptime,3,4) as integer) * 60;

--create index origin_path_cost on ttmatrices.path_cost (origin);
--create index deptime_sec_path_cost on ttmatrices.path_cost (deptime_sec);
--create index origin_deptime_sec_path_cost on ttmatrices.path_cost (origin, deptime_sec);

--select * from ttmatrices.path_cost limit 10;
