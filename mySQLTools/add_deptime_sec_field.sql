--alter table ttmatrices.o2pnr
--alter column deptime type char(4);
--select * from ttmatrices.o2pnr limit 10;

--alter table ttmatrices.pnr2d15
--alter column deptime type char(4);
--select * from ttmatrices.pnr2d15 limit 10;

--alter table ttmatrices.o2pnr
--add column deptime_sec integer;
--update ttmatrices.o2pnr set deptime_sec = cast(substring(deptime,1,1) as integer) * 3600 + cast(substring(deptime,2,3) as integer) * 60;

--alter table ttmatrices.pnr2d15
--add column deptime_sec integer;
--update ttmatrices.pnr2d15 set deptime_sec = cast(substring(deptime,1,1) as integer) * 3600 + cast(substring(deptime,2,3) as integer) * 60;
--select * from ttmatrices.o2pnr limit 10;

--DROP INDEX ttmatrices.origin_deptime;
--create index origin_o2pnr on ttmatrices.o2pnr (origin);
--create index deptime_sec_o2pnr on ttmatrices.o2pnr (deptime_sec);

--drop index ttmatrices.origin_deptime_destination;
--create index origin_pnr2d15 on ttmatrices.pnr2d15 (origin);
--create index deptime_sec_pnr2d15 on ttmatrices.pnr2d15 (deptime_sec);
--create index destination_pnr2d15 on ttmatrices.pnr2d15 (destination);