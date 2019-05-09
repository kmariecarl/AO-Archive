--select * from test.o2pnr limit 10;
--alter table test.o2pnr
--add column depsum integer;
--update test.o2pnr set depsum = cast(substring(deptime, 1, 2) as integer) * 3600 + cast(substring(deptime, 3, 4) as integer) * 60 + traveltime;
--select * from test.o2pnr limit 10;
--alter table test.o2pnr
--add column depsum_bin integer;
--update test.o2pnr set depsum_bin = 0615 where depsum > 21600 and depsum <= 22500;
select * from test.o2pnr where depsum_bin = 0615 limit 10;