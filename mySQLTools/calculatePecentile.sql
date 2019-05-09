create table if not exists test.pnr2d153 (origin varchar, destination varchar, deptime varchar, traveltime bigint, deptime_sec integer, bin integer);
insert into test.pnr2d153 (select origin, destination, deptime, traveltime, deptime_sec, bin from
(select origin, destination, deptime, traveltime, deptime_sec, bin,
row_number() over(partition by origin, destination, bin order by traveltime asc) as rn
from test.pnr2d) x
where x.rn=2);