--THIS SCRIPT UPDATE THE WALK TT + PARKING COST JOINED TABLE WITH THE NEW CAPACITY INFO.
--DROP TABLE cbdparkingcosts.walk_cost_2018 CASCADE;

CREATE TABLE cbdparkingcosts.walk_cost_2018 as
SELECT walKtt2018_8am.origin, walktt2018_8am.destination, walktt2018_8AM.deptime, walktt2018_8am.traveltime,
parkingramp2018.Ramp_Name, parkingramp2018.daily_rate, parkingramp2018.capacity, parkingramp2018.month_rate
FROM cbdparkingcosts.parkingramp2018
LEFT JOIN cbdparkingcosts.walktt2018_8am ON cbdparkingcosts.parkingramp2018.id_new=walktt2018_8am.destination;

select * from cbdparkingcosts.walk_cost_2018 limit 10;