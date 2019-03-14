CREATE VIEW cbdparkingcosts.walk_cost_2018_0800_600 
AS SELECT * FROM cbdparkingcosts.walk_cost_2018
WHERE cbdparkingcosts.walk_cost_2018.traveltime <= 600;

SELECT * FROM cbdparkingcosts.walk_cost_2018_0800_600;