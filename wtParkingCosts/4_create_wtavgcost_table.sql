--DROP TABLE cbdparkingcosts.costavg_2018;

--BELOW GIVES THE AVERAGE PARKING COST PER ORIGIN
--SELECT origin,  
--AVG(daily_rate) as avg_or_park_cost  
--FROM cbdparkingcosts.walk_cost_2018_0800_600  
--GROUP BY origin;  

--BELOW GIVES THE AVERAGE PARKING COST PER ORIGIN WEIGHTED BY THE RAMP CAPACITY--CONFIRMED AS ACCURATE BY DOING A HAND CALCULATION
--CREATE TABLE cbdparkingcosts.wtavgcost_2018
--AS SELECT origin, SUM(cast(capacity as int) * daily_rate) / SUM(cast(capacity as int)) AS avgCostByOrigin
--FROM cbdparkingcosts.walk_cost_2018_0800_600
--GROUP BY origin;

SELECT * FROM cbdparkingcosts.wtavgcost_2018 order by origin;
