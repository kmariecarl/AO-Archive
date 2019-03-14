--THIS SCRIPT UPDATES THE PARKING COST TABLE WITH THE COMPLETE CAPACITY INFORMATION AS UPDATED ON MAY 23RD.
--DROP TABLE cbdparkingcosts.parkingramp2016;
--drop table cbdparkingcosts.parkingramp2018;
--CREATE TABLE cbdparkingcosts.parkingramp2018 ( ramp_name character varying, id_new integer, daily_rate decimal(4,2), capacity decimal(6,2), month_rate integer);

COPY cbdparkingcosts.parkingramp2018 from '/Users/kristincarlson/Dropbox/Bus-Highway/data/ParkingCosts/ParkingsKristin/ParkingRamp2018_IDNew_4326.csv' 
DELIMITER ',' CSV HEADER;