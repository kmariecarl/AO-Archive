# How to Calculate Weighted Parking Costs

The series of SQL files in this folder assist in creating parking cost tables in a PostgreSQL DB. The process relies on travel time matrix input, lot capacity, and prices.
Users should refer to file contents in:
```
/Users/kristincarlson/Dropbox/Bus-Highway/Task4/ParkingCostCalc/WalkingTT_2018_Weighted/SQL
```
## Script Names and Steps

* 1_Create_parkrampcost_table.sql<br/>
* 2_create_walk+cost_table.sql<br/>
* 3_Create_10min_view.sql<br/>
* 4_create_wtavgcost_table.sql<br/>
  * Weight each ramp price by capacity then average to the origin<br/>
  * To aggregate to TAZ level, optionally you can figure out a way to weight the capacity weighted price values by the number of jobs in the Census block, then average to the TAZ.<br/>
* Export results to file<br/>
* Join Census block level weighted average parking price values to the GEOID10 field of the Census block polygon layer tl_2010_27_55.<br/>

To aggregate to TAZ level (without doing step d_ii) use the "Join attributes by location" vector function in QGIS to get the average weighted parking cost per TAZ.<br/>

