# GTFS speed updater script steps
1. Use stopTimesEditor8 to get new gtfs data
2. Run accessibility calc on original and updated gtfs data. Break calcs into separate hours, 7:00-7:59 and 8:00-9:00.
3. Merge the two accessibility output .csv files using the process described below.
4. Use AverageTransitLocal to get an average accessibility value over the 2 hour window, one value per block and per threshold. Repeat for baseline files. (15 min)
5. Use processAccessResults4 to find the abs. change, percent change, and raw values and output a new .csv file. (1 min)
6. Add the processed_access_results.csv file to QGIS and join to a polygon vector layer that uses GEOID10 as the join key.
7. Join the raw worker values for each block to the same layer as in step 6 (MN_Rac_S000_2014). Now save the layer with a new name.
8. Export layer as a .csv and use with the workerWeightAccess.py program
Use Mac to run worker weighted accessibility program.

# Steps to combine existing GTFS feeds for new scenario
How to Splice GTFS Records together for Transit Scenario Evaluations
1. Designate your baseline GTFS records and calculate the baseline transit accessibility for your analysis zone 
2. Obtain the GTFS records for the modified or new transit route that is going to be tested
3. For each GTFS file, do the following
  * Agency.txt: use $ diff base_gtfs_file1.txt alt_gtfs_file1.txt  copy base agency file and append any new information from the alternative agency file
  * Calendar_dates.txt: check for differences, copy base file and append new info from alternative
  * Calendar.txt: copy basefile, append service dates from alternative file, change start and end dates of alternative lines to match "weekday", "Saturday", "Sunday" dates.
  * PAUSE: Do routes need to be 1) added 2) subtracted 3) added/subtracted?
  * Routes.txt: copy base file, append new route lines to bottom of file, remove routes should you need to
  * Trips.txt: Either append new trips to the copy of base trips.txt file or run remv_trips.py to specify which lines to remove based on the routes you give it
  * Stop_times.txt: Either append new stop_times to the copy of stop_times.txt or run remv_stoptimes.py to specify which lines to remove based on the route->trips you gave it
  * Shapes.txt: Either append new shapes to the copy of the base file or run remv_shapes.py to remove all rows/shapes that correspond with the routes that were removed
  * Stops.txt: first run bashCommand = sort -u stops1.txt stops2.txt > stops_combined.txt, then run the combine_stops.py tool for combining all possible stops between the two files into one combined file for the new scenario.
